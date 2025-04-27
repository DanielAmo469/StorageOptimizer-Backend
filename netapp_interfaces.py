from datetime import datetime
import json
import os
import subprocess
import tempfile
from typing import Dict, List, Optional, Tuple
import smbclient
import shutil
from sqlalchemy import desc, func
from sqlalchemy.orm import Session
from netapp_ontap.resources import VolumeMetrics, Volume, PerformanceMetric, PerformanceCifsMetric
from netapp_ontap import HostConnection

from models import FileMovement, ActionType
from netapp_btc import filter_files, get_archive_path, get_svm_data_volumes, get_svm_uuid, get_vol_uuid, get_volume_name_by_share, normalize_path, scan_volume
from database import get_db

def log_file_movement(
    db: Session,
    full_path: str,
    destination_path: str,
    creation_time: str,
    last_access_time: str,
    last_modified_time: str,
    file_size: int,
    action_type: ActionType
):
    file_movement = FileMovement(
        full_path=full_path,
        destination_path=destination_path,
        creation_time=datetime.strptime(creation_time, '%Y-%m-%d %H:%M:%S'),
        last_access_time=datetime.strptime(last_access_time, '%Y-%m-%d %H:%M:%S'),
        last_modified_time=datetime.strptime(last_modified_time, '%Y-%m-%d %H:%M:%S'),
        file_size=file_size,
        action_type=action_type
    )
    db.add(file_movement)
    db.commit()


def move_file(file_info: dict) -> tuple[str, FileMovement] | tuple[None, None]:
    try:
        src_path = normalize_path(file_info['full_path'])
        dest_folder = normalize_path(get_archive_path(src_path))

        if src_path.endswith("_shortcut.bat") or src_path.endswith(".bat"):
            print(f"Skipped: Shortcut or batch file detected → {src_path}")
            return None, None

        if not dest_folder:
            print(f"Skipping file {src_path} (Invalid archive destination)")
            return None, None

        print(f"Normalized Source: {src_path}")
        print(f"Normalized Destination Folder: {dest_folder}")

        # Capture metadata BEFORE moving
        creation_time = datetime.strptime(file_info["creation_time"], '%Y-%m-%d %H:%M:%S')
        access_time = datetime.strptime(file_info["last_access_time"], '%Y-%m-%d %H:%M:%S')
        modified_time = datetime.strptime(file_info["last_modified_time"], '%Y-%m-%d %H:%M:%S')
        file_size = file_info["file_size"]

        filename = os.path.basename(src_path)
        dest_path = normalize_path(f"{dest_folder}\\{filename}")

        # Copy file from source to destination via temp file
        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            temp_file_path = temp_file.name
            try:
                with open(temp_file_path, "wb") as f:
                    with smbclient.open_file(src_path, mode="rb") as remote_file:
                        shutil.copyfileobj(remote_file, f)
            except Exception as e:
                print(f"Failed to download file from {src_path}: {e}")
                return None, None

        try:
            with open(temp_file_path, "rb") as f:
                with smbclient.open_file(dest_path, mode="wb") as remote_file:
                    shutil.copyfileobj(f, remote_file)
        except Exception as e:
            print(f"Failed to upload file to archive: {dest_path} → {e}")
            return None, None

        try:
            smbclient.remove(src_path)
        except Exception as e:
            print(f"Failed to delete original file: {src_path} → {e}")
            return None, None

        os.utime(temp_file_path, (access_time.timestamp(), modified_time.timestamp()))

        # Try creating shortcut (using original src path)
        try:
            created = create_shortcut(src_path, dest_path)
            if not created:
                print(f"Shortcut creation failed for: {src_path}")
        except Exception as e:
            print(f"Shortcut exception: {src_path} → {e}")

        os.remove(temp_file_path)

        # Log movement
        file_movement = FileMovement(
            full_path=src_path,
            destination_path=dest_path,
            creation_time=creation_time,
            last_access_time=access_time,
            last_modified_time=modified_time,
            file_size=file_size,
            action_type=ActionType.moved_to_archive
        )

        print(f"Successfully moved file: {src_path}")
        return dest_path, file_movement

    except Exception as e:
        print(f"FATAL error in move_file for {file_info.get('full_path')}: {e}")
        return None, None



def create_shortcut(original_path, archive_path):
    shortcut_path = original_path + "_shortcut.bat"  # Create a .bat file
    
    try:
        with open(shortcut_path, 'w') as shortcut:
            shortcut.write(f'@echo off\nstart "" "{archive_path}"\n')  # Opens the file when double-clicked
        
        print(f"Shortcut created: {shortcut_path} → {archive_path}")
        return True
    except Exception as e:
        print(f"Failed to create shortcut for {original_path}: {e}")
        return False


def process_files_for_archival(files):
    for file_list in files.values():
        for file_info in file_list:
            archive_path = move_file(file_info)
            if archive_path:
                create_shortcut(file_info['full_path'], archive_path)

def set_creation_time_windows(file_path: str, creation_time: datetime):
    creation_str = creation_time.strftime("%m/%d/%Y %H:%M:%S")
    powershell_command = f"""
    $(Get-Item '{file_path}').CreationTime=('{creation_str}')
    """
    subprocess.call(["powershell", "-Command", powershell_command], shell=True)

def restore_file(archive_folder, filename):
    archive_path = normalize_path(os.path.join(archive_folder, filename))
    print(f"Preparing to restore {filename} from {archive_path}")

    db_gen = get_db()
    db = next(db_gen)
    try:
        archive_entry = db.query(FileMovement)\
            .filter(FileMovement.destination_path == archive_path)\
            .filter(FileMovement.action_type == ActionType.moved_to_archive)\
            .order_by(desc(FileMovement.timestamp))\
            .first()

        if not archive_entry:
            print(f"No archive record found for: {filename}")
            return False

        original_path = normalize_path(archive_entry.full_path)
        print(f"Restoring {filename}")
        print(f"  Source (Archive): {archive_path}")
        print(f"  Destination (Original): {original_path}")

        # Download to local temp
        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            temp_file_path = temp_file.name
            with open(temp_file_path, "wb") as f:
                with smbclient.open_file(archive_path, mode="rb") as remote_file:
                    shutil.copyfileobj(remote_file, f)
        print(f"Downloaded file from archive to local temp: {temp_file_path}")

        # Upload to original location
        with open(temp_file_path, "rb") as f:
            with smbclient.open_file(original_path, mode="wb") as remote_file:
                shutil.copyfileobj(f, remote_file)
        print(f"Restored file to: {original_path}")

        # Delete archive copy if successful
        try:
            smbclient.stat(original_path)
            smbclient.remove(archive_path)
            print(f"Deleted file from archive: {archive_path}")
        except FileNotFoundError:
            print(f"Could not verify restored file. Skipping archive deletion.")
            return False

        # Restore timestamps
        os.utime(original_path, (
            archive_entry.last_access_time.timestamp(),
            archive_entry.last_modified_time.timestamp()
        ))
        print(f"Timestamps restored for {original_path}")
        set_creation_time_windows(original_path, archive_entry.creation_time)


        # Remove shortcut
        shortcut_path = normalize_path(original_path + "_shortcut.bat")
        if os.path.exists(shortcut_path):
            os.remove(shortcut_path)
            print(f"Removed shortcut: {shortcut_path}")

        # Log restore
        log_file_movement(
            db,
            full_path=original_path,
            destination_path=archive_path,
            creation_time=archive_entry.creation_time.strftime('%Y-%m-%d %H:%M:%S'),
            last_access_time=archive_entry.last_access_time.strftime('%Y-%m-%d %H:%M:%S'),
            last_modified_time=archive_entry.last_modified_time.strftime('%Y-%m-%d %H:%M:%S'),
            file_size=archive_entry.file_size,
            action_type=ActionType.restored_from_archive
        )

        return original_path

    except FileNotFoundError:
        print(f"File not found in archive: {archive_path}")
        return False
    except PermissionError:
        print(f"Permission denied: {archive_path}")
        return False
    except Exception as e:
        print(f"Failed to restore {filename}: {e}")
        return False
    finally:
        db_gen.close()


def bulk_restore_files(file_paths: list[str], db_session: Session) -> dict:
    restored_files = []
    skipped_files = []

    for archived_path in file_paths:
        try:
            metadata = db_session.query(FileMovement).filter(
                FileMovement.full_path == archived_path,
                FileMovement.action_type == ActionType.archive
            ).order_by(FileMovement.id.desc()).first()

            if not metadata:
                raise Exception(f"Metadata not found in database for: {archived_path}")

            # Extract metadata
            original_dest_path = metadata.destination_path
            creation_time = metadata.creation_time
            last_access_time = metadata.last_access_time
            last_modified_time = metadata.last_modified_time
            file_size = metadata.file_size

            if not original_dest_path:
                raise Exception(f"Missing destination path in DB for: {archived_path}")

            # Download the archived file into a local temp file
            temp_fd, temp_path = tempfile.mkstemp()
            os.close(temp_fd)

            try:
                with smbclient.open_file(archived_path, mode="rb") as src_file:
                    with open(temp_path, mode="wb") as dst_file:
                        shutil.copyfileobj(src_file, dst_file)
            except Exception:
                os.remove(temp_path)
                raise Exception(f"Failed to download file from archive: {archived_path}")

            # Upload the file to its original destination
            try:
                with smbclient.open_file(original_dest_path, mode="wb") as dest_file:
                    with open(temp_path, mode="rb") as src_temp:
                        shutil.copyfileobj(src_temp, dest_file)
            except Exception:
                os.remove(temp_path)
                raise Exception(f"Failed to restore file to original path: {original_dest_path}")

            os.remove(temp_path)  # Cleanup local temp

            # Restore timestamps remotely (optional)
            try:
                with smbclient.open_file(original_dest_path, mode="rb+") as f:
                    f.set_times(
                        created=creation_time,
                        accessed=last_access_time,
                        modified=last_modified_time
                    )
            except Exception:
                pass  # Ignore if unable to set times

            # Delete archive copy
            try:
                os.remove(archived_path)
            except Exception:
                pass  # If already deleted, no issue

            # Delete shortcut if exists
            shortcut_path = original_dest_path + "_shortcut.bat"
            try:
                os.remove(shortcut_path)
            except Exception:
                pass

            restored_files.append(
                FileMovement(
                    full_path=archived_path,
                    destination_path=original_dest_path,
                    file_size=file_size,
                    creation_time=creation_time,
                    last_access_time=last_access_time,
                    last_modified_time=last_modified_time,
                    action_type=ActionType.restore
                )
            )

        except Exception as e:
            skipped_files.append({
                "path": archived_path,
                "error": str(e)
            })
            print(f"Skipping {archived_path}: {e}")

    if restored_files:
        db_session.bulk_save_objects(restored_files)
        db_session.commit()

    return {
        "restored": [r.destination_path for r in restored_files],
        "skipped": skipped_files
    }

def move_files_and_commit(files: List[Dict]) -> Tuple[List[str], List[Dict]]:
    successful_moves = []
    failed_files = []

    db_gen = get_db()
    db = next(db_gen)

    try:
        for file_info in files:
            result = move_file(file_info)
            if result and result[0] and result[1]:
                archive_path, movement = result
                successful_moves.append(movement)
                print(f"Moved: {movement.full_path} to {movement.destination_path}")
            else:
                failed_files.append(file_info)
                print(f"Failed to move: {file_info['full_path']}")

        if successful_moves:
            db.bulk_save_objects(successful_moves)
            db.commit()
            print(f"Committed {len(successful_moves)} file movements to the database.")
        else:
            print("No successful file moves to commit.")

    except Exception as e:
        db.rollback()
        print(f"Database error occurred: {e}")

    finally:
        db_gen.close()

    return [m.full_path for m in successful_moves], failed_files




#Scan, filter, and archive files from a specific share
def archive_filtered_files(filters: dict, blacklist: list, share_name: str, volume: dict):
    print(f"Starting archive process for share: {share_name}")

    all_files = scan_volume(share_name, volume, blacklist)
    if not all_files:
        return {"status": "no_files", "reason": f"No files found in {share_name}"}

    scanned_dict = {share_name: all_files}
    filtered = filter_files(scanned_dict, filters, blacklist, share_name)

    if not filtered or not filtered.get(share_name):
        return {"status": "no_matches"}

    # Move files and commit in one go
    moved_files, failed_files = move_files_and_commit(filtered.get(share_name, []))

    return {
        "status": "success" if moved_files else "no_matches",
        "archived_count": len(moved_files),
        "moved_files": moved_files,
        "failed_files": [f['full_path'] for f in failed_files]
    }

# def test_filter_files_direct():
#     filters = {
#         "file_type": [".pdf"],
#         "date_filters": {
#             "creation_time": {
#                 "start_date": None,
#                 "end_date": None
#             },
#             "last_modified_time": {
#                 "start_date": None,
#                 "end_date": None
#             },
#             "last_access_time": {
#                 "start_date": None,
#                 "end_date": None
#             }
#         },
#         "min_size": 0,
#         "max_size": 500414
#     }

#     blacklist = ["KZvbbIYi", "Ep7Dw5zn", "aNgc8Dds", "6JKm3t54"]

#     # Step 1: Get volume config
#     data = get_svm_data_volumes()
#     volumes = data.get("volumes", [])
#     if not volumes:
#         print("❌ No volumes found.")
#         return

#     # Step 2: Pick first available volume
#     first_volume = volumes[0]

#     # Step 3: Extract share_name
#     share_name = first_volume.get("share_name")
#     if not share_name:
#         print("❌ No share_name found in volume.")
#         return

#     # 🚀 PATCH: Add fake nas_server if missing
#     if "nas_server" not in first_volume:
#         print("⚠️ Adding fake nas_server info for testing.")
#         first_volume["nas_server"] = {
#             "interfaces": [{"ip": "192.168.16.14"}]
#         }

#     # Step 4: Scan
#     scanned = scan_volume(share_name, first_volume, blacklist)
#     if not scanned:
#         print(f"❌ No files scanned in share {share_name}.")
#         return

#     # Step 5: Prepare dict for filtering
#     scanned_dict = {share_name: scanned}

#     # Step 6: Filter
#     filtered = filter_files(
#         scanned_dict,
#         filters,
#         blacklist,
#         share_name
#     )

#     print(f"\n✅ Filtered files in {share_name}:")
#     if not filtered or not filtered.get(share_name):
#         print("❌ No matching files found after filtering.")
#     else:
#         for file_info in filtered.get(share_name, []):
#             print(f"- {file_info['full_path']}, size: {file_info['file_size']} bytes")

# if __name__ == "__main__":
#     test_filter_files_direct()


#    print(get_svm_data_volumes())

# Example usage:
# filters = {
#     'file_type': '.txt',
#     'date_filters': {  # Allows independent date filters for different types
#         'creation_time': {'start_date': '2023-01-01 00:00:00', 'end_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S')},
#         'last_access_time': {'start_date': None, 'end_date': None},  # If None, this filter is ignored
#         'last_modified_time': {'start_date': None, 'end_date': None},
#     },
#     'min_size': 100,  # Minimum file size in bytes
#     'max_size': 50000  # Maximum file size in bytes
# }
# blacklist = ['6Uh24TE', '3liOYfQA']

# Scan and filter files
#filtered_files = filter_files(scan_volume(get_svm_data_volumes()), filters, blacklist)
#print(filtered_files)

# file1 = {
#     "data2": [
#     {
        # 'full_path': "\\\\192.168.16.14\\data2\\t12vnFc8\\cmBsxD3W\\UK7vuCi6\\YY5dETB0.txt",
        # 'creation_time': '2025-01-12 07:15:49',
        # 'last_access_time': '2024-08-25 15:15:49',
        # 'last_modified_time': '2024-08-25 15:15:49',
        # 'file_size': 32166
#     }
#     ]
# }

# file_path = "\\\\192.168.16.14\\data2\\t12vnFc8\\cmBsxD3W\\UK7vuCi6\\YY5dETB0.txt"

# try:
#     smbclient.stat(file_path)
#     print("File exists and is accessible.")
# except FileNotFoundError:
#     print("File does not exist or cannot be accessed.")

#process_files_for_archival(file1)

#restore_file("\\\\192.168.16.15\\archive2", "YY5dETB0.txt")



# test_files = [
#     {
#         "full_path": r"\\192.168.16.14\data2\fhFM8iuT\BRGt58Wu\BbaJnYvp\d0oIe0pE.json",
#         "creation_time": "2025-01-12 07:14:53",
#         "last_access_time": "2025-01-14 14:11:23",
#         "last_modified_time": "2024-05-04 15:14:53",
#         "file_size": 801831
#     },
#     {
#         "full_path": r"\\192.168.16.14\data2\fhFM8iuT\BRGt58Wu\BbaJnYvp\K32Ghazo.pdf",
#         "creation_time": "2025-01-12 07:14:53",
#         "last_access_time": "2025-01-14 13:41:44",
#         "last_modified_time": "2024-06-30 15:14:54",
#         "file_size": 676112
#     },
#     {
#         "full_path": r"\\192.168.16.14\data2\fhFM8iuT\BRGt58Wu\BbaJnYvp\IwE7a5xt.pdf",
#         "creation_time": "2025-01-12 07:14:53",
#         "last_access_time": "2025-01-14 13:40:59",
#         "last_modified_time": "2024-03-30 15:14:54",
#         "file_size": 171685
#     },
#     {
#         "full_path": r"\\192.168.16.14\data2\fhFM8iuT\BRGt58Wu\BbaJnYvp\this_file_does_not_exist.txt",
#         "creation_time": "2025-01-01 00:00:00",
#         "last_access_time": "2024-01-01 00:00:00",
#         "last_modified_time": "2024-01-01 00:00:00",
#         "file_size": 12345
#     }
# ]

# for f in test_files:
#     print("Before call:", normalize_path(f['full_path']))


# moved, failed = move_files_and_commit(test_files)

# print("\nFinal Results:")
# print("Moved files:")
# for f in moved:
#     print(" -", f)

# print("Failed files:")
# for f in failed:
#     print(" -", f['full_path'])
