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

        # Pre-check: permissions and size
        try:
            with smbclient.open_file(src_path, mode="rb") as f:
                f.read(1)
            file_stat = smbclient.stat(src_path)
            if file_stat.st_size == 0:
                print(f"Skipped: Zero size file {src_path}")
                return None, None
        except Exception as e:
            print(f"Permission or access check failed for {src_path}: {e}")
            return None, None

        # Capture metadata
        creation_time = datetime.strptime(file_info["creation_time"], '%Y-%m-%d %H:%M:%S')
        access_time = datetime.strptime(file_info["last_access_time"], '%Y-%m-%d %H:%M:%S')
        modified_time = datetime.strptime(file_info["last_modified_time"], '%Y-%m-%d %H:%M:%S')
        file_size = file_info["file_size"]

        filename = os.path.basename(src_path)
        dest_path = normalize_path(f"{dest_folder}\\{filename}")

        # Move via temp file
        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            temp_file_path = temp_file.name
            try:
                with open(temp_file_path, "wb") as local_file:
                    with smbclient.open_file(src_path, mode="rb") as remote_file:
                        shutil.copyfileobj(remote_file, local_file)
            except Exception as e:
                print(f"Failed to download {src_path}: {e}")
                return None, None

        try:
            with open(temp_file_path, "rb") as local_file:
                with smbclient.open_file(dest_path, mode="wb") as remote_file:
                    shutil.copyfileobj(local_file, remote_file)
        except Exception as e:
            print(f"Failed to upload {dest_path}: {e}")
            return None, None

        try:
            smbclient.remove(src_path)
        except Exception as e:
            print(f"Failed to delete {src_path}: {e}")
            return None, None

        os.utime(temp_file_path, (access_time.timestamp(), modified_time.timestamp()))

        # Create shortcut (ignore failure)
        try:
            create_shortcut(src_path, dest_path)
        except Exception as e:
            print(f"Shortcut error for {src_path}: {e}")

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

        print(f"Moved: {src_path} → {dest_path}")
        return dest_path, file_movement

    except Exception as e:
        print(f"FATAL error in move_file: {file_info.get('full_path')} → {e}")
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

#Restore multiple files from archive back to original location
def bulk_restore_files(file_paths: list[str], db_session: Session) -> dict:
    restored_files = []
    skipped_files = []

    for archived_path in file_paths:
        try:
            # Find metadata by destination_path (where it was archived)
            metadata = db_session.query(FileMovement).filter(
                FileMovement.destination_path == archived_path,
                FileMovement.action_type == ActionType.moved_to_archive
            ).order_by(FileMovement.id.desc()).first()

            if not metadata:
                raise Exception(f"Metadata not found for archive path: {archived_path}")

            # Metadata fields
            original_full_path = metadata.full_path
            creation_time = metadata.creation_time
            last_access_time = metadata.last_access_time
            last_modified_time = metadata.last_modified_time
            file_size = metadata.file_size

            if not original_full_path:
                raise Exception(f"Original path missing for: {archived_path}")

            # Download archived file to temp
            temp_fd, temp_path = tempfile.mkstemp()
            os.close(temp_fd)

            try:
                with smbclient.open_file(archived_path, mode="rb") as src_file:
                    with open(temp_path, mode="wb") as dst_file:
                        shutil.copyfileobj(src_file, dst_file)
            except Exception:
                os.remove(temp_path)
                raise Exception(f"Failed to download archived file: {archived_path}")

            # Upload to original location
            try:
                with smbclient.open_file(original_full_path, mode="wb") as dest_file:
                    with open(temp_path, mode="rb") as src_temp:
                        shutil.copyfileobj(src_temp, dest_file)
            except Exception:
                os.remove(temp_path)
                raise Exception(f"Failed to upload to original path: {original_full_path}")

            os.remove(temp_path)  # Clean up temp

            # Attempt to restore timestamps (optional)
            try:
                with smbclient.open_file(original_full_path, mode="rb+") as f:
                    f.set_times(
                        created=creation_time,
                        accessed=last_access_time,
                        modified=last_modified_time
                    )
            except Exception:
                pass

            # Delete archive file
            try:
                os.remove(archived_path)
            except Exception:
                pass

            # Delete shortcut if exists
            shortcut_path = original_full_path + "_shortcut.bat"
            try:
                os.remove(shortcut_path)
            except Exception:
                pass

            # Log restoration
            restored_files.append(
                FileMovement(
                    full_path=archived_path,
                    destination_path=original_full_path,
                    file_size=file_size,
                    creation_time=creation_time,
                    last_access_time=last_access_time,
                    last_modified_time=last_modified_time,
                    action_type=ActionType.restored_from_archive
                )
            )

            print(f"Restored: {archived_path} -> {original_full_path}")

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


# Bulk move and archive a list of files
# - Moves files from source to archive destination
# - Creates shortcuts at original locations
# - Collects successful movements and commits them to database
# - Prints moved and failed files with reasons

def bulk_move_files(files: list[dict]) -> tuple[list[str], list[dict]]:
    successful_movements = []
    failed_files = []

    db_gen = get_db()
    db = next(db_gen)

    try:
        for file_info in files:
            try:
                src_path = normalize_path(file_info['full_path'])
                dest_folder = normalize_path(get_archive_path(src_path))

                if src_path.endswith("_shortcut.bat") or src_path.endswith(".bat"):
                    print(f"Skipped shortcut or batch file: {src_path}")
                    continue

                if not dest_folder:
                    print(f"Invalid archive destination for {src_path}")
                    continue

                # Precheck permission and ensure non-zero size
                try:
                    with smbclient.open_file(src_path, mode="rb") as f:
                        f.read(1)
                    file_stat = smbclient.stat(src_path)
                    if file_stat.st_size == 0:
                        print(f"Skipped zero-size file: {src_path}")
                        continue
                except Exception as e:
                    reason = f"Permission check or stat failed: {e}"
                    print(f"{reason} - {src_path}")
                    failed_files.append({"file": file_info, "reason": reason})
                    continue

                filename = os.path.basename(src_path)
                dest_path = normalize_path(f"{dest_folder}\\{filename}")

                creation_time = datetime.strptime(file_info["creation_time"], '%Y-%m-%d %H:%M:%S')
                access_time = datetime.strptime(file_info["last_access_time"], '%Y-%m-%d %H:%M:%S')
                modified_time = datetime.strptime(file_info["last_modified_time"], '%Y-%m-%d %H:%M:%S')
                file_size = file_info["file_size"]

                # Download the source file to a temporary local file
                with tempfile.NamedTemporaryFile(delete=False) as temp_file:
                    temp_file_path = temp_file.name
                    try:
                        with open(temp_file_path, "wb") as local_file:
                            with smbclient.open_file(src_path, mode="rb") as remote_file:
                                shutil.copyfileobj(remote_file, local_file)
                    except Exception as e:
                        reason = f"Failed to download: {e}"
                        print(f"{reason} - {src_path}")
                        failed_files.append({"file": file_info, "reason": reason})
                        continue

                # Upload the temporary file to archive destination
                try:
                    with open(temp_file_path, "rb") as local_file:
                        with smbclient.open_file(dest_path, mode="wb") as remote_file:
                            shutil.copyfileobj(local_file, remote_file)
                except Exception as e:
                    reason = f"Failed to upload: {e}"
                    print(f"{reason} - {dest_path}")
                    failed_files.append({"file": file_info, "reason": reason})
                    continue

                # Delete the original source file
                try:
                    smbclient.remove(src_path)
                except Exception as e:
                    reason = f"Failed to delete source file: {e}"
                    print(f"{reason} - {src_path}")
                    failed_files.append({"file": file_info, "reason": reason})
                    continue

                os.utime(temp_file_path, (access_time.timestamp(), modified_time.timestamp()))

                # Create a shortcut at original path
                try:
                    create_shortcut(src_path, dest_path)
                except Exception as e:
                    print(f"Shortcut creation failed for {src_path}: {e}")

                os.remove(temp_file_path)

                # Log successful movement
                movement = FileMovement(
                    full_path=src_path,
                    destination_path=dest_path,
                    creation_time=creation_time,
                    last_access_time=access_time,
                    last_modified_time=modified_time,
                    file_size=file_size,
                    action_type=ActionType.moved_to_archive
                )
                successful_movements.append(movement)

                print(f"Moved successfully: {src_path} -> {dest_path}")

            except Exception as e:
                reason = f"Fatal error during move: {e}"
                print(f"{reason} - {file_info.get('full_path')}")
                failed_files.append({"file": file_info, "reason": reason})

        # Commit all successful movements at once
        if successful_movements:
            db.bulk_save_objects(successful_movements)
            db.commit()
            print(f"Committed {len(successful_movements)} file movements to database.")
        else:
            print("No file movements to commit.")

    except Exception as e:
        db.rollback()
        print(f"Database commit error: {e}")

    finally:
        db_gen.close()

    # Print summary
    print("\nSummary of bulk move operation:")
    print(f"Successfully moved files: {len(successful_movements)}")
    for m in successful_movements:
        print(f"  - {m.full_path} -> {m.destination_path}")

    print(f"Failed to move files: {len(failed_files)}")
    for f in failed_files:
        print(f"  - {f['file']['full_path']} (Reason: {f['reason']})")

    return [m.full_path for m in successful_movements], failed_files





#Scan, filter, and archive files from a specific share
def archive_filtered_files(filters: dict, blacklist: list, share_name: str, volume: dict):
    print(f"Starting archive process for share: {share_name}")

    #Fresh scan of volume
    all_files = scan_volume(share_name, volume, blacklist)
    if not all_files:
        return {"status": "no_files", "reason": f"No files found in {share_name}"}

    #Filter files
    scanned_dict = {share_name: all_files}
    filtered = filter_files(scanned_dict, filters, blacklist, share_name)

    matching_files = filtered.get(share_name, [])
    if not matching_files:
        return {"status": "no_matches", "reason": "No files matched filters"}

    print(f"Found {len(matching_files)} files to archive...")

    # Step 3: Move files safely, one by one, with retries
    moved_files, failed_files = bulk_move_files(matching_files)

    return {
        "status": "success" if moved_files else "no_matches",
        "archived_count": len(moved_files),
        "moved_files": [{"full_path": path} for path in moved_files],
        "failed_files": [{"full_path": file['full_path'], "reason": file['reason']} for file in failed_files]
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
