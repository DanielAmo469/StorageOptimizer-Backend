from datetime import datetime
import json
import os
import tempfile
import smbclient
import shutil
from sqlalchemy.orm import Session


from models import FileMovement, ActionType
from netapp_btc import filter_files, get_archive_path, get_svm_data_volumes, normalize_path, scan_volume
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


def move_file(file_info):
    
    src_path = normalize_path(file_info['full_path'])
    dest_folder = normalize_path(get_archive_path(src_path))

    if src_path.endswith("_shortcut.bat") or src_path.endswith(".bat"):
        print(f"⛔ Skipped: Shortcut or batch file detected → {src_path}")
        return None, None

    print(f"DEBUG: Attempting to move file")
    print(f"  Source: {src_path}")
    print(f"  Destination Folder: {dest_folder}")

    if not dest_folder:
        print(f"Skipping file {src_path} (Invalid archive destination)")
        return None, None

    try:
        smbclient.stat(src_path)  
        print("File is accessible, proceeding with move...")

        filename = os.path.basename(src_path)
        dest_path = f"{dest_folder}\\{filename}"
        dest_path = normalize_path(dest_path)

        print(f"Final Destination Path: {dest_path}")

        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            temp_file_path = temp_file.name
            with open(temp_file_path, "wb") as f:
                with smbclient.open_file(src_path, mode="rb") as remote_file:
                    shutil.copyfileobj(remote_file, f)

        print(f"Downloaded file to local temp: {temp_file_path}")

        with open(temp_file_path, "rb") as f:
            with smbclient.open_file(dest_path, mode="wb") as remote_file:
                shutil.copyfileobj(f, remote_file)

        print(f"Uploaded file to archive: {dest_path}")

        try:
            smbclient.stat(dest_path)
            smbclient.remove(src_path)
            print(f"Deleted original file: {src_path}")
        except FileNotFoundError:
            print(f"Failed to verify copied file at {dest_path}. Not deleting original.")
            return None, None

        os.utime(dest_path, (
            datetime.strptime(file_info["last_access_time"], '%Y-%m-%d %H:%M:%S').timestamp(),
            datetime.strptime(file_info["last_modified_time"], '%Y-%m-%d %H:%M:%S').timestamp()
        ))

        create_shortcut(src_path, dest_path)

        os.remove(temp_file_path)

        file_movement = FileMovement(
            full_path=src_path,
            destination_path=dest_path,
            creation_time=datetime.strptime(file_info['creation_time'], '%Y-%m-%d %H:%M:%S'),
            last_access_time=datetime.strptime(file_info['last_access_time'], '%Y-%m-%d %H:%M:%S'),
            last_modified_time=datetime.strptime(file_info['last_modified_time'], '%Y-%m-%d %H:%M:%S'),
            file_size=file_info['file_size'],
            action_type=ActionType.moved_to_archive
        )

        return dest_path, file_movement

    except FileNotFoundError:
        print(f"File not found: {src_path}")
        return None, None
    except PermissionError:
        print(f"Permission denied: {src_path}")
        return None, None
    except Exception as e:
        print(f"Failed to move {src_path}: {e}")
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



def restore_file(archive_folder, filename):
    from sqlalchemy import desc

    archive_path = os.path.join(archive_folder, filename)
    print(f"Preparing to restore {filename} from {archive_path}")

    db_gen = get_db()
    db = next(db_gen)
    try:
        # Find the most recent archive entry for the given file
        archive_entry = db.query(FileMovement)\
            .filter(FileMovement.destination_path == normalize_path(archive_path))\
            .filter(FileMovement.action_type == ActionType.moved_to_archive)\
            .order_by(desc(FileMovement.timestamp))\
            .first()

        if not archive_entry:
            print(f"No matching archive entry found in DB for: {filename}")
            return False

        original_path = archive_entry.full_path
        print(f"Restoring {filename}")
        print(f"  Source (Archive): {archive_path}")
        print(f"  Destination (Original): {original_path}")

        # Copy from archive to temp file
        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            temp_file_path = temp_file.name
            with open(temp_file_path, "wb") as f:
                with smbclient.open_file(archive_path, mode="rb") as remote_file:
                    shutil.copyfileobj(remote_file, f)
        print(f"Downloaded file from archive to local temp: {temp_file_path}")

        # Copy from temp to original location
        with open(temp_file_path, "rb") as f:
            with smbclient.open_file(original_path, mode="wb") as remote_file:
                shutil.copyfileobj(f, remote_file)
        print(f"Restored file to: {original_path}")

        # Remove the file from archive
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

        # Remove the shortcut file if exists
        shortcut_path = original_path + "_shortcut.bat"
        if os.path.exists(shortcut_path):
            os.remove(shortcut_path)
            print(f"Removed shortcut: {shortcut_path}")

        # Log restore operation
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

    
def archive_filtered_files(filters: dict, blacklist: list, share_name: str):
    """
    Scans all SVM volumes, filters files, archives matching ones, and logs all moves to DB in bulk.
    Returns a summary.
    """
    print(f"🔍 Starting archive process for share: {share_name}")

    svm_data = get_svm_data_volumes()
    if not svm_data:
        return {"status": "failed", "reason": "No SVM volumes found"}

    all_files = scan_volume(svm_data)
    if not all_files or share_name not in all_files:
        return {"status": "no_files", "reason": f"No files found in {share_name}"}

    filtered = filter_files(all_files, filters, blacklist, share_name)

    if not filtered:
        return {"status": "no_matches"}

    archived_files = []
    movements = []

    for file_info in filtered.get(share_name, []):
        archive_path, movement = move_file(file_info)
        if archive_path and movement:
            archived_files.append({
                "filename": os.path.basename(file_info["full_path"]),
                "original_path": file_info["full_path"],
                "archived_path": archive_path
            })
            movements.append(movement)

    if movements:
        db_gen = get_db()
        db = next(db_gen)
        try:
            db.bulk_save_objects(movements)
            db.commit()
        except Exception as e:
            print(f"❌ Failed to save file movements to DB: {e}")
            db.rollback()
        finally:
            db_gen.close()

    return {
        "status": "success" if archived_files else "no_matches",
        "archived_count": len(archived_files),
        "files": archived_files
    }


    #    print(scan_volume(get_svm_data_volumes()))
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
