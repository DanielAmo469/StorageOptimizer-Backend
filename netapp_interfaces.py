from datetime import datetime
import json
import os
import tempfile
import smbclient
import shutil
from sqlalchemy.orm import Session


from models import FileMovement, ActionType
from netapp_btc import filter_files, get_archive_path, get_svm_data_volumes, load_metadata, normalize_path, save_metadata, scan_volume


def log_file_movement(db: Session, filename, original_path, destination_path, action_type):
    file_movement = FileMovement(
        filename=filename,
        original_path=original_path,
        destination_path=destination_path,
        action_type=action_type
    )
    db.add(file_movement)
    db.commit()

def move_file(file_info):
    src_path = normalize_path(file_info['full_path'])
    dest_folder = normalize_path(get_archive_path(src_path))

    print(f"DEBUG: Attempting to move file")
    print(f"  Source: {src_path}")
    print(f"  Destination Folder: {dest_folder}")

    if not dest_folder:
        print(f"Skipping file {src_path} (Invalid archive destination)")
        return False

    try:
        smbclient.stat(src_path)  
        print("File is accessible, proceeding with move...")

        filename = os.path.basename(src_path)
        dest_path = f"{dest_folder}\\{filename}"
        dest_path = normalize_path(dest_path)

        print(f"Final Destination Path: {dest_path}")

        # ✅ Ensure metadata is set before moving
        if not save_metadata(dest_folder, filename, src_path):
            print(f"ERROR: Failed to save metadata for {filename}. Aborting move.")
            return False

        # ✅ Download file to local temp
        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            temp_file_path = temp_file.name
            with open(temp_file_path, "wb") as f:
                with smbclient.open_file(src_path, mode="rb") as remote_file:
                    shutil.copyfileobj(remote_file, f)

        print(f"Successfully downloaded file to local temp: {temp_file_path}")

        # ✅ Upload file to archive
        with open(temp_file_path, "rb") as f:
            with smbclient.open_file(dest_path, mode="wb") as remote_file:
                shutil.copyfileobj(f, remote_file)

        print(f"Uploaded file to archive: {dest_path}")

        # ✅ Verify the file before deleting the original
        try:
            smbclient.stat(dest_path)
            smbclient.remove(src_path)
            print(f"Deleted original file: {src_path}")
        except FileNotFoundError:
            print(f"Failed to verify copied file at {dest_path}. Not deleting original.")
            return False

        # ✅ Restore timestamps
        os.utime(dest_path, (datetime.strptime(file_info["last_access_time"], '%Y-%m-%d %H:%M:%S').timestamp(),
                             datetime.strptime(file_info["last_modified_time"], '%Y-%m-%d %H:%M:%S').timestamp()))

        # ✅ Create shortcut in the original location
        create_shortcut(src_path, dest_path)

        # ✅ Remove temp file
        os.remove(temp_file_path)
        
        log_file_movement(db, filename, src_path, dest_path, ActionType.moved_to_archive)


        return dest_path

    except FileNotFoundError:
        print(f"File not found: {src_path}")
        return False
    except PermissionError:
        print(f"Permission denied: {src_path}")
        return False
    except Exception as e:
        print(f"Failed to move {src_path}: {e}")
        return False


def create_shortcut(original_path, archive_path):
    shortcut_path = original_path + "_shortcut.bat"  # Create a .bat file instead of .lnk
    
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
    metadata = load_metadata(archive_folder)

    if filename not in metadata:
        print(f"Original location not found for {filename}")
        return False

    original_path = metadata[filename]
    archive_path = os.path.join(archive_folder, filename)

    print(f"Restoring {filename}")
    print(f"  Source (Archive): {archive_path}")
    print(f"  Destination (Original): {original_path}")

    try:
        # ✅ Step 1: Download from archive to local temp
        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            temp_file_path = temp_file.name
            with open(temp_file_path, "wb") as f:
                with smbclient.open_file(archive_path, mode="rb") as remote_file:
                    shutil.copyfileobj(remote_file, f)

        print(f"Downloaded file from archive to local temp: {temp_file_path}")

        # ✅ Step 2: Upload file back to original location
        with open(temp_file_path, "rb") as f:
            with smbclient.open_file(original_path, mode="wb") as remote_file:
                shutil.copyfileobj(f, remote_file)

        print(f"Restored file to: {original_path}")

        # ✅ Step 3: Verify and delete from archive
        try:
            smbclient.stat(original_path)
            smbclient.remove(archive_path)
            print(f"Deleted file from archive: {archive_path}")
        except FileNotFoundError:
            print(f"Failed to verify file at {original_path}. Not deleting archive copy.")
            return False

        # ✅ Step 4: Restore timestamps
        if isinstance(metadata[filename], dict):  # Ensure it has timestamps
            os.utime(original_path, (
                datetime.strptime(metadata[filename]["last_access_time"], '%Y-%m-%d %H:%M:%S').timestamp(),
                datetime.strptime(metadata[filename]["last_modified_time"], '%Y-%m-%d %H:%M:%S').timestamp()
            ))
            print(f"Timestamps restored for {original_path}")

        # ✅ Step 5: Remove shortcut from original path
        shortcut_path = original_path + "_shortcut.bat"  # Fix: Remove `.bat`
        if os.path.exists(shortcut_path):
            os.remove(shortcut_path)
            print(f"Removed shortcut: {shortcut_path}")

        # ✅ Step 6: Remove file entry from metadata.json
        del metadata[filename]
        metadata_file = os.path.join(archive_folder, "metadata.json")
        try:
            with smbclient.open_file(metadata_file, mode="w") as f:
                json.dump(metadata, f, indent=4)
            print(f"Updated metadata.json: Removed entry for {filename}")
        except Exception as e:
            print(f"ERROR: Failed to update metadata.json: {e}")

        log_file_movement(db, filename, archive_path, original_path, ActionType.restored_from_archive)


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
    

    #    print(scan_volume(get_svm_data_volumes()))
#    print(get_svm_data_volumes())



# Example usage:
filters = {
    'file_type': '.txt',
    'date_filters': {  # Allows independent date filters for different types
        'creation_time': {'start_date': '2023-01-01 00:00:00', 'end_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S')},
        'last_access_time': {'start_date': None, 'end_date': None},  # If None, this filter is ignored
        'last_modified_time': {'start_date': None, 'end_date': None},
    },
    'min_size': 100,  # Minimum file size in bytes
    'max_size': 50000  # Maximum file size in bytes
}
blacklist = ['6Uh24TE', '3liOYfQA']

# Scan and filter files
#filtered_files = filter_files(scan_volume(get_svm_data_volumes()), filters, blacklist)
#print(filtered_files)

file1 = {
    "data2": [
    {
        'full_path': "\\\\192.168.16.14\\data2\\t12vnFc8\\cmBsxD3W\\UK7vuCi6\\YY5dETB0.txt",
        'creation_time': '2025-01-12 07:15:49',
        'last_access_time': '2024-08-25 15:15:49',
        'last_modified_time': '2024-08-25 15:15:49',
        'file_size': 32166
    }
    ]
}

file_path = "\\\\192.168.16.14\\data2\\t12vnFc8\\cmBsxD3W\\UK7vuCi6\\YY5dETB0.txt"

try:
    smbclient.stat(file_path)
    print("File exists and is accessible.")
except FileNotFoundError:
    print("File does not exist or cannot be accessed.")

process_files_for_archival(file1)

#restore_file("\\\\192.168.16.15\\archive2", "YY5dETB0.txt")
