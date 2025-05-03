from datetime import datetime, timezone
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

from models import FileMovement, ActionType, ArchivedScannedLog
from netapp_btc import filter_files, get_archive_path, get_svm_data_volumes, get_svm_uuid, get_vol_uuid, get_volume_name_by_share, scan_archive_volume_corrected, scan_volume
from database import SessionLocal, get_db
from netapp_volume_stats import get_archive_volume_free_space, get_recently_accessed_archive_files, get_volume_space_metrics
from schemas import RestoreRequest
from services import load_settings, normalize_path, parse_datetime_safe

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

# Restore multiple files from archive back to original location
# - Accepts full file information already provided (no database fetching)
# - Restores files efficiently based on provided metadata
def bulk_restore_files(files_to_restore: list, db_session: Session) -> dict:
    restored_movements = []
    skipped_files = []

    print(f"Starting bulk restore of {len(files_to_restore)} files...")

    for file_info in files_to_restore:
        try:
            # Validate and normalize required paths
            raw_archived_path = file_info.get('archived_path') or file_info.get('destination_path')
            if not raw_archived_path:
                raise Exception("Missing archived_path or destination_path in file info")

            raw_original_path = file_info.get('original_path') or file_info.get('full_path')
            if not raw_original_path:
                raise Exception("Missing original_path or full_path in file info")

            archived_path = normalize_path(raw_archived_path)
            original_full_path = normalize_path(raw_original_path)

            # Parse timestamps safely
            def safe_parse(ts):
                try:
                    return datetime.strptime(ts, '%Y-%m-%d %H:%M:%S')
                except Exception:
                    print(f"⚠️ Warning: Invalid timestamp '{ts}', using now() instead")
                    return datetime.now()

            creation_time = safe_parse(file_info['creation_time'])
            access_time = safe_parse(file_info['last_access_time'])
            modified_time = safe_parse(file_info['last_modified_time'])
            file_size = file_info['file_size']

            print(f"Restoring archived file: {archived_path} → {original_full_path}")

            # Download from archive to temp file
            temp_fd, temp_path = tempfile.mkstemp()
            os.close(temp_fd)

            try:
                with smbclient.open_file(archived_path, mode="rb") as src_file, open(temp_path, mode="wb") as dst_file:
                    shutil.copyfileobj(src_file, dst_file)
            except Exception as e:
                os.remove(temp_path)
                raise Exception(f"Failed downloading archived file: {e}")

            # Upload to original location
            try:
                with open(temp_path, mode="rb") as src_temp, smbclient.open_file(original_full_path, mode="wb") as dest_file:
                    shutil.copyfileobj(src_temp, dest_file)
            except Exception as e:
                os.remove(temp_path)
                raise Exception(f"Failed restoring file to original location: {e}")

            # Restore access and modified timestamps
            try:
                smbclient.utime(
                    original_full_path,
                    times=(int(access_time.timestamp()), int(modified_time.timestamp()))
                )
                print(f"SMB timestamps set for {original_full_path}")
            except Exception as e1:
                print(f"Warning: SMB timestamp set failed for {original_full_path}: {e1}")

            #Note: SMB cannot set creation time; log it
            print(f"Note: Creation time ({creation_time}) cannot be set over SMB for {original_full_path}.")

            os.remove(temp_path)

            # Delete archive copy
            try:
                smbclient.remove(archived_path)
            except Exception:
                print(f"Warning: Failed to delete archived file {archived_path}")

            # Delete shortcut if exists
            shortcut_path = original_full_path + "_shortcut.bat"
            try:
                smbclient.remove(shortcut_path)
            except Exception:
                print(f"Note: Shortcut {shortcut_path} did not exist.")

            # Log restored movement
            restored_movements.append(
                FileMovement(
                    full_path=archived_path,
                    destination_path=original_full_path,
                    creation_time=creation_time,
                    last_access_time=access_time,
                    last_modified_time=modified_time,
                    file_size=file_size,
                    action_type=ActionType.restored_from_archive
                )
            )

            print(f"Restored: {archived_path} -> {original_full_path}")

        except Exception as e:
            skipped_files.append({
                "path": file_info.get('archived_path') or file_info.get('full_path') or 'UNKNOWN',
                "error": str(e)
            })
            print(f"Skipped {file_info.get('archived_path') or 'UNKNOWN'}: {e}")

    if restored_movements:
        db_session.bulk_save_objects(restored_movements)
        db_session.commit()
        print(f"Committed {len(restored_movements)} restored file records to DB.")

    return {
        "restored": [movement.destination_path for movement in restored_movements],
        "skipped": skipped_files
    }

# Bulk move and archive a list of files
# - Moves files from source to archive destination
# - Creates shortcuts at original locations
# - Collects successful movements and commits them to database
# - Prints moved and failed files with reasons
def bulk_move_files(files: List[dict]) -> tuple[List[str], List[dict]]:
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

                filename = os.path.basename(src_path)
                dest_path = normalize_path(f"{dest_folder}\\{filename}")

                creation_time = datetime.strptime(file_info["creation_time"], '%Y-%m-%d %H:%M:%S')
                access_time = datetime.strptime(file_info["last_access_time"], '%Y-%m-%d %H:%M:%S')
                modified_time = datetime.strptime(file_info["last_modified_time"], '%Y-%m-%d %H:%M:%S')
                file_size = file_info["file_size"]

                print(f"Archiving file: {src_path} -> {dest_path}")

                # Download source file to temporary local file
                temp_fd, temp_path = tempfile.mkstemp()
                os.close(temp_fd)

                try:
                    with open(temp_path, "wb") as local_file:
                        with smbclient.open_file(src_path, mode="rb") as remote_file:
                            shutil.copyfileobj(remote_file, local_file)
                except Exception as e:
                    reason = f"Failed to download: {e}"
                    print(f"{reason} - {src_path}")
                    failed_files.append({"file": file_info, "reason": reason})
                    os.remove(temp_path)
                    continue

                # Upload temporary file to archive destination
                try:
                    with open(temp_path, "rb") as local_file:
                        with smbclient.open_file(dest_path, mode="wb") as remote_file:
                            shutil.copyfileobj(local_file, remote_file)
                except Exception as e:
                    reason = f"Failed to upload: {e}"
                    print(f"{reason} - {dest_path}")
                    failed_files.append({"file": file_info, "reason": reason})
                    os.remove(temp_path)
                    continue

                # Delete source file
                try:
                    smbclient.remove(src_path)
                except Exception as e:
                    reason = f"Failed to delete source: {e}"
                    print(f"{reason} - {src_path}")
                    failed_files.append({"file": file_info, "reason": reason})
                    os.remove(temp_path)
                    continue

                # Create shortcut at original location
                try:
                    create_shortcut(src_path, dest_path)
                except Exception as e:
                    print(f"Shortcut creation failed for {src_path}: {e}")

                os.remove(temp_path)

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
                print(f"{reason} - {file_info.get('full_path', 'UNKNOWN')}")
                failed_files.append({"file": file_info, "reason": reason})

        if successful_movements:
            db.bulk_save_objects(successful_movements)
            db.commit()
            print(f"Committed {len(successful_movements)} file movements to database.")
        else:
            print("No successful movements to commit.")

    except Exception as e:
        db.rollback()
        print(f"Database commit error: {e}")

    finally:
        db_gen.close()

    # Summary
    print("\nSummary of bulk move operation:")
    print(f"Successfully moved files: {len(successful_movements)}")
    for m in successful_movements:
        print(f"  - {m.full_path} -> {m.destination_path}")

    print(f"Failed to move files: {len(failed_files)}")
    for f in failed_files:
        print(f"  - {f['file']['full_path']} (Reason: {f['reason']})")

    return [m.full_path for m in successful_movements], failed_files



def restore_multiple_files(restore_requests: List[dict]) -> dict:
    db = SessionLocal()
    try:
        if not restore_requests:
            return {"restored": [], "skipped": []}

        result = bulk_restore_files(restore_requests, db)

        return {
            "restored": result.get("restored", []),
            "skipped": result.get("skipped", [])
        }

    except Exception as e:
        print(f"Error in restore_multiple_files: {e}")
        return {
            "restored": [],
            "skipped": [{"error": str(e)}]
        }
    finally:
        db.close()



# Analyze a data share and its archive share:
# - Scan the data volume and archive volume
# - Correct archive file metadata based on database (creation, access, modified times)
# - Merge scanned data files and archive files together
# - Filter the merged files based on admin's new filters
# - Sort merged files by last_accessed_time ascending
# - Select files to archive until available archive space is filled
# - Collect non-filtered files as restore candidates
# - Return two lists: archive_candidates and restore_candidates
def analyze_volume_for_archive_and_restore(share_name: str, filters: dict, blacklist: list) -> dict:
    print(f"Analyzing share: {share_name} for archive and restore decisions.")

    available_space_gb, archive_volume_name = get_archive_volume_free_space(share_name)
    if available_space_gb <= 0:
        raise Exception(f"No available space in archive volume for share '{share_name}'.")

    print(f"Available archive space for {share_name}: {available_space_gb} GB")

    volume_data = get_svm_data_volumes()
    volumes = volume_data.get("volumes", [])
    volume_info = next((v for v in volumes if v.get('share_name') == share_name), None)

    if not volume_info:
        raise Exception(f"Volume info for share '{share_name}' not found.")

    data_files = scan_volume(
        share_name=share_name,
        volume=volume_info,
        blacklist=blacklist or []
    ) or []

    for f in data_files:
        f["source"] = "data"
        f["volume"] = share_name

    archive_path = get_archive_path(normalize_path(f"\\\\192.168.16.14\\{share_name}\\dummy.txt"))
    if not archive_path:
        raise Exception(f"Could not resolve archive path for {share_name}")
    archive_share_name = archive_path.split("\\")[3]

    archive_files = scan_archive_volume_corrected(archive_share_name) or []

    for f in archive_files:
        f["source"] = "archive"
        f["volume"] = archive_share_name
        f["archived_path"] = f["full_path"]
        f["original_path"] = f.get("destination_path") or f["full_path"]
        f["creation_time"] = f["creation_time"].strftime('%Y-%m-%d %H:%M:%S')
        f["last_access_time"] = f["last_access_time"].strftime('%Y-%m-%d %H:%M:%S')
        f["last_modified_time"] = f["last_modified_time"].strftime('%Y-%m-%d %H:%M:%S')

    merged_files = data_files + archive_files
    merged_dict = {share_name: merged_files}

    filtered = filter_files(
        files=merged_dict,
        filters=filters,
        blacklist=blacklist or [],
        share_name=share_name
    )

    matching_files = filtered.get(share_name, [])

    blacklist_keywords = blacklist or []
    safe_filtered_files = []
    forced_restore_files = []

    for file in matching_files:
        file_path = file.get("full_path", "").lower()
        if any(blk.lower() in file_path for blk in blacklist_keywords):
            if file.get("source") == "archive":
                forced_restore_files.append(file)
            continue
        safe_filtered_files.append(file)

    matching_files = safe_filtered_files

    all_paths = set(normalize_path(f['full_path']) for f in merged_files)
    filtered_paths = set(normalize_path(f['full_path']) for f in matching_files)
    restore_paths = all_paths - filtered_paths

    restore_candidates = forced_restore_files + [
        file for file in merged_files
        if normalize_path(file["full_path"]) in restore_paths and file.get("source") == "archive"
    ]


    matching_files_sorted = sorted(
        matching_files,
        key=lambda f: parse_datetime_safe(f['last_access_time'])
    )

    available_space_bytes = available_space_gb * 1024**3
    archive_candidates = []
    total_bytes_selected = 0

    for file in matching_files_sorted:
        if file.get("source") != "data":
            continue
        file_size = file.get("file_size", 0)
        if total_bytes_selected + file_size > available_space_bytes:
            break
        archive_candidates.append(file)
        total_bytes_selected += file_size

    # Final cleanup for restore format
    for file in restore_candidates:
        file["archived_path"] = file["full_path"]
        file["original_path"] = file.get("original_path") or file["full_path"]

    print(f"Archive candidates selected: {len(archive_candidates)}")
    print(f"Restore candidates detected: {len(restore_candidates)}")

    return {
        "archive_candidates": archive_candidates,
        "restore_candidates": restore_candidates
    }




# Scan, filter, archive files from a share, and log the operation
def archive_filtered_files(
    cold_files: list,
    share_name: str,
    volume: dict,
    blacklist: list = []
) -> dict:
    print(f"Starting archive process for share: {share_name}")

    if not cold_files:
        print(f"No cold files provided for {share_name}")
        return {"status": "no_files", "reason": "No cold files found"}

    #Get archive volume free space
    available_space_gb = get_archive_volume_free_space(share_name)
    available_space_bytes = available_space_gb * 1024**3

    if available_space_bytes <= 0:
        print(f"No available archive space for {share_name}")
        return {"status": "no_space", "reason": "No archive space available"}

    #Get existing archive files
    db = SessionLocal()
    try:
        settings = load_settings()
        archive_path = get_archive_path(f"\\\\192.168.16.14\\{share_name}\\dummy.txt")
        if not archive_path:
            raise Exception(f"Could not resolve archive path for {share_name}")

        archive_info = get_recently_accessed_archive_files(archive_path, db, settings)
        existing_archive_files = archive_info.get("existing_archive_files", [])
        existing_paths = set(normalize_path(f['full_path']) for f in existing_archive_files)

    finally:
        db.close()

    merged_files = cold_files + existing_archive_files

    #Sort merged files by last_access_time ascending
    def parse_datetime_safe(date_str):
        try:
            return datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S')
        except Exception:
            return datetime.now(timezone.utc)

    merged_files_sorted = sorted(
        merged_files,
        key=lambda f: parse_datetime_safe(f['last_access_time'])
    )

    print(f"Total candidates to archive after merge and sort: {len(merged_files_sorted)}")

    #Select files to move
    files_to_archive = []
    already_archived = []
    skipped_files = []

    total_bytes_selected = 0

    for file in merged_files_sorted:
        full_path = normalize_path(file['full_path'])

        if full_path in existing_paths:
            already_archived.append(full_path)
            continue

        file_size = file.get('file_size', 0)

        if total_bytes_selected + file_size > available_space_bytes:
            skipped_files.append(full_path)
            continue

        files_to_archive.append(file)
        total_bytes_selected += file_size

    print(f"Selected {len(files_to_archive)} files to archive.")
    print(f"Already archived files skipped: {len(already_archived)}")
    print(f"Skipped due to space: {len(skipped_files)}")

    #Move files
    if not files_to_archive:
        print("No new files selected for archiving.")
        return {
            "status": "no_space" if skipped_files else "already_archived",
            "archived_count": 0,
            "already_archived_count": len(already_archived),
            "skipped_due_to_space": len(skipped_files),
        }

    moved_files, failed_files = bulk_move_files(files_to_archive)

    return {
        "status": "success" if moved_files else "partial_success",
        "archived_count": len(moved_files),
        "already_archived_count": len(already_archived),
        "skipped_due_to_space": len(skipped_files),
        "moved_files": [{"full_path": path} for path in moved_files],
        "failed_files": [{"full_path": f['file']['full_path'], "reason": f['reason']} for f in failed_files],
        "already_archived_files": already_archived,
        "space_skipped_files": skipped_files
    }
