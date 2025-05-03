import os
from typing import Dict
from netapp_ontap.resources import Volume, PerformanceCifsMetric, VolumeMetrics
import smbclient
from sqlalchemy import func
from sqlalchemy.orm import Session
from netapp_ontap import HostConnection

from database import SessionLocal
from models import EvaluationMode, FileMovement, ActionType, ArchivedScannedLog, VolumeScanDecisionLog
from netapp_btc import access_CIFS_share, get_first_ip_address, get_svm_data_volumes, get_svm_uuid, get_vol_uuid, get_volume_name_by_share
from datetime import datetime, timedelta, timezone
from services import load_settings, normalize_path


def get_svm_performance_metric(svm_name):
    with HostConnection("192.168.16.4", username="admin", password="Netapp1!", verify=False):
        uuid = get_svm_uuid(svm_name)
        if not uuid:
            print(f"Svm '{svm_name}' not found.")
            return None
        
        metrics_cifs = PerformanceCifsMetric.get_collection(uuid, fields="iops,latency")
        print(list(metrics_cifs))        


def get_volume_performance_by_share(share_name: str):
    volume_name = get_volume_name_by_share(share_name)
    if not volume_name:
        print(f"Share '{share_name}' not found in CIFS volumes.")
        return None

    uuid = get_vol_uuid(volume_name)
    if not uuid:
        print(f"UUID not found for volume '{volume_name}'")
        return None

    with HostConnection("192.168.16.4", "admin", "Netapp1!", verify=False):
        metrics = VolumeMetrics.get_collection(uuid, fields="iops,latency")
        metrics_list = [m.to_dict() for m in metrics]

        for entry in metrics_list:
            print(entry["timestamp"], "→ Latency:", entry["latency"]["total"], "ms")
        return metrics_list


def get_volume_space_metrics(volume_name: str):
    with HostConnection("192.168.16.4", "admin", "Netapp1!", verify=False):
        volumes = Volume.get_collection(name=volume_name, fields="space.size,space.used")
        for vol in volumes:
            size = vol.space.size or 0
            used = vol.space.used or 0
            percent_used = (used / size * 100) if size else 0
            return {"size": size, "used": used, "percent_used": percent_used}
        return None

def get_archive_volume_free_space(share_name: str) -> float:
    archive_volume_mapping = {
        "data1": "vol_archive1",
        "data2": "vol_archive2"
    }

    archive_volume_name = archive_volume_mapping.get(share_name.lower())
    if not archive_volume_name:
        print(f"Unknown share_name '{share_name}' for archive volume mapping.")
        return 0.0

    metrics = get_volume_space_metrics(archive_volume_name)
    if not metrics:
        print(f"Failed to get space metrics for archive volume '{archive_volume_name}'")
        return 0.0

    size_bytes = metrics.get("size", 0)
    used_bytes = metrics.get("used", 0)

    if size_bytes == 0:
        return 0.0, archive_volume_name

    free_bytes = max(size_bytes - used_bytes, 0)
    free_gb = free_bytes / (1024 ** 3)

    return round(free_gb, 2), archive_volume_name


def get_total_unique_archived_files(db: Session, share_name: str) -> int:
    svm_data = get_svm_data_volumes()
    ip = svm_data.get("ip_addresses", [None])[0]
    if not ip:
        return 0

    # Build UNC match pattern for SQL LIKE
    pattern = f"%\\\\{ip}\\\\{share_name}\\\\%"

    subquery = db.query(
        FileMovement.full_path,
        func.max(FileMovement.timestamp).label("latest")
    ).filter(
        FileMovement.full_path.like(pattern),
        FileMovement.action_type == ActionType.moved_to_archive
    ).group_by(FileMovement.full_path).subquery()

    return db.query(subquery.c.full_path).count()



def get_last_scan_time(db: Session, share_name: str):
    result = db.query(ArchivedScannedLog.timestamp)\
        .filter(ArchivedScannedLog.share_name == share_name)\
        .order_by(ArchivedScannedLog.timestamp.desc())\
        .first()
    return result[0] if result else None


def get_total_archived_files_from_logs(db: Session, share_name: str):
    return db.query(func.sum(ArchivedScannedLog.files_archived))\
        .filter(ArchivedScannedLog.share_name == share_name)\
        .scalar() or 0



def is_in_cooldown(db: Session, share_name: str, cooldown_hours: int) -> bool:
    last_scan_time = get_last_scan_time(db, share_name)
    if not last_scan_time:
        return False

    if isinstance(last_scan_time, str):
        last_scan_time = datetime.strptime(last_scan_time, "%Y-%m-%d %H:%M:%S")
    
    if last_scan_time.tzinfo is None:
        last_scan_time = last_scan_time.replace(tzinfo=timezone.utc)
    
    return datetime.now(timezone.utc) - last_scan_time < timedelta(hours=cooldown_hours)




#Dynamically fetch cold and old days, supporting both per-mode or global settings
def get_cold_old_days(settings: dict, mode: str) -> tuple[int, int]:
    try:
        thresholds = settings.get("modes", {}).get(mode, {}).get("thresholds", {})
        min_cold = thresholds.get("min_cold_file_age_days")
        min_old = thresholds.get("min_old_file_age_days")

        if min_cold is not None and min_old is not None:
            return min_cold, min_old

    except Exception as e:
        print(f"[WARNING] Failed to fetch cold/old thresholds for mode '{mode}': {e}")

    # Fallback if not found
    return 500, 1000


# Scan a share and collect detailed statistics:
# - Cold and old file detection based on settings
# - Count blacklisted folders and their file counts
# - Return full list of files with metadata, cold files, and summary stats
def scan_volume_with_stats(share_name: str, volume: dict, blacklist: list[str], settings: dict) -> dict:
    print(f"Entered scan_volume_with_stats for: {share_name}")

    with HostConnection('192.168.16.4', 'admin', 'Netapp1!', verify=False):
        print(f"Volume received: {volume}")

        volume["nas_server"] = {
            "interfaces": [{"ip": "192.168.16.14"}]
        }

        ip_address = get_first_ip_address(volume)
        if not ip_address:
            print(f"No IP address found for volume: {volume}")
            return {}

        print(f"IP address: {ip_address}")

        share_path, confirmed_share_name = access_CIFS_share({"share_name": share_name}, ip_address)
        if not share_path or confirmed_share_name != share_name:
            print(f"Share '{share_name}' not found or path invalid.")
            return {}

        print(f"Resolved path: {share_path}")
        print(f"Confirmed share name: {confirmed_share_name}")

        files = []
        cold_files = []
        old_file_count = 0
        blacklisted_folders_hit = 0
        blacklisted_file_total = 0

        now = datetime.now(timezone.utc)

        mode = settings.get("mode", "default")
        min_cold_file_age_days, min_old_file_age_days = get_cold_old_days(settings, mode)

        cold_cutoff = now - timedelta(days=min_cold_file_age_days)
        old_cutoff = now - timedelta(days=min_old_file_age_days)

        try:
            for walk_result in smbclient.walk(share_path):
                if not isinstance(walk_result, (list, tuple)) or len(walk_result) != 3:
                    print(f"Unexpected walk result for {share_name}: {walk_result}")
                    break

                dirpath, dirnames, filenames = walk_result

                if any(bad.lower() in dirpath.lower() for bad in blacklist):
                    blacklisted_folders_hit += 1
                    blacklisted_file_total += len(filenames)
                    continue

                for file in filenames:
                    if file.endswith(".bat"):
                        continue

                    full_path = os.path.join(dirpath, file)

                    try:
                        ctime = os.path.getctime(full_path)
                        atime = os.path.getatime(full_path)
                        mtime = os.path.getmtime(full_path)

                        creation_time = datetime.fromtimestamp(ctime, tz=timezone.utc)
                        last_access_time = datetime.fromtimestamp(atime, tz=timezone.utc)
                        last_modified_time = datetime.fromtimestamp(mtime, tz=timezone.utc)
                        file_size = os.path.getsize(full_path)
                    except Exception as e:
                        print(f"Skipping unreadable file: {full_path} → {e}")
                        continue

                    file_info = {
                        'full_path': full_path,
                        'creation_time': creation_time.strftime('%Y-%m-%d %H:%M:%S'),
                        'last_access_time': last_access_time.strftime('%Y-%m-%d %H:%M:%S'),
                        'last_modified_time': last_modified_time.strftime('%Y-%m-%d %H:%M:%S'),
                        'file_size': file_size
                    }

                    files.append(file_info)

                    if last_access_time < cold_cutoff:
                        cold_files.append(file_info)

                    if last_access_time < old_cutoff and last_modified_time < old_cutoff:
                        old_file_count += 1

        except Exception as e:
            print(f"Error walking share {share_path}: {e}")
            return {}

        total_size = sum(f['file_size'] for f in files)


    total_files_including_blacklisted = blacklisted_file_total + len(files)

    blacklist_ratio = (blacklisted_file_total / total_files_including_blacklisted) if total_files_including_blacklisted else 0
    blacklist_ratio *= 100

    return {
        "all_files": files,
        "cold_files": cold_files,
        "old_file_count": old_file_count,
        "total_file_count": len(files),
        "total_file_size": total_size,
        "blacklisted_folders_hit": blacklisted_folders_hit,
        "blacklisted_file_total": blacklisted_file_total,
        "blacklist_ratio": blacklist_ratio,
        "fullness_percent": (total_size / (1024 ** 3)) * 100 * 100 # Estimate based on size in GB, to set percentage not a unitInterval
    }




# Scan an archive volume, sort files by last access time, and return metadata of recently accessed files to consider for restoration.
# - Use DB records to correct creation/access/modified times if needed
# - Tag each file with full metadata + original destination path
# - Returns: restorable files + all valid archive file metadata
def get_recently_accessed_archive_files(archive_path: str, db_session, settings: dict) -> Dict[str, list]:
    restorable_files = []
    existing_archive_files = []

    mode = settings.get("mode", "default")
    min_cold_file_age_days, _ = get_cold_old_days(settings, mode)
    cold_cutoff = datetime.now(timezone.utc) - timedelta(days=min_cold_file_age_days)

    for root, _, files in os.walk(archive_path):
        for name in files:
            full_path = normalize_path(os.path.join(root, name))

            try:
                stat = os.stat(full_path)
                real_creation_time = datetime.fromtimestamp(stat.st_ctime, tz=timezone.utc)
                real_last_access_time = datetime.fromtimestamp(stat.st_atime, tz=timezone.utc)
                real_last_modified_time = datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc)
                file_size = stat.st_size

                # Defaults
                creation_time = real_creation_time
                last_access_time = real_last_access_time
                last_modified_time = real_last_modified_time
                original_path = None

                # DB lookup for true path + access info
                record = db_session.query(FileMovement).filter(
                    FileMovement.destination_path == full_path,
                    FileMovement.action_type == ActionType.moved_to_archive
                ).order_by(FileMovement.id.desc()).first()

                if record:
                    original_path = record.full_path
                    creation_time = record.creation_time

                    if real_creation_time == real_last_access_time and record.last_access_time:
                        last_access_time = record.last_access_time
                    else:
                        last_access_time = real_last_access_time

                    if real_creation_time == real_last_modified_time and record.last_modified_time:
                        last_modified_time = record.last_modified_time
                    else:
                        last_modified_time = real_last_modified_time

                if not original_path:
                    original_path = "UNKNOWN"

                metadata = {
                    "archived_path": full_path,  # archive UNC path
                    "original_path": original_path,  # data UNC path
                    "creation_time": creation_time.strftime('%Y-%m-%d %H:%M:%S'),
                    "last_access_time": last_access_time.strftime('%Y-%m-%d %H:%M:%S'),
                    "last_modified_time": last_modified_time.strftime('%Y-%m-%d %H:%M:%S'),
                    "file_size": file_size
                }

                existing_archive_files.append(metadata)

                # Add to restorable if recently accessed
                if last_access_time > cold_cutoff:
                    restorable_files.append(metadata)

            except Exception as e:
                print(f"Skipping archive file {full_path} due to error: {e}")
                continue

    return {
        "restorable_files": restorable_files,
        "existing_archive_files": existing_archive_files
    }



def log_scan_result(
    db: Session,
    share_name: str,
    volume_name: str,
    files_scanned: int,
    files_archived: int,
    files_restored: int,
    filters_used: dict,
    triggered_by_user: bool = False
):
    entry = ArchivedScannedLog(
        share_name=share_name,
        volume_name=volume_name,
        files_scanned=files_scanned,
        files_archived=files_archived,
        files_restored=files_restored,
        filters_used=filters_used,
        triggered_by_user=triggered_by_user
    )
    db.add(entry)
    db.commit()

def log_volume_evaluation(
    db: Session,
    share_name: str,
    volume_name: str,
    mode: str,
    should_scan: bool,
    score: float,
    reason: str,
    raw_scores: dict,
    weighted_scores: dict,
    cold_files: int,
    restored_files: int
):
    try:
        log = VolumeScanDecisionLog(
            share_name=share_name,
            volume_name=volume_name,
            timestamp=datetime.utcnow(),
            mode=EvaluationMode(mode),
            should_scan=should_scan,
            scan_score=score,
            reason=reason,
            raw_scores=raw_scores,
            weighted_scores=weighted_scores,
            cold_file_count=cold_files,
            restore_file_count=restored_files
        )
        db.add(log)
        db.commit()
    except Exception as e:
        print(f"Failed to log evaluation for {share_name}: {e}")

