from netapp_ontap.resources import Volume, PerformanceCifsMetric, VolumeMetrics
from sqlalchemy import func
from sqlalchemy.orm import Session
from netapp_ontap import HostConnection

from database import SessionLocal
from models import FileMovement, ActionType, VolumeScanLog
from netapp_btc import filter_files, get_svm_data_volumes, get_svm_uuid, get_vol_uuid, get_volume_name_by_share, scan_volume
from datetime import datetime, timedelta, timezone
import json

min_archive_ratio_pct = 10.0


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
    result = db.query(VolumeScanLog.timestamp)\
        .filter(VolumeScanLog.share_name == share_name)\
        .order_by(VolumeScanLog.timestamp.desc())\
        .first()
    return result[0] if result else None


def get_total_archived_files_from_logs(db: Session, share_name: str):
    return db.query(func.sum(VolumeScanLog.files_archived))\
        .filter(VolumeScanLog.share_name == share_name)\
        .scalar() or 0

def is_in_cooldown(last_scan_time: datetime | None, cooldown_hours: int = 6) -> bool:
    if not last_scan_time:
        return False
    return datetime.now(timezone.utc) - last_scan_time < timedelta(hours=cooldown_hours)


def count_old_files(files: list, days_threshold: int = 180) -> int:
    count = 0
    cutoff = datetime.now(timezone.utc) - timedelta(days=days_threshold)

    for file in files:
        last_access = datetime.strptime(file["last_access_time"], "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
        last_modified = datetime.strptime(file["last_modified_time"], "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)


        if last_access < cutoff and last_modified < cutoff:
            count += 1

    return count


def count_files_and_total_size(files: list) -> tuple[int, int]:
    count = len(files)
    total_size = sum(file.get("file_size", 0) for file in files)
    return count, total_size

def is_blacklisted(full_path: str, blacklist: list[str]) -> bool:
    return any(bad.lower() in full_path.lower() for bad in blacklist)

def get_blacklist_from_file(filepath="settings.json") -> list[str]:
    try:
        with open(filepath, "r") as f:
            config = json.load(f)
            return config.get("blacklist", [])
    except FileNotFoundError:
        return []

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
    entry = VolumeScanLog(
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


def generate_scan_metrics(
    db: Session,
    share_name: str,
    all_files: list,
    filtered_files: list,
    old_file_days: int = 180
) -> dict:
    volume_name = get_volume_name_by_share(share_name)
    space = get_volume_space_metrics(volume_name)

    # Time-based threshold for cold files
    old_file_count = count_old_files(filtered_files, days_threshold=old_file_days)

    # General file stats (from full list)
    total_files, total_size = count_files_and_total_size(all_files)

    # Historical archive count
    archive_count = get_total_unique_archived_files(db, share_name)

    return {
        "percent_used": space["percent_used"],
        "old_file_count": old_file_count,
        "total_files": total_files,
        "total_size_bytes": total_size,
        "archived_file_count": archive_count
    }


#This function decides whether a volume should be scanned by checking if it's overused, out of cooldown, contains many old files, or has a history of archived files, and returns reasons and metrics for that decision.
def should_scan_volume(
    db: Session,
    share_name: str,
    all_files: list[dict],
    filtered_files: list[dict],
    last_scan_time: datetime | None,
    min_fullness_pct: float = 85.0,
    cooldown_hours: int = 6,
    old_file_days: int = 180,
    min_old_file_count: int = 10
) -> dict:
    if is_in_cooldown(last_scan_time, cooldown_hours):
        return {
            "should_scan": False,
            "reasons": ["Recently scanned"],
            "metrics": {},
            "stats": {}
        }

    stats = generate_scan_metrics(db, share_name, all_files, filtered_files, old_file_days)
    reasons = []

    if stats["percent_used"] >= min_fullness_pct:
        reasons.append("High volume usage")
    if stats["old_file_count"] >= min_old_file_count:
        reasons.append("Many old files detected")
    archive_ratio = (stats["archived_file_count"] / stats["total_files"]) * 100 if stats["total_files"] > 0 else 0
    if archive_ratio >= min_archive_ratio_pct:
        reasons.append(f"High archive history ({archive_ratio:.1f}%)")


    return {
        "should_scan": bool(reasons),
        "reasons": reasons or ["No scan triggers met"],
        "metrics": {
            "percent_used": stats["percent_used"],
            "old_file_count": stats["old_file_count"],
            "archived_file_count": stats["archived_file_count"]
        },
        "stats": stats  # full metrics for visualization or export
    }



if __name__ == "__main__":
    svm_data = get_svm_data_volumes()
    all_files = scan_volume(svm_data)

    share_name = "data2"
    if share_name not in all_files:
        print(f"No files found under share '{share_name}'.")
        exit()

    #Apply blacklist filters
    blacklist = get_blacklist_from_file()
    filters = {
        "file_type": None,  # No extension filtering
        "date_filters": {},  # No date filtering
        "min_size": None,
        "max_size": None
    }

    filtered_result = filter_files(all_files, filters, blacklist, share_name)
    filtered_files = filtered_result.get(share_name, [])

    #Load DB and last scan info
    db = SessionLocal()
    last_scan = get_last_scan_time(db, share_name)

    #Decide whether to scan
    result = should_scan_volume(db, share_name, all_files[share_name], filtered_files, last_scan)

    #Display
    print("\nSCAN DECISION REPORT for 'data2'")
    print("Should Scan:", result["should_scan"])
    print("Reasons:", result["reasons"])
    print("Metrics:", result["metrics"])
    print("Full Stats:", result["stats"])

    db.close()