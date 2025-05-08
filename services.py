from datetime import datetime, timedelta, timezone
import json
import os
from typing import Any
from fastapi import Depends, HTTPException, status
from pydantic import create_model
from sqlalchemy import Integer, case, func
from auth import get_current_user
from database import SessionLocal, get_db
from models import ArchivedScannedLog, FileMovement, Role, User, VolumeScanDecisionLog
from sqlalchemy.orm import Session

from schemas import ArchiveFilterRequest

SETTINGS_FILE = "settings.json"


def get_user_id_by_username(username: str, db: Session) -> int:
    user = db.query(User).filter(User.username == username).first()
    if user:
        return user.id
    else:
        return 0
    

def get_user_by_id(user_id: int, db: Session):
    user = db.query(User).filter(User.ID == user_id).first()
    if not user:
        raise HTTPException(status_code=401, detail="User not found")
    return user


def verify_manager(user: User = Depends(get_current_user), db: Session = Depends(get_db)):
    if user.role != Role.manager:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN, 
            detail="You do not have permission to view registration requests."
        )
    return user

def verify_viewonly(user:User =  Depends(get_current_user)):
    if user.role !="viewonly":
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Access forbidden: View and Managers only"
        )
    return user


def normalize_path(file_path):
    file_path = file_path.replace("/", "\\")  
    if not file_path.startswith("\\\\"):
        file_path = "\\\\" + file_path.lstrip("\\")
    return file_path

def parse_datetime_safe(date_str):
    try:
        return datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S')
    except Exception:
        return datetime.now(timezone.utc)

# Build filter dictionary from request
def build_filters_from_request(filter_request: ArchiveFilterRequest) -> dict:
    return {
        "file_type": filter_request.file_type or [],
        "date_filters": filter_request.date_filters.dict() if filter_request.date_filters else {},
        "min_size": filter_request.min_size,
        "max_size": filter_request.max_size,
    }

def load_settings():
    with open("settings.json", "r") as f:
        return json.load(f)
    
def get_settings_for_mode(mode: str = "default") -> dict:
    settings_path = os.path.join(os.path.dirname(__file__), "settings.json")

    with open(settings_path, "r") as f:
        settings = json.load(f)

    all_modes = settings.get("modes", {})
    mode_config = all_modes.get(mode)

    if not mode_config:
        print(f"Mode '{mode}' not found in settings.json. Falling back to 'default'.")
        mode_config = all_modes.get("default", {})

    return mode_config


def get_field_type(value):
    if isinstance(value, str):
        return (str, ...)
    elif isinstance(value, int):
        return (int, ...)
    elif isinstance(value, float):
        return (float, ...)
    elif isinstance(value, bool):
        return (bool, ...)
    elif isinstance(value, list):
        return (list, ...)
    elif isinstance(value, dict):
        return (dict, ...)
    else:
        return (Any, ...)

def get_dynamic_settings_model():
    with open(SETTINGS_FILE, "r") as f:
        current_settings = json.load(f)

    fields = {key: get_field_type(value) for key, value in current_settings.items()}
    DynamicSettingsModel = create_model("DynamicSettingsModel", **fields)
    return DynamicSettingsModel


def get_time_range(filter: str):
    now = datetime.utcnow()
    if filter == "last_hour":
        return now - timedelta(hours=1)
    if filter == "last_day":
        return now - timedelta(days=1)
    elif filter == "last_week":
        return now - timedelta(weeks=1)
    elif filter == "last_month":
        return now - timedelta(days=30)
    elif filter == "last_3_months":
        return now - timedelta(days=90)
    elif filter == "last_6_months":
        return now - timedelta(days=180)
    elif filter == "last_year":
        return now - timedelta(days=365)
    elif filter == "all_time" or filter is None:
        return None
    else:
        return None


def get_file_movement_stats(db, filter=None):
    start_time = get_time_range(filter)

    archived_query = db.query(FileMovement).filter(FileMovement.action_type == 'moved_to_archive')
    restored_query = db.query(FileMovement).filter(FileMovement.action_type == 'restored_from_archive')

    if start_time:
        archived_query = archived_query.filter(FileMovement.timestamp >= start_time)
        restored_query = restored_query.filter(FileMovement.timestamp >= start_time)

    total_archived_files = archived_query.count()
    total_restored_files = restored_query.count()

    total_archive_size = archived_query.with_entities(func.sum(FileMovement.file_size)).scalar() or 0
    total_restore_size = restored_query.with_entities(func.sum(FileMovement.file_size)).scalar() or 0

    archived_trend = archived_query.with_entities(
        func.date_trunc('month', FileMovement.timestamp).label('month'),
        func.count().label('archived_count'),
        func.sum(FileMovement.file_size).label('archive_size')
    ).group_by('month').order_by('month').all()

    restored_trend = restored_query.with_entities(
        func.date_trunc('month', FileMovement.timestamp).label('month'),
        func.count().label('restored_count')
    ).group_by('month').order_by('month').all()

    month_dict = {}
    for row in archived_trend:
        month_str = row.month.strftime('%Y-%m')
        month_dict[month_str] = {
            "month": month_str,
            "archived_count": row.archived_count,
            "restored_count": 0,
            "archive_size": float(row.archive_size or 0) / (1024*1024*1024)
        }
    for row in restored_trend:
        month_str = row.month.strftime('%Y-%m')
        if month_str not in month_dict:
            month_dict[month_str] = {
                "month": month_str,
                "archived_count": 0,
                "restored_count": row.restored_count,
                "archive_size": 0
            }
        else:
            month_dict[month_str]["restored_count"] = row.restored_count

    monthly_trends = list(month_dict.values())

    age_buckets = db.query(
        (func.floor(func.extract('epoch', FileMovement.timestamp - FileMovement.creation_time) / 86400 / 30) * 30).cast(Integer).label('age_start'),
        func.count().label('count')
    ).filter(
        FileMovement.action_type == 'moved_to_archive'
    ).group_by('age_start').order_by('age_start').all()

    age_distribution = [{"age_range": row[0], "count": row[1]} for row in age_buckets]


    restore_success_count = db.query(func.count(FileMovement.id)).filter(FileMovement.action_type == 'restored_from_archive').scalar() or 0
    restore_failure_count = 0

    avg_archive_file_size = (total_archive_size / total_archived_files) if total_archived_files else 0
    avg_restore_file_size = (total_restore_size / total_restored_files) if total_restored_files else 0

    return {
        "total_archived_files": total_archived_files,
        "total_restored_files": total_restored_files,
        "total_archive_size": total_archive_size,
        "total_restore_size": total_restore_size,
        "avg_archive_file_size": avg_archive_file_size,
        "avg_restore_file_size": avg_restore_file_size,
        "monthly_trends": monthly_trends,
        "age_distribution": age_distribution,
        "restore_success_count": restore_success_count,
        "restore_failure_count": restore_failure_count
    }


def get_scan_stats(db, filter=None):
    start_time = get_time_range(filter)

    query = db.query(ArchivedScannedLog)
    if start_time:
        query = query.filter(ArchivedScannedLog.timestamp >= start_time)

    total_scans = query.count()
    total_files_scanned = query.with_entities(func.sum(ArchivedScannedLog.files_scanned)).scalar() or 0
    total_files_archived = query.with_entities(func.sum(ArchivedScannedLog.files_archived)).scalar() or 0
    total_files_restored = query.with_entities(func.sum(ArchivedScannedLog.files_restored)).scalar() or 0
    manual_scans = query.filter(ArchivedScannedLog.triggered_by_user == True).count()
    system_scans = query.filter(ArchivedScannedLog.triggered_by_user == False).count()

    # Monthly scan trend
    scan_trend = query.with_entities(
        func.date_trunc('month', ArchivedScannedLog.timestamp).label('month'),
        func.count().label('scan_count'),
        func.sum(ArchivedScannedLog.files_archived).label('archived_count'),
        func.sum(ArchivedScannedLog.files_restored).label('restored_count')
    ).group_by('month').order_by('month').all()

    monthly_trends = []
    for row in scan_trend:
        monthly_trends.append({
            "month": row.month.strftime('%Y-%m'),
            "scan_count": row.scan_count,
            "archived_count": row.archived_count or 0,
            "restored_count": row.restored_count or 0
        })

    return {
        "total_scans": total_scans,
        "total_files_scanned": total_files_scanned,
        "total_files_archived": total_files_archived,
        "total_files_restored": total_files_restored,
        "manual_scans": manual_scans,
        "system_scans": system_scans,
        "monthly_trends": monthly_trends
    }


def get_volume_scan_stats(db, filter=None):
    start_time = get_time_range(filter)

    volume_query = db.query(VolumeScanDecisionLog)
    if start_time:
        volume_query = volume_query.filter(VolumeScanDecisionLog.timestamp >= start_time)

    total_decisions = volume_query.count()
    mode_counts = dict(volume_query.with_entities(
        VolumeScanDecisionLog.mode,
        func.count()
    ).group_by(VolumeScanDecisionLog.mode).all())

    should_scan_true = volume_query.filter(VolumeScanDecisionLog.should_scan == True).count()
    avg_scan_score = volume_query.with_entities(func.avg(VolumeScanDecisionLog.scan_score)).scalar() or 0
    avg_cold_files = volume_query.with_entities(func.avg(VolumeScanDecisionLog.cold_file_count)).scalar() or 0
    avg_restore_files = volume_query.with_entities(func.avg(VolumeScanDecisionLog.restore_file_count)).scalar() or 0

    mode_scores = {}
    for mode in mode_counts.keys():
        avg_score = volume_query.filter(VolumeScanDecisionLog.mode == mode).with_entities(func.avg(VolumeScanDecisionLog.scan_score)).scalar() or 0
        mode_scores[mode] = avg_score

    return {
        "total_decisions": total_decisions,
        "mode_counts": mode_counts,
        "should_scan_true_count": should_scan_true,
        "avg_scan_score": avg_scan_score,
        "avg_cold_file_count": avg_cold_files,
        "avg_restore_file_count": avg_restore_files,
        "mode_scores": mode_scores
    }


def get_recent_archived_scans(db, limit=5):

    scans = db.query(ArchivedScannedLog)\
              .order_by(ArchivedScannedLog.timestamp.desc())\
              .limit(limit).all()
    return [
        {
            "id": scan.id,
            "share_name": scan.share_name,
            "timestamp": scan.timestamp.isoformat(),
            "triggered_by_user": scan.triggered_by_user,
            "files_scanned": scan.files_scanned,
            "files_archived": scan.files_archived,
            "files_restored": scan.files_restored,
            "filters_used": scan.filters_used
        }
        for scan in scans
    ]

def get_recent_file_movements(db, limit=25):

    movements = db.query(FileMovement)\
                .order_by(FileMovement.id.desc())\
                .limit(limit).all()
    return [
        {
            "id": move.id,
            "full_path": move.full_path,
            "destination_path": move.destination_path,
            "creation_time": move.creation_time.isoformat() if move.creation_time else None,
            "last_access_time": move.last_access_time.isoformat() if move.last_access_time else None,
            "last_modified_time": move.last_modified_time.isoformat() if move.last_modified_time else None,
            "file_size": move.file_size,
            "timestamp": move.timestamp.isoformat(),
            "action_type": move.action_type.name if move.action_type else None
        }
        for move in movements
    ]

def get_recent_volume_scan_decisions(db, limit=5):

    decisions = db.query(VolumeScanDecisionLog)\
                  .order_by(VolumeScanDecisionLog.timestamp.desc())\
                  .limit(limit).all()
    return [
        {
            "id": decision.id,
            "share_name": decision.share_name,
            "volume_name": decision.volume_name,
            "timestamp": decision.timestamp.isoformat(),
            "mode": decision.mode.name if decision.mode else None,
            "should_scan": decision.should_scan,
            "scan_score": decision.scan_score,
            "reason": decision.reason,
            "raw_scores": decision.raw_scores,
            "weighted_scores": decision.weighted_scores,
            "cold_file_count": decision.cold_file_count,
            "restore_file_count": decision.restore_file_count
        }
        for decision in decisions
    ]
