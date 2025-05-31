from datetime import datetime, timedelta, timezone
from models import ArchivedScannedLog
from netapp_btc import get_svm_data_volumes
from database import SessionLocal
from feature_vector import should_scan_volume
from netapp_volume_stats import get_archive_volume_free_space
from services import load_settings, normalize_path
from netapp_interfaces import bulk_move_files, bulk_restore_files

def process_archive_restore_decision(share_name: str, cold_files: list, restorable_files: list, existing_archive_files: list) -> dict:
    restore_candidates = []
    restored_size = 0
    for file in restorable_files:
        file["source"] = "archive"
        file["volume"] = share_name.replace("data", "archive")
        file["archived_path"] = file["full_path"]
        file["original_path"] = file["destination_path"]
        restore_candidates.append(file)
        restored_size += file.get("file_size", 0)

    for file in cold_files:
        file["source"] = "data"
        file["volume"] = share_name
    for file in existing_archive_files:
        file["source"] = "archive"
        file["volume"] = share_name.replace("data", "archive")

    merged = cold_files + existing_archive_files

    def safe_time(f):
        try:
            return datetime.strptime(f["last_access_time"], "%Y-%m-%d %H:%M:%S")
        except:
            return datetime.max  # fallback far future so it sorts last

    merged_sorted = sorted(merged, key=safe_time)

    available_gb, _ = get_archive_volume_free_space(share_name)
    available_bytes = available_gb * 1024 ** 3

    total_bytes = 0
    archive_candidates = []
    stay_in_archive = []

    restore_paths = {normalize_path(f["archived_path"]) for f in restore_candidates}

    for file in merged_sorted:
        file_size = file.get("file_size", 0)
        file_path_norm = normalize_path(file["full_path"])

        if file.get("source") == "archive" and file_path_norm in restore_paths:
            continue

        if total_bytes + file_size <= available_bytes:
            total_bytes += file_size
            if file.get("source") == "archive":
                stay_in_archive.append(file)
            else:
                archive_candidates.append(file)
        else:
            if file.get("source") == "archive":
                restore_candidates.append({
                    "archived_path": file["full_path"],
                    "original_path": file.get("original_path", file["full_path"]),
                    "creation_time": file["creation_time"],
                    "last_access_time": file["last_access_time"],
                    "last_modified_time": file["last_modified_time"],
                    "file_size": file["file_size"]
                })



    return {
        "archive_candidates": archive_candidates,
        "restore_candidates": restore_candidates,
        "existing_archive_files": stay_in_archive
    }

def scan_all_volumes_and_process():
    settings = load_settings()
    blacklist = settings.get("blacklist", [])
    db = SessionLocal()
    archive_summary = {"moved": [], "failed": []}
    restore_summary = {"restored": [], "skipped": []}
    decision_log = []


    volumes = get_svm_data_volumes().get("volumes", [])
    if not volumes:
        print("No volumes to scan.")
        return

    full_summary = []

    for volume_info in volumes:
        share_name = volume_info.get("share_name")
        volume_name = volume_info.get("volume", "Unknown")

        if not share_name:
            continue

        try:
            result = should_scan_volume(
                share_name=share_name,
                volume=volume_info,
                settings=settings,
                blacklist=blacklist,
                db=db
            )

            print(f"=== Evaluating Volume: {share_name} ===")
            print(f"Should Scan: {result['should_scan']}")
            print(f"Score: {result['score']}")
            print(f"Reason: {result.get('reason', 'Feature vector analysis')}")
            print("------------------------------------------------------------")

            if not result["should_scan"]:
                decision_log.append({
                "volume": share_name,
                "should_scan": result["should_scan"],
                "score": result["score"],
                "reason": result.get("reason", "Feature vector analysis"),
                "archive_success": len(archive_summary.get("moved", [])),
                "archive_failed": len(archive_summary.get("failed", [])),
                "restore_success": len(restore_summary.get("restored", [])),
                "restore_failed": len(restore_summary.get("skipped", [])),
                "archive_files": archive_summary.get("moved", []),
                "restore_files": restore_summary.get("restored", []),
                })
                continue

            cold_files = result.get("cold_files") or []
            recently_accessed = result.get("restorable_files") or []
            existing_archive = result.get("existing_archive_files") or []



            decision_result = process_archive_restore_decision(
                share_name=share_name,
                cold_files=cold_files,
                restorable_files=recently_accessed,
                existing_archive_files=existing_archive
            )

            archive_candidates = decision_result["archive_candidates"]
            restore_candidates = decision_result["restore_candidates"]


            if restore_candidates:
                print(f"Restoring {len(restore_candidates)} files for {share_name}...")
                restore_summary = bulk_restore_files(restore_candidates, db)

            if archive_candidates:
                print(f"Archiving {len(archive_candidates)} files for {share_name}...")
                moved_files, failed_files = bulk_move_files(archive_candidates)
                archive_summary = {
                    "moved": moved_files,
                    "failed": failed_files
                }

            full_summary.append({
                "volume": share_name,
                "archive_success": len(archive_summary.get("moved", [])),
                "archive_failed": len(archive_summary.get("failed", [])),
                "restore_success": len(restore_summary.get("restored", [])),
                "restore_failed": len(restore_summary.get("skipped", [])),
            })
            decision_log.append({
            "volume": share_name,
            "should_scan": result["should_scan"],
            "score": result["score"],
            "reason": result.get("reason", "Feature vector analysis"),
            "archive_success": len(archive_summary.get("moved", [])),
            "archive_failed": len(archive_summary.get("failed", [])),
            "restore_success": len(restore_summary.get("restored", [])),
            "restore_failed": len(restore_summary.get("skipped", [])),
            "archive_files": archive_summary.get("moved", []),
            "restore_files": restore_summary.get("restored", []),
            })

        except Exception as e:
            import traceback
            print(f"Error processing volume {share_name}: {e}")
            traceback.print_exc()

        print("\n------------------------------------------------------------\n")

    print("scan_all_volumes_and_process completed successfully.")
    print("\nFinal Summary:")
    for entry in full_summary:
        print(entry)

    try:
        log_entries = []
        for entry in full_summary:
            log_entries.append(ArchivedScannedLog(
                share_name=entry["volume"],
                files_scanned=entry["archive_success"] + entry["archive_failed"] + entry["restore_success"] + entry["restore_failed"],
                files_archived=entry["archive_success"],
                files_restored=entry["restore_success"],
                triggered_by_user=False
            ))
        if log_entries:
            db.add_all(log_entries)
            db.commit()
            print(f"Logged {len(log_entries)} scan summaries to DB")
    except Exception as log_error:
        db.rollback()
        print(f"Failed to log scan summaries to DB: {log_error}")

    db.close()
    return decision_log




def scan_single_volume_and_process(volume_name_to_scan):
    settings = load_settings()
    blacklist = settings.get("blacklist", [])
    db = SessionLocal()
    archive_summary = {"moved": [], "failed": []}
    restore_summary = {"restored": [], "skipped": []}
    decision_log = []

    volumes = get_svm_data_volumes().get("volumes", [])
    if not volumes:
        print("No volumes available.")
        return []

    volume_info = next((v for v in volumes if v.get("share_name") == volume_name_to_scan), None)

    if not volume_info:
        print(f"Volume {volume_name_to_scan} not found.")
        return [{"error": f"Volume {volume_name_to_scan} not found"}]

    share_name = volume_info.get("share_name")

    try:
        result = should_scan_volume(
            share_name=share_name,
            volume=volume_info,
            settings=settings,
            blacklist=blacklist,
            db=db
        )

        print(f"=== Evaluating Volume: {share_name} ===")
        print(f"Should Scan: {result['should_scan']}")
        print(f"Score: {result['score']}")
        print(f"Reason: {result.get('reason', 'Feature vector analysis')}")
        print("------------------------------------------------------------")

        if not result["should_scan"]:
            decision_log.append({
                "volume": share_name,
                "should_scan": result["should_scan"],
                "score": result["score"],
                "reason": result.get("reason", "Feature vector analysis"),
                "archive_success": 0,
                "archive_failed": 0,
                "restore_success": 0,
                "restore_failed": 0,
                "archive_files": [],
                "restore_files": [],
            })
            db.close()
            return decision_log

        cold_files = result.get("cold_files") or []
        recently_accessed = result.get("restorable_files") or []
        existing_archive = result.get("existing_archive_files") or []

        print(f"=== DEBUG === Cold files count: {len(cold_files)}")
        print(f"=== DEBUG === Restorable files count: {len(recently_accessed)}")
        print(f"=== DEBUG === Existing archive files count: {len(existing_archive)}")

        decision_result = process_archive_restore_decision(
            share_name=share_name,
            cold_files=cold_files,
            restorable_files=recently_accessed,
            existing_archive_files=existing_archive
        )

        archive_candidates = decision_result["archive_candidates"]
        restore_candidates = decision_result["restore_candidates"]


        if restore_candidates:
            print(f"Restoring {len(restore_candidates)} files for {share_name}...")
            restore_summary = bulk_restore_files(restore_candidates, db)

        if archive_candidates:
            print(f"Archiving {len(archive_candidates)} files for {share_name}...")
            moved_files, failed_files = bulk_move_files(archive_candidates)
            archive_summary = {
                "moved": moved_files,
                "failed": failed_files
            }

        decision_log.append({
            "volume": share_name,
            "should_scan": result["should_scan"],
            "score": result["score"],
            "reason": result.get("reason", "Feature vector analysis"),
            "archive_success": len(archive_summary.get("moved", [])),
            "archive_failed": len(archive_summary.get("failed", [])),
            "restore_success": len(restore_summary.get("restored", [])),
            "restore_failed": len(restore_summary.get("skipped", [])),
            "archive_files": archive_summary.get("moved", []),
            "restore_files": restore_summary.get("restored", []),
        })

    except Exception as e:
        import traceback
        print(f"Error processing volume {share_name}: {e}")
        traceback.print_exc()
        decision_log.append({"volume": share_name, "error": str(e)})

    try:
        db_log_entry = ArchivedScannedLog(
            share_name=share_name,
            files_scanned=len(archive_summary.get("moved", [])) + len(archive_summary.get("failed", [])) + len(restore_summary.get("restored", [])) + len(restore_summary.get("skipped", [])),
            files_archived=len(archive_summary.get("moved", [])),
            files_restored=len(restore_summary.get("restored", [])),
            triggered_by_user=False
        )
        db.add(db_log_entry)
        db.commit()
        print("Logged scan summary to DB")
    except Exception as log_error:
        db.rollback()
        print(f"Failed to log scan summary: {log_error}")

    db.close()
    return decision_log


