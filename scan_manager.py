from netapp_btc import get_svm_data_volumes, get_volume_name_by_share
from database import SessionLocal
from feature_vector import should_scan_volume
from netapp_interfaces import bulk_restore_files, move_files_and_commit
from services import load_settings

# Scan all volumes and decide whether to archive cold files and restore accessed files
# - Uses the decision engine (should_scan_volume)
# - If scanning is needed, archives cold files and restores accessed files
def scan_all_volumes_and_process():
    settings = load_settings()
    blacklist = settings.get("blacklist", [])

    db = SessionLocal()

    data = get_svm_data_volumes()
    volumes = data.get("volumes", [])

    if not volumes:
        print("No volumes detected.")
        return

    print(f"Detected {len(volumes)} volumes to evaluate.\n")

    for volume_info in volumes:
        share_name = volume_info.get("share_name")

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
            print(f"Reason: {result.get('reason', 'Feature vector analysis')}\n")

            if result["should_scan"]:
                cold_files = result.get("cold_files", [])
                recently_accessed_files = result.get("recently_accessed_files", [])

                if cold_files:
                    print(f"Archiving {len(cold_files)} cold files from {share_name}...")
                    move_files_and_commit(cold_files, db)

                if recently_accessed_files:
                    print(f"Restoring {len(recently_accessed_files)} accessed files from {share_name}...")
                    bulk_restore_files(recently_accessed_files, db)

            print("\n------------------------------------------------------------\n")

        except Exception as e:
            print(f"Error processing volume {share_name}: {e}")
            import traceback
            traceback.print_exc()

    db.close()

