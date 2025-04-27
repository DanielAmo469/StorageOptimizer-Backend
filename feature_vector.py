

from netapp_btc import get_svm_data_volumes, get_volume_name_by_share
from netapp_volume_stats import get_cold_old_days, get_recently_accessed_archive_files, get_total_archived_files_from_logs, get_total_unique_archived_files, get_volume_performance_by_share, get_volume_space_metrics, is_in_cooldown, scan_volume_with_stats
from services import get_settings_for_mode, load_settings
from sqlalchemy.orm import Session


# Build a feature vector for a scanned volume to evaluate whether it should be scanned:
# - Includes cold file ratio, old file ratio, blacklist ratio, size-to-access ratio
# - Integrates system performance metrics: IOPS, latency, fullness
# - Dynamically adjusts weights based on the mode (eco, default, super)
# - Returns: scan_score, detailed feature_weights, and raw extracted stats
def build_feature_vector(stats: dict, mode: str = "default") -> dict:
    settings = load_settings()
    mode_settings = settings.get("modes", {}).get(mode, {})
    weights = mode_settings.get("weights", {})
    thresholds = mode_settings.get("thresholds", {})

    # Extract values directly from scan_volume_with_stats
    file_count = stats.get("total_file_count", 0)
    total_size = stats.get("total_file_size", 0)
    old_file_count = stats.get("old_file_count", 0)
    cold_file_count = len(stats.get("cold_files", []))
    blacklist_ratio = stats.get("blacklist_ratio", 0)
    fullness_percent = stats.get("fullness_percent", 0)
    iops = stats.get("iops", 0)
    latency = stats.get("latency", 0)
    files_restorable = stats.get("restorable_file_count", 0)

    # Get cold and old file age thresholds based on current mode
    min_cold_file_age_days, min_old_file_age_days = get_cold_old_days(settings, mode)

    # Calculate individual feature scores (raw feature values)
    small_volume_score = 0 if total_size < thresholds.get("small_volume_threshold_gb", 1) * 1024**3 else 1 
    iops_score = 1 - min(iops / thresholds.get("iops_idle_threshold", 10), 1) if isinstance(iops, (int, float)) else 0
    latency_score = 1 - min(latency / thresholds.get("latency_idle_threshold_ms", 5), 1) if isinstance(latency, (int, float)) else 0
    fullness_score = min(fullness_percent / 100, 1) if isinstance(fullness_percent, (int, float)) else 0
    cold_ratio_score = cold_file_count / file_count if file_count else 0
    old_ratio_score = old_file_count / file_count if file_count else 0
    blacklist_score = min(blacklist_ratio / 100, 1)
    restore_score = 1 - min(files_restorable / file_count, 1) if file_count else 1
    ratio_score = stats.get("size_access_ratio_score", 0.5)

    # Raw scores (no weights applied yet)
    raw_scores = {
        "small_volume": small_volume_score,
        "iops": iops_score,
        "latency": latency_score,
        "fullness": fullness_score,
        "cold_ratio": cold_ratio_score,
        "old_ratio": old_ratio_score,
        "blacklist": blacklist_score,
        "restore": restore_score,
        "size_access_ratio": ratio_score
    }

    # Calculate the weighted contribution for each feature
    weighted_scores = {
        "small_volume": weights.get("small_volume_weight", 0) * small_volume_score,
        "iops": weights.get("iops_weight", 0) * iops_score,
        "latency": weights.get("latency_weight", 0) * latency_score,
        "fullness": weights.get("fullness_weight", 0) * fullness_score,
        "cold_ratio": weights.get("cold_file_ratio_weight", 0) * cold_ratio_score,
        "old_ratio": weights.get("old_file_ratio_weight", 0) * old_ratio_score,
        "blacklist": weights.get("blacklist_file_ratio_weight", 0) * blacklist_score,
        "restore": weights.get("restore_pressure_weight", 0) * restore_score,
        "size_access_ratio": weights.get("size_access_ratio_weight", 0) * ratio_score
    }

    score = round(sum(weighted_scores.values()), 4)

    return {
        "score": round(score, 4),
        "raw_scores": raw_scores,
        "weighted_scores": weighted_scores
    }




# Decide whether a volume should be scanned based on its feature vector score
# - Loads settings for the active mode
# - Uses feature vector score and threshold to decide
# - Returns a structured decision (True/False with explanation)
# Evaluate whether a volume should be scanned based on all metrics and mode weights
def should_scan_volume(share_name: str, volume: dict, settings: dict, blacklist: list[str], db: Session, override_mode: str = None):
    mode = override_mode or settings["mode"]
    thresholds = settings["modes"][mode]["thresholds"]

    cooldown_hours = thresholds.get("min_hours_between_scans", 6)
    if is_in_cooldown(db, share_name, cooldown_hours):
        return {
            "should_scan": False,
            "reason": "In cooldown window",
            "score": 0.0
        }

    try:
        scan_stats = scan_volume_with_stats(share_name, volume, blacklist, settings)
    except Exception as e:
        print(f"Error in scan_volume_with_stats for '{share_name}': {e}")
        return {
            "should_scan": False,
            "reason": f"Scan failed: {e}",
            "score": 0.0
        }

    if not scan_stats:
        return {
            "should_scan": False,
            "reason": "No scan stats returned",
            "score": 0.0
        }

    # NetApp metrics
    perf = get_volume_performance_by_share(share_name)
    if isinstance(perf, list) and perf:
        perf = perf[0]
    elif isinstance(perf, list) and not perf:
        perf = {}

    scan_stats["iops"] = perf.get("iops", 0)
    scan_stats["latency"] = perf.get("latency", 0)

    # Archive history
    scan_stats["archived_total"] = get_total_archived_files_from_logs(db, share_name)
    scan_stats["archived_unique"] = get_total_unique_archived_files(db, share_name)

    # Recently accessed files
    archive_volume = volume.get("archive")
    if archive_volume:
        recently_accessed = get_recently_accessed_archive_files(archive_volume, db, settings)
        scan_stats["restorable_file_count"] = len(recently_accessed)
    else:
        recently_accessed = []
        scan_stats["restorable_file_count"] = 0

    # Feature vector
    vector_result = build_feature_vector(scan_stats, mode)
    score = vector_result["score"]
    threshold = thresholds.get("scan_score_threshold", 0.5)

    return {
        "should_scan": score >= threshold,
        "score": score,
        "vector_details_raw": vector_result.get("raw_scores", {}),
        "vector_details_weighted": vector_result.get("weighted_scores", {}),
        "cold_files": scan_stats.get("cold_files", []), #Return also all the cold files for archiving if the desicion is true
        "recently_accessed_files": recently_accessed #Return also all the files that needs to be restored if the decision is true
    }

from database import SessionLocal


def check():
    settings = load_settings()
    print("Loaded settings:", settings)
    blacklist = settings.get("blacklist", [])
    modes = settings.get("modes", {})

    print(f"System Mode: {settings['mode']}")
    print("Starting scan decision test...\n")

    data = get_svm_data_volumes()
    volumes = data.get("volumes", [])

    if not volumes:
        print("No volumes found from get_svm_data_volumes().")
        return

    print("Volumes detected:")
    for v in volumes:
        print(f" - {v.get('share_name')} | volume: {v.get('volume')}")

    first_volume = volumes[0]
    share_name = first_volume["share_name"]
    print(f"\nRunning scan decision test on: {share_name}\n")

    db = SessionLocal()

    # Save results first
    results_by_mode = {}

    for mode_name in ["default", "eco", "super"]:
        if mode_name not in modes:
            print(f"Mode '{mode_name}' not found in settings.json.\n")
            continue

        try:
            fresh_settings = load_settings()
            fresh_settings["mode"] = mode_name

            result = should_scan_volume(
                share_name=share_name,
                volume=first_volume,
                settings=fresh_settings,
                blacklist=blacklist,
                db=db,
                override_mode=mode_name
            )

            # Save the result for later printing
            results_by_mode[mode_name] = result

        except Exception as e:
            print(f"Error in mode {mode_name}: {e}")
            import traceback
            traceback.print_exc()

    db.close()

    for mode_name, result in results_by_mode.items():
        print(f"=== Mode: {mode_name.upper()} ===")
        print(f"Should Scan: {result['should_scan']}")
        print(f"Score: {result['score']}")
        print(f"Reason: {result.get('reason', 'Feature vector analysis')}\n")

        print("Raw Feature Scores:")
        for k, v in result.get("vector_details_raw", {}).items():
            print(f"  {k}: {v}")

        print("\nWeighted Feature Scores:")
        for k, v in result.get("vector_details_weighted", {}).items():
            print(f"  {k}: {v}")

        print(f"\nCold files to archive: {len(result.get('cold_files', []))}")
        print(f"Files to restore: {len(result.get('recently_accessed_files', []))}")
        print("\n" + "-" * 60 + "\n")


if __name__ == "__main__":
    check()
