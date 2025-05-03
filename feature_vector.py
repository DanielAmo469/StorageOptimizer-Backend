

from netapp_btc import get_svm_data_volumes, get_volume_name_by_share
from netapp_volume_stats import get_cold_old_days, get_recently_accessed_archive_files, get_total_archived_files_from_logs, get_total_unique_archived_files, get_volume_performance_by_share, get_volume_space_metrics, is_in_cooldown, log_volume_evaluation, scan_volume_with_stats
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


# Decide whether a volume should be scanned based on feature vector analysis
# - Evaluates cooldown window
# - Runs scan_volume_with_stats to get cold files
# - Retrieves NetApp IOPS and latency
# - Retrieves archive history counts
# - Retrieves recently accessed archive files
# - Builds feature vector and compares to threshold
# - Returns decision and supporting data
def should_scan_volume(
    share_name: str,
    volume: dict,
    settings: dict,
    blacklist: list[str],
    db: Session,
    override_mode: str = None
):
    # Mode selection (override if provided)
    mode = override_mode or settings["mode"]
    thresholds = settings["modes"][mode]["thresholds"]

    # Cooldown check
    cooldown_hours = thresholds.get("min_hours_between_scans", 6)
    if is_in_cooldown(db, share_name, cooldown_hours):
        log_volume_evaluation(
            db=db,
            share_name=share_name,
            volume_name=volume.get("name", "unknown"),
            mode=mode,
            should_scan=False,
            score=0.0,
            reason="In cooldown window",
            raw_scores={},
            weighted_scores={},
            cold_files=0,
            restored_files=0
        )
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

    # NetApp performance metrics
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

    # Recently accessed files and full archive contents
    archive_volume_path = volume.get("archive")
    if archive_volume_path:
        archive_analysis = get_recently_accessed_archive_files(archive_volume_path, db, settings)
        restorable_files = archive_analysis["restorable_files"]
        existing_archive_files = archive_analysis["existing_archive_files"]
        scan_stats["restorable_file_count"] = len(restorable_files)
    else:
        restorable_files = []
        existing_archive_files = []
        scan_stats["restorable_file_count"] = 0

    # Feature vector scoring
    vector_result = build_feature_vector(scan_stats, mode)
    score = vector_result["score"]
    threshold = thresholds.get("scan_score_threshold", 0.5)

    volume_name = get_volume_name_by_share(share_name)
    should_scan = score >= threshold

    # Always log evaluation
    log_volume_evaluation(
        db=db,
        share_name=share_name,
        volume_name=volume_name,
        mode=mode,
        should_scan=should_scan,
        score=score,
        reason="Feature vector analysis",
        raw_scores=vector_result.get("raw_scores", {}),
        weighted_scores=vector_result.get("weighted_scores", {}),
        cold_files=len(scan_stats.get("cold_files", [])),
        restored_files=len(restorable_files)
    )

    cold_files = scan_stats.get("cold_files") or []
    restorable_files = restorable_files or []
    existing_archive_files = existing_archive_files or []

    # Return full decision
    return {
        "should_scan": should_scan,
        "score": score,
        "vector_details_raw": vector_result.get("raw_scores", {}),
        "vector_details_weighted": vector_result.get("weighted_scores", {}),
        "cold_files": cold_files,
        "restorable_files": restorable_files,
        "existing_archive_files": existing_archive_files
    }

