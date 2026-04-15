"""
Reusable profile enrichment with auto-scaling concurrency.
Extracts full LinkedIn profile data (including actual job_title from experiences)
using the extract-people skill in managed mode.

Concurrency is auto-calculated from the user's Edges plan tier (via /v1/workspaces),
or can be overridden with max_workers.

Usage:
    from linkedin_network_cleaner.core.enrich_profiles import enrich_profiles
    results, meta = enrich_profiles(client, profile_urls)
    results, meta = enrich_profiles(client, profile_urls, max_workers=1)  # force sequential
"""

import csv
import json
import glob as globmod
import logging
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

from . import config
from .edges_client import compute_max_workers

logger = logging.getLogger(__name__)

DEFAULT_CHECKPOINT_INTERVAL = 200
PROGRESS_LOG_INTERVAL = 50


def enrich_profiles(client, profile_urls, enrichment_params=None,
                    checkpoint_name="enrichment", checkpoint_interval=DEFAULT_CHECKPOINT_INTERVAL,
                    resume=False, save=True, max_workers=None, progress_callback=None):
    """
    Enrich a list of LinkedIn profile URLs via extract-people (managed mode).

    Args:
        client: EdgesClient instance
        profile_urls: list of LinkedIn profile URLs to enrich
        enrichment_params: dict of enrichment flags (experiences, skills, sections, highlight).
                          Defaults to all four enabled.
        checkpoint_name: name prefix for checkpoint files
        checkpoint_interval: save checkpoint every N profiles
        resume: if True, load latest checkpoint and skip already-done URLs
        save: if True, save final results to extracts/
        max_workers: override concurrent workers (None = auto-detect from plan)

    Returns:
        (results, metadata) -- results is list of enriched profile dicts
    """
    if enrichment_params is None:
        enrichment_params = {
            "experiences": True,
            "skills": True,
            "sections": True,
            "highlight": True,
        }

    config.ensure_dirs()

    # Resume from checkpoint
    results = []
    done_urls = set()
    if resume:
        checkpoint = _load_checkpoint(checkpoint_name)
        if checkpoint:
            results = checkpoint.get("data", [])
            # Backwards-compatible: old checkpoints may not have done_urls
            if "done_urls" in checkpoint:
                done_urls = set(checkpoint["done_urls"])
            else:
                # Fall back to index-based skip
                skip_index = checkpoint.get("index", 0)
                done_urls = set(profile_urls[:skip_index])
            logger.info("Resuming %s: %d already done, %d results loaded",
                        checkpoint_name, len(done_urls), len(results))

    # Filter remaining URLs
    remaining = [u for u in profile_urls if u and u not in done_urls]
    total = len(profile_urls)
    logger.info("%d remaining out of %d total profiles", len(remaining), total)

    if not remaining:
        logger.info("Nothing to enrich — all profiles already done")
        meta = _build_meta(profile_urls, results, [], False, enrichment_params, max_workers or 0)
        if save:
            _save_results(checkpoint_name, results, meta)
        return results, meta

    # Auto-detect workers
    workers = _resolve_workers(client, max_workers, len(remaining))

    # Shared state
    errors = []
    limit_reached = False
    lock = threading.Lock()
    stop_event = threading.Event()
    completed_count = [0]  # mutable counter for progress

    if workers == 1:
        # Sequential path — zero threading overhead
        limit_reached = _enrich_sequential(
            client, remaining, enrichment_params, results, errors, done_urls,
            checkpoint_name, checkpoint_interval, total, completed_count,
            progress_callback=progress_callback,
        )
    else:
        # Concurrent path
        limit_reached = _enrich_concurrent(
            client, remaining, enrichment_params, results, errors, done_urls,
            checkpoint_name, checkpoint_interval, total, completed_count,
            workers, lock, stop_event,
            progress_callback=progress_callback,
        )

    meta = _build_meta(profile_urls, results, errors, limit_reached, enrichment_params, workers)

    if save:
        _save_results(checkpoint_name, results, meta)

    return results, meta


# ── Sequential enrichment ──────────────────────────────────────────────────

def _enrich_sequential(client, remaining, params, results, errors, done_urls,
                       cp_name, cp_interval, total, completed_count,
                       progress_callback=None):
    """Enrich profiles one by one. Returns limit_reached bool."""
    for url in remaining:
        profile, error = _enrich_single(client, url, params)

        if error is not None:
            error_label = error.get("error_label", "UNKNOWN")
            errors.append({"url": url, "error": error})

            if error_label == "LIMIT_REACHED":
                logger.warning("LIMIT_REACHED — saving progress")
                _save_checkpoint(cp_name, results, done_urls)
                return True

            logger.warning("Error enriching %s: %s", url, error_label)
            continue

        if profile:
            results.append(profile)
        done_urls.add(url)
        completed_count[0] += 1

        if progress_callback:
            progress_callback(len(done_urls), total, "")

        # Progress
        done_total = len(done_urls)
        if completed_count[0] % PROGRESS_LOG_INTERVAL == 0:
            pct = done_total / total * 100
            logger.info("Enriched %d/%d (%.1f%%) — 1 worker", done_total, total, pct)

        # Checkpoint
        if completed_count[0] % cp_interval == 0:
            _save_checkpoint(cp_name, results, done_urls)

    return False


# ── Concurrent enrichment ──────────────────────────────────────────────────

def _enrich_concurrent(client, remaining, params, results, errors, done_urls,
                       cp_name, cp_interval, total, completed_count,
                       workers, lock, stop_event, progress_callback=None):
    """Enrich profiles with ThreadPoolExecutor. Returns limit_reached bool."""
    limit_reached = False

    with ThreadPoolExecutor(max_workers=workers) as executor:
        future_to_url = {
            executor.submit(_enrich_single, client, url, params, stop_event): url
            for url in remaining
        }

        for future in as_completed(future_to_url):
            if stop_event.is_set():
                break

            url = future_to_url[future]
            try:
                profile, error = future.result()
            except Exception as exc:
                with lock:
                    errors.append({"url": url, "error": {"error_label": "EXCEPTION", "message": str(exc)}})
                logger.warning("Exception enriching %s: %s", url, exc)
                continue

            if error is not None:
                error_label = error.get("error_label", "UNKNOWN")
                with lock:
                    errors.append({"url": url, "error": error})

                if error_label == "LIMIT_REACHED":
                    logger.warning("LIMIT_REACHED — stopping all workers")
                    stop_event.set()
                    limit_reached = True
                    # Cancel pending futures
                    for f in future_to_url:
                        f.cancel()
                    break

                logger.warning("Error enriching %s: %s", url, error_label)
                continue

            with lock:
                if profile:
                    results.append(profile)
                done_urls.add(url)
                completed_count[0] += 1

                if progress_callback:
                    progress_callback(len(done_urls), total, "")

                # Progress
                done_total = len(done_urls)
                if completed_count[0] % PROGRESS_LOG_INTERVAL == 0:
                    pct = done_total / total * 100
                    logger.info("Enriched %d/%d (%.1f%%) — %d workers",
                                done_total, total, pct, workers)

                # Checkpoint
                if completed_count[0] % cp_interval == 0:
                    _save_checkpoint(cp_name, results, done_urls)

    # Final checkpoint on limit_reached
    if limit_reached:
        with lock:
            _save_checkpoint(cp_name, results, done_urls)

    return limit_reached


# ── Single profile enrichment ─────────────────────────────────────────────

def _enrich_single(client, url, params, stop_event=None):
    """
    Enrich one profile. Returns (profile_dict, error_dict).
    If stop_event is set, returns (None, None) immediately.
    """
    if stop_event and stop_event.is_set():
        return None, None

    data, _, error = client.call_action(
        "extract-people",
        input_data={"url": url},
        direct_mode=False,
        parameters=params,
    )

    if error is not None:
        return None, error

    if data is None:
        return None, None

    # extract-people returns a single dict
    profile = data if isinstance(data, dict) else data[0] if isinstance(data, list) and data else None
    if profile:
        # Extract actual job title from current experience
        experiences = profile.get("experiences", [])
        if experiences and isinstance(experiences, list):
            current_job = _extract_current_job(experiences)
            if current_job:
                profile["current_job_title"] = current_job.get("title", "")
                profile["current_company"] = current_job.get("company_name", "")
        profile["_source_url"] = url

    return profile, None


# ── Worker resolution ─────────────────────────────────────────────────────

def _resolve_workers(client, max_workers, num_profiles):
    """Determine number of concurrent workers."""
    if max_workers is not None:
        logger.info("Using user-specified workers: %d", max_workers)
        return max(1, max_workers)

    # Auto-detect from plan
    ws = client.get_workspace_info()
    if ws is None:
        logger.warning("Could not fetch workspace info — defaulting to 3 workers")
        return min(3, max(1, num_profiles // 100))

    # workspace response may be a list or dict
    if isinstance(ws, list) and ws:
        ws = ws[0]

    credits = (ws.get("credits_max") or ws.get("credits") or 0) if isinstance(ws, dict) else 0
    workers = compute_max_workers(credits, num_profiles)
    logger.info("Auto-detected plan: %d credits → %d workers (for %d profiles)",
                credits, workers, num_profiles)
    return workers


# ── Helpers ────────────────────────────────────────────────────────────────

def _extract_current_job(experiences):
    """Find the current/most recent job from an experiences list."""
    for exp in experiences:
        if not isinstance(exp, dict):
            continue
        end_date = exp.get("date_end", "") or ""
        is_current = exp.get("is_current")
        if is_current or "present" in end_date.lower() or "current" in end_date.lower() or not end_date:
            return exp
    if experiences and isinstance(experiences[0], dict):
        return experiences[0]
    return None


def _build_meta(profile_urls, results, errors, limit_reached, enrichment_params, workers):
    """Build the metadata dict returned alongside results."""
    return {
        "total_requested": len(profile_urls),
        "total_enriched": len(results),
        "total_errors": len(errors),
        "errors": errors,
        "limit_reached": limit_reached,
        "enrichment_params": enrichment_params,
        "workers": workers,
    }


# ── Save / Checkpoint ─────────────────────────────────────────────────────

def _save_results(name, data, meta):
    """Save enrichment results as JSON + CSV."""
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

    # JSON
    json_file = config.EXTRACTS_DIR / f"{name}_{ts}.json"
    payload = {
        "extract_name": name,
        "timestamp": ts,
        "record_count": len(data),
        "metadata": meta,
        "data": data,
    }
    json_file.write_text(json.dumps(payload, default=str, ensure_ascii=False), encoding="utf-8")
    logger.info("Saved %s → %s (%d records)", name, json_file.name, len(data))

    # CSV
    if data and isinstance(data[0], dict):
        csv_file = config.EXTRACTS_DIR / f"{name}_{ts}.csv"
        fieldnames = list(dict.fromkeys(k for row in data for k in row.keys()))
        with open(csv_file, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(data)
        logger.info("Saved %s → %s", name, csv_file.name)

    # Action log
    log_entry = {
        "action": "enrich",
        "extract_name": name,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "record_count": len(data),
        "metadata": meta,
    }
    log_file = config.ACTIONS_LOG_DIR / f"enrich_{name}_{ts}.json"
    log_file.write_text(json.dumps(log_entry, default=str, ensure_ascii=False), encoding="utf-8")


def _save_checkpoint(name, data, done_urls):
    """Save checkpoint with URL-based tracking for resume."""
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    filepath = config.EXTRACTS_DIR / f"{name}_checkpoint_{ts}.json"
    payload = {
        "checkpoint_name": name,
        "timestamp": ts,
        "index": len(done_urls),
        "done_urls": list(done_urls),
        "data": data,
    }
    filepath.write_text(json.dumps(payload, default=str, ensure_ascii=False), encoding="utf-8")
    logger.info("Checkpoint saved: %d done → %s", len(done_urls), filepath.name)


def _load_checkpoint(name):
    """Load the most recent checkpoint."""
    pattern = str(config.EXTRACTS_DIR / f"{name}_checkpoint_*.json")
    files = sorted(globmod.glob(pattern))
    if not files:
        return None
    latest = files[-1]
    logger.info("Loading checkpoint: %s", Path(latest).name)
    with open(latest, encoding="utf-8") as f:
        return json.load(f)
