"""linkedin-cleaner status — Dashboard showing workspace state."""

import json
import logging
from datetime import datetime
from pathlib import Path

import typer

from ..ui import (
    console,
    print_header,
    print_suggested_next,
    show_info,
    make_extract_status_table,
    make_pipeline_status_table,
    make_summary_table,
    theme,
)
from ...core import config

logger = logging.getLogger(__name__)

# Known extract types in display order
EXTRACT_NAMES = [
    "connections",
    "followers",
    "profile_viewers",
    "conversations",
    "messages",
    "posts",
    "reaction_activity",
    "comment_activity",
    "sent_invitations",
    "enrichment",
    "post_engagement",
]

PIPELINE_STEPS = [
    (1, "Build base"),
    (2, "Analyze inbox"),
    (3, "Analyze post engagement"),
    (4, "Analyze content interactions"),
    (5, "Enrich for matching"),
    (6, "Match customers"),
    (7, "Match target accounts"),
    (8, "Match target prospects"),
    (9, "AI scoring"),
]


def status_command():
    """Show workspace dashboard — extracts, pipeline, and cleanup state."""
    # Check if workspace initialized
    env_path = config.WORKSPACE_DIR / ".env"
    if not env_path.exists():
        show_info(
            "Workspace not initialized",
            "No .env found in the current directory.\n\n"
            "Run: [cyan]linkedin-cleaner init[/cyan]",
        )
        raise typer.Exit(0)

    print_header(theme.COPY["status_header"])

    # ── Extracts ─────────────────────────────────────────────────────────
    import glob as globmod

    extracts_info = []
    has_any_extract = False

    for name in EXTRACT_NAMES:
        pattern = str(config.EXTRACTS_DIR / f"{name}_*.json")
        # Exclude checkpoints
        files = [
            f for f in sorted(globmod.glob(pattern))
            if "checkpoint" not in Path(f).name
        ]
        if files:
            has_any_extract = True
            latest = files[-1]
            try:
                with open(latest, encoding="utf-8") as f:
                    payload = json.load(f)
                record_count = payload.get("record_count", "-")
                ts = payload.get("timestamp", "")
                if ts:
                    try:
                        dt = datetime.strptime(ts, "%Y%m%d_%H%M%S")
                        last_run = dt.strftime("%b %-d, %H:%M")
                    except ValueError:
                        last_run = ts
                else:
                    last_run = "unknown"
            except Exception:
                record_count = "-"
                last_run = "error"
            extracts_info.append({"name": name, "records": record_count, "last_run": last_run})
        else:
            extracts_info.append({"name": name, "records": "-", "last_run": "never"})

    console.print(make_extract_status_table(extracts_info))

    # ── Pipeline state ───────────────────────────────────────────────────
    state_path = config.ANALYSIS_DIR / "pipeline_state.json"
    completed_steps = []
    if state_path.exists():
        try:
            state = json.loads(state_path.read_text(encoding="utf-8"))
            completed_steps = state.get("completed_steps", [])
        except Exception:
            pass

    steps_info = []
    for step_num, step_name in PIPELINE_STEPS:
        if step_num in completed_steps:
            status = "done"
            # Try to get row count from snapshot
            rows = None
            for ext in (".parquet", ".csv"):
                snapshot = config.ANALYSIS_DIR / f"pipeline_step_{step_num}{ext}"
                if snapshot.exists():
                    try:
                        if ext == ".parquet":
                            import pyarrow.parquet as pq
                            rows = pq.read_metadata(snapshot).num_rows
                        else:
                            with open(snapshot) as f:
                                rows = sum(1 for _ in f) - 1
                    except Exception:
                        pass
                    break
            steps_info.append({"step": step_num, "name": step_name, "status": status, "rows": rows, "notes": ""})
        else:
            steps_info.append({"step": step_num, "name": step_name, "status": "pending", "rows": None, "notes": ""})

    console.print()
    console.print(make_pipeline_status_table(steps_info))

    # ── Time estimate for remaining steps ────────────────────────────────
    if has_any_extract and completed_steps and max(completed_steps) < 9:
        conn_extract = next((e for e in extracts_info if e["name"] == "connections" and e["records"] != "-"), None)
        if conn_extract:
            conn_count = int(conn_extract["records"])
            remaining_steps = [s for s in range(max(completed_steps) + 1, 10)]

            est_minutes = 0
            for s in remaining_steps:
                if s <= 8:
                    est_minutes += 0.1
                else:
                    # Step 9: AI scoring
                    est_scoring = conn_count * 0.7
                    est_minutes += (est_scoring / 50 * 1.5 + est_scoring * 0.15 / 20 * 2.0) / 60

            if est_minutes > 1:
                console.print(f"\n  [dim]Estimated time to complete: ~{est_minutes:.0f} min[/dim]")

    # ── Cleanup actions ──────────────────────────────────────────────────
    action_counts = {}
    if config.ACTIONS_LOG_DIR.exists():
        for log_file in config.ACTIONS_LOG_DIR.glob("*.json"):
            action_type = log_file.stem.rsplit("_", 2)[0]
            action_counts[action_type] = action_counts.get(action_type, 0) + 1

    if action_counts:
        console.print()
        console.print(make_summary_table("Cleanup Actions Logged", action_counts))

    # ── Suggested next ───────────────────────────────────────────────────
    if not has_any_extract:
        print_suggested_next(
            "linkedin-cleaner extract --all",
            "Get started: extract your LinkedIn data",
        )
    elif not completed_steps:
        print_suggested_next(
            "linkedin-cleaner analyze",
            "Next: run the analysis pipeline",
        )
    elif max(completed_steps) < 9:
        print_suggested_next(
            "linkedin-cleaner analyze --resume",
            f"Next: continue analysis (step {max(completed_steps) + 1}/9)",
        )
    elif not action_counts:
        print_suggested_next(
            "linkedin-cleaner clean connections --dry-run",
            "Next: preview cleanup decisions",
        )
    else:
        console.print()
        console.print("  [green]✓[/green] [bold]Your network analysis is complete![/bold]")
        console.print()
