"""
Session logger — append-only markdown log for cross-session handoff.

Writes timestamped entries to logs/session_log.md so any new agent session
can read one file and understand the full history of what's been done.
"""

import logging
from datetime import datetime, timezone
from . import config

logger = logging.getLogger(__name__)


def log_session_event(event_type, summary, details=None):
    """
    Append a session event to logs/session_log.md.

    Args:
        event_type: Category — "extraction", "pipeline", "cleanup", "error"
        summary: One-line description of what happened
        details: Optional dict or list of additional context lines
    """
    config.ensure_dirs()
    log_file = config.WORKSPACE_DIR / "logs" / "session_log.md"

    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    lines = [f"### [{ts}] {event_type.upper()}", f"{summary}", ""]

    if details:
        if isinstance(details, dict):
            for k, v in details.items():
                lines.append(f"- **{k}**: {v}")
        elif isinstance(details, list):
            for item in details:
                lines.append(f"- {item}")
        lines.append("")

    lines.append("---\n")

    entry = "\n".join(lines)

    # Create file with header if it doesn't exist
    if not log_file.exists():
        header = "# Session Log\n\nAppend-only log of all agent operations. Read this to understand what's been done.\n\n---\n\n"
        log_file.write_text(header + entry, encoding="utf-8")
    else:
        with open(log_file, "a", encoding="utf-8") as f:
            f.write(entry)

    logger.debug("Session log updated: %s — %s", event_type, summary)
