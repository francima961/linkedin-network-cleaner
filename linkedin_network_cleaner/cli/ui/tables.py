"""Reusable Rich table formatters."""

from rich.table import Table
from rich import box
from . import theme


def make_status_table(title: str, columns: list[tuple[str, str]], rows: list[list[str]]) -> Table:
    """
    Generic status table with consistent styling.

    Args:
        title: Table title
        columns: List of (name, justify) tuples
        rows: List of row data (list of strings)
    """
    table = Table(
        title=f"  {title}",
        box=box.SIMPLE,
        show_header=True,
        header_style="bold dim",
        padding=(0, 2),
        title_justify="left",
    )
    for col_name, justify in columns:
        table.add_column(col_name, justify=justify)
    for row in rows:
        table.add_row(*row)
    return table


def make_extract_status_table(extracts_info: list[dict]) -> Table:
    """
    Table for `status` command showing extraction state.

    Each dict: {"name": str, "records": int|str, "last_run": str}
    """
    table = Table(
        box=box.SIMPLE,
        show_header=True,
        header_style="bold dim",
        padding=(0, 2),
    )
    table.add_column("EXTRACT", style="bold", min_width=25)
    table.add_column("RECORDS", justify="right")
    table.add_column("LAST RUN", justify="right", style="dim")

    for info in extracts_info:
        records = str(info.get("records", "-"))
        if records != "-":
            records = f"{int(records):,}"
        last_run = info.get("last_run", "never")
        style = "dim" if last_run == "never" else ""
        table.add_row(info["name"], records, last_run, style=style)

    return table


def make_pipeline_status_table(steps_info: list[dict]) -> Table:
    """
    Table for `status` command showing pipeline state.

    Each dict: {"step": int, "name": str, "status": str, "rows": int|str, "notes": str}
    """
    table = Table(
        box=box.SIMPLE,
        show_header=True,
        header_style="bold dim",
        padding=(0, 2),
    )
    table.add_column("ANALYZE", style="bold", min_width=30)
    table.add_column("STATUS", justify="center")
    table.add_column("ROWS", justify="right")
    table.add_column("NOTES", style="dim")

    status_styles = {
        "done": f"[{theme.SUCCESS}]done[/{theme.SUCCESS}]",
        "partial": f"[{theme.WARNING}]partial[/{theme.WARNING}]",
        "pending": f"[{theme.DIM}]pending[/{theme.DIM}]",
        "skipped": f"[{theme.DIM}]skipped[/{theme.DIM}]",
    }

    for info in steps_info:
        name = f"{info['step']}. {info['name']}"
        status = status_styles.get(info.get("status", "pending"), info.get("status", ""))
        rows = f"{int(info['rows']):,}" if info.get("rows") else ""
        notes = info.get("notes", "")
        table.add_row(name, status, rows, notes)

    return table


def make_cleanup_preview_table(
    decisions: dict[str, int],
    total: int,
    sample_rows: list[dict] | None = None,
) -> Table:
    """
    Table for `clean --dry-run` showing decision breakdown.

    Args:
        decisions: {"keep": N, "remove": N, "review": N}
        total: Total connections
        sample_rows: Optional list of dicts with name, title, score, reason
    """
    # Decision breakdown
    table = Table(
        title="  Decision Breakdown",
        box=box.SIMPLE,
        show_header=True,
        header_style="bold dim",
        padding=(0, 2),
        title_justify="left",
    )
    table.add_column("DECISION", style="bold", min_width=15)
    table.add_column("COUNT", justify="right")
    table.add_column("%", justify="right")
    table.add_column("DESCRIPTION", style="dim")

    descriptions = {
        "keep": "Customers, targets, high AI score",
        "remove": "Low relevance, no engagement",
        "review": "Ambiguous — needs your judgment",
    }

    for decision, count in sorted(decisions.items()):
        pct = f"{count / total * 100:.1f}%" if total else "0%"
        desc = descriptions.get(decision, "")
        style = ""
        if decision == "remove":
            decision_display = theme.TAG_REMOVE.replace("\\[", "[")
        elif decision == "keep":
            decision_display = theme.TAG_KEEP.replace("\\[", "[")
        elif decision == "review":
            decision_display = theme.TAG_REVIEW.replace("\\[", "[")
        else:
            decision_display = decision.title()
        table.add_row(decision_display, f"{count:,}", pct, desc, style=style)

    table.add_row("", "", "", "", end_section=True)
    table.add_row("[bold]Total[/bold]", f"[bold]{total:,}[/bold]", "", "")

    return table


def make_sample_table(rows: list[dict], title: str = "Sample") -> Table:
    """Table showing sample rows with name, title, score, reason."""
    table = Table(
        title=f"  {title}",
        box=box.SIMPLE,
        show_header=True,
        header_style="bold dim",
        padding=(0, 1),
        title_justify="left",
    )
    table.add_column("", width=8)  # tag column
    table.add_column("Name", min_width=20)
    table.add_column("Title", min_width=25)
    table.add_column("Score", justify="right")
    table.add_column("Reason", style="dim", max_width=35)

    for row in rows:
        decision = row.get("decision", "")
        if decision == "keep":
            tag = theme.TAG_KEEP.replace("\\[", "[")
        elif decision == "remove":
            tag = theme.TAG_REMOVE.replace("\\[", "[")
        elif decision in ("review", "withdraw", "withdraw_and_tag"):
            tag = theme.TAG_REVIEW.replace("\\[", "[")
        else:
            tag = ""
        name = str(row.get("full_name", "Unknown"))[:25]
        title_text = str(row.get("current_job_title", row.get("headline", "")))[:30]
        score = str(row.get("ai_audience_fit", ""))
        reason = str(row.get("decision_reason", row.get("ai_reasoning", "")))[:35]
        table.add_row(tag, name, title_text, score, reason)

    return table


def make_summary_table(title: str, data: dict) -> Table:
    """Key-value summary table for completion screens."""
    table = Table(
        title=f"  {title}",
        box=box.SIMPLE,
        show_header=False,
        padding=(0, 2),
        title_justify="left",
    )
    table.add_column("Metric", style="bold", min_width=25)
    table.add_column("Value", justify="right")

    for key, value in data.items():
        if isinstance(value, int):
            value = f"{value:,}"
        table.add_row(key, str(value))

    return table
