"""Progress bar factories for long-running operations."""

from rich.progress import (
    Progress,
    SpinnerColumn,
    TextColumn,
    BarColumn,
    TaskProgressColumn,
    TimeRemainingColumn,
    MofNCompleteColumn,
)
from .console import console


def create_extraction_progress() -> Progress:
    """Progress bar for paginated API extraction (pages)."""
    return Progress(
        SpinnerColumn(),
        TextColumn("[bold]{task.description}"),
        BarColumn(bar_width=30),
        TaskProgressColumn(),
        MofNCompleteColumn(),
        TimeRemainingColumn(),
        console=console,
        transient=False,
    )


def create_enrichment_progress() -> Progress:
    """Progress bar for profile enrichment (profiles)."""
    return Progress(
        SpinnerColumn(),
        TextColumn("[bold]{task.description}"),
        BarColumn(bar_width=30),
        TaskProgressColumn(),
        MofNCompleteColumn(),
        TimeRemainingColumn(),
        console=console,
        transient=False,
    )


def create_scoring_progress() -> Progress:
    """Progress bar for AI scoring (batches)."""
    return Progress(
        SpinnerColumn(),
        TextColumn("[bold]{task.description}"),
        BarColumn(bar_width=25),
        TaskProgressColumn(),
        MofNCompleteColumn(),
        TextColumn("[dim]{task.fields[cost]}[/dim]"),
        TimeRemainingColumn(),
        console=console,
        transient=False,
    )


def create_action_progress() -> Progress:
    """Progress bar for cleanup actions (remove/withdraw/unfollow)."""
    return Progress(
        SpinnerColumn(),
        TextColumn("[bold]{task.description}"),
        BarColumn(bar_width=30),
        MofNCompleteColumn(),
        TextColumn("[dim]{task.fields[current]}[/dim]"),
        console=console,
        transient=False,
    )


def create_simple_progress(description: str = "Working") -> Progress:
    """Minimal spinner + description for short operations."""
    return Progress(
        SpinnerColumn(),
        TextColumn(f"[bold]{description}..."),
        console=console,
        transient=True,
    )
