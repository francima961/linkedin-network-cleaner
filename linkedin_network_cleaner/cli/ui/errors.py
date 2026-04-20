"""Structured error, warning, and info display helpers."""

import typer
from rich.panel import Panel
from .console import console
from . import theme


def show_error(title: str, body: str, fix: str | None = None) -> None:
    """
    Display a red error panel and exit.

    Args:
        title: Short error title (e.g., "Edges API key is invalid")
        body: Explanation of what went wrong
        fix: Suggested fix command (e.g., "linkedin-cleaner init")
    """
    content = body
    if fix:
        content += f"\n\n[dim]Fix:[/dim] [{theme.BRAND_ORANGE}]{fix}[/{theme.BRAND_ORANGE}]"

    console.print()
    console.print(Panel(
        content,
        title=f"[{theme.ERROR}] ERROR [/{theme.ERROR}]  {title}",
        border_style=theme.ERROR_PANEL_STYLE,
        padding=(1, 2),
    ))
    console.print()
    raise typer.Exit(1)


def show_warning(title: str, body: str) -> None:
    """Display a yellow warning panel. Non-blocking (returns, doesn't exit)."""
    console.print()
    console.print(Panel(
        body,
        title=f"[{theme.WARNING}] WARNING [/{theme.WARNING}]  {title}",
        border_style=theme.WARNING_PANEL_STYLE,
        padding=(1, 2),
    ))


def show_info(title: str, body: str) -> None:
    """Display a blue info panel. Non-blocking."""
    console.print()
    console.print(Panel(
        body,
        title=f"[{theme.INFO}] INFO [/{theme.INFO}]  {title}",
        border_style=theme.INFO_PANEL_STYLE,
        padding=(1, 2),
    ))
