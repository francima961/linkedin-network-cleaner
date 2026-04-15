"""Shared Rich console and display helpers."""

from rich.console import Console

from . import theme

# Single shared console instance for consistent output
console = Console()


def print_header(title: str) -> None:
    """Print a styled section header with underline."""
    console.print()
    console.print(f"  [bold]{title}[/bold]")
    console.print(f"  [dim]{'─' * len(title)}[/dim]")
    console.print()


def print_subheader(title: str) -> None:
    """Print a smaller subsection header."""
    console.print(f"\n  [bold dim]{title}[/bold dim]")


def print_divider() -> None:
    """Print a horizontal divider."""
    console.print(f"\n  [dim]{'─' * 50}[/dim]\n")


def print_success(message: str) -> None:
    """Print a green success message."""
    console.print(f"  [{theme.BRAND_GREEN}]✓[/{theme.BRAND_GREEN}] {message}")


def print_step(number: int, total: int, name: str) -> None:
    """Print a pipeline step header."""
    console.print()
    console.print(f"  [bold]Step {number}/{total}[/bold]  {name}")
    console.print(f"  [dim]{'─' * 50}[/dim]")


def print_suggested_next(command: str, description: str = "") -> None:
    """Print a suggested next command."""
    console.print()
    if description:
        console.print(f"  [dim]{description}[/dim]")
    console.print(f"  [{theme.ACCENT}]  {command}[/{theme.ACCENT}]")
    console.print()


def print_funnel(data: list[tuple[int, str, str]]) -> None:
    """
    Print a horizontal bar funnel chart.

    Args:
        data: list of (count, label, style) tuples.
              First entry is the widest bar (total).
    """
    if not data:
        return
    max_count = max(d[0] for d in data)
    max_bar_width = 35

    console.print()
    for count, label, style in data:
        bar_width = int(count / max_count * max_bar_width) if max_count > 0 else 0
        bar = "█" * bar_width
        console.print(f"  [{style}]{bar}[/{style}]  {count:,} {label}")
    console.print()


def print_breakdown(items: list[tuple[str, int, int]], title: str = "") -> None:
    """
    Print a percentage breakdown with mini bar charts.

    Args:
        items: list of (label, count, total) tuples
        title: optional section title
    """
    if title:
        console.print(f"\n  [bold]{title}[/bold]")
        console.print(f"  [dim]{'─' * len(title)}[/dim]")

    for label, count, total in items:
        pct = count / total * 100 if total > 0 else 0
        bar_width = int(pct / 100 * 20)
        bar = "█" * max(bar_width, 1)
        console.print(f"  [{theme.BRAND_DIM}]{bar}[/{theme.BRAND_DIM}] {pct:4.1f}%  {label}")


def print_sample_row(symbol: str, name: str, tag: str, detail: str) -> None:
    """Print a single sample row with keep/remove tag."""
    console.print(f"\n  {symbol} [bold]{name}[/bold] {tag}")
    console.print(f"    [dim]{detail}[/dim]")


def print_metric_line(label: str, value: str) -> None:
    """Print a single metric line for summaries."""
    console.print(f"  {label:.<30s} {value}")
