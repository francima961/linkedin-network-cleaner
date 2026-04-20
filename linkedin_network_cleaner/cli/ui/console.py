"""Shared Rich console and display helpers — vintage 70s retro terminal."""

from rich.console import Console

from . import theme

# Single shared console instance for consistent output
console = Console()


def _pad_line(content: str, width: int = 59) -> str:
    """Pad a line to fit inside the banner frame."""
    # Strip Rich markup for length calculation (rough)
    visible = content
    padding = max(width - len(visible), 0)
    return content + " " * padding


def print_banner() -> None:
    """Print the compact gradient punch banner."""
    W = theme.BRAND_WHITE
    G = theme.BRAND_GREEN
    D = theme.BRAND_DIM

    console.print()
    console.print(f"  [{W}]{theme.BANNER_TOP}[/{W}]")
    console.print(f"  [{W}]{theme.BANNER_SIDE_L}[/{W}]{'':59s}[{W}]{theme.BANNER_SIDE_R}[/{W}]")
    for line in theme.BANNER_LOGO:
        console.print(f"  [{W}]{theme.BANNER_SIDE_L}[/{W}][bold {G}]{line}[/bold {G}][{W}]{theme.BANNER_SIDE_R}[/{W}]")
    console.print(f"  [{W}]{theme.BANNER_SIDE_L}[/{W}]{'':59s}[{W}]{theme.BANNER_SIDE_R}[/{W}]")
    console.print(f"  [{W}]{theme.BANNER_SIDE_L}[/{W}]   [{W}]N E T W O R K    C L E A N E R[/{W}]    [{D}]v{theme.APP_VERSION}[/{D}]            [{W}]{theme.BANNER_SIDE_R}[/{W}]")
    console.print(f"  [{W}]{theme.BANNER_SIDE_L}[/{W}]   [{D}]{theme.APP_TAGLINE}[/{D}]                 [{W}]{theme.BANNER_SIDE_R}[/{W}]")
    console.print(f"  [{W}]{theme.BANNER_SIDE_L}[/{W}]{'':59s}[{W}]{theme.BANNER_SIDE_R}[/{W}]")
    console.print(f"  [{W}]{theme.BANNER_BOT}[/{W}]")
    console.print()


def print_section(title: str, right: str = "") -> None:
    """Print a section header: ░▒▓ Title ▓▒░ with heavy underline."""
    G = theme.BRAND_GREEN
    A = theme.BRAND_AMBER

    right_str = f"  [{A}]{right}[/{A}]" if right else ""

    console.print()
    console.print(f"  [{G}]░▒▓[/{G}] [bold {G}]{title}[/bold {G}] [{G}]▓▒░[/{G}]{right_str}")
    console.print(f"  [{G}]━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━[/{G}]")
    console.print()


def print_header(title: str) -> None:
    """Print a simpler section header for mid-flow use."""
    print_section(title)


def print_subheader(title: str) -> None:
    """Print a smaller subsection header."""
    console.print(f"\n  [bold dim]{title}[/bold dim]")


def print_divider() -> None:
    """Print a horizontal divider."""
    console.print(f"  [{theme.BRAND_DIM}]{theme.DIVIDER_LIGHT}[/{theme.BRAND_DIM}]")


def print_dot_divider() -> None:
    """Print a dotted divider for lighter separation."""
    console.print(f"  [{theme.BRAND_DIM}]{theme.DIVIDER_DOT}[/{theme.BRAND_DIM}]")


def print_tree(label: str, is_last: bool = False, indent: int = 0) -> None:
    """Print a tree branch line."""
    D = theme.BRAND_DIM
    prefix = "  " * indent
    branch = "└──" if is_last else "├──"
    console.print(f"  {prefix}[{D}]{branch}[/{D}] {label}")


def print_tree_item(label: str, indent: int = 1) -> None:
    """Print a tree continuation line with content."""
    D = theme.BRAND_DIM
    prefix = "  " * indent
    console.print(f"  {prefix}[{D}]│[/{D}]   {label}")


def print_tree_pipe(indent: int = 0) -> None:
    """Print a tree continuation pipe."""
    D = theme.BRAND_DIM
    prefix = "  " * indent
    console.print(f"  {prefix}[{D}]│[/{D}]")


def print_success(message: str) -> None:
    """Print a green success message."""
    console.print(f"  [{theme.BRAND_GREEN}]✓[/{theme.BRAND_GREEN}] {message}")


def print_step(number: int, total: int, name: str) -> None:
    """Print a pipeline step header with block progress bar."""
    G = theme.BRAND_GREEN
    D = theme.BRAND_DIM
    filled = int(number / total * 20)
    remaining = 20 - filled
    bar = f"[{G}]{'█' * filled}[/{G}][{D}]{'░' * remaining}[/{D}]"
    console.print()
    console.print(f"  {bar}  [bold]Step {number}/{total}[/bold]")
    console.print(f"  [{theme.BRAND_AMBER}]{theme.BULLET}[/{theme.BRAND_AMBER}] {name}")
    console.print(f"  [{D}]{theme.DIVIDER_LIGHT}[/{D}]")


def print_wizard_step(number: int, total: int, name: str) -> None:
    """Print a wizard step with progress dots and section header style."""
    A = theme.BRAND_AMBER
    G = theme.BRAND_GREEN
    D = theme.BRAND_DIM

    dots = ""
    for i in range(1, total + 1):
        if i < number:
            dots += f"[{G}]●[/{G}] "
        elif i == number:
            dots += f"[{A}]◉[/{A}] "
        else:
            dots += f"[{D}]○[/{D}] "

    console.print()
    console.print(f"  {dots.strip()}")
    console.print(f"  [{G}]░▒▓[/{G}] [bold {G}]{name}[/bold {G}] [{G}]▓▒░[/{G}]  [{D}]{number}/{total}[/{D}]")
    console.print(f"  [{G}]━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━[/{G}]")


def print_suggested_next(command: str, description: str = "") -> None:
    """Print a suggested next command."""
    if description:
        console.print(f"  [dim]{description}[/dim]")
    console.print(f"  [{theme.ACCENT}]{theme.ARROW}  {command}[/{theme.ACCENT}]")


def print_phase(number: int, name: str, estimate: str = "") -> None:
    """Print a phase header for multi-phase operations."""
    icon = theme.PHASE_ICONS.get(number, "")
    est = f" [{theme.BRAND_DIM}]({estimate})[/{theme.BRAND_DIM}]" if estimate else ""
    console.print(f"\n  [{theme.BRAND_AMBER}]Phase {number}[/{theme.BRAND_AMBER}]: {icon} {name}{est}")


def print_bar(label: str, value: int, total: int, color: str = "", width: int = 30) -> None:
    """Print a gradient bar chart line: ░░▒▒▓▓████"""
    bar_color = color or theme.BRAND_GREEN
    D = theme.BRAND_DIM
    pct = value / total if total > 0 else 0
    filled = int(pct * width)
    if filled == 0 and value > 0:
        filled = 1

    # Build gradient: ░▒▓█ from light to heavy
    quarter = max(filled // 4, 1) if filled > 0 else 0
    light = "░" * quarter
    med = "▒" * quarter
    heavy = "▓" * quarter
    solid = "█" * max(filled - quarter * 3, 0)
    bar_str = light + med + heavy + solid
    pad = " " * max(width - len(bar_str), 0)

    pct_str = f"{pct * 100:.0f}%"
    console.print(f"  [{D}]│[/{D}]  [{bar_color}]{bar_str}[/{bar_color}]{pad}  {value:>6,}  {pct_str:>4s}  {label}")


def print_funnel(data: list[tuple[int, str, str]]) -> None:
    """Print a horizontal bar funnel chart."""
    if not data:
        return
    max_count = max(d[0] for d in data)
    max_bar_width = 35

    console.print()
    for count, label, style in data:
        bar_width = int(count / max_count * max_bar_width) if max_count > 0 else 0
        bar = "█" * bar_width + "░" * (max_bar_width - bar_width)
        console.print(f"  [{style}]{bar}[/{style}]  {count:,} {label}")
    console.print()


def print_breakdown(items: list[tuple[str, int, int]], title: str = "") -> None:
    """Print a percentage breakdown with mini bar charts."""
    if title:
        console.print(f"\n  [bold]{title}[/bold]")
        console.print(f"  [{theme.BRAND_DIM}]{theme.DIVIDER_LIGHT[:len(title)]}[/{theme.BRAND_DIM}]")

    for label, count, total in items:
        pct = count / total * 100 if total > 0 else 0
        bar_width = int(pct / 100 * 20)
        bar = "█" * max(bar_width, 1) + "░" * (20 - max(bar_width, 1))
        console.print(f"  [{theme.BRAND_DIM}]{bar}[/{theme.BRAND_DIM}] {pct:4.1f}%  {label}")


def print_sample_row(symbol: str, name: str, tag: str, detail: str) -> None:
    """Print a single sample row with keep/remove tag."""
    console.print(f"\n  {symbol} [bold]{name}[/bold] {tag}")
    console.print(f"    [dim]{detail}[/dim]")


def print_metric_line(label: str, value: str) -> None:
    """Print a single metric line for summaries."""
    console.print(f"  {label:.<30s} {value}")


def print_kv(label: str, value, width: int = 40) -> None:
    """Print a key-value pair with right-aligned value."""
    val_str = f"{value:,}" if isinstance(value, int) else str(value)
    padding = max(width - len(label), 1)
    console.print(f"  {label}{val_str:>{padding}s}")


def print_comment(text: str) -> None:
    """Print a witty comment in the brand's voice."""
    D = theme.BRAND_DIM
    console.print(f"  [{D}]│[/{D}]  [{D}]{text}[/{D}]")


def print_boxed(title: str, lines: list[str], style: str = "") -> None:
    """Print content in a simple box with title."""
    border_color = style or theme.BRAND_DIM
    width = max(len(title) + 4, max((len(l) for l in lines), default=20) + 4)
    top = f"  [{border_color}]┌{'─' * (width - 2)}┐[/{border_color}]"
    bot = f"  [{border_color}]└{'─' * (width - 2)}┘[/{border_color}]"

    console.print(top)
    console.print(f"  [{border_color}]│[/{border_color}] [bold]{title}[/bold]{' ' * (width - len(title) - 3)}[{border_color}]│[/{border_color}]")
    console.print(f"  [{border_color}]├{'─' * (width - 2)}┤[/{border_color}]")
    for line in lines:
        padding = width - len(line) - 3
        console.print(f"  [{border_color}]│[/{border_color}] {line}{' ' * max(padding, 0)}[{border_color}]│[/{border_color}]")
    console.print(bot)
