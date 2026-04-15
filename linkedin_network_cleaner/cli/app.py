"""LinkedIn Network Cleaner — CLI entry point."""

import typer

from .commands.init_cmd import init_command
from .commands.extract import extract_command
from .commands.analyze import analyze_command
from .commands.clean import clean_app
from .commands.status import status_command
from .commands.doctor import doctor_command

app = typer.Typer(
    name="linkedin-cleaner",
    help="Extract, score, and clean your LinkedIn network using AI.",
    rich_markup_mode="rich",
    no_args_is_help=False,
)

app.command(name="init")(init_command)
app.command(name="extract")(extract_command)
app.command(name="analyze")(analyze_command)
app.add_typer(clean_app, name="clean", help="Clean your network — withdraw invites, unfollow, manage connections.")
app.command(name="status")(status_command)
app.command(name="doctor")(doctor_command)


@app.callback(invoke_without_command=True)
def main_callback(ctx: typer.Context):
    """LinkedIn Network Cleaner — Clean your network. Keep your people."""
    if ctx.invoked_subcommand is None:
        from .ui import console, theme
        console.print()
        console.print(f"  [bold {theme.BRAND_GREEN}]linkedin-network-cleaner[/bold {theme.BRAND_GREEN}]  [dim]v{theme.APP_VERSION}[/dim]")
        console.print(f"  [dim]{theme.APP_TAGLINE}[/dim]")
        console.print()
        console.print(f"  [{theme.ACCENT}]Commands:[/{theme.ACCENT}]")
        console.print(f"    [bold]init[/bold]      Set up your workspace")
        console.print(f"    [bold]extract[/bold]   Pull your LinkedIn data")
        console.print(f"    [bold]analyze[/bold]   Score every connection")
        console.print(f"    [bold]clean[/bold]     Preview & execute cleanup")
        console.print(f"    [bold]status[/bold]    See where you are")
        console.print(f"    [bold]doctor[/bold]    Check your setup")
        console.print()
        console.print(f"  [dim]Run linkedin-cleaner <command> --help for details[/dim]")
        console.print()


@app.command(name="roast", hidden=True)
def roast_command():
    """The legendary --roast-my-network command."""
    from .ui import console, theme
    console.print()
    console.print(f"  [{theme.BRAND_GREEN}]~ ❯ linkedin-cleaner --roast-my-network[/{theme.BRAND_GREEN}]")
    console.print()
    console.print(f"  [{theme.BRAND_AMBER}][warn] This is going to be awkward for some of you.[/{theme.BRAND_AMBER}]")
    console.print()
    from .commands.clean import clean_connections
    clean_connections(
        dry_run=True, export=False, execute=False,
        ai_threshold=None, batch_size=None, delay_opt=None, review_file=None,
    )


def main():
    app()
