"""linkedin-cleaner extract — Run data extractions from LinkedIn via Edges API."""

import json
import logging
import time as time_module
from pathlib import Path

import typer

from ..ui import (
    console,
    print_header,
    print_success,
    print_suggested_next,
    show_error,
    show_warning,
    make_summary_table,
    create_extraction_progress,
    theme,
)
from ...core import config
from ...core.edges_client import EdgesClient
from ...core.extractors import AudienceExtractor
from ...core.enrich_profiles import enrich_profiles

logger = logging.getLogger(__name__)


def extract_command(
    all: bool = typer.Option(False, "--all", help="Run all extractions"),
    connections: bool = typer.Option(False, help="Extract connections"),
    followers: bool = typer.Option(False, help="Extract followers"),
    profile_viewers: bool = typer.Option(False, "--profile-viewers", help="Extract profile viewers"),
    conversations: bool = typer.Option(False, help="Extract conversations"),
    messages: bool = typer.Option(False, help="Extract messages from conversations"),
    posts: bool = typer.Option(False, help="Extract posts"),
    post_engagement: bool = typer.Option(False, "--post-engagement", help="Extract post engagement"),
    reaction_activity: bool = typer.Option(False, "--reaction-activity", help="Extract reaction activity"),
    comment_activity: bool = typer.Option(False, "--comment-activity", help="Extract comment activity"),
    sent_invites: bool = typer.Option(False, "--sent-invites", help="Extract sent invitations"),
    enrichment: bool = typer.Option(False, help="Enrich connection profiles"),
    delay: float = typer.Option(None, help="Seconds between API calls"),
    workers: int = typer.Option(None, help="Concurrent enrichment workers (0=auto)"),
    resume: bool = typer.Option(False, help="Resume from checkpoint"),
):
    """Extract LinkedIn network data via the Edges API."""
    # Check if any flags given
    any_flag = (
        all or connections or followers or profile_viewers or conversations
        or messages or posts or post_engagement or reaction_activity
        or comment_activity or sent_invites or enrichment
    )
    if not any_flag:
        console.print()
        console.print("  [bold]No extraction selected.[/bold] Use one or more flags:")
        console.print()
        console.print("  [cyan]linkedin-cleaner extract --all[/cyan]          Run all extractions")
        console.print("  [cyan]linkedin-cleaner extract --connections[/cyan]   Extract connections only")
        console.print("  [cyan]linkedin-cleaner extract --followers[/cyan]     Extract followers only")
        console.print("  [cyan]linkedin-cleaner extract --sent-invites[/cyan]  Extract sent invitations")
        console.print("  [cyan]linkedin-cleaner extract --enrichment[/cyan]    Enrich connection profiles")
        console.print()
        console.print("  [dim]Run linkedin-cleaner extract --help for all options[/dim]")
        raise typer.Exit(0)

    # Validate environment
    try:
        config.validate()
    except ValueError as e:
        show_error("Missing credentials", str(e), fix="linkedin-cleaner init")

    # Load config defaults
    cfg = config.load_config()
    api_delay = delay if delay is not None else cfg["extract"]["delay"]
    enrichment_workers = workers if workers is not None else cfg["extract"]["enrichment_workers"]

    # Acquire lock
    config.acquire_lock()
    config.ensure_dirs()

    if all:
        console.print()
        console.print("  [bold]Full Extraction Plan[/bold]")
        console.print("  [dim]────────────────────[/dim]")
        console.print()
        console.print("  Phase 1: Core data [dim](~5 min)[/dim]")
        console.print("    • Connections       Your full connection list")
        console.print("    • Followers         Who follows you")
        console.print("    • Profile Viewers   Who looked at your profile")
        console.print("    • Conversations     All inbox threads")
        console.print("    • Sent Invitations  Pending connection requests")
        console.print()
        console.print("  Phase 2: Content signals [dim](~15 min)[/dim]")
        console.print("    • Posts             Your published posts")
        console.print("    • Post Engagement   Who liked, commented, reposted")
        console.print("    • Your Activity     Posts you liked/commented on")
        console.print()
        console.print("  Phase 3: Deep data [dim](~60-120 min)[/dim]")
        console.print("    • Messages          Full message history (all threads)")
        console.print("    • Enrichment        Full profile data for every connection")
        console.print()
        console.print("  [dim]Total estimated time: 1.5-2.5 hours (depends on network size)[/dim]")
        console.print("  [dim]API cost: ~$3 (Edges API credits)[/dim]")
        console.print()

        typer.prompt("  Press Enter to start (Ctrl+C to cancel)", default="")

    print_header(theme.COPY["extract_header"])

    # Create client and extractor
    client = EdgesClient(
        api_key=config.API_KEY,
        identity_uuid=config.IDENTITY_UUID,
        delay=api_delay,
    )
    extractor = AudienceExtractor(client)

    totals = {}
    profile_url = None

    def _resolve_profile_url():
        """Resolve the user's LinkedIn profile URL via the me endpoint."""
        nonlocal profile_url
        if profile_url:
            return profile_url
        console.print("  [dim]Resolving your profile URL...[/dim]")
        data, _headers, error = client.call_action("me", direct_mode=True)
        if error is not None:
            show_error(
                "Could not resolve profile URL",
                f"The 'me' endpoint returned: {error.get('error_label', 'UNKNOWN')}",
                fix="Check your EDGES_IDENTITY_UUID in .env",
            )
        if isinstance(data, list) and data:
            profile_url = data[0].get("linkedin_profile_url", "")
        elif isinstance(data, dict):
            profile_url = data.get("linkedin_profile_url", "")
        if not profile_url:
            show_error("No profile URL", "Could not find linkedin_profile_url in me response.")
        return profile_url

    def _run_extract(name, fn, *args, **kwargs):
        """Run an extraction with error handling and summary tracking."""
        console.print(f"  [bold]Extracting {name}...[/bold]")
        start = time_module.time()
        try:
            results, meta = fn(*args, **kwargs)
            elapsed = time_module.time() - start
            count = len(results) if isinstance(results, (list, dict)) else 0
            if isinstance(results, dict):
                count = sum(len(v) if isinstance(v, list) else 1 for v in results.values())
            totals[name] = count

            elapsed_str = f"{elapsed:.0f}s" if elapsed < 60 else f"{elapsed/60:.1f}m"

            limit_reached = meta.get("limit_reached", False) if meta else False
            if limit_reached:
                show_warning(
                    "API limit reached",
                    f"Extraction '{name}' hit the 24h API limit.\n"
                    f"Resume later with: [cyan]linkedin-cleaner extract --{name.replace('_', '-')} --resume[/cyan]",
                )
            else:
                print_success(f"{name}: {count:,} records [dim]({elapsed_str})[/dim]")
            return results
        except Exception as e:
            logger.exception("Extraction %s failed", name)
            console.print(f"  [red]✗[/red] {name} failed: {e}")
            return None

    # ── Phase 1: Core extractions ────────────────────────────────────────
    if all or connections:
        _run_extract("connections", extractor.extract_connections)

    if all or followers:
        _run_extract("followers", extractor.extract_followers)

    if all or profile_viewers:
        _run_extract("profile_viewers", extractor.extract_profile_viewers)

    if all or conversations:
        _run_extract("conversations", extractor.extract_conversations)

    if all or sent_invites:
        _run_extract("sent_invitations", extractor.extract_sent_invitations)

    # ── Phase 2: Profile-URL-dependent extractions ───────────────────────
    if all or posts:
        url = _resolve_profile_url()
        _run_extract("posts", extractor.extract_posts, url)

    if all or reaction_activity:
        url = _resolve_profile_url()
        _run_extract("reaction_activity", extractor.extract_reaction_activity, url)

    if all or comment_activity:
        url = _resolve_profile_url()
        _run_extract("comment_activity", extractor.extract_comment_activity, url)

    # ── Phase 3: Chain extractions ───────────────────────────────────────
    if all or messages:
        console.print("  [bold]Extracting messages (all threads)...[/bold]")
        try:
            flat_messages, meta = extractor.extract_all_messages(resume=resume)
            if flat_messages is not None:
                totals["messages"] = len(flat_messages)
                print_success(f"messages: {len(flat_messages):,} total across all threads")
            elif meta and meta.get("error"):
                console.print(f"  [yellow]!![/yellow] Messages skipped: {meta['error']}")
        except Exception as e:
            logger.exception("Message extraction failed")
            console.print(f"  [red]✗[/red] messages failed: {e}")

    if all or post_engagement:
        url = _resolve_profile_url()
        console.print("  [bold]Extracting post engagement (all posts)...[/bold]")
        try:
            engagement, meta = extractor.extract_all_post_engagement(resume=resume)
            if engagement:
                total_eng = sum(
                    len(v) for group in engagement.values()
                    for v in (group.values() if isinstance(group, dict) else [group])
                )
                totals["post_engagement"] = total_eng
                print_success(f"post_engagement: {total_eng:,} engagement records")
        except Exception as e:
            logger.exception("Post engagement extraction failed")
            console.print(f"  [red]✗[/red] post_engagement failed: {e}")

    # ── Phase 4: Enrichment ──────────────────────────────────────────────
    if all or enrichment:
        console.print("  [bold]Enriching connection profiles...[/bold]")
        # Load connection profile URLs
        conn_data = extractor._load_latest_extract("connections")
        if not conn_data:
            console.print("  [yellow]!![/yellow] No connections extract found. Run --connections first.")
        else:
            profile_urls = [
                c.get("linkedin_profile_url")
                for c in conn_data
                if c.get("linkedin_profile_url")
            ]
            console.print(f"  [dim]{len(profile_urls):,} profiles to enrich[/dim]")
            try:
                results, meta = enrich_profiles(
                    client,
                    profile_urls,
                    resume=resume,
                    max_workers=enrichment_workers if enrichment_workers else None,
                )
                totals["enrichment"] = meta.get("total_enriched", len(results))
                if meta.get("limit_reached"):
                    show_warning(
                        "API limit reached during enrichment",
                        "Resume later: [cyan]linkedin-cleaner extract --enrichment --resume[/cyan]",
                    )
                else:
                    print_success(f"enrichment: {totals['enrichment']:,} profiles enriched")
            except Exception as e:
                logger.exception("Enrichment failed")
                console.print(f"  [red]✗[/red] enrichment failed: {e}")

    # ── Summary ──────────────────────────────────────────────────────────
    if totals:
        console.print()
        summary = {k: f"{v:,}" for k, v in totals.items()}
        summary["Total records"] = f"{sum(totals.values()):,}"
        console.print(make_summary_table(theme.COPY["extract_complete"], summary))

    print_suggested_next(
        "linkedin-cleaner analyze",
        "Next: run the analysis pipeline",
    )
