"""linkedin-cleaner extract — Run data extractions from LinkedIn via Edges API."""

import glob as globmod
import json
import logging
import time as time_module
from pathlib import Path

import typer

from ..ui import (
    console,
    print_banner,
    print_header,
    print_section,
    print_success,
    print_suggested_next,
    print_comment,
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

# Credit estimates per extraction type (1 credit = 1 API page)
# Page sizes vary per endpoint — these are the actual LinkedIn/Edges page sizes
PAGE_SIZES = {
    "connections": 40,
    "followers": 20,
    "profile_viewers": 10,
    "conversations": 40,
    "sent_invitations": 100,
    "posts": 40,
    "reaction_activity": 40,
    "comment_activity": 40,
    "messages": 20,          # per thread call
    "post_engagement": 30,   # likers=30, commenters=10, reposters=10 (avg ~17)
    "enrichment": 1,         # 1 profile per call
}

CREDIT_ESTIMATES = {
    "connections": lambda n: max(1, (n + 39) // 40),
    "followers": lambda n: max(1, (n + 19) // 20),
    "profile_viewers": lambda n: max(1, (n + 9) // 10),
    "conversations": lambda n: max(1, (n + 39) // 40),
    "sent_invitations": lambda n: max(1, (n + 99) // 100),
    "posts": lambda n: max(1, (n + 39) // 40),
    "reaction_activity": lambda n: max(1, (n + 39) // 40),
    "comment_activity": lambda n: max(1, (n + 39) // 40),
    "messages": lambda n: n,       # 1 call per thread
    "post_engagement": lambda n: n * 3,  # likers + commenters + reposters per post
    "enrichment": lambda n: n,     # 1 call per profile
}


def _estimate_credits(selected_types, limit=None, network_size=None):
    """Estimate total credits needed for selected extraction types."""
    # Use limit if set, otherwise estimate from network size or default
    base = limit if limit else (network_size if network_size else 5000)
    total = 0
    breakdown = {}
    for ext_type in selected_types:
        estimator = CREDIT_ESTIMATES.get(ext_type)
        if estimator:
            est = estimator(base)
            breakdown[ext_type] = est
            total += est
    return total, breakdown


def _estimate_time(selected_types, limit=None):
    """Estimate time in minutes based on selected types and limit."""
    if limit and limit <= 100:
        return 1, 3
    if limit and limit <= 500:
        return 3, 10

    # Full extraction estimates
    has_messages = "messages" in selected_types
    has_enrichment = "enrichment" in selected_types
    has_engagement = "post_engagement" in selected_types

    if has_messages and has_enrichment:
        return 90, 150
    if has_enrichment:
        return 60, 120
    if has_messages:
        return 30, 60
    if has_engagement:
        return 10, 20
    return 3, 10


def _show_no_flag_help():
    """Show branded help when no extraction flag is given."""
    print_banner()
    console.print(f"  [{theme.BRAND_DIM}]v{theme.APP_VERSION}[/{theme.BRAND_DIM}]  [{theme.BRAND_WHITE}]Extract Command[/{theme.BRAND_WHITE}]")
    console.print()
    console.print(f"  [{theme.BRAND_DIM}]{theme.DIVIDER_HEAVY}[/{theme.BRAND_DIM}]")
    console.print()
    console.print("  [bold]What it does[/bold]")
    console.print("  Pulls your LinkedIn data via the Edges API and saves it locally.")
    console.print("  Each extraction type captures a different signal for analysis.")
    console.print()
    console.print(f"  [{theme.BRAND_DIM}]{theme.DIVIDER_LIGHT}[/{theme.BRAND_DIM}]")
    console.print()
    console.print(f"  [{theme.BRAND_AMBER}]Quick start (test with 100 records, ~2 min):[/{theme.BRAND_AMBER}]")
    console.print(f"  [{theme.ACCENT}]  {theme.ARROW}  linkedin-cleaner extract --connections --limit 100[/{theme.ACCENT}]")
    console.print()
    console.print(f"  [{theme.BRAND_AMBER}]Full extraction (everything, 1.5-2.5 hours):[/{theme.BRAND_AMBER}]")
    console.print(f"  [{theme.ACCENT}]  {theme.ARROW}  linkedin-cleaner extract --all[/{theme.ACCENT}]")
    console.print()
    console.print(f"  [{theme.BRAND_AMBER}]Minimum set for useful analysis:[/{theme.BRAND_AMBER}]")
    console.print(f"  [{theme.ACCENT}]  {theme.ARROW}  linkedin-cleaner extract --connections --followers --conversations[/{theme.ACCENT}]")
    console.print()
    console.print(f"  [{theme.BRAND_DIM}]{theme.DIVIDER_LIGHT}[/{theme.BRAND_DIM}]")
    console.print()
    console.print("  [bold]Available extractions[/bold]")
    console.print()
    console.print(f"  [{theme.BRAND_AMBER}]Core data[/{theme.BRAND_AMBER}]             What you get")
    console.print(f"  --connections        Your full connection list")
    console.print(f"  --followers          Who follows you (and mutual followers)")
    console.print(f"  --conversations      Your inbox threads")
    console.print(f"  --profile-viewers    Who looked at your profile")
    console.print(f"  --sent-invites       Pending connection requests")
    console.print()
    console.print(f"  [{theme.BRAND_AMBER}]Content signals[/{theme.BRAND_AMBER}]")
    console.print(f"  --posts              Your published posts")
    console.print(f"  --post-engagement    Who liked, commented, reposted your posts")
    console.print(f"  --reaction-activity  Posts you liked/reacted to")
    console.print(f"  --comment-activity   Posts you commented on")
    console.print()
    console.print(f"  [{theme.BRAND_AMBER}]Deep data[/{theme.BRAND_AMBER}]")
    console.print(f"  --messages           Full message history from all threads")
    console.print(f"  --enrichment         Detailed profile data for every connection")
    console.print()
    console.print(f"  [{theme.BRAND_AMBER}]Options[/{theme.BRAND_AMBER}]")
    console.print(f"  --limit N            Cap results per extraction (for testing)")
    console.print(f"  --resume             Resume from last checkpoint")
    console.print(f"  --delay SECONDS      Custom delay between API calls")
    console.print()
    console.print(f"  [{theme.BRAND_DIM}]{theme.DIVIDER_LIGHT}[/{theme.BRAND_DIM}]")
    console.print()
    console.print(f"  [dim]Edges API credits: each extraction uses ~1 credit per 100 results.[/dim]")
    console.print(f"  [dim]Trial accounts start with 100 credits. Check your balance at https://app.edges.run[/dim]")
    console.print()


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
    limit: int = typer.Option(None, "--limit", help="Max results per extraction (for testing)"),
):
    """Extract LinkedIn network data via the Edges API."""
    # Check if any flags given
    any_flag = (
        all or connections or followers or profile_viewers or conversations
        or messages or posts or post_engagement or reaction_activity
        or comment_activity or sent_invites or enrichment
    )
    if not any_flag:
        _show_no_flag_help()
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

    # Build list of selected extraction types
    selected_types = []
    if all or connections: selected_types.append("connections")
    if all or followers: selected_types.append("followers")
    if all or profile_viewers: selected_types.append("profile_viewers")
    if all or conversations: selected_types.append("conversations")
    if all or sent_invites: selected_types.append("sent_invitations")
    if all or posts: selected_types.append("posts")
    if all or reaction_activity: selected_types.append("reaction_activity")
    if all or comment_activity: selected_types.append("comment_activity")
    if all or messages: selected_types.append("messages")
    if all or post_engagement: selected_types.append("post_engagement")
    if all or enrichment: selected_types.append("enrichment")

    # ── Extraction plan display ──────────────────────────────────────────
    print_section("Extraction Plan")

    if limit:
        console.print(f"  [{theme.BRAND_AMBER}]Limit mode:[/{theme.BRAND_AMBER}] max [bold]{limit:,}[/bold] results per extraction")
        console.print()

    # Show what will be extracted
    phase_map = {
        "Phase 1": [
            ("connections", "Connections", "Your connection list"),
            ("followers", "Followers", "Who follows you"),
            ("profile_viewers", "Profile Viewers", "Who looked at your profile"),
            ("conversations", "Conversations", "Your inbox threads"),
            ("sent_invitations", "Sent Invitations", "Pending connection requests"),
        ],
        "Phase 2": [
            ("posts", "Posts", "Your published posts"),
            ("post_engagement", "Post Engagement", "Likers, commenters, reposters"),
            ("reaction_activity", "Your Reactions", "Posts you liked/reacted to"),
            ("comment_activity", "Your Comments", "Posts you commented on"),
        ],
        "Phase 3": [
            ("messages", "Messages", "Full message history (all threads)"),
            ("enrichment", "Enrichment", "Full profile data per connection"),
        ],
    }

    for phase_name, items in phase_map.items():
        phase_items = [(key, label, desc) for key, label, desc in items if key in selected_types]
        if not phase_items:
            continue
        console.print(f"  [{theme.BRAND_AMBER}]{phase_name}[/{theme.BRAND_AMBER}]")
        for key, label, desc in phase_items:
            console.print(f"    {theme.BULLET} {label:<20s} [dim]{desc}[/dim]")
        console.print()

    # Credit and time estimates
    credit_est, credit_breakdown = _estimate_credits(selected_types, limit=limit)
    time_min, time_max = _estimate_time(selected_types, limit=limit)

    console.print(f"  [{theme.BRAND_DIM}]{theme.DIVIDER_LIGHT}[/{theme.BRAND_DIM}]")
    console.print()
    console.print(f"  [bold]Estimated cost[/bold]")
    console.print(f"  Edges API credits:  [bold]~{credit_est:,}[/bold] credits")
    if time_min == time_max:
        console.print(f"  Estimated time:     [bold]~{time_min} min[/bold]")
    else:
        console.print(f"  Estimated time:     [bold]~{time_min}-{time_max} min[/bold]")
    console.print()
    console.print(f"  [dim]1 credit = 1 API page (page size varies: 10-100 results depending on endpoint).[/dim]")
    console.print(f"  [dim]Check your balance at https://app.edges.run[/dim]")
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
    credits_used = 0
    hit_limit = False
    limit_hit_extraction = None

    # Resolve profile URL upfront — needed by several extraction types
    # and avoids a mid-extraction pause that confuses users
    profile_url = None
    needs_profile_url = any(t in selected_types for t in [
        "posts", "reaction_activity", "comment_activity", "post_engagement",
    ])
    if needs_profile_url:
        console.print("  [dim]Resolving your LinkedIn profile...[/dim]")
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
        console.print()

    def _run_extract(name, fn, *args, max_results=None, **kwargs):
        """Run an extraction with progress bar, error handling, and summary tracking."""
        nonlocal credits_used, hit_limit, limit_hit_extraction
        if hit_limit:
            console.print(f"  {theme.CHECK_SKIP} {name} [dim]— skipped (credit limit reached)[/dim]")
            return None

        start = time_module.time()
        try:
            with create_extraction_progress() as progress:
                task = progress.add_task(name, total=max_results)

                def on_page(page, page_count, total_so_far):
                    progress.update(
                        task,
                        completed=total_so_far,
                        description=f"{name} [dim](page {page}, {total_so_far:,} records)[/dim]",
                    )

                if max_results:
                    kwargs["max_results"] = max_results
                kwargs["progress_callback"] = on_page

                results, meta = fn(*args, **kwargs)

                # Final update
                count = len(results) if isinstance(results, (list, dict)) else 0
                if isinstance(results, dict):
                    count = sum(len(v) if isinstance(v, list) else 1 for v in results.values())
                progress.update(task, completed=count, description=f"{name}")

            elapsed = time_module.time() - start
            totals[name] = count
            pages_used = meta.get("page_count", 1) if meta else 1
            credits_used += pages_used
            elapsed_str = f"{elapsed:.0f}s" if elapsed < 60 else f"{elapsed / 60:.1f}m"

            limit_reached = meta.get("limit_reached", False) if meta else False
            if limit_reached:
                hit_limit = True
                limit_hit_extraction = name
                _show_limit_reached(name, totals, credits_used, selected_types)
            else:
                print_success(f"{name}: {count:,} records [dim]({elapsed_str})[/dim]")
            return results
        except Exception as e:
            logger.exception("Extraction %s failed", name)
            console.print(f"  [red]✗[/red] {name} failed: {e}")
            return None

    def _run_chain_extract(name, fn, total_items_label, *args, **kwargs):
        """Run a chain extraction (messages/post_engagement) with thread/post-level progress."""
        nonlocal hit_limit, limit_hit_extraction
        if hit_limit:
            console.print(f"  {theme.CHECK_SKIP} {name} [dim]— skipped (credit limit reached)[/dim]")
            return None, None, None

        start = time_module.time()
        try:
            with create_extraction_progress() as progress:
                task = progress.add_task(name, total=None)

                def on_item(current, total_items, records_so_far):
                    progress.update(
                        task,
                        completed=current,
                        total=total_items,
                        description=f"{name} [dim]({total_items_label} {current}/{total_items}, {records_so_far:,} records)[/dim]",
                    )

                kwargs["progress_callback"] = on_item
                results, meta = fn(*args, **kwargs)

            elapsed = time_module.time() - start
            elapsed_str = f"{elapsed:.0f}s" if elapsed < 60 else f"{elapsed / 60:.1f}m"

            limit_reached = meta.get("limit_reached", False) if meta else False
            if limit_reached:
                hit_limit = True
                limit_hit_extraction = name
                _show_limit_reached(name, totals, credits_used, selected_types)

            return results, meta, elapsed_str
        except Exception as e:
            logger.exception("%s extraction failed", name)
            console.print(f"  [red]✗[/red] {name} failed: {e}")
            return None, None, None

    def _show_limit_reached(failed_name, completed, credits_so_far, all_selected):
        """Show a clear stop message when Edges credit limit is hit."""
        remaining = [t for t in all_selected if t not in completed and t != failed_name]
        remaining_credits, _ = _estimate_credits(remaining + [failed_name], limit=limit)

        console.print()
        console.print(f"  [{theme.BRAND_RED}]░▒▓[/{theme.BRAND_RED}] [bold {theme.BRAND_RED}]Credit Limit Reached[/bold {theme.BRAND_RED}] [{theme.BRAND_RED}]▓▒░[/{theme.BRAND_RED}]")
        console.print(f"  [{theme.BRAND_RED}]━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━[/{theme.BRAND_RED}]")
        console.print()
        console.print(f"  Stopped during: [bold]{failed_name}[/bold]")
        console.print(f"  Credits used:   [bold]~{credits_so_far:,}[/bold]")
        console.print()

        if completed:
            console.print(f"  [{theme.BRAND_GREEN}]Completed:[/{theme.BRAND_GREEN}]")
            for name, count in completed.items():
                console.print(f"    {theme.CHECK_OK} {name}: {count:,} records")
            console.print()

        if remaining:
            console.print(f"  [{theme.BRAND_AMBER}]Still needed:[/{theme.BRAND_AMBER}]")
            for name in remaining:
                console.print(f"    {theme.CHECK_SKIP} {name}")
            console.print()

        console.print(f"  [bold]To resume:[/bold]")
        console.print(f"  1. Add credits at [{theme.BRAND_AMBER}]https://app.edges.run[/{theme.BRAND_AMBER}]")
        console.print(f"     You'll need approximately [bold]~{remaining_credits:,}[/bold] more credits")
        console.print(f"  2. Re-run the extraction:")
        remaining_flags = " ".join(f"--{n.replace('_', '-')}" for n in ([failed_name] + remaining))
        console.print(f"  [{theme.ACCENT}]  {theme.ARROW}  linkedin-cleaner extract {remaining_flags} --resume[/{theme.ACCENT}]")
        console.print()

    # ── Phase 1: Core extractions ────────────────────────────────────────
    if all or connections:
        _run_extract("connections", extractor.extract_connections, max_results=limit)

    if all or followers:
        _run_extract("followers", extractor.extract_followers, max_results=limit)

    if all or profile_viewers:
        _run_extract("profile_viewers", extractor.extract_profile_viewers, max_results=limit)

    if all or conversations:
        _run_extract("conversations", extractor.extract_conversations, max_results=limit)

    if all or sent_invites:
        _run_extract("sent_invitations", extractor.extract_sent_invitations, max_results=limit)

    # ── Phase 2: Profile-URL-dependent extractions ───────────────────────
    if (all or posts) and not hit_limit:
        _run_extract("posts", extractor.extract_posts, profile_url, max_results=limit)

    if (all or reaction_activity) and not hit_limit:
        _run_extract("reaction_activity", extractor.extract_reaction_activity, profile_url, max_results=limit)

    if (all or comment_activity) and not hit_limit:
        _run_extract("comment_activity", extractor.extract_comment_activity, profile_url, max_results=limit)

    # ── Phase 3: Chain extractions ───────────────────────────────────────
    if (all or messages) and not hit_limit:
        chain_kwargs = {"resume": resume}
        if limit:
            chain_kwargs["max_items"] = limit
        results, meta, elapsed_str = _run_chain_extract(
            "messages", extractor.extract_all_messages, "thread", **chain_kwargs,
        )
        if results is not None and not hit_limit:
            totals["messages"] = len(results)
            print_success(f"messages: {len(results):,} total across all threads [dim]({elapsed_str})[/dim]")
        elif meta and meta.get("error"):
            console.print(f"  {theme.CHECK_WARN} Messages skipped: {meta['error']}")

    if (all or post_engagement) and not hit_limit:
        chain_kwargs = {"resume": resume}
        if limit:
            chain_kwargs["max_items"] = limit
        results, meta, elapsed_str = _run_chain_extract(
            "post_engagement", extractor.extract_all_post_engagement, "post", **chain_kwargs,
        )
        if results and not hit_limit:
            total_eng = sum(
                len(v) for group in results.values()
                for v in (group.values() if isinstance(group, dict) else [group])
            )
            totals["post_engagement"] = total_eng
            print_success(f"post_engagement: {total_eng:,} engagement records [dim]({elapsed_str})[/dim]")

    # ── Phase 4: Enrichment ──────────────────────────────────────────────
    if (all or enrichment) and not hit_limit:
        console.print("  [bold]Enriching connection profiles...[/bold]")
        conn_data = extractor._load_latest_extract("connections")
        if not conn_data:
            console.print(f"  {theme.CHECK_WARN} No connections extract found. Run --connections first.")
        else:
            profile_urls = [
                c.get("linkedin_profile_url")
                for c in conn_data
                if c.get("linkedin_profile_url")
            ]
            if limit:
                profile_urls = profile_urls[:limit]
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
                    hit_limit = True
                    _show_limit_reached("enrichment", totals, credits_used, selected_types)
                else:
                    print_success(f"enrichment: {totals['enrichment']:,} profiles enriched")
            except Exception as e:
                logger.exception("Enrichment failed")
                console.print(f"  [red]✗[/red] enrichment failed: {e}")

    # ── Summary ──────────────────────────────────────────────────────────
    if totals and not hit_limit:
        console.print()
        summary = {k: f"{v:,}" for k, v in totals.items()}
        summary["Total records"] = f"{sum(totals.values()):,}"
        if credits_used:
            summary["Credits used"] = f"~{credits_used:,}"
        console.print(make_summary_table(theme.COPY["extract_complete"], summary))

    # ── Context-aware next suggestion ────────────────────────────────────
    if not hit_limit:
        extracted_types = set(totals.keys())

        for name in ["connections", "followers", "conversations", "enrichment",
                      "posts", "post_engagement_by_post", "reaction_activity",
                      "comment_activity", "sent_invitations"]:
            pattern = str(config.EXTRACTS_DIR / f"{name}_*.json")
            files = [f for f in globmod.glob(pattern) if "_checkpoint_" not in Path(f).name]
            if files:
                extracted_types.add(name)

        minimum_for_analysis = {"connections", "followers", "conversations"}
        have_minimum = minimum_for_analysis.issubset(extracted_types)

        if have_minimum:
            print_suggested_next(
                "linkedin-cleaner analyze",
                "Next: run the analysis pipeline",
            )
        else:
            still_needed = minimum_for_analysis - extracted_types
            if still_needed:
                console.print()
                console.print(f"  [dim]For useful analysis, you also need: {', '.join(sorted(still_needed))}[/dim]")
                next_flags = " ".join(f"--{t.replace('_', '-')}" for t in sorted(still_needed))
                print_suggested_next(
                    f"linkedin-cleaner extract {next_flags}",
                    "Next: extract more data for meaningful analysis",
                )
            else:
                print_suggested_next(
                    "linkedin-cleaner analyze",
                    "Next: run the analysis pipeline",
                )
