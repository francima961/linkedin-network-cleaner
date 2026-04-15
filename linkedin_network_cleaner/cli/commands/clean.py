"""linkedin-cleaner clean — Withdraw invites, manage connections, unfollow profiles."""

import csv
import json
import logging
import time
from datetime import datetime, timezone
from pathlib import Path

import typer

from ..ui import (
    console,
    print_header,
    print_success,
    print_suggested_next,
    show_error,
    show_info,
    show_warning,
    make_cleanup_preview_table,
    make_sample_table,
    make_summary_table,
    create_action_progress,
    theme,
)
from ..ui.console import print_funnel, print_breakdown, print_sample_row, print_metric_line
from ...core import config
from ...core.edges_client import EdgesClient
from ...core.extractors import AudienceExtractor
from ...core.invite_analyzer import InviteAnalyzer
from ...core.config import load_safelist, load_keep_rules
from ...core.decision_engine import DecisionEngine
from ...core.linkedin_actions import LinkedInActions

logger = logging.getLogger(__name__)

clean_app = typer.Typer(
    help="Clean your network — withdraw invites, unfollow, manage connections.",
    no_args_is_help=True,
)


# ── Helper: load latest extract JSON ────────────────────────────────────────

def _load_latest_extract(name):
    """Load the data field from the most recent extract JSON."""
    import glob as globmod
    pattern = str(config.EXTRACTS_DIR / f"{name}_*.json")
    files = sorted(globmod.glob(pattern))
    if not files:
        return None
    with open(files[-1], encoding="utf-8") as f:
        payload = json.load(f)
    return payload.get("data")


def _load_latest_analysis_csv():
    """Load the latest network_master CSV from analysis/."""
    import glob as globmod
    import pandas as pd
    pattern = str(config.ANALYSIS_DIR / "network_master_*.csv")
    files = sorted(globmod.glob(pattern))
    if not files:
        return None
    return pd.read_csv(files[-1])


# ── clean invites ───────────────────────────────────────────────────────────

@clean_app.command(name="invites")
def clean_invites(
    dry_run: bool = typer.Option(True, "--dry-run/--no-dry-run", help="Preview only"),
    export: bool = typer.Option(False, help="Export decisions to CSV"),
    execute: bool = typer.Option(False, help="Execute withdrawal actions"),
    ai_threshold: int = typer.Option(None, "--ai-threshold", help="Min AI score to keep"),
    batch_size: int = typer.Option(None, "--batch-size", help="Max actions per run"),
    delay_opt: float = typer.Option(None, "--delay", help="Seconds between actions"),
    review_file: str = typer.Option(None, "--review-file", help="Use pre-reviewed CSV"),
):
    """Analyze and withdraw stale sent invitations."""
    if execute:
        dry_run = False

    try:
        config.validate()
    except ValueError as e:
        show_error("Missing credentials", str(e), fix="linkedin-cleaner init")

    cfg = config.load_config()
    threshold = ai_threshold if ai_threshold is not None else cfg["clean"]["ai_threshold"]
    batch = batch_size if batch_size is not None else cfg["clean"]["batch_size"]
    action_delay = delay_opt if delay_opt is not None else cfg["clean"]["delay"]

    print_header(theme.COPY["clean_invites_header"])

    # Load invitations
    invites_data = _load_latest_extract("sent_invitations")
    if not invites_data:
        show_error(
            "No sent invitations extract found",
            "Run the extraction first to get your pending invitations.",
            fix="linkedin-cleaner extract --sent-invites",
        )

    # Analyze
    invite_analyzer = InviteAnalyzer()
    invites_df = invite_analyzer.analyze(invites_data)

    if invites_df.empty:
        console.print("  [dim]No sent invitations to analyze.[/dim]")
        raise typer.Exit(0)

    # Optional AI scoring
    if config.ANTHROPIC_API_KEY:
        console.print("  [dim]AI-scoring invitations...[/dim]")
        try:
            invites_df = invite_analyzer.ai_score(invites_df, api_key=config.ANTHROPIC_API_KEY)
        except Exception as e:
            logger.warning("AI scoring of invites failed: %s", e)
            console.print(f"  [yellow]!![/yellow] AI scoring skipped: {e}")

    # Apply decisions
    engine = DecisionEngine(ai_threshold=threshold)
    invites_df = engine.decide_invites(invites_df)

    # Build decision counts
    decisions = invites_df["decision"].value_counts().to_dict()
    total = len(invites_df)

    # Dry run (default)
    if not execute or dry_run:
        console.print(make_cleanup_preview_table(decisions, total))

        # Sample of withdrawals
        withdraw_rows = invites_df[invites_df["decision"].isin(["withdraw", "withdraw_and_tag"])]
        if not withdraw_rows.empty:
            sample = withdraw_rows.head(10).to_dict("records")
            console.print(make_sample_table(sample, title="Sample — To Withdraw"))

        if not execute:
            print_suggested_next(
                "linkedin-cleaner clean invites --execute",
                "To execute withdrawals:",
            )

    # Export
    if export:
        ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        export_path = config.ANALYSIS_DIR / f"invite_decisions_{ts}.csv"
        invites_df.to_csv(export_path, index=False)
        print_success(f"Exported to {export_path.name}")

    # Execute
    if execute and not dry_run:
        config.acquire_lock()
        config.ensure_dirs()

        # Use review file if provided
        if review_file:
            import pandas as pd
            invites_df = pd.read_csv(review_file)

        actionable = invites_df[invites_df["decision"].isin(["withdraw", "withdraw_and_tag"])]
        if actionable.empty:
            console.print("  [dim]No invitations to withdraw.[/dim]")
            raise typer.Exit(0)

        count = min(len(actionable), batch)

        # Confirmation
        console.print(f"\n  About to withdraw [bold]{count}[/bold] invitation(s)")
        console.print(f"  Delay: {action_delay}s between actions\n")
        confirmation = typer.prompt(f"  Type 'withdraw {count}' to confirm")
        if confirmation.strip() != f"withdraw {count}":
            console.print("  Aborted.")
            raise typer.Exit(0)

        # Execute withdrawals
        client = EdgesClient(api_key=config.API_KEY, identity_uuid=config.IDENTITY_UUID)
        actions = LinkedInActions(edges_client=client)

        success_count = 0
        fail_count = 0

        with create_action_progress() as progress:
            task = progress.add_task("Withdrawing invites", total=count, current="")
            for _, row in actionable.head(count).iterrows():
                invite_record = row.to_dict()
                name = invite_record.get("full_name", "Unknown")
                progress.update(task, current=name)

                result = actions.withdraw_invite(invite_record, dry_run=False)
                if result.get("success"):
                    success_count += 1
                else:
                    fail_count += 1
                    logger.warning("Failed: %s — %s", name, result.get("error"))

                progress.advance(task)
                time.sleep(action_delay)

        # Summary
        summary = {
            "Withdrawn": success_count,
            "Failed": fail_count,
            "Remaining": len(actionable) - count,
        }
        console.print()
        console.print(make_summary_table("Withdrawal Complete", summary))


# ── clean connections ───────────────────────────────────────────────────────

@clean_app.command(name="connections")
def clean_connections(
    dry_run: bool = typer.Option(True, "--dry-run/--no-dry-run", help="Preview only"),
    export: bool = typer.Option(False, help="Export decisions to CSV"),
    execute: bool = typer.Option(False, help="Execute removal actions"),
    ai_threshold: int = typer.Option(None, "--ai-threshold", help="Min AI score to keep"),
    batch_size: int = typer.Option(None, "--batch-size", help="Max actions per run"),
    delay_opt: float = typer.Option(None, "--delay", help="Seconds between actions"),
    review_file: str = typer.Option(None, "--review-file", help="Use pre-reviewed CSV"),
):
    """Preview and manage connection removal decisions."""
    if execute:
        dry_run = False

    cfg = config.load_config()
    threshold = ai_threshold if ai_threshold is not None else cfg["clean"]["ai_threshold"]

    print_header(theme.COPY["clean_header"])

    # Load analysis master
    master_df = _load_latest_analysis_csv()
    if master_df is None:
        show_error(
            "No analysis data found",
            "Run the analysis pipeline first to generate connection scores.",
            fix="linkedin-cleaner analyze",
        )

    # Apply decisions
    safelist = load_safelist()
    keep_rules = load_keep_rules()
    engine = DecisionEngine(ai_threshold=threshold, safelist=safelist, keep_rules=keep_rules)
    master_df = engine.decide_connections(master_df)

    import pandas as pd
    total = len(master_df)
    keep_count = len(master_df[master_df["decision"] == "keep"])
    remove_count = len(master_df[master_df["decision"] == "remove"])

    # Section header with personality
    console.print(f"\n  [bold]{theme.COPY['clean_connections_reveal']}[/bold]")
    console.print(f"  {'━' * 45}")

    # Funnel visualization
    print_funnel([
        (total, "Analyzed", theme.BRAND_WHITE),
        (keep_count, theme.TAG_KEEP.replace("\\[", "["), theme.BRAND_GREEN),
        (remove_count, theme.TAG_REMOVE.replace("\\[", "["), theme.BRAND_RED),
    ])

    # Keep breakdown by category
    keep_df = master_df[master_df["decision"] == "keep"]
    keep_categories = []
    if "real_network" in keep_df.columns:
        rn = keep_df["real_network"].sum()
        if rn > 0:
            keep_categories.append(("Real Network (active inbox)", int(rn), keep_count))
    if "is_customer" in keep_df.columns:
        cust = keep_df["is_customer"].sum() + keep_df.get("is_former_customer", pd.Series(0)).sum()
        if cust > 0:
            keep_categories.append(("Customers (current + former)", int(cust), keep_count))
    if "is_target_account" in keep_df.columns:
        ta = keep_df["is_target_account"].sum() + keep_df.get("is_target_prospect", pd.Series(0)).sum()
        if ta > 0:
            keep_categories.append(("Target Accounts & Prospects", int(ta), keep_count))
    if "total_engagements" in keep_df.columns:
        eng = (keep_df["total_engagements"] > 0).sum()
        if eng > 0:
            keep_categories.append(("Content Engagers", int(eng), keep_count))
    if "ai_audience_fit" in keep_df.columns:
        ai_keep = (keep_df["ai_audience_fit"] >= threshold).sum()
        if ai_keep > 0:
            keep_categories.append(("High AI Score (ICP match)", int(ai_keep), keep_count))

    if keep_categories:
        print_breakdown(keep_categories, "Your curated network")

    # Removal reasons
    if remove_count > 0:
        remove_df = master_df[master_df["decision"] == "remove"]
        reason_counts = remove_df["decision_reason"].value_counts().head(5)
        removal_items = [(reason, count, remove_count) for reason, count in reason_counts.items()]
        print_breakdown(removal_items, "What's getting cut")

    # Sample rows (4 removes + 2 keeps for contrast)
    console.print(f"\n  [bold]Sample decisions[/bold]")
    console.print(f"  [dim]{'─' * 20}[/dim]")

    remove_samples = master_df[master_df["decision"] == "remove"].head(4)
    keep_samples = keep_df.sample(min(2, keep_count), random_state=42).head(2) if keep_count > 0 else master_df.head(0)

    for _, row in remove_samples.iterrows():
        name = f"{row.get('full_name', 'Unknown')}"
        title_text = row.get("current_job_title", row.get("headline", ""))
        if title_text:
            name += f" — {str(title_text)[:40]}"
        score = row.get("ai_audience_fit", "")
        reasoning = row.get("ai_reasoning", row.get("decision_reason", ""))
        detail = f"Score: {score}" + (f" — {reasoning}" if reasoning else "")
        tag = theme.TAG_REMOVE.replace("\\[", "[")
        print_sample_row(f"[{theme.BRAND_RED}]✗[/{theme.BRAND_RED}]", name, tag, detail)

    for _, row in keep_samples.iterrows():
        name = f"{row.get('full_name', 'Unknown')}"
        title_text = row.get("current_job_title", row.get("headline", ""))
        if title_text:
            name += f" — {str(title_text)[:40]}"
        score = row.get("ai_audience_fit", "")
        reasoning = row.get("ai_reasoning", row.get("decision_reason", ""))
        detail = f"Score: {score}" + (f" — {reasoning}" if reasoning else "")
        tag = theme.TAG_KEEP.replace("\\[", "[")
        print_sample_row(f"[{theme.BRAND_GREEN}]✓[/{theme.BRAND_GREEN}]", name, tag, detail)

    # Network metrics
    if remove_count > 0:
        reduction_pct = remove_count / total * 100
        console.print(f"\n  [bold]Network metrics[/bold]")
        console.print(f"  [dim]{'─' * 20}[/dim]")
        print_metric_line("Network reduction", f"{reduction_pct:.0f}%")
        if "ai_audience_fit" in master_df.columns:
            avg_keep_score = keep_df["ai_audience_fit"].mean()
            if not pd.isna(avg_keep_score):
                print_metric_line("Avg keep score", f"{avg_keep_score:.0f}/100")
        print_metric_line("Connections after cleanup", f"{keep_count:,}")

    # Export
    if export:
        ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        export_path = config.ANALYSIS_DIR / f"connection_decisions_{ts}.csv"
        master_df.to_csv(export_path, index=False)
        print_success(f"Exported to {export_path.name}")
        console.print(f"  [dim]Use this file for manual review or with --review-file[/dim]")

    # Execute — COMING SOON
    if execute:
        show_info(
            "Connection removal coming soon",
            "Connection removal requires LinkedIn API access that isn't yet available "
            "in this version.\n\n"
            "You can still:\n"
            "• Use --dry-run to preview removal decisions\n"
            "• Use --export to save decisions for manual review\n"
            "• Use 'linkedin-cleaner clean unfollow' to unfollow removed profiles",
        )
    elif not export:
        print_suggested_next(
            "linkedin-cleaner clean connections --export",
            "Export decisions for review:",
        )


# ── clean unfollow ──────────────────────────────────────────────────────────

@clean_app.command(name="unfollow")
def clean_unfollow(
    profile_url: str = typer.Option(None, "--profile-url", help="Unfollow a single profile URL"),
    from_file: str = typer.Option(None, "--from-file", help="Batch unfollow from CSV file"),
    dry_run: bool = typer.Option(True, "--dry-run/--no-dry-run", help="Preview only"),
    execute: bool = typer.Option(False, help="Execute unfollow actions"),
    batch_size: int = typer.Option(None, "--batch-size", help="Max actions per run"),
    delay_opt: float = typer.Option(None, "--delay", help="Seconds between actions"),
):
    """Unfollow LinkedIn profiles via Edges API."""
    if execute:
        dry_run = False

    try:
        config.validate()
    except ValueError as e:
        show_error("Missing credentials", str(e), fix="linkedin-cleaner init")

    cfg = config.load_config()
    batch = batch_size if batch_size is not None else cfg["clean"]["batch_size"]
    action_delay = delay_opt if delay_opt is not None else cfg["clean"]["delay"]

    print_header("Clean — Unfollow")

    # Collect profiles to unfollow
    profiles = []

    if profile_url:
        profiles.append({
            "linkedin_profile_url": profile_url,
            "full_name": profile_url.rstrip("/").split("/")[-1],
        })
    elif from_file:
        import pandas as pd
        file_path = Path(from_file)
        if not file_path.exists():
            show_error("File not found", f"Cannot find: {from_file}")
        df = pd.read_csv(file_path)

        # Filter to "remove" decisions if column exists
        if "decision" in df.columns:
            df = df[df["decision"] == "remove"]
            console.print(f"  [dim]Filtered to {len(df):,} 'remove' decisions from file[/dim]")

        if "linkedin_profile_url" not in df.columns:
            show_error(
                "Missing column",
                "CSV must contain a 'linkedin_profile_url' column.",
                fix="Export with: linkedin-cleaner clean connections --export",
            )

        for _, row in df.iterrows():
            profiles.append({
                "linkedin_profile_url": row["linkedin_profile_url"],
                "linkedin_profile_id": row.get("linkedin_profile_id"),
                "full_name": row.get("full_name", ""),
            })
    else:
        console.print("  [bold]Specify what to unfollow:[/bold]")
        console.print()
        console.print("  [cyan]linkedin-cleaner clean unfollow --profile-url URL[/cyan]")
        console.print("  [cyan]linkedin-cleaner clean unfollow --from-file decisions.csv[/cyan]")
        console.print()
        raise typer.Exit(0)

    if not profiles:
        console.print("  [dim]No profiles to unfollow.[/dim]")
        raise typer.Exit(0)

    count = min(len(profiles), batch)

    # Dry run
    if not execute or dry_run:
        console.print(f"\n  Would unfollow [bold]{count}[/bold] profile(s)")
        if count <= 10:
            for p in profiles[:count]:
                name = p.get("full_name", p.get("linkedin_profile_url", ""))
                console.print(f"    [dim]•[/dim] {name}")
        else:
            for p in profiles[:5]:
                name = p.get("full_name", p.get("linkedin_profile_url", ""))
                console.print(f"    [dim]•[/dim] {name}")
            console.print(f"    [dim]... and {count - 5} more[/dim]")
        console.print()
        if not execute:
            print_suggested_next(
                "linkedin-cleaner clean unfollow --from-file FILE --execute",
                "To execute:",
            )
        return

    # Execute
    config.acquire_lock()
    config.ensure_dirs()

    console.print(f"\n  About to unfollow [bold]{count}[/bold] profile(s)")
    console.print(f"  Delay: {action_delay}s between actions\n")
    confirmation = typer.prompt(f"  Type 'unfollow {count}' to confirm")
    if confirmation.strip() != f"unfollow {count}":
        console.print("  Aborted.")
        raise typer.Exit(0)

    client = EdgesClient(api_key=config.API_KEY, identity_uuid=config.IDENTITY_UUID)
    actions = LinkedInActions(edges_client=client)

    success_count = 0
    fail_count = 0

    with create_action_progress() as progress:
        task = progress.add_task("Unfollowing", total=count, current="")
        for profile in profiles[:count]:
            name = profile.get("full_name", "Unknown")
            progress.update(task, current=name)

            result = actions.unfollow(profile, dry_run=False)
            if result.get("success"):
                success_count += 1
            else:
                fail_count += 1
                logger.warning("Failed: %s — %s", name, result.get("error"))

            progress.advance(task)
            time.sleep(action_delay)

    summary = {
        "Unfollowed": success_count,
        "Failed": fail_count,
        "Remaining": len(profiles) - count,
    }
    console.print()
    console.print(make_summary_table("Unfollow Complete", summary))
