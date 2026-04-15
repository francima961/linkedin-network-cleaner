"""linkedin-cleaner analyze — Run the 9-step analysis pipeline."""

import json
import logging
from datetime import datetime, timezone
from pathlib import Path

import typer

from ..ui import (
    console,
    print_header,
    print_step,
    print_success,
    print_suggested_next,
    show_error,
    show_info,
    make_summary_table,
    create_scoring_progress,
    theme,
)
from ...core import config
from ...core.analyzer import NetworkAnalyzer
from ...core.ai_scorer import TwoTierScorer

logger = logging.getLogger(__name__)

# Step definitions: (number, name, description)
STEPS = [
    (1, "build_base", "Build base — merge connections + followers"),
    (2, "analyze_inbox", "Analyze inbox activity"),
    (3, "analyze_post_engagement", "Analyze post engagement"),
    (4, "analyze_content_interactions", "Analyze content interactions"),
    (5, "enrich_for_matching", "Enrich for matching"),
    (6, "match_customers", "Match customers"),
    (7, "match_target_accounts", "Match target accounts"),
    (8, "match_target_prospects", "Match target prospects"),
    (9, "ai_scoring", "AI scoring (two-tier)"),
]

STATE_FILE = "pipeline_state.json"


def _load_state():
    """Load pipeline state from analysis/pipeline_state.json."""
    state_path = config.ANALYSIS_DIR / STATE_FILE
    if state_path.exists():
        try:
            return json.loads(state_path.read_text(encoding="utf-8"))
        except Exception:
            return None
    return None


def _save_state(state):
    """Save pipeline state to analysis/pipeline_state.json."""
    config.ANALYSIS_DIR.mkdir(parents=True, exist_ok=True)
    state_path = config.ANALYSIS_DIR / STATE_FILE
    state_path.write_text(json.dumps(state, indent=2, default=str), encoding="utf-8")


def _clear_state():
    """Remove pipeline state file."""
    state_path = config.ANALYSIS_DIR / STATE_FILE
    if state_path.exists():
        state_path.unlink()


def _save_snapshot(df, step_num, name="pipeline"):
    """Save a parquet snapshot after each step."""
    try:
        path = config.ANALYSIS_DIR / f"{name}_step_{step_num}.parquet"
        df.to_parquet(path, index=False)
        return str(path)
    except Exception as e:
        logger.warning("Failed to save parquet snapshot for step %d: %s", step_num, e)
        # Fallback to CSV
        path = config.ANALYSIS_DIR / f"{name}_step_{step_num}.csv"
        df.to_csv(path, index=False)
        return str(path)


def _load_snapshot(path_str):
    """Load a DataFrame from a snapshot path (parquet or csv)."""
    import pandas as pd
    path = Path(path_str)
    if path.suffix == ".parquet":
        return pd.read_parquet(path)
    return pd.read_csv(path)


def analyze_command(
    resume: bool = typer.Option(False, help="Resume from last completed step"),
    no_ai: bool = typer.Option(False, "--no-ai", help="Skip AI scoring (steps 1-8 only)"),
    step: int = typer.Option(None, "--step", help="Run only step N (1-9)", min=1, max=9),
    inbox_max: int = typer.Option(None, "--inbox-max", help="Max inbox message threshold"),
    inbox_min: int = typer.Option(None, "--inbox-min", help="Min inbox message threshold"),
    ai_batch_size: int = typer.Option(None, "--ai-batch-size", help="Profiles per AI API call"),
    profile_url: str = typer.Option(None, "--profile-url", help="Your LinkedIn profile URL"),
    limit: int = typer.Option(None, "--limit", help="Limit to N rows after step 1 (testing)"),
    delay: float = typer.Option(None, help="Delay between AI API calls"),
):
    """Run the 9-step analysis pipeline on extracted data."""
    # Validate environment
    try:
        config.validate()
    except ValueError as e:
        show_error("Missing credentials", str(e), fix="linkedin-cleaner init")

    # Load config
    cfg = config.load_config()
    inbox_max_val = inbox_max if inbox_max is not None else cfg["analyze"]["inbox_max"]
    inbox_min_val = inbox_min if inbox_min is not None else cfg["analyze"]["inbox_min"]
    ai_batch = ai_batch_size if ai_batch_size is not None else cfg["analyze"]["ai_batch_size"]
    ai_delay = delay if delay is not None else 1.0

    # Check for Anthropic key
    if not config.ANTHROPIC_API_KEY and not no_ai:
        no_ai = True
        show_info(
            "No Anthropic API key",
            "ANTHROPIC_API_KEY not found in .env.\n"
            "AI scoring (step 9) will be skipped. Steps 1-8 will run normally.\n"
            "Add the key to .env and re-run to enable AI scoring.",
        )

    total_steps = 8 if no_ai else 9

    # Pre-flight: check extracts exist
    import glob as globmod
    conn_files = sorted(globmod.glob(str(config.EXTRACTS_DIR / "connections_*.json")))
    if not conn_files:
        show_error(
            "No connections extract found",
            "The analysis pipeline requires at least a connections extract.",
            fix="linkedin-cleaner extract --connections",
        )

    # Resolve profile URL for step 5
    if not profile_url:
        # Try to get from me endpoint
        from ...core.edges_client import EdgesClient
        try:
            client = EdgesClient(api_key=config.API_KEY, identity_uuid=config.IDENTITY_UUID)
            data, _headers, error = client.call_action("me", direct_mode=True)
            if error is None:
                if isinstance(data, list) and data:
                    profile_url = data[0].get("linkedin_profile_url", "")
                elif isinstance(data, dict):
                    profile_url = data.get("linkedin_profile_url", "")
        except Exception:
            pass
        if not profile_url:
            console.print("  [yellow]!![/yellow] Could not resolve profile URL. Step 5 may be limited.")
            console.print("  [dim]Provide with: --profile-url https://linkedin.com/in/yourname[/dim]")

    config.ensure_dirs()

    print_header(theme.COPY["analyze_header"])

    analyzer = NetworkAnalyzer(config.EXTRACTS_DIR, config.ASSETS_DIR, config.ANALYSIS_DIR)

    # Resume state
    state = None
    master_df = None
    followers_aside_df = None
    start_step = 1

    if resume:
        state = _load_state()
        if state and state.get("completed_steps"):
            start_step = max(state["completed_steps"]) + 1
            snapshot_path = state.get("master_snapshot")
            if snapshot_path and Path(snapshot_path).exists():
                master_df = _load_snapshot(snapshot_path)
                console.print(f"  [dim]Resumed from step {start_step - 1} ({len(master_df):,} rows)[/dim]")
            followers_path = state.get("followers_aside_snapshot")
            if followers_path and Path(followers_path).exists():
                followers_aside_df = _load_snapshot(followers_path)
        else:
            console.print("  [dim]No previous state found — starting from step 1[/dim]")

    if state is None:
        state = {
            "started_at": datetime.now(timezone.utc).isoformat(),
            "completed_steps": [],
            "current_step": None,
            "master_snapshot": None,
            "followers_aside_snapshot": None,
        }

    # If --step is set, override start/end
    if step is not None:
        start_step = step
        end_step = step
    else:
        end_step = total_steps

    # ── Run pipeline ─────────────────────────────────────────────────────
    for step_num, method_name, step_name in STEPS:
        if step_num < start_step or step_num > end_step:
            continue
        if no_ai and step_num == 9:
            continue

        print_step(step_num, total_steps, step_name)
        state["current_step"] = step_num

        try:
            if step_num == 1:
                master_df, followers_aside_df = analyzer.build_base()
                if limit:
                    master_df = master_df.head(limit)
                    console.print(f"  [dim]Limited to {limit} rows for testing[/dim]")
                console.print(f"  [dim]{len(master_df):,} connections, {len(followers_aside_df):,} follower-only[/dim]")
                _save_snapshot(followers_aside_df, step_num, "followers_aside")
                state["followers_aside_snapshot"] = str(
                    config.ANALYSIS_DIR / f"followers_aside_step_{step_num}.parquet"
                )

            elif step_num == 2:
                master_df = analyzer.analyze_inbox(master_df, inbox_max=inbox_max_val, inbox_min=inbox_min_val)
                real_count = master_df["real_network"].sum() if "real_network" in master_df.columns else 0
                console.print(f"  [dim]Real network: {int(real_count):,} connections[/dim]")

            elif step_num == 3:
                master_df = analyzer.analyze_post_engagement(master_df)
                engaged = (master_df.get("total_engagements", 0) > 0).sum()
                console.print(f"  [dim]Engaged with your posts: {int(engaged):,}[/dim]")

            elif step_num == 4:
                master_df = analyzer.analyze_content_interactions(master_df)
                interacted = 0
                for col in ("i_liked_their_posts", "i_commented_their_posts"):
                    if col in master_df.columns:
                        interacted += (master_df[col] > 0).sum()
                console.print(f"  [dim]You interacted with: {int(interacted):,} connections' content[/dim]")

            elif step_num == 5:
                master_df = analyzer.enrich_for_matching(master_df, profile_url=profile_url or "")
                enriched = master_df["current_job_title"].notna().sum() if "current_job_title" in master_df.columns else 0
                console.print(f"  [dim]Enriched profiles: {int(enriched):,}[/dim]")

            elif step_num == 6:
                master_df = analyzer.match_customers(master_df)
                customers = master_df["is_customer"].sum() if "is_customer" in master_df.columns else 0
                console.print(f"  [dim]Current customers: {int(customers):,}[/dim]")

            elif step_num == 7:
                master_df = analyzer.match_target_accounts(master_df)
                targets = master_df["is_target_account"].sum() if "is_target_account" in master_df.columns else 0
                console.print(f"  [dim]Target accounts: {int(targets):,}[/dim]")

            elif step_num == 8:
                master_df = analyzer.match_target_prospects(master_df)
                prospects = master_df["is_target_prospect"].sum() if "is_target_prospect" in master_df.columns else 0
                console.print(f"  [dim]Target prospects: {int(prospects):,}[/dim]")

            elif step_num == 9:
                # AI scoring
                brand_path, persona_path = config.find_asset_files(config.ASSETS_DIR)
                if not brand_path:
                    show_error(
                        "Brand strategy file not found",
                        "AI scoring requires a brand strategy markdown file in assets/.",
                        fix="linkedin-cleaner init",
                    )
                if not persona_path:
                    show_error(
                        "Persona/ICP file not found",
                        "AI scoring requires a persona/ICP markdown file in assets/.",
                        fix="linkedin-cleaner init",
                    )

                scorer = TwoTierScorer(
                    api_key=config.ANTHROPIC_API_KEY,
                    brand_strategy_path=brand_path,
                    persona_path=persona_path,
                )

                # Load enrichment data for deep scoring
                enrichment_data = []
                import glob as globmod
                enrich_files = sorted(globmod.glob(str(config.EXTRACTS_DIR / "enrichment_*.json")))
                if enrich_files:
                    enrich_payload = json.loads(Path(enrich_files[-1]).read_text(encoding="utf-8"))
                    enrichment_data = enrich_payload.get("data", [])

                # Estimate profiles needing scoring (after signal pre-filter)
                needs_scoring_est = len(master_df[
                    (master_df.get("real_network", False) != True) &
                    (master_df.get("total_messages", 0).fillna(0).astype(float) == 0) &
                    (master_df.get("is_customer", False).fillna(False) == False) &
                    (master_df.get("is_target_account", False).fillna(False) == False) &
                    (master_df.get("total_engagements", 0).fillna(0).astype(float) == 0)
                ])

                # Cost estimate
                haiku_batches = (needs_scoring_est + 49) // 50
                haiku_cost = haiku_batches * 0.01
                sonnet_review_est = int(needs_scoring_est * 0.15)
                sonnet_batches = (sonnet_review_est + 19) // 20
                sonnet_cost = sonnet_batches * 0.04
                total_cost = haiku_cost + sonnet_cost
                est_time_min = (haiku_batches * 1.5 + sonnet_batches * 2.0) / 60

                console.print(f"\n  Profiles to score:  [bold]{needs_scoring_est:,}[/bold]")
                console.print(f"  Estimated cost:     [bold]~${total_cost:.2f}[/bold] [dim](Haiku: ~${haiku_cost:.2f}, Sonnet: ~${sonnet_cost:.2f})[/dim]")
                console.print(f"  Estimated time:     [bold]~{est_time_min:.0f} minutes[/bold]")
                console.print()

                proceed = typer.prompt("  Continue with AI scoring? [Y/n]", default="Y")
                if proceed.strip().lower() == "n":
                    console.print("  [dim]Skipping AI scoring. Steps 1-8 complete.[/dim]")
                    # Save snapshot + update state, skip scoring
                    snapshot = _save_snapshot(master_df, step_num)
                    state["master_snapshot"] = snapshot
                    state["completed_steps"].append(step_num)
                    _save_state(state)
                    break

                # Scoring with progress callback
                with create_scoring_progress() as progress:
                    task = progress.add_task("AI Scoring", total=needs_scoring_est, cost="$0.00")

                    def scoring_progress(current, total_profiles, message=""):
                        cost_so_far = current * (total_cost / needs_scoring_est) if needs_scoring_est > 0 else 0
                        progress.update(task, completed=current, cost=f"~${cost_so_far:.2f}")

                    master_df = scorer.score_network(
                        master_df,
                        enrichment_data,
                        haiku_batch_size=ai_batch,
                        sonnet_batch_size=min(ai_batch, 20),
                        delay=ai_delay,
                        analysis_dir=config.ANALYSIS_DIR,
                        progress_callback=scoring_progress,
                    )

                scored = master_df["ai_audience_fit"].notna().sum() if "ai_audience_fit" in master_df.columns else 0
                console.print(f"  [dim]AI-scored: {int(scored):,} profiles[/dim]")

            # Save snapshot + update state
            snapshot = _save_snapshot(master_df, step_num)
            state["master_snapshot"] = snapshot
            state["completed_steps"].append(step_num)
            _save_state(state)
            print_success(f"Step {step_num} complete")

        except Exception as e:
            logger.exception("Step %d (%s) failed", step_num, method_name)
            _save_state(state)
            show_error(
                f"Step {step_num} failed: {step_name}",
                str(e),
                fix=f"linkedin-cleaner analyze --resume  (resumes from step {step_num})",
            )

    # ── Save final output ────────────────────────────────────────────────
    if master_df is not None:
        ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        final_csv = config.ANALYSIS_DIR / f"network_master_{ts}.csv"
        master_df.to_csv(final_csv, index=False)
        print_success(f"Saved {final_csv.name} ({len(master_df):,} rows)")

    # ── Summary ──────────────────────────────────────────────────────────
    if master_df is not None:
        summary = {
            "Total connections": len(master_df),
        }
        for col, label in [
            ("real_network", "Real network"),
            ("is_customer", "Customers"),
            ("is_target_account", "Target accounts"),
            ("is_target_prospect", "Target prospects"),
        ]:
            if col in master_df.columns:
                summary[label] = int(master_df[col].sum())
        if "ai_audience_fit" in master_df.columns:
            scored = master_df["ai_audience_fit"].notna()
            summary["AI scored"] = int(scored.sum())
            summary["Avg AI score"] = f"{master_df.loc[scored, 'ai_audience_fit'].mean():.1f}"

        summary["Steps completed"] = f"{len(state['completed_steps'])}/{total_steps}"

        console.print()
        console.print(make_summary_table(theme.COPY["analyze_complete"], summary))

    print_suggested_next(
        "linkedin-cleaner clean connections --dry-run",
        "Next: preview cleanup decisions",
    )
