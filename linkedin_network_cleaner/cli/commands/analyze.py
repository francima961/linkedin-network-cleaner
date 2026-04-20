"""linkedin-cleaner analyze — Run the 9-step analysis pipeline."""

import glob as globmod
import json
import logging
from datetime import datetime, timezone
from pathlib import Path

import typer

from ..ui import (
    console,
    print_banner,
    print_header,
    print_section,
    print_step,
    print_success,
    print_suggested_next,
    print_comment,
    show_error,
    show_info,
    show_warning,
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

# Map steps to the extract data they need to produce meaningful results
STEP_DATA_REQUIREMENTS = {
    2: (["conversations", "messages_by_thread"], "inbox scoring"),
    3: (["post_engagement_by_post"], "post engagement scoring"),
    4: (["reaction_activity", "comment_activity"], "content interaction scoring"),
    5: (["enrichment"], "profile matching & shared experience detection"),
    6: (["customers"], "customer matching"),      # special: checks assets/Customers/
    7: (["accounts"], "target account matching"),  # special: checks assets/Accounts/
    8: (["prospects"], "target prospect matching"), # special: checks assets/Prospects/
}

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


def _check_extract_exists(name):
    """Check if an extract file exists (excluding checkpoints)."""
    pattern = str(config.EXTRACTS_DIR / f"{name}_*.json")
    files = [f for f in sorted(globmod.glob(pattern)) if "_checkpoint_" not in Path(f).name]
    if not files:
        return False, 0
    # Get record count from latest file
    try:
        payload = json.loads(Path(files[-1]).read_text(encoding="utf-8"))
        count = payload.get("record_count", 0)
    except Exception:
        count = 0
    return True, count


def _check_asset_dir_has_csvs(subdir_name):
    """Check if an assets subdirectory has CSV files."""
    asset_dir = config.ASSETS_DIR / subdir_name
    if not asset_dir.exists():
        return False
    return bool(list(asset_dir.glob("*.csv")))


def _preflight_check():
    """Scan extracts and assets for data availability. Returns dict of {name: (exists, count/bool, purpose)}."""
    data_status = {}

    # Extract-based data
    extract_types = {
        "connections": "connections list (required)",
        "followers": "follower overlap detection",
        "conversations": "inbox activity scoring",
        "messages_by_thread": "message depth analysis",
        "posts": "post extraction",
        "post_engagement_by_post": "engagement scoring",
        "post_likers": "liker tracking",
        "post_commenters": "commenter tracking",
        "reaction_activity": "content interaction scoring",
        "comment_activity": "content interaction scoring",
        "sent_invitations": "invitation analysis",
        "enrichment": "profile matching & AI scoring",
    }
    for name, purpose in extract_types.items():
        exists, count = _check_extract_exists(name)
        data_status[name] = (exists, count, purpose)

    # Asset-based data
    for subdir, purpose in [
        ("Customers", "customer matching"),
        ("Accounts", "target account matching"),
        ("Prospects", "target prospect matching"),
    ]:
        has_csvs = _check_asset_dir_has_csvs(subdir)
        data_status[subdir.lower()] = (has_csvs, 0, purpose)

    return data_status


def _has_data_for_step(step_num, data_status):
    """Check if a step has the data it needs to produce meaningful results."""
    if step_num == 1:
        return True  # connections already verified
    if step_num == 9:
        return True  # AI scoring has its own checks

    reqs = STEP_DATA_REQUIREMENTS.get(step_num)
    if not reqs:
        return True

    required_names, _ = reqs
    # For asset-based steps (6-8), check the asset dirs
    if step_num == 6:
        return data_status.get("customers", (False,))[0]
    if step_num == 7:
        return data_status.get("accounts", (False,))[0]
    if step_num == 8:
        return data_status.get("prospects", (False,))[0]

    # For extract-based steps, at least one of the required extracts must exist
    return any(data_status.get(name, (False,))[0] for name in required_names)


def analyze_command(
    resume: bool = typer.Option(False, help="Resume from last completed step"),
    no_ai: bool = typer.Option(False, "--no-ai", help="Skip AI scoring (steps 1-8 only)"),
    step: int = typer.Option(None, "--step", help="Run only step N (1-9)", min=1, max=9),
    dm_threshold: int = typer.Option(None, "--dm-threshold", help="Min total DMs for active relationship (default 5)"),
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
    dm_threshold_val = dm_threshold if dm_threshold is not None else cfg["analyze"]["dm_threshold"]
    ai_batch = ai_batch_size if ai_batch_size is not None else cfg["analyze"]["ai_batch_size"]
    ai_delay = delay if delay is not None else 1.0

    # Signal config from toml
    signal_config = {
        "keep_likers": cfg["analyze"].get("keep_likers", True),
        "keep_commenters": cfg["analyze"].get("keep_commenters", True),
        "keep_reposters": cfg["analyze"].get("keep_reposters", True),
        "keep_content_interactions": cfg["analyze"].get("keep_content_interactions", True),
    }

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

    # Pre-flight: check connections exist (hard requirement)
    conn_files = sorted(globmod.glob(str(config.EXTRACTS_DIR / "connections_*.json")))
    if not conn_files:
        show_error(
            "No connections extract found",
            "The analysis pipeline requires at least a connections extract.",
            fix="linkedin-cleaner extract --connections",
        )

    config.ensure_dirs()

    # ── Banner + Pre-flight ──────────────────────────────────────────────
    print_banner()
    console.print(f"  [{theme.BRAND_DIM}]v{theme.APP_VERSION}[/{theme.BRAND_DIM}]  [{theme.BRAND_WHITE}]Analysis Pipeline[/{theme.BRAND_WHITE}]")
    console.print(f"  [{theme.BRAND_DIM}]Let's find out who actually belongs in your network.[/{theme.BRAND_DIM}]")

    # Pre-flight: data availability check
    data_status = _preflight_check()

    print_section("Pre-flight Check")

    key_extracts = [
        ("connections", "Connections"),
        ("followers", "Followers"),
        ("conversations", "Conversations"),
        ("enrichment", "Enrichment"),
        ("post_engagement_by_post", "Post engagement"),
        ("reaction_activity", "Reaction activity"),
        ("comment_activity", "Comment activity"),
    ]
    key_assets = [
        ("customers", "Customer list"),
        ("accounts", "Target accounts"),
        ("prospects", "Target prospects"),
    ]

    available_count = 0
    for name, label in key_extracts:
        exists, count, purpose = data_status.get(name, (False, 0, ""))
        if exists:
            available_count += 1
            console.print(f"  {theme.CHECK_OK} {label:<22} [bold]{count:,}[/bold] records")
        else:
            console.print(f"  {theme.CHECK_SKIP} {label:<22} [dim]not extracted {theme.ARROW} {purpose} skipped[/dim]")

    console.print()
    missing_assets = []
    for name, label in key_assets:
        exists, _, purpose = data_status.get(name, (False, 0, ""))
        if exists:
            available_count += 1
            console.print(f"  {theme.CHECK_OK} {label:<22} configured")
        else:
            missing_assets.append(name)
            console.print(f"  {theme.CHECK_WARN} {label:<22} [{theme.BRAND_AMBER}]none {theme.ARROW} {purpose} skipped[/{theme.BRAND_AMBER}]")

    # Check safelist
    safelist = config.load_safelist()
    safelist_indicator = theme.CHECK_OK if safelist else theme.CHECK_WARN
    if safelist:
        console.print(f"  {safelist_indicator} {'Safelist':<22} {len(safelist)} protected")
    else:
        console.print(f"  {safelist_indicator} {'Safelist':<22} [{theme.BRAND_AMBER}]none — no special protection[/{theme.BRAND_AMBER}]")

    total_signals = len(key_extracts) + len(key_assets)
    console.print()
    if available_count == total_signals:
        print_comment("All signals loaded. Maximum analysis accuracy.")
    elif available_count >= 5:
        print_comment(f"{available_count}/{total_signals} signals available. Good enough to be useful.")
    elif available_count >= 3:
        print_comment(f"{available_count}/{total_signals} signals. We'll work with what we have.")
    else:
        print_comment(f"{available_count}/{total_signals} signals. Results will be limited — consider extracting more data.")

    # Protection gap warning — shown before pipeline starts
    if missing_assets or not safelist:
        console.print()
        console.print(f"  [{theme.BRAND_AMBER}]{theme.CHECK_WARN} [bold]Heads up:[/bold] Without protection lists, the analysis might[/{theme.BRAND_AMBER}]")
        console.print(f"  [{theme.BRAND_AMBER}]  recommend removing people you actually want to keep.[/{theme.BRAND_AMBER}]")
        if "customers" in missing_assets:
            console.print(f"  [{theme.BRAND_AMBER}]  {theme.BULLET} No customer list — customer connections won't be auto-kept[/{theme.BRAND_AMBER}]")
        if "accounts" in missing_assets:
            console.print(f"  [{theme.BRAND_AMBER}]  {theme.BULLET} No target accounts — prospect connections may score low[/{theme.BRAND_AMBER}]")
        if not safelist:
            console.print(f"  [{theme.BRAND_AMBER}]  {theme.BULLET} No safelist — family and VIPs have no special protection[/{theme.BRAND_AMBER}]")
        console.print()
        console.print(f"  [dim]The cleanup is always dry-run first, so nothing happens without your approval.[/dim]")
        console.print(f"  [dim]But for best results: linkedin-cleaner init to add these files.[/dim]")

    # Check minimum viable set
    minimum_set = ["connections", "followers", "conversations"]
    missing_minimum = [n for n in minimum_set if not data_status.get(n, (False,))[0]]
    if missing_minimum:
        missing_flags = " --".join(n.replace("_", "-") for n in missing_minimum)
        show_warning(
            "Limited data available",
            f"For meaningful analysis, extract at least: connections, followers, conversations.\n"
            f"Missing: {', '.join(missing_minimum)}\n\n"
            f"Run: [#ff8c00]linkedin-cleaner extract --{missing_flags}[/#ff8c00]",
        )
        proceed = typer.prompt("  Continue anyway? [y/N]", default="N")
        if proceed.strip().lower() != "y":
            raise typer.Exit(0)

    # ── Signal configuration — let user review and adjust before analysis ──
    print_section("Keep Signals")

    G = theme.BRAND_GREEN
    P = theme.BRAND_PURPLE
    D = theme.BRAND_DIM
    A = theme.BRAND_AMBER
    O = theme.BRAND_ORANGE

    console.print(f"  [{D}]The analysis uses these signals to decide who to keep.[/{D}]")
    console.print(f"  [{D}]Review and adjust before running — or press Enter to use defaults.[/{D}]")
    console.print()

    # DM threshold
    console.print(f"  [{G}]▸[/{G}] [bold]DM threshold[/bold]")
    console.print(f"    Keep connections with [{P}]{dm_threshold_val}[/{P}]+ total messages (both parties replied)")
    change_dm = typer.prompt(f"  Change threshold? (Enter = {dm_threshold_val})", default=str(dm_threshold_val))
    try:
        dm_threshold_val = int(change_dm.strip())
    except ValueError:
        pass
    console.print()

    # Engagement signals
    signal_labels = [
        ("keep_likers", "Likers", "People who liked your posts"),
        ("keep_commenters", "Commenters", "People who commented on your posts"),
        ("keep_reposters", "Reposters", "People who reposted your content"),
        ("keep_content_interactions", "Your interactions", "People whose posts you liked or commented on"),
    ]

    console.print(f"  [{G}]▸[/{G}] [bold]Engagement signals[/bold]")
    console.print(f"    [{D}]Each signal adds people to the KEEP list. Disable to be stricter.[/{D}]")
    console.print()

    for key, label, desc in signal_labels:
        current = signal_config[key]
        indicator = f"[{G}]ON[/{G}]" if current else f"[{theme.BRAND_RED}]OFF[/{theme.BRAND_RED}]"
        console.print(f"    {indicator}  {label:<22s} [{D}]{desc}[/{D}]")

    console.print()
    change_signals = typer.prompt("  Adjust signals? [y/N]", default="N")
    if change_signals.strip().lower() == "y":
        for key, label, desc in signal_labels:
            current = signal_config[key]
            current_str = "Y" if current else "N"
            answer = typer.prompt(f"    Keep {label}? [Y/n]", default=current_str)
            signal_config[key] = answer.strip().lower() != "n"
        console.print()
        console.print(f"  [{G}]✓[/{G}] Signals updated")

    console.print()

    # Suppress raw logger warnings from analyzer (pre-flight check covers this now)
    logging.getLogger("linkedin_network_cleaner.core.analyzer").setLevel(logging.ERROR)

    # Resolve profile URL for step 5
    if not profile_url:
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
            console.print(f"  {theme.CHECK_WARN} Could not resolve profile URL. Step 5 may be limited.")
            console.print("  [dim]Provide with: --profile-url https://linkedin.com/in/yourname[/dim]")

    print_section("Running Pipeline")

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
            "step_outcomes": {},
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

        # Check if this step has data to work with
        has_data = _has_data_for_step(step_num, data_status)

        if not has_data:
            # Skip this step — no data available
            reqs = STEP_DATA_REQUIREMENTS.get(step_num, ([], ""))
            _, purpose = reqs
            console.print(f"\n  {theme.CHECK_SKIP} Step {step_num}/{total_steps}  {step_name} [dim]— skipped (no data for {purpose})[/dim]")
            state["step_outcomes"][str(step_num)] = "skipped"
            state["completed_steps"].append(step_num)
            _save_state(state)
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
                master_df = analyzer.analyze_inbox(master_df, dm_threshold=dm_threshold_val)
                active_dms = master_df["active_dms"].sum() if "active_dms" in master_df.columns else 0
                console.print(f"  [dim]Active DM relationships ({dm_threshold_val}+ msgs): [{theme.BRAND_PURPLE}]{int(active_dms):,}[/{theme.BRAND_PURPLE}][/dim]")

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

                # Pre-check: verify Anthropic API credits before starting
                try:
                    import anthropic as _anthropic
                    _test_client = _anthropic.Anthropic(api_key=config.ANTHROPIC_API_KEY)
                    _test_client.messages.create(
                        model="claude-haiku-4-5-20251001", max_tokens=1,
                        messages=[{"role": "user", "content": "."}],
                    )
                except _anthropic.AuthenticationError:
                    show_error(
                        "Invalid Anthropic API key",
                        "The key in .env is not valid.",
                        fix="Update ANTHROPIC_API_KEY in .env with a valid key",
                    )
                except Exception as _e:
                    _err = str(_e)
                    if "credit balance" in _err.lower() or "billing" in _err.lower():
                        show_warning(
                            "No Anthropic credits",
                            "Your Anthropic account has no credits. AI scoring cannot run.\n"
                            "Add credits at [#ff8c00]console.anthropic.com[/#ff8c00], then re-run:\n\n"
                            "  [#ff8c00]linkedin-cleaner analyze --resume --step 9[/#ff8c00]",
                        )
                        console.print(f"  [dim]Steps 1-8 are complete. Skipping AI scoring.[/dim]")
                        state["step_outcomes"][str(step_num)] = "skipped"
                        state["completed_steps"].append(step_num)
                        _save_state(state)
                        break

                scorer = TwoTierScorer(
                    api_key=config.ANTHROPIC_API_KEY,
                    brand_strategy_path=brand_path,
                    persona_path=persona_path,
                )

                # Load enrichment data for deep scoring
                enrichment_data = []
                enrich_files = sorted(globmod.glob(str(config.EXTRACTS_DIR / "enrichment_*.json")))
                if enrich_files:
                    enrich_payload = json.loads(Path(enrich_files[-1]).read_text(encoding="utf-8"))
                    enrichment_data = enrich_payload.get("data", [])

                # Estimate profiles needing scoring (after signal pre-filter)
                needs_scoring_est = len(master_df[
                    (master_df.get("active_dms", False) != True) &
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
                    snapshot = _save_snapshot(master_df, step_num)
                    state["master_snapshot"] = snapshot
                    state["step_outcomes"][str(step_num)] = "skipped"
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
            state["step_outcomes"][str(step_num)] = "complete"
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

    # ── Results ───────────────────────────────────────────────────────────
    if master_df is not None:
        outcomes = state.get("step_outcomes", {})
        completed_count = sum(1 for v in outcomes.values() if v == "complete")
        skipped_count = sum(1 for v in outcomes.values() if v == "skipped")

        # Dynamic title
        if skipped_count == 0:
            result_title = "The Verdict"
            result_comment = "Every connection has been weighed. Here's what we found."
        elif completed_count <= 2:
            result_title = "Preliminary Results"
            result_comment = "Limited data means limited insight. Extract more for the full picture."
        else:
            result_title = "Partial Results"
            result_comment = f"{skipped_count} step{'s' if skipped_count != 1 else ''} had no data. It's a start."

        print_section(result_title)
        print_comment(result_comment)
        console.print()

        total = len(master_df)
        console.print(f"  [bold]Total connections[/bold]{total:>30,}")

        if "active_dms" in master_df.columns:
            real = int(master_df["active_dms"].sum())
            pct = f"({real / total * 100:.0f}%)" if total else ""
            console.print(f"  Real network (10+ msgs){real:>22,}  [dim]{pct}[/dim]")

        for col, label in [
            ("is_customer", "Customers"),
            ("is_target_account", "Target accounts"),
            ("is_target_prospect", "Target prospects"),
        ]:
            if col in master_df.columns:
                val = int(master_df[col].sum())
                if val > 0:
                    console.print(f"  {label}{val:>35,}")

        if "ai_audience_fit" in master_df.columns:
            scored = master_df["ai_audience_fit"].notna()
            ai_count = int(scored.sum())
            if ai_count > 0:
                avg_score = master_df.loc[scored, "ai_audience_fit"].mean()
                console.print(f"  AI scored{ai_count:>35,}")
                console.print(f"  Avg audience fit{avg_score:>28.1f}/100")

        # Step summary
        console.print()
        console.print(f"  [{theme.BRAND_DIM}]{theme.DIVIDER_LIGHT}[/{theme.BRAND_DIM}]")
        console.print(f"  [bold]  Steps completed[/bold]{completed_count:>20}/{completed_count + skipped_count}")
        if skipped_count > 0:
            console.print(f"  [bold]  Steps skipped[/bold]{skipped_count:>22}")
        console.print(f"  [{theme.BRAND_DIM}]{theme.DIVIDER_LIGHT}[/{theme.BRAND_DIM}]")

        # What's next
        console.print()
        if skipped_count > 0:
            console.print(f"  [dim]For better results, extract more data and re-run:[/dim]")
            console.print(f"  [{theme.ACCENT}]  {theme.ARROW}  linkedin-cleaner extract --all[/{theme.ACCENT}]")
            console.print()
        console.print(f"  [{theme.BRAND_AMBER}]See your full dashboard:[/{theme.BRAND_AMBER}]")
        console.print(f"  [{theme.ACCENT}]  {theme.ARROW}  linkedin-cleaner status[/{theme.ACCENT}]")
        console.print()
        console.print(f"  [{theme.BRAND_AMBER}]Preview cleanup decisions:[/{theme.BRAND_AMBER}]")
        console.print(f"  [{theme.ACCENT}]  {theme.ARROW}  linkedin-cleaner clean connections --dry-run[/{theme.ACCENT}]")
        console.print()
