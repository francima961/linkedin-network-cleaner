"""linkedin-cleaner status — Dashboard showing workspace state."""

import glob as globmod
import json
import logging
from datetime import datetime
from pathlib import Path

import typer

from ..ui import (
    console,
    print_banner,
    print_suggested_next,
    show_info,
    theme,
)
from ...core import config

logger = logging.getLogger(__name__)

# Known extract types in display order with labels
EXTRACT_DISPLAY = [
    ("connections", "Connections"),
    ("followers", "Followers"),
    ("profile_viewers", "Profile Viewers"),
    ("conversations", "Conversations"),
    ("messages", "Messages"),
    ("posts", "Posts"),
    ("reaction_activity", "Reactions"),
    ("comment_activity", "Comments"),
    ("sent_invitations", "Sent Invites"),
    ("enrichment", "Enrichment"),
    ("post_engagement_by_post", "Post Engagement"),
]

PIPELINE_STEPS = [
    (1, "Build base"),
    (2, "Analyze inbox"),
    (3, "Post engagement"),
    (4, "Content interactions"),
    (5, "Enrich for matching"),
    (6, "Match customers"),
    (7, "Match target accounts"),
    (8, "Match target prospects"),
    (9, "AI scoring"),
]


def _get_extract_info(name):
    """Get record count and timestamp for an extract type."""
    pattern = str(config.EXTRACTS_DIR / f"{name}_*.json")
    files = [f for f in sorted(globmod.glob(pattern)) if "checkpoint" not in Path(f).name]
    if not files:
        return None, None
    latest = files[-1]
    try:
        with open(latest, encoding="utf-8") as f:
            payload = json.load(f)
        record_count = payload.get("record_count", 0)
        ts = payload.get("timestamp", "")
        if ts:
            try:
                dt = datetime.strptime(ts, "%Y%m%d_%H%M%S")
                last_run = dt.strftime("%b %-d, %H:%M")
            except ValueError:
                last_run = ts
        else:
            last_run = "unknown"
        return record_count, last_run
    except Exception:
        return 0, "error"


def _count_csvs_in(directory):
    """Count CSV files and total rows in a directory."""
    if not directory.exists():
        return 0, 0
    csv_files = list(directory.glob("*.csv"))
    if not csv_files:
        return 0, 0
    total_rows = 0
    for f in csv_files:
        try:
            with open(f) as fh:
                total_rows += sum(1 for _ in fh) - 1  # subtract header
        except Exception:
            pass
    return len(csv_files), max(total_rows, 0)


def _count_personas(persona_path):
    """Count persona definitions in a persona markdown file."""
    if not persona_path or not persona_path.exists():
        return 0
    try:
        content = persona_path.read_text(encoding="utf-8")
        return content.count("## Persona") + content.count("## persona")
    except Exception:
        return 0


def _check_file_quality(path):
    """Check if a markdown file looks like it has real content vs a template."""
    if not path or not path.exists():
        return "missing"
    try:
        content = path.read_text(encoding="utf-8")
        word_count = len(content.split())
        if word_count < 30:
            return "empty"
        if "PLACEHOLDER" in content.upper() or "[YOUR " in content.upper():
            return "template"
        if word_count < 100:
            return "thin"
        return "good"
    except Exception:
        return "error"


# ── Witty comments ──────────────────────────────────────────────────────

def _extract_data_comment(extract_count, total_records):
    if extract_count == 0:
        return "Your network is a mystery. Let's change that."
    if extract_count <= 3:
        return "We've barely scratched the surface. There's more to know."
    if extract_count <= 7:
        return "Getting somewhere. A few blind spots remain."
    if total_records > 50000:
        return "That's... a lot of data. You've been busy on LinkedIn."
    if extract_count >= 10:
        return "Full picture loaded. We see everything."
    return "Solid data set. Enough to work with."


def _config_comment(brand_quality, persona_count, account_count, safelist_count):
    if brand_quality == "missing" and persona_count == 0:
        return "Flying blind. The AI has nothing to work with."
    if brand_quality == "template":
        return "Template detected. Edit your brand strategy before running AI scoring."
    if brand_quality == "thin":
        return "Your brand strategy is light. More detail = smarter AI scores."
    if persona_count == 0:
        return "No personas defined. The AI won't know who to keep."
    if persona_count == 1:
        return "One persona. That's a start — consider adding 2-3 more."
    if account_count > 0 and safelist_count > 0:
        return "Well configured. The AI knows what it's looking for."
    if brand_quality == "good" and persona_count >= 2:
        return "Solid setup. The AI should score accurately."
    return "Basics are covered."


def _pipeline_comment(completed_count, skipped_count, total):
    if completed_count == 0:
        return "Pipeline hasn't run yet. Your connections are living in blissful ignorance."
    if skipped_count > completed_count:
        return "Most steps had nothing to chew on. Extract more data for the full picture."
    if completed_count == total:
        return "Every connection has been judged. Impartially, of course."
    if completed_count >= 7:
        return "Almost there. Just a few more steps to complete enlightenment."
    return f"{completed_count} down, {total - completed_count - skipped_count} to go."


def _network_verdict(total, keeps, removes, reviews):
    if total == 0:
        return ""
    remove_pct = removes / total * 100 if total else 0
    keep_pct = keeps / total * 100 if total else 0
    if remove_pct > 60:
        return "Oof. More than half your network didn't make the cut."
    if remove_pct > 40:
        return "Significant cleanup ahead. Your future self will thank you."
    if remove_pct > 20:
        return "A healthy trim. Nothing dramatic."
    if keep_pct > 80:
        return "Your network is surprisingly clean. Well curated."
    if reviews > removes:
        return "A lot of borderline cases. You'll want to review those."
    return "Looking good. A few loose ends to tie up."


def _section_header(title):
    """Print a retro terminal section header."""
    from ..ui import print_section
    print_section(title)


def status_command():
    """Show workspace dashboard — extracts, pipeline, and cleanup state."""
    env_path = config.WORKSPACE_DIR / ".env"
    if not env_path.exists():
        show_info(
            "Workspace not initialized",
            "No .env found in the current directory.\n\n"
            "Run: [#ff8c00]linkedin-cleaner init[/#ff8c00]",
        )
        raise typer.Exit(0)

    print_banner()
    console.print(f"  [{theme.BRAND_DIM}]v{theme.APP_VERSION}[/{theme.BRAND_DIM}]  [{theme.BRAND_WHITE}]Network Dashboard[/{theme.BRAND_WHITE}]")

    # ── Configuration & Assets ───────────────────────────────────────────
    _section_header("Configuration")

    # Credentials
    has_edges = bool(config.API_KEY and config.IDENTITY_UUID)
    has_anthropic = bool(config.ANTHROPIC_API_KEY)
    console.print(f"  {theme.CHECK_OK if has_edges else theme.CHECK_FAIL} {'Edges API':<22s} [bold]{'connected' if has_edges else 'not configured'}[/bold]")
    console.print(f"  {theme.CHECK_OK if has_anthropic else theme.CHECK_SKIP} {'Anthropic API':<22s} [bold]{'configured' if has_anthropic else 'not set (AI scoring disabled)'}[/bold]")
    console.print()

    # Brand strategy & personas
    brand_path, persona_path = config.find_asset_files(config.ASSETS_DIR)
    brand_quality = _check_file_quality(brand_path)
    persona_count = _count_personas(persona_path)

    quality_display = {
        "good": f"[{theme.BRAND_GREEN}]ready[/{theme.BRAND_GREEN}]",
        "thin": f"[{theme.BRAND_AMBER}]needs more detail[/{theme.BRAND_AMBER}]",
        "template": f"[{theme.BRAND_AMBER}]template — edit before scoring[/{theme.BRAND_AMBER}]",
        "empty": f"[{theme.BRAND_RED}]empty[/{theme.BRAND_RED}]",
        "missing": f"[{theme.BRAND_DIM}]not created[/{theme.BRAND_DIM}]",
        "error": f"[{theme.BRAND_RED}]error reading file[/{theme.BRAND_RED}]",
    }

    brand_indicator = theme.CHECK_OK if brand_quality == "good" else (theme.CHECK_WARN if brand_quality in ("thin", "template") else theme.CHECK_SKIP)
    console.print(f"  {brand_indicator} {'Brand strategy':<22s} {quality_display.get(brand_quality, 'unknown')}")

    persona_indicator = theme.CHECK_OK if persona_count >= 2 else (theme.CHECK_WARN if persona_count == 1 else theme.CHECK_SKIP)
    persona_text = f"[bold {theme.BRAND_PURPLE}]{persona_count}[/bold {theme.BRAND_PURPLE}] persona{'s' if persona_count != 1 else ''} defined" if persona_count > 0 else f"[{theme.BRAND_DIM}]not created[/{theme.BRAND_DIM}]"
    console.print(f"  {persona_indicator} {'ICP & Personas':<22s} {persona_text}")
    console.print()

    # Target lists
    acct_files, acct_rows = _count_csvs_in(config.ASSETS_DIR / "Accounts")
    prospect_files, prospect_rows = _count_csvs_in(config.ASSETS_DIR / "Prospects")
    cust_files, cust_rows = _count_csvs_in(config.CUSTOMERS_DIR)

    for label, files, rows in [
        ("Target accounts", acct_files, acct_rows),
        ("Target prospects", prospect_files, prospect_rows),
        ("Customer list", cust_files, cust_rows),
    ]:
        if files > 0:
            console.print(f"  {theme.CHECK_OK} {label:<22s} [bold {theme.BRAND_PURPLE}]{rows:,}[/bold {theme.BRAND_PURPLE}] records [dim]({files} file{'s' if files != 1 else ''})[/dim]")
        else:
            console.print(f"  {theme.CHECK_WARN} {label:<22s} [{theme.BRAND_AMBER}]none[/{theme.BRAND_AMBER}]")

    # Safelist & keep rules
    safelist = config.load_safelist()
    keep_rules = config.load_keep_rules()
    keep_rules_count = sum(len(v) for v in keep_rules.values() if isinstance(v, list))

    console.print()
    safelist_indicator = theme.CHECK_OK if safelist else theme.CHECK_WARN
    safelist_text = f"[bold {theme.BRAND_PURPLE}]{len(safelist)}[/bold {theme.BRAND_PURPLE}] protected profile{'s' if len(safelist) != 1 else ''}" if safelist else f"[{theme.BRAND_AMBER}]none[/{theme.BRAND_AMBER}]"
    console.print(f"  {safelist_indicator} {'Safelist':<22s} {safelist_text}")

    keep_indicator = theme.CHECK_OK if keep_rules_count > 0 else theme.CHECK_SKIP
    keep_text = f"[bold {theme.BRAND_PURPLE}]{keep_rules_count}[/bold {theme.BRAND_PURPLE}] custom rule{'s' if keep_rules_count != 1 else ''}" if keep_rules_count > 0 else f"[{theme.BRAND_DIM}]none[/{theme.BRAND_DIM}]"
    console.print(f"  {keep_indicator} {'Keep rules':<22s} {keep_text}")

    console.print()
    console.print(f"  [{theme.BRAND_DIM}]{theme.DIVIDER_LIGHT}[/{theme.BRAND_DIM}]")
    console.print(f"  [dim italic]  {_config_comment(brand_quality, persona_count, acct_rows, len(safelist))}[/dim italic]")

    # Protection warnings — these are critical for accurate results
    missing_protection = []
    if cust_rows == 0:
        missing_protection.append("customers")
    if acct_rows == 0:
        missing_protection.append("target accounts")
    if not safelist:
        missing_protection.append("safelist")

    if missing_protection:
        console.print()
        console.print(f"  [{theme.BRAND_AMBER}]░▒▓[/{theme.BRAND_AMBER}] [bold {theme.BRAND_AMBER}]Protection Gaps[/bold {theme.BRAND_AMBER}] [{theme.BRAND_AMBER}]▓▒░[/{theme.BRAND_AMBER}]")
        console.print(f"  [{theme.BRAND_AMBER}]━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━[/{theme.BRAND_AMBER}]")
        console.print()
        console.print()
        if cust_rows == 0:
            console.print(f"  {theme.CHECK_WARN} [bold]No customer list.[/bold] Connections at your customer companies")
            console.print(f"     could be flagged for removal. They won't be auto-protected.")
        if acct_rows == 0:
            console.print(f"  {theme.CHECK_WARN} [bold]No target accounts.[/bold] Connections at companies you're")
            console.print(f"     prospecting won't get priority — the AI might score them low.")
        if not safelist:
            console.print(f"  {theme.CHECK_WARN} [bold]No safelist.[/bold] Family, close friends, and key partners")
            console.print(f"     have no special protection. Add them to avoid accidents.")
        console.print()
        console.print(f"  [dim]Without these, the analysis may recommend removing people you want to keep.[/dim]")
        console.print(f"  [dim]The cleanup is always dry-run first — but better to get it right from the start.[/dim]")
        console.print()
        console.print(f"  [dim]To fix:[/dim]")
        console.print(f"  [dim]  linkedin-cleaner init                 Re-run setup to add files[/dim]")
        console.print(f"  [dim]  Edit linkedin-cleaner.toml             Add safelist URLs directly[/dim]")
        console.print(f"  [dim]  Copy CSVs to assets/Customers/        Add customer companies[/dim]")
        console.print(f"  [dim]  Copy CSVs to assets/Accounts/         Add target companies[/dim]")

    # ── Extracted Data ───────────────────────────────────────────────────
    _section_header("Extracted Data")

    total_records = 0
    has_any_extract = False
    extract_count = 0

    for name, label in EXTRACT_DISPLAY:
        count, last_run = _get_extract_info(name)
        if count is not None:
            has_any_extract = True
            extract_count += 1
            total_records += count
            count_str = f"[bold {theme.BRAND_PURPLE}]{count:,}[/bold {theme.BRAND_PURPLE}]"
            console.print(f"  {theme.CHECK_OK} {label:<22s} {count_str:>15s}  [{theme.BRAND_PURPLE}]{last_run}[/{theme.BRAND_PURPLE}]")
        else:
            console.print(f"  {theme.CHECK_SKIP} {label:<22s} [dim]{'not extracted':>15s}[/dim]")

    console.print()
    if has_any_extract:
        console.print(f"  [{theme.BRAND_DIM}]{theme.DIVIDER_LIGHT}[/{theme.BRAND_DIM}]")
        console.print(f"  [bold]  Total records[/bold]  [{theme.BRAND_PURPLE}]{total_records:>22,}[/{theme.BRAND_PURPLE}]")
        console.print(f"  [bold]  Data types[/bold]     [{theme.BRAND_PURPLE}]{extract_count:>19}/{len(EXTRACT_DISPLAY)}[/{theme.BRAND_PURPLE}]")
        console.print(f"  [{theme.BRAND_DIM}]{theme.DIVIDER_LIGHT}[/{theme.BRAND_DIM}]")
    console.print(f"  [dim italic]  {_extract_data_comment(extract_count, total_records)}[/dim italic]")

    # ── Analysis Pipeline ────────────────────────────────────────────────
    _section_header("Analysis Pipeline")

    state_path = config.ANALYSIS_DIR / "pipeline_state.json"
    completed_steps = []
    step_outcomes = {}
    if state_path.exists():
        try:
            state = json.loads(state_path.read_text(encoding="utf-8"))
            completed_steps = state.get("completed_steps", [])
            step_outcomes = state.get("step_outcomes", {})
        except Exception:
            pass

    completed_count = 0
    skipped_count = 0

    for step_num, step_name in PIPELINE_STEPS:
        outcome = step_outcomes.get(str(step_num), "")

        if step_num in completed_steps:
            if outcome == "skipped":
                skipped_count += 1
                console.print(f"  {theme.CHECK_SKIP} {step_num}. {step_name:<30s} [dim]skipped (no data)[/dim]")
            else:
                completed_count += 1
                rows_str = ""
                for ext in (".parquet", ".csv"):
                    snapshot = config.ANALYSIS_DIR / f"pipeline_step_{step_num}{ext}"
                    if snapshot.exists():
                        try:
                            if ext == ".parquet":
                                import pyarrow.parquet as pq
                                rows = pq.read_metadata(snapshot).num_rows
                                rows_str = f"[{theme.BRAND_PURPLE}]{rows:,} rows[/{theme.BRAND_PURPLE}]"
                            else:
                                with open(snapshot) as f:
                                    rows = sum(1 for _ in f) - 1
                                rows_str = f"[{theme.BRAND_PURPLE}]{rows:,} rows[/{theme.BRAND_PURPLE}]"
                        except Exception:
                            pass
                        break
                console.print(f"  {theme.CHECK_OK} {step_num}. {step_name:<30s} {rows_str}")
        else:
            console.print(f"  [{theme.BRAND_DIM}]  ○ {step_num}. {step_name}[/{theme.BRAND_DIM}]")

    console.print()
    if completed_steps:
        console.print(f"  [{theme.BRAND_DIM}]{theme.DIVIDER_LIGHT}[/{theme.BRAND_DIM}]")
        console.print(f"  [bold]  Steps completed[/bold]  [{theme.BRAND_PURPLE}]{completed_count:>17}/{len(PIPELINE_STEPS)}[/{theme.BRAND_PURPLE}]")
        if skipped_count:
            console.print(f"  [bold]  Steps skipped[/bold]   [{theme.BRAND_PURPLE}]{skipped_count:>19}[/{theme.BRAND_PURPLE}]")
        console.print(f"  [{theme.BRAND_DIM}]{theme.DIVIDER_LIGHT}[/{theme.BRAND_DIM}]")
    console.print(f"  [dim italic]  {_pipeline_comment(completed_count, skipped_count, len(PIPELINE_STEPS))}[/dim italic]")

    # ── The Verdict ──────────────────────────────────────────────────────
    master_files = sorted(globmod.glob(str(config.ANALYSIS_DIR / "network_master_*.csv")))
    master_df = None
    if master_files:
        _section_header("The Verdict")

        try:
            import pandas as pd
            master_df = pd.read_csv(master_files[-1])
            total = len(master_df)

            console.print(f"  [bold]Total connections[/bold]  [{theme.BRAND_PURPLE}]{total:>27,}[/{theme.BRAND_PURPLE}]")

            if "active_dms" in master_df.columns:
                active = int(master_df["active_dms"].sum())
                pct = f"({active / total * 100:.0f}%)" if total else ""
                console.print(f"  Active DM relationships  [{theme.BRAND_PURPLE}]{active:>19,}[/{theme.BRAND_PURPLE}]  [dim]{pct}[/dim]")

            for col, label in [
                ("is_customer", "Customers"),
                ("is_target_account", "Target accounts"),
                ("is_target_prospect", "Target prospects"),
            ]:
                if col in master_df.columns:
                    val = int(master_df[col].sum())
                    if val > 0:
                        console.print(f"  {label}  [{theme.BRAND_PURPLE}]{val:>32,}[/{theme.BRAND_PURPLE}]")

            if "ai_audience_fit" in master_df.columns:
                scored = master_df["ai_audience_fit"].notna()
                ai_count = int(scored.sum())
                if ai_count > 0:
                    avg_score = master_df.loc[scored, "ai_audience_fit"].mean()
                    console.print(f"  AI scored  [{theme.BRAND_PURPLE}]{ai_count:>32,}[/{theme.BRAND_PURPLE}]")
                    console.print(f"  Avg audience fit  [{theme.BRAND_PURPLE}]{avg_score:>25.1f}/100[/{theme.BRAND_PURPLE}]")

            if "decision" in master_df.columns:
                keeps = int((master_df["decision"] == "keep").sum())
                removes = int((master_df["decision"] == "remove").sum())
                reviews = int((master_df["decision"] == "review").sum())

                console.print()
                console.print(f"  [{theme.BRAND_DIM}]{theme.DIVIDER_LIGHT}[/{theme.BRAND_DIM}]")
                keep_pct = f"({keeps / total * 100:.0f}%)" if total else ""
                remove_pct = f"({removes / total * 100:.0f}%)" if total else ""
                review_pct = f"({reviews / total * 100:.0f}%)" if total else ""
                console.print(f"  [{theme.BRAND_GREEN}]  KEEP[/{theme.BRAND_GREEN}]{keeps:>30,}  [{theme.BRAND_DIM}]{keep_pct}[/{theme.BRAND_DIM}]")
                console.print(f"  [{theme.BRAND_RED}]  REMOVE[/{theme.BRAND_RED}]{removes:>28,}  [{theme.BRAND_DIM}]{remove_pct}[/{theme.BRAND_DIM}]")
                console.print(f"  [{theme.BRAND_AMBER}]  REVIEW[/{theme.BRAND_AMBER}]{reviews:>28,}  [{theme.BRAND_DIM}]{review_pct}[/{theme.BRAND_DIM}]")
                console.print(f"  [{theme.BRAND_DIM}]{theme.DIVIDER_LIGHT}[/{theme.BRAND_DIM}]")
                console.print(f"  [dim italic]  {_network_verdict(total, keeps, removes, reviews)}[/dim italic]")

        except Exception as e:
            console.print(f"  [dim]Could not read analysis: {e}[/dim]")

    # ── Actions Taken ────────────────────────────────────────────────────
    action_counts = {}
    cleanup_types = {"remove_connection", "withdraw_invite", "unfollow"}
    if config.ACTIONS_LOG_DIR.exists():
        for log_file in config.ACTIONS_LOG_DIR.glob("*.json"):
            action_type = log_file.stem.rsplit("_", 2)[0]
            if any(ct in action_type for ct in cleanup_types):
                action_counts[action_type] = action_counts.get(action_type, 0) + 1

    if action_counts:
        _section_header("Actions Taken")
        total_actions = 0
        for action_type, count in sorted(action_counts.items()):
            label = action_type.replace("_", " ").title()
            console.print(f"  {theme.CHECK_OK} {label:<30s} [bold {theme.BRAND_PURPLE}]{count:,}[/bold {theme.BRAND_PURPLE}]")
            total_actions += count
        console.print()
        console.print(f"  [dim italic]  {total_actions:,} connections dealt with. No hard feelings.[/dim italic]")

    # ── What's Next ──────────────────────────────────────────────────────
    _section_header("What's Next")

    has_decisions = master_df is not None and "decision" in master_df.columns if master_files else False

    if not has_any_extract:
        console.print(f"  [dim italic]  Time to pull your LinkedIn data. Quick test first, or go all in?[/dim italic]")
        console.print()
        console.print(f"  [{theme.BRAND_AMBER}]Recommended next:[/{theme.BRAND_AMBER}]")
        console.print(f"  [{theme.BRAND_BLUE}]  {theme.ARROW}  linkedin-cleaner extract --connections --limit 100[/{theme.BRAND_BLUE}]")
        console.print()
        console.print(f"  [{theme.BRAND_AMBER}]Full extraction:[/{theme.BRAND_AMBER}]")
        console.print(f"  [{theme.BRAND_BLUE}]  {theme.ARROW}  linkedin-cleaner extract --all[/{theme.BRAND_BLUE}]")
    elif not completed_steps:
        console.print(f"  [dim italic]  Data's ready. Let's find out who actually belongs in your network.[/dim italic]")
        console.print()
        console.print(f"  [{theme.BRAND_AMBER}]Recommended next:[/{theme.BRAND_AMBER}]")
        console.print(f"  [{theme.BRAND_BLUE}]  {theme.ARROW}  linkedin-cleaner analyze[/{theme.BRAND_BLUE}]")
    elif completed_steps and max(completed_steps) < 9:
        console.print(f"  [dim italic]  Pick up where you left off. The pipeline remembers.[/dim italic]")
        console.print()
        console.print(f"  [{theme.BRAND_AMBER}]Recommended next:[/{theme.BRAND_AMBER}]")
        console.print(f"  [{theme.BRAND_BLUE}]  {theme.ARROW}  linkedin-cleaner analyze --resume[/{theme.BRAND_BLUE}]")
    elif not has_decisions:
        console.print(f"  [dim italic]  The moment of truth. Let's see who stays and who goes.[/dim italic]")
        console.print()
        console.print(f"  [{theme.BRAND_AMBER}]Recommended next:[/{theme.BRAND_AMBER}]")
        console.print(f"  [{theme.BRAND_BLUE}]  {theme.ARROW}  linkedin-cleaner clean connections --dry-run[/{theme.BRAND_BLUE}]")
    else:
        console.print(f"  [dim italic]  Your network is analyzed. The power is in your hands now.[/dim italic]")
        console.print()
        console.print(f"  [{theme.BRAND_AMBER}]Review & act:[/{theme.BRAND_AMBER}]")
        console.print(f"  [{theme.BRAND_BLUE}]  {theme.ARROW}  linkedin-cleaner clean connections --dry-run[/{theme.BRAND_BLUE}]")

    # Always show other available commands
    console.print()
    console.print(f"  [{theme.BRAND_DIM}]{theme.DIVIDER_LIGHT}[/{theme.BRAND_DIM}]")
    console.print()
    console.print(f"  [dim]Other commands:[/dim]")
    console.print(f"  [dim]  linkedin-cleaner extract --help      All extraction options[/dim]")
    console.print(f"  [dim]  linkedin-cleaner clean invites        Manage sent invitations[/dim]")
    console.print(f"  [dim]  linkedin-cleaner clean unfollow       Unfollow non-connections[/dim]")
    console.print(f"  [dim]  linkedin-cleaner doctor               Check your setup[/dim]")
    console.print(f"  [dim]  linkedin-cleaner init                 Reconfigure workspace[/dim]")
    console.print()
