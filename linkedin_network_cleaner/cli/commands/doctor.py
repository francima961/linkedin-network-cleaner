"""linkedin-cleaner doctor — Environment diagnostics."""

import json
import logging
import sys
from pathlib import Path

import typer

from ..ui import (
    console,
    print_header,
    theme,
    create_simple_progress,
)
from ...core import config

logger = logging.getLogger(__name__)


def doctor_command():
    """Validate environment, credentials, and workspace setup."""
    print_header("Environment Check")

    issues = []

    # ── 1. Python version ────────────────────────────────────────────────
    py_ver = f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}"
    if sys.version_info >= (3, 10):
        console.print(f"  {theme.CHECK_OK}  Python {py_ver}")
    else:
        console.print(f"  {theme.CHECK_FAIL}  Python {py_ver} (requires >= 3.10)")
        issues.append("Python 3.10+ is required")

    # ── 2. Dependencies ──────────────────────────────────────────────────
    deps = ["requests", "pandas", "pyarrow", "anthropic", "typer", "rich"]
    installed = 0
    missing_deps = []
    for dep in deps:
        try:
            __import__(dep)
            installed += 1
        except ImportError:
            missing_deps.append(dep)

    if installed == len(deps):
        console.print(f"  {theme.CHECK_OK}  Dependencies ({installed}/{len(deps)} installed)")
    else:
        console.print(f"  {theme.CHECK_FAIL}  Dependencies ({installed}/{len(deps)}) — missing: {', '.join(missing_deps)}")
        issues.append(f"Install missing dependencies: pip install {' '.join(missing_deps)}")

    # ── 3. Edges API key ─────────────────────────────────────────────────
    if config.API_KEY:
        # Validate with API call
        identity_name = None
        api_ok = False
        with create_simple_progress("Validating API keys") as progress:
            task = progress.add_task("Validating", total=None)
            try:
                from ...core.edges_client import EdgesClient
                client = EdgesClient(api_key=config.API_KEY, identity_uuid=config.IDENTITY_UUID)
                data, _headers, error = client.call_action("me", direct_mode=True)
                if error is None:
                    api_ok = True
                    if isinstance(data, list) and data:
                        identity_name = data[0].get("full_name", "")
                    elif isinstance(data, dict):
                        identity_name = data.get("full_name", "")
            except Exception as e:
                logger.debug("API validation failed: %s", e)
            progress.update(task, completed=1, total=1)

        if api_ok:
            label = f"Edges API key (connected as {identity_name})" if identity_name else "Edges API key"
            console.print(f"  {theme.CHECK_OK}  {label}")
        else:
            console.print(f"  {theme.CHECK_FAIL}  Edges API key (invalid or connection error)")
            issues.append("Check your EDGES_API_KEY in .env")
    else:
        console.print(f"  {theme.CHECK_FAIL}  Edges API key (not set)")
        issues.append("Set EDGES_API_KEY in .env — run: linkedin-cleaner init")

    # ── 4. Edges Identity UUID ───────────────────────────────────────────
    if config.IDENTITY_UUID:
        console.print(f"  {theme.CHECK_OK}  Edges Identity UUID")
    else:
        console.print(f"  {theme.CHECK_FAIL}  Edges Identity UUID (not set)")
        issues.append("Set EDGES_IDENTITY_UUID in .env — run: linkedin-cleaner init")

    # ── 5. Anthropic API key ─────────────────────────────────────────────
    if config.ANTHROPIC_API_KEY:
        console.print(f"  {theme.CHECK_OK}  Anthropic API key")
    else:
        console.print(f"  {theme.CHECK_WARN}  No Anthropic API key (AI scoring will be skipped)")

    # ── 6. Brand strategy file ───────────────────────────────────────────
    brand_path, persona_path = config.find_asset_files(config.ASSETS_DIR)

    if brand_path:
        size_kb = brand_path.stat().st_size / 1024
        console.print(f"  {theme.CHECK_OK}  Brand strategy: {brand_path.name} ({size_kb:.1f} KB)")
    else:
        console.print(f"  {theme.CHECK_WARN}  No brand strategy file found")
        issues.append("Place a .md file with 'brand' in its name in assets/")

    # ── 7. Persona/ICP file ──────────────────────────────────────────────
    if persona_path:
        size_kb = persona_path.stat().st_size / 1024
        console.print(f"  {theme.CHECK_OK}  Persona/ICP: {persona_path.name} ({size_kb:.1f} KB)")
    else:
        console.print(f"  {theme.CHECK_WARN}  No persona/ICP file found")
        issues.append("Place a .md file with 'persona' or 'icp' in its name in assets/")

    # ── 8. Target accounts ───────────────────────────────────────────────
    accounts_dir = config.ASSETS_DIR / "Accounts"
    if accounts_dir.exists():
        csv_files = list(accounts_dir.glob("*.csv"))
        if csv_files:
            total_rows = 0
            for f in csv_files:
                try:
                    with open(f, newline="", encoding="utf-8") as fh:
                        total_rows += sum(1 for _ in fh) - 1  # minus header
                except Exception:
                    pass
            console.print(f"  {theme.CHECK_OK}  Target accounts: {len(csv_files)} file(s) ({total_rows:,} companies)")
        else:
            console.print(f"  {theme.CHECK_SKIP}  No target account files")
    else:
        console.print(f"  {theme.CHECK_SKIP}  No target account files")

    # ── 9. Target prospects ──────────────────────────────────────────────
    prospects_dir = config.ASSETS_DIR / "Prospects"
    if prospects_dir.exists():
        csv_files = list(prospects_dir.glob("*.csv"))
        if csv_files:
            total_rows = 0
            for f in csv_files:
                try:
                    with open(f, newline="", encoding="utf-8") as fh:
                        total_rows += sum(1 for _ in fh) - 1
                except Exception:
                    pass
            console.print(f"  {theme.CHECK_OK}  Target prospects: {len(csv_files)} file(s) ({total_rows:,} prospects)")
        else:
            console.print(f"  {theme.CHECK_SKIP}  No target prospect files")
    else:
        console.print(f"  {theme.CHECK_SKIP}  No target prospect files")

    # ── 10. Extracts directory ───────────────────────────────────────────
    if config.EXTRACTS_DIR.exists():
        import glob as globmod
        extract_types = set()
        latest_date = None
        for f in config.EXTRACTS_DIR.glob("*.json"):
            if "checkpoint" in f.name:
                continue
            parts = f.stem.rsplit("_", 2)
            if len(parts) >= 3:
                extract_types.add(parts[0])
                try:
                    ts = parts[-2] + "_" + parts[-1]
                    dt = __import__("datetime").datetime.strptime(ts, "%Y%m%d_%H%M%S")
                    if latest_date is None or dt > latest_date:
                        latest_date = dt
                except (ValueError, IndexError):
                    pass

        if extract_types:
            date_str = latest_date.strftime("%b %-d") if latest_date else "unknown"
            console.print(f"  {theme.CHECK_OK}  Extracts: {len(extract_types)} types, latest {date_str}")
        else:
            console.print(f"  {theme.CHECK_SKIP}  Extracts directory empty")
    else:
        console.print(f"  {theme.CHECK_SKIP}  No extracts directory")

    # ── 11. Analysis state ───────────────────────────────────────────────
    state_path = config.ANALYSIS_DIR / "pipeline_state.json"
    if state_path.exists():
        try:
            state = json.loads(state_path.read_text(encoding="utf-8"))
            completed = state.get("completed_steps", [])
            max_step = max(completed) if completed else 0
            console.print(f"  {theme.CHECK_OK}  Analysis: step {max_step}/9 complete")
        except Exception:
            console.print(f"  {theme.CHECK_WARN}  Analysis: state file corrupted")
            issues.append("Delete analysis/pipeline_state.json and re-run")
    else:
        console.print(f"  {theme.CHECK_SKIP}  Analysis not started")

    # ── 12. Lock file ────────────────────────────────────────────────────
    if config.LOCK_FILE.exists():
        try:
            pid = int(config.LOCK_FILE.read_text().strip())
            import os
            try:
                os.kill(pid, 0)
                console.print(f"  {theme.CHECK_WARN}  Lock file: active (PID {pid})")
            except ProcessLookupError:
                console.print(f"  {theme.CHECK_WARN}  Lock file: stale (PID {pid} dead)")
                issues.append(f"Remove stale lock: rm {config.LOCK_FILE}")
        except ValueError:
            console.print(f"  {theme.CHECK_WARN}  Lock file: invalid contents")
            issues.append(f"Remove invalid lock: rm {config.LOCK_FILE}")
    else:
        console.print(f"  {theme.CHECK_OK}  No lock file")

    # ── Issues summary ───────────────────────────────────────────────────
    if issues:
        console.print(f"\n  [bold]{len(issues)} issue(s) found:[/bold]")
        for issue in issues:
            console.print(f"  {theme.CHECK_WARN} {issue}")
    else:
        console.print(f"\n  [green]✓[/green] [bold]All checks passed![/bold]")
    console.print()
