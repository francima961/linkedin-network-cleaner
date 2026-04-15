"""linkedin-cleaner init — Guided setup wizard."""

import shutil
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
)
from ...core import config
from ...core.edges_client import EdgesClient


def init_command():
    """Interactive setup wizard — configure credentials, assets, and workspace."""
    print_header("LinkedIn Network Cleaner — Setup")

    env_path = config.WORKSPACE_DIR / ".env"
    toml_path = config.WORKSPACE_DIR / "linkedin-cleaner.toml"
    assets_dir = config.ASSETS_DIR
    templates_dir = config.PACKAGE_DIR / "templates"

    # Idempotent: check for existing config
    if env_path.exists():
        reconfigure = typer.prompt("  .env already exists. Reconfigure? [y/N]", default="N")
        if reconfigure.strip().lower() != "y":
            console.print("  Keeping existing configuration.")
            raise typer.Exit(0)

    env_vars = {}
    identity_name = ""
    safelist_profiles = []

    # ── Step 1/7: Edges API credentials ──────────────────────────────────
    print_step(1, 7, "Edges API Credentials")

    while True:
        api_key = typer.prompt("  Edges API Key", hide_input=True)
        identity_uuid = typer.prompt("  Edges Identity UUID")

        console.print("  [dim]Validating...[/dim]")
        client = EdgesClient(api_key=api_key, identity_uuid=identity_uuid)
        data, _, error = client.call_action("me", direct_mode=True)

        if error is not None:
            console.print(f"  [red]✗[/red] Validation failed: {error.get('error_label', 'UNKNOWN')}")
            console.print("  [dim]Check your keys at https://app.edges.run/settings/api-keys[/dim]")
            retry = typer.prompt("  Try again? [Y/n]", default="Y")
            if retry.strip().lower() == "n":
                raise typer.Exit(1)
            continue

        # Extract identity name
        if isinstance(data, list) and data:
            identity_name = data[0].get("full_name", "")
        elif isinstance(data, dict):
            identity_name = data.get("full_name", "")
        print_success(f"Connected as [bold]{identity_name}[/bold]")
        break

    env_vars["EDGES_API_KEY"] = api_key
    env_vars["EDGES_IDENTITY_UUID"] = identity_uuid

    # ── Step 2/7: Anthropic API key ──────────────────────────────────────
    print_step(2, 7, "Anthropic API Key (optional)")

    console.print("  [dim]Enables AI-powered audience scoring (~$0.002 per profile)[/dim]")
    anthropic_key = typer.prompt("  Anthropic API Key (Enter to skip)", default="", hide_input=True)
    if anthropic_key:
        try:
            import anthropic
            c = anthropic.Anthropic(api_key=anthropic_key)
            c.messages.create(model="claude-haiku-4-5-20251001", max_tokens=10, messages=[{"role": "user", "content": "hi"}])
            print_success("Anthropic API key validated")
            env_vars["ANTHROPIC_API_KEY"] = anthropic_key
        except Exception as e:
            console.print(f"  [yellow]!![/yellow] Validation failed: {e}")
            console.print("  [dim]You can add the key to .env later[/dim]")
            anthropic_key = ""
    else:
        show_info(
            "AI scoring skipped",
            "Steps 1-8 work without it. Add the key to .env later to enable AI scoring.",
        )

    # ── Step 3/7: Brand Strategy ─────────────────────────────────────────
    print_step(3, 7, "Brand Strategy")

    assets_dir.mkdir(parents=True, exist_ok=True)

    console.print("  [dim]The AI reads this to understand YOUR target audience[/dim]\n")
    choice = typer.prompt("  Do you have a brand strategy document? [path / build / skip]", default="skip")

    if choice.strip().lower() not in ("build", "skip") and Path(choice).expanduser().exists():
        # IMPORT PATH
        src = Path(choice).expanduser()
        shutil.copy2(src, assets_dir / src.name)
        content = src.read_text(encoding="utf-8")
        if len(content) < 100:
            console.print("  [yellow]!![/yellow] File is very short — AI scoring works better with detailed context")
        if "[" in content and "]" in content and "PLACEHOLDER" in content.upper():
            console.print("  [yellow]!![/yellow] Template placeholders detected — edit before running analyze")
        print_success(f"Copied {src.name} → assets/ ({len(content.split())} words)")

    elif choice.strip().lower() == "build":
        # INTERACTIVE BUILDER
        console.print("\n  [dim]Answer these questions about your business:[/dim]\n")
        q1 = typer.prompt("  What does your company do? (1-2 sentences)")
        q2 = typer.prompt("  Who do you sell to? (industry, company size, geography)")
        q3 = typer.prompt("  What problems do you solve for them?")
        q4 = typer.prompt("  What makes you different from alternatives?")

        brand_content = f"""# Brand Strategy

## Company Overview
{q1}

## Target Market
{q2}

## Value Proposition
{q3}

## Key Differentiators
{q4}
"""
        brand_path = assets_dir / "brand_strategy.md"
        brand_path.write_text(brand_content, encoding="utf-8")
        print_success(f"Generated brand_strategy.md ({len(brand_content.split())} words)")
        console.print("  [dim]Review and edit assets/brand_strategy.md to add more detail[/dim]")

    else:
        # SKIP — copy template
        template_src = templates_dir / "brand_strategy_example.md"
        if template_src.exists():
            shutil.copy2(template_src, assets_dir / "brand_strategy.md")
            print_success("Created assets/brand_strategy.md from template")
            console.print("  [yellow]!![/yellow] Edit this file before running analyze — templates produce poor AI scores")

    # ── Step 4/7: ICP & Personas ─────────────────────────────────────────
    print_step(4, 7, "ICP & Personas")

    console.print("  [dim]The AI uses personas to classify your connections[/dim]\n")
    choice = typer.prompt("  Do you have a persona/ICP document? [path / build / skip]", default="skip")

    if choice.strip().lower() not in ("build", "skip") and Path(choice).expanduser().exists():
        # IMPORT PATH
        src = Path(choice).expanduser()
        shutil.copy2(src, assets_dir / src.name)
        content = src.read_text(encoding="utf-8")
        if len(content) < 100:
            console.print("  [yellow]!![/yellow] File is very short — AI scoring works better with detailed context")
        if "[" in content and "]" in content and "PLACEHOLDER" in content.upper():
            console.print("  [yellow]!![/yellow] Template placeholders detected — edit before running analyze")
        print_success(f"Copied {src.name} → assets/ ({len(content.split())} words)")

    elif choice.strip().lower() == "build":
        # INTERACTIVE BUILDER
        personas = []
        console.print("\n  [dim]Define your target personas (the AI uses these to classify connections):[/dim]\n")
        while True:
            label = typer.prompt("  Persona label (e.g., Decision Maker, Champion)")
            titles = typer.prompt("  Typical job titles (comma-separated)")
            company = typer.prompt("  Company characteristics (industry, size, stage)")
            why = typer.prompt("  Why do they matter to your business?")
            personas.append({"label": label, "titles": titles, "company": company, "why": why})
            print_success(f"Added persona: {label}")
            another = typer.prompt("  Add another persona? [y/N]", default="N")
            if another.strip().lower() != "y":
                break

        # Generate persona markdown
        persona_content = "# Target Personas (ICP)\n\n"
        for i, p in enumerate(personas, 1):
            persona_content += f"## Persona {i}: {p['label']}\n"
            persona_content += f"- **Typical titles**: {p['titles']}\n"
            persona_content += f"- **Company profile**: {p['company']}\n"
            persona_content += f"- **Why they matter**: {p['why']}\n\n"

        persona_path = assets_dir / "Persona_ICP.md"
        persona_path.write_text(persona_content, encoding="utf-8")
        print_success(f"Generated Persona_ICP.md ({len(personas)} personas)")

    else:
        # SKIP — copy template
        template_src = templates_dir / "persona_example.md"
        if template_src.exists():
            shutil.copy2(template_src, assets_dir / "Persona_ICP.md")
            print_success("Created assets/Persona_ICP.md from template — [dim]edit with your info[/dim]")

    # ── Step 5/7: Target Lists ───────────────────────────────────────────
    print_step(5, 7, "Target Lists (optional)")

    account_count = 0
    prospect_count = 0
    customer_count = 0

    for list_type, subdir, explanation in [
        ("Account", "Accounts", "Companies you're targeting — connections there are kept"),
        ("Prospect", "Prospects", "Specific people — matched by LinkedIn ID"),
        ("Customer", "Customers", "Your customer companies — connections there are kept"),
    ]:
        console.print(f"\n  [dim]{explanation}[/dim]")
        csv_paths = typer.prompt(f"  {list_type} CSV paths (comma-separated, Enter to skip)", default="")
        if not csv_paths.strip():
            continue

        target_dir = assets_dir / subdir
        target_dir.mkdir(parents=True, exist_ok=True)
        count = 0

        for p in csv_paths.split(","):
            p = Path(p.strip()).expanduser()
            if not p.exists():
                console.print(f"  [yellow]!![/yellow] Not found: {p}")
                continue

            # Copy and preview
            shutil.copy2(p, target_dir / p.name)
            import pandas as pd
            try:
                df = pd.read_csv(p, nrows=5)
                full_df = pd.read_csv(p)
                count += len(full_df)
                console.print(f"\n  [dim]Preview of {p.name} ({len(full_df):,} rows):[/dim]")
                from rich.table import Table
                from rich import box
                table = Table(box=box.SIMPLE, padding=(0, 1))
                for col in df.columns[:5]:
                    table.add_column(str(col), style="dim")
                for _, row in df.head(3).iterrows():
                    table.add_row(*[str(v)[:30] for v in row.values[:5]])
                console.print(table)
            except Exception:
                pass
            print_success(f"Copied {p.name} → assets/{subdir}/")

        if list_type == "Account":
            account_count = count
        elif list_type == "Prospect":
            prospect_count = count
        elif list_type == "Customer":
            customer_count = count

    # ── Step 6/7: Family & VIP Safelist ──────────────────────────────────
    print_step(6, 7, "Family & VIP Safelist")
    console.print("  [dim]These people will NEVER be removed, regardless of score.[/dim]\n")

    choice = typer.prompt("  Add protected profiles? [urls / csv / skip]", default="skip")

    if choice.strip().lower() == "urls":
        console.print("  [dim]Enter LinkedIn profile URLs (one per line, empty line when done):[/dim]")
        while True:
            url = typer.prompt("  URL", default="")
            if not url.strip():
                break
            safelist_profiles.append(url.strip())
        if safelist_profiles:
            print_success(f"Added {len(safelist_profiles)} protected profiles")

    elif choice.strip().lower() == "csv" or (
        choice.strip().lower() not in ("urls", "skip")
        and Path(choice).expanduser().exists()
    ):
        csv_path = (
            Path(choice).expanduser()
            if choice.strip().lower() != "csv"
            else Path(typer.prompt("  CSV path")).expanduser()
        )
        if csv_path.exists():
            import pandas as pd
            df = pd.read_csv(csv_path)
            url_col = None
            for candidate in ["linkedin_profile_url", "url", "profile_url", "LinkedIn URL"]:
                if candidate in df.columns:
                    url_col = candidate
                    break
            if url_col:
                safelist_profiles = df[url_col].dropna().tolist()
                print_success(f"Loaded {len(safelist_profiles)} profiles from {csv_path.name}")
            else:
                console.print(f"  [yellow]!![/yellow] No URL column found. Available: {list(df.columns)}")

    # ── Step 7/7: Review & Generate ──────────────────────────────────────
    print_step(7, 7, "Review & Generate")

    # Write .env
    env_lines = [f"{k}={v}" for k, v in env_vars.items()]
    env_path.write_text("\n".join(env_lines) + "\n", encoding="utf-8")
    print_success(f".env written ({len(env_vars)} credentials)")

    # Write linkedin-cleaner.toml (including safelist)
    if not toml_path.exists():
        toml_lines = [
            "[extract]",
            "delay = 1.5",
            "enrichment_workers = 0",
            "",
            "[analyze]",
            "inbox_max = 10",
            "inbox_min = 5",
            'ai_model = "claude-sonnet-4-6"',
            "ai_batch_size = 20",
            "",
            "[clean]",
            "ai_threshold = 50",
            "stale_days = 21",
            "batch_size = 25",
            "delay = 5",
            "",
            "[safelist]",
            "# These profiles are NEVER removed, regardless of score",
            "profiles = [",
        ]
        for url in safelist_profiles:
            toml_lines.append(f'    "{url}",')
        toml_lines.extend([
            "]",
            "",
            "[keep_rules]",
            "# Additional keep signals — profiles matching ANY rule are kept",
            "keep_locations = []",
            "keep_companies = []",
            "keep_title_keywords = []",
        ])
        toml_path.write_text("\n".join(toml_lines) + "\n", encoding="utf-8")
        print_success("linkedin-cleaner.toml written with defaults")

    # Create all workspace directories
    config.ensure_dirs()
    for d in (config.CUSTOMERS_DIR, assets_dir / "Accounts", assets_dir / "Prospects"):
        d.mkdir(parents=True, exist_ok=True)
    print_success("Workspace directories created")

    # ── Summary ──────────────────────────────────────────────────────────
    summary_data = {
        "Edges API": "connected" + (f" ({identity_name})" if identity_name else ""),
        "Anthropic API": "configured" if "ANTHROPIC_API_KEY" in env_vars else "skipped",
        "Target accounts": f"{account_count:,} records" if account_count else "none",
        "Target prospects": f"{prospect_count:,} records" if prospect_count else "none",
        "Target customers": f"{customer_count:,} records" if customer_count else "none",
        "Safelist": f"{len(safelist_profiles)} protected" if safelist_profiles else "none",
    }
    console.print()
    console.print(make_summary_table("Setup Complete", summary_data))

    print_suggested_next(
        "linkedin-cleaner extract --all",
        "Next: extract your LinkedIn network data",
    )
