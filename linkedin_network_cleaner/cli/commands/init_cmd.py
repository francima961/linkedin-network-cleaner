"""linkedin-cleaner init — Guided setup wizard."""

import getpass
import shutil
import time
from pathlib import Path

import typer

from rich.panel import Panel

from ..ui import (
    console,
    print_banner,
    print_header,
    print_step,
    print_wizard_step,
    print_success,
    print_suggested_next,
    show_error,
    show_info,
    show_warning,
    make_summary_table,
    theme,
)
from ...core import config
from ...core.edges_client import EdgesClient

# ── Country → IANA timezone mapping ──────────────────────────────────────
# Common countries mapped to their primary timezone.
# For countries with multiple zones, we pick the most common one.
COUNTRY_TIMEZONES = {
    "france": "Europe/Paris",
    "fr": "Europe/Paris",
    "germany": "Europe/Berlin",
    "de": "Europe/Berlin",
    "uk": "Europe/London",
    "united kingdom": "Europe/London",
    "gb": "Europe/London",
    "england": "Europe/London",
    "ireland": "Europe/Dublin",
    "ie": "Europe/Dublin",
    "spain": "Europe/Madrid",
    "es": "Europe/Madrid",
    "italy": "Europe/Rome",
    "it": "Europe/Rome",
    "portugal": "Europe/Lisbon",
    "pt": "Europe/Lisbon",
    "netherlands": "Europe/Amsterdam",
    "nl": "Europe/Amsterdam",
    "belgium": "Europe/Brussels",
    "be": "Europe/Brussels",
    "switzerland": "Europe/Zurich",
    "ch": "Europe/Zurich",
    "austria": "Europe/Vienna",
    "at": "Europe/Vienna",
    "sweden": "Europe/Stockholm",
    "se": "Europe/Stockholm",
    "norway": "Europe/Oslo",
    "no": "Europe/Oslo",
    "denmark": "Europe/Copenhagen",
    "dk": "Europe/Copenhagen",
    "finland": "Europe/Helsinki",
    "fi": "Europe/Helsinki",
    "poland": "Europe/Warsaw",
    "pl": "Europe/Warsaw",
    "czech republic": "Europe/Prague",
    "czechia": "Europe/Prague",
    "cz": "Europe/Prague",
    "romania": "Europe/Bucharest",
    "ro": "Europe/Bucharest",
    "greece": "Europe/Athens",
    "gr": "Europe/Athens",
    "turkey": "Europe/Istanbul",
    "tr": "Europe/Istanbul",
    "russia": "Europe/Moscow",
    "ru": "Europe/Moscow",
    "ukraine": "Europe/Kyiv",
    "ua": "Europe/Kyiv",
    "us": "America/New_York",
    "usa": "America/New_York",
    "united states": "America/New_York",
    "canada": "America/Toronto",
    "ca": "America/Toronto",
    "mexico": "America/Mexico_City",
    "mx": "America/Mexico_City",
    "brazil": "America/Sao_Paulo",
    "br": "America/Sao_Paulo",
    "argentina": "America/Argentina/Buenos_Aires",
    "ar": "America/Argentina/Buenos_Aires",
    "colombia": "America/Bogota",
    "co": "America/Bogota",
    "chile": "America/Santiago",
    "cl": "America/Santiago",
    "peru": "America/Lima",
    "pe": "America/Lima",
    "japan": "Asia/Tokyo",
    "jp": "Asia/Tokyo",
    "south korea": "Asia/Seoul",
    "korea": "Asia/Seoul",
    "kr": "Asia/Seoul",
    "china": "Asia/Shanghai",
    "cn": "Asia/Shanghai",
    "india": "Asia/Kolkata",
    "in": "Asia/Kolkata",
    "singapore": "Asia/Singapore",
    "sg": "Asia/Singapore",
    "australia": "Australia/Sydney",
    "au": "Australia/Sydney",
    "new zealand": "Pacific/Auckland",
    "nz": "Pacific/Auckland",
    "israel": "Asia/Jerusalem",
    "il": "Asia/Jerusalem",
    "uae": "Asia/Dubai",
    "united arab emirates": "Asia/Dubai",
    "saudi arabia": "Asia/Riyadh",
    "sa": "Asia/Riyadh",
    "south africa": "Africa/Johannesburg",
    "za": "Africa/Johannesburg",
    "nigeria": "Africa/Lagos",
    "ng": "Africa/Lagos",
    "egypt": "Africa/Cairo",
    "eg": "Africa/Cairo",
    "morocco": "Africa/Casablanca",
    "ma": "Africa/Casablanca",
    "lebanon": "Asia/Beirut",
    "lb": "Asia/Beirut",
    "thailand": "Asia/Bangkok",
    "th": "Asia/Bangkok",
    "vietnam": "Asia/Ho_Chi_Minh",
    "vn": "Asia/Ho_Chi_Minh",
    "indonesia": "Asia/Jakarta",
    "id": "Asia/Jakarta",
    "malaysia": "Asia/Kuala_Lumpur",
    "my": "Asia/Kuala_Lumpur",
    "philippines": "Asia/Manila",
    "ph": "Asia/Manila",
    "taiwan": "Asia/Taipei",
    "tw": "Asia/Taipei",
    "hong kong": "Asia/Hong_Kong",
    "hk": "Asia/Hong_Kong",
    "pakistan": "Asia/Karachi",
    "pk": "Asia/Karachi",
    "bangladesh": "Asia/Dhaka",
    "bd": "Asia/Dhaka",
}


def _resolve_timezone(country_input):
    """Resolve a country name/code to IANA timezone. Returns timezone string or None."""
    normalized = country_input.strip().lower()
    return COUNTRY_TIMEZONES.get(normalized)


def _get_linkedin_status(identity):
    """
    Determine LinkedIn connection status for an identity.
    Returns (is_connected, account_name, status_label).

    Uses multiple signals:
    1. accounts array with status (most reliable, from retrieve_accounts=true)
    2. integrations list as fallback (["linkedin"] means connected)
    """
    accounts = identity.get("accounts", [])
    integrations = identity.get("integrations", [])

    # Check detailed account info first
    for account in accounts:
        if account.get("integration") == "linkedin":
            status = account.get("status", "UNKNOWN")
            account_name = account.get("account_name", "")
            is_connected = status in ("VALID", "LIMIT_REACHED")
            return is_connected, account_name, status

    # Fallback: if "linkedin" is in integrations but no account details,
    # treat as connected (API didn't return account details)
    if "linkedin" in integrations:
        return True, "", "VALID"

    return False, "", "NOT_CONNECTED"


def _prompt_identity_selection(api_key):
    """
    Auto-discover identities, let user pick or create one.
    Returns (identity_uuid, identity_name) or raises typer.Exit.
    """
    console.print("  [dim]Checking your Edges account...[/dim]")

    try:
        identities = EdgesClient.list_identities(api_key)
    except Exception as e:
        show_error(
            "Could not list identities",
            f"API returned: {e}",
            fix="Check your API key at https://app.edges.run/settings/developers",
        )
        return None, None  # unreachable, show_error exits

    if not identities:
        # No identities — create one
        return _create_new_identity(api_key)

    # Show identity picker — always, even for 1 identity
    console.print()
    console.print("  [bold]Available identities:[/bold]")
    console.print()

    from rich.table import Table
    from rich import box
    table = Table(box=box.SIMPLE, padding=(0, 1), show_header=True)
    table.add_column("#", style="bold", width=3)
    table.add_column("Name", style=f"{theme.BRAND_WHITE}")
    table.add_column("LinkedIn", style=f"{theme.BRAND_GREEN}")
    table.add_column("Status", style="dim")

    for i, identity in enumerate(identities, 1):
        name = identity.get("name", "Unknown")
        is_connected, li_name, status = _get_linkedin_status(identity)

        status_display = {
            "VALID": f"[{theme.BRAND_GREEN}]connected[/{theme.BRAND_GREEN}]",
            "INVALID": f"[{theme.BRAND_RED}]disconnected[/{theme.BRAND_RED}]",
            "PENDING": f"[{theme.BRAND_AMBER}]pending login[/{theme.BRAND_AMBER}]",
            "LIMIT_REACHED": f"[{theme.BRAND_AMBER}]rate limited[/{theme.BRAND_AMBER}]",
            "NOT_CONNECTED": f"[{theme.BRAND_AMBER}]not connected[/{theme.BRAND_AMBER}]",
        }.get(status, f"[dim]{status}[/dim]")

        table.add_row(str(i), name, li_name, status_display)

    console.print(table)
    console.print(f"  [dim]Or type 'new' to create a new identity[/dim]")
    console.print()

    while True:
        choice = typer.prompt(f"  Select identity [1-{len(identities)} / new]")
        choice = choice.strip().lower()

        if choice == "new":
            return _create_new_identity(api_key)

        try:
            idx = int(choice) - 1
            if 0 <= idx < len(identities):
                selected = identities[idx]
                identity_uuid = selected["uid"]
                identity_name = selected.get("name", "")
                is_connected, _, _ = _get_linkedin_status(selected)

                if is_connected:
                    # Already connected — validate with me endpoint
                    console.print("  [dim]Validating LinkedIn connection...[/dim]")
                    client = EdgesClient(api_key=api_key, identity_uuid=identity_uuid)
                    data, _, error = client.call_action("me", direct_mode=True)
                    if error is not None:
                        error_label = error.get("error_label", "UNKNOWN")
                        if error_label in ("AUTH_EXPIRED", "NO_VALID_ACCOUNT_CONFIGURED", "LK_BAD_COOKIE"):
                            # Session expired — need to re-authenticate
                            console.print(f"  {theme.CHECK_WARN} LinkedIn session expired ({error_label})")
                            console.print(f"  [dim]Your LinkedIn cookie needs to be refreshed.[/dim]")
                            _prompt_linkedin_login(api_key, identity_uuid, identity_name)
                        else:
                            # Unknown error — still show recovery links instead of dead end
                            console.print(f"  {theme.CHECK_WARN} Validation returned: {error_label}")
                            _prompt_linkedin_login(api_key, identity_uuid, identity_name)
                    else:
                        # Extract connected name
                        if isinstance(data, list) and data:
                            connected_name = data[0].get("full_name", identity_name)
                        elif isinstance(data, dict):
                            connected_name = data.get("full_name", identity_name)
                        else:
                            connected_name = identity_name
                        print_success(f"Connected as [bold]{connected_name}[/bold]")
                        identity_name = connected_name
                else:
                    # Not connected — prompt to connect
                    _prompt_linkedin_login(api_key, identity_uuid, identity_name)

                return identity_uuid, identity_name
            else:
                console.print(f"  [red]Pick a number between 1 and {len(identities)}[/red]")
        except ValueError:
            console.print(f"  [red]Enter a number (1-{len(identities)}) or 'new'[/red]")


def _create_new_identity(api_key):
    """Create a new identity interactively. Returns (uuid, name)."""
    console.print()
    console.print("  [bold]Create a new identity[/bold]")
    console.print("  [dim]This represents the LinkedIn account you want to analyze[/dim]")
    console.print()

    name = typer.prompt("  Full name (as shown on LinkedIn)")

    # Timezone from country
    while True:
        country = typer.prompt("  Country (where you log into LinkedIn)")
        tz = _resolve_timezone(country)
        if tz:
            console.print(f"  [dim]Timezone: {tz}[/dim]")
            break
        console.print(f"  [yellow]!![/yellow] Country '{country}' not recognized. Try a common name (e.g., France, US, Germany)")

    console.print("  [dim]Creating identity...[/dim]")

    try:
        result = EdgesClient.create_identity(api_key, name, timezone=tz)
    except Exception as e:
        show_error(
            "Failed to create identity",
            str(e),
            fix="Check your API key and try again",
        )
        return None, None  # unreachable

    identity_uuid = result.get("uid", "")
    login_link = _extract_login_link(result)
    chrome_ext_link = f"https://app.edges.run/integrations/linkedin/identities/{identity_uuid}"

    print_success(f"Identity created: {name}")

    if login_link:
        _show_connection_options(api_key, identity_uuid, login_link, chrome_ext_link, name)
    else:
        # No native login link — offer Chrome extension only
        console.print()
        console.print("  [bold]Connect your LinkedIn account[/bold]")
        console.print("  [dim]Open this link to set up the Chrome extension:[/dim]")
        console.print()
        console.print(f"  [{theme.BRAND_AMBER}]{chrome_ext_link}[/{theme.BRAND_AMBER}]", soft_wrap=True)
        console.print()
        typer.prompt("  Press Enter once you've connected", default="")
        _validate_linkedin_connection(api_key, identity_uuid, name)

    return identity_uuid, name


def _extract_login_link(result):
    """Extract LinkedIn login link from various API response shapes."""
    # Try all known response shapes
    for key in ("login_links", "identity_login_links"):
        container = result.get(key)
        if isinstance(container, dict) and container.get("linkedin"):
            return container["linkedin"]
    # Check if the result itself has a linkedin key (flat response)
    if isinstance(result, dict) and result.get("linkedin"):
        return result["linkedin"]
    return None


def _prompt_linkedin_login(api_key, identity_uuid, identity_name):
    """Generate login links and wait for user to connect LinkedIn."""
    console.print()
    console.print(f"  {theme.CHECK_WARN} LinkedIn not connected for [bold]{identity_name}[/bold]")
    console.print("  [dim]Generating login link...[/dim]")

    # Chrome extension link is always available (deterministic URL)
    chrome_ext_link = f"https://app.edges.run/integrations/linkedin/identities/{identity_uuid}"

    # Try to generate native login link
    login_link = None
    try:
        result = EdgesClient.generate_login_link(api_key, identity_uuid)
        login_link = _extract_login_link(result)
    except Exception as e:
        console.print(f"  [dim]Could not generate native login link: {e}[/dim]")

    if login_link:
        # Both options available
        _show_connection_options(api_key, identity_uuid, login_link, chrome_ext_link, identity_name)
    else:
        # Only Chrome extension available
        console.print()
        console.print("  [bold]Connect your LinkedIn account[/bold]")
        console.print("  [dim]Open this link to set up the Chrome extension:[/dim]")
        console.print()
        console.print(f"  [{theme.BRAND_AMBER}]{chrome_ext_link}[/{theme.BRAND_AMBER}]", soft_wrap=True)
        console.print()

        typer.prompt("  Press Enter once you've connected", default="")
        _validate_linkedin_connection(api_key, identity_uuid, identity_name)


def _show_connection_options(api_key, identity_uuid, login_link, chrome_ext_link, identity_name):
    """Display both login link and Chrome extension link, wait for connection."""
    console.print()
    console.print("  [bold]Connect your LinkedIn account[/bold]")
    console.print("  [dim]Choose one of these methods:[/dim]")
    console.print()
    console.print(f"  [bold {theme.BRAND_AMBER}]Option 1[/bold {theme.BRAND_AMBER}]  Native login link")
    console.print(f"  [dim]Log in with LinkedIn credentials. Handles 2FA/OTP.[/dim]")
    console.print()
    console.print(f"  [{theme.BRAND_AMBER}]{login_link}[/{theme.BRAND_AMBER}]", soft_wrap=True)
    console.print()
    console.print(f"  [bold {theme.BRAND_AMBER}]Option 2[/bold {theme.BRAND_AMBER}]  Chrome extension")
    console.print(f"  [dim]Uses your existing LinkedIn browser session. No password needed.[/dim]")
    console.print()
    console.print(f"  [{theme.BRAND_AMBER}]{chrome_ext_link}[/{theme.BRAND_AMBER}]", soft_wrap=True)
    console.print()

    typer.prompt("  Press Enter once you've connected", default="")
    _validate_linkedin_connection(api_key, identity_uuid, identity_name)


def _validate_linkedin_connection(api_key, identity_uuid, identity_name):
    """Validate that LinkedIn is actually connected. Retries once if needed."""
    console.print("  [dim]Verifying connection...[/dim]")
    client = EdgesClient(api_key=api_key, identity_uuid=identity_uuid)
    data, _, error = client.call_action("me", direct_mode=True)

    if error is not None:
        error_label = error.get("error_label", "UNKNOWN")
        console.print(f"  {theme.CHECK_WARN} LinkedIn not connected yet (status: {error_label})")
        console.print("  [dim]Make sure you completed the login or extension setup.[/dim]")
        retry = typer.prompt("  Try verifying again? [Y/n]", default="Y")
        if retry.strip().lower() != "n":
            time.sleep(3)
            data, _, error = client.call_action("me", direct_mode=True)
            if error is not None:
                # Show both recovery links instead of a dead-end error
                chrome_ext_link = f"https://app.edges.run/integrations/linkedin/identities/{identity_uuid}"
                console.print()
                console.print(f"  [{theme.BRAND_AMBER}]{theme.CHECK_WARN} LinkedIn still not connected ({error.get('error_label', 'UNKNOWN')})[/{theme.BRAND_AMBER}]")
                console.print()
                console.print("  [bold]Reconnect your LinkedIn account:[/bold]")
                console.print()
                # Try to generate a native login link
                login_link = None
                try:
                    result = EdgesClient.generate_login_link(api_key, identity_uuid)
                    login_link = _extract_login_link(result)
                except Exception:
                    pass
                if login_link:
                    console.print(f"  [bold]Option 1[/bold]  Native login link")
                    console.print(f"  [{theme.BRAND_AMBER}]{login_link}[/{theme.BRAND_AMBER}]", soft_wrap=True)
                    console.print()
                console.print(f"  [bold]Option {'2' if login_link else '1'}[/bold]  Chrome extension")
                console.print(f"  [{theme.BRAND_AMBER}]{chrome_ext_link}[/{theme.BRAND_AMBER}]", soft_wrap=True)
                console.print()
                console.print(f"  [dim]After reconnecting, re-run: linkedin-cleaner init[/dim]")
                raise typer.Exit(1)

    # Extract name from me response
    if isinstance(data, list) and data:
        connected_name = data[0].get("full_name", identity_name)
    elif isinstance(data, dict):
        connected_name = data.get("full_name", identity_name)
    else:
        connected_name = identity_name

    print_success(f"Connected as [bold]{connected_name}[/bold]")


def init_command():
    """Interactive setup wizard — configure credentials, assets, and workspace."""
    print_banner()
    console.print(f"  [{theme.BRAND_DIM}]v{theme.APP_VERSION}[/{theme.BRAND_DIM}]  [{theme.BRAND_WHITE}]Setup Wizard[/{theme.BRAND_WHITE}]")
    console.print(f"  [{theme.BRAND_DIM}]{theme.APP_TAGLINE}[/{theme.BRAND_DIM}]")

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
    print_wizard_step(1, 7, "Edges API Credentials")

    console.print(Panel(
        "[dim]The Edges API connects to your LinkedIn account.[/dim]",
        border_style=theme.BRAND_DIM,
        padding=(0, 2),
    ))
    console.print(f"  [dim]Sign up at:[/dim]      [{theme.BRAND_AMBER}]https://edges.run[/{theme.BRAND_AMBER}]")
    console.print(f"  [dim]Get your key at:[/dim]  [{theme.BRAND_AMBER}]https://app.edges.run/settings/developers[/{theme.BRAND_AMBER}]")
    console.print()

    while True:
        api_key = getpass.getpass("  Edges API Key: ")
        if not api_key.strip():
            console.print("  [red]API key is required[/red]")
            continue

        api_key = api_key.strip()

        # Validate the key by listing identities
        console.print("  [dim]Validating...[/dim]")
        try:
            EdgesClient.list_identities(api_key)
        except Exception as e:
            console.print(f"  [red]✗[/red] Invalid API key: {e}")
            console.print("  [dim]Get your key at https://app.edges.run/settings/developers[/dim]")
            retry = typer.prompt("  Try again? [Y/n]", default="Y")
            if retry.strip().lower() == "n":
                raise typer.Exit(1)
            continue

        print_success("API key validated")
        break

    env_vars["EDGES_API_KEY"] = api_key

    # Identity selection — auto-discover, picker, or create
    identity_uuid, identity_name = _prompt_identity_selection(api_key)
    env_vars["EDGES_IDENTITY_UUID"] = identity_uuid

    # ── Step 2/7: Anthropic API key ──────────────────────────────────────
    print_wizard_step(2, 7, "Anthropic API Key (optional)")

    console.print("  [dim]Enables AI-powered audience scoring (~$0.002 per profile)[/dim]")
    anthropic_key = getpass.getpass("  Anthropic API Key (Enter to skip): ")

    if anthropic_key.strip():
        anthropic_key = anthropic_key.strip()
        try:
            import anthropic
            c = anthropic.Anthropic(api_key=anthropic_key)
            c.messages.create(model="claude-haiku-4-5-20251001", max_tokens=10, messages=[{"role": "user", "content": "hi"}])
            print_success("Anthropic API key validated")
            env_vars["ANTHROPIC_API_KEY"] = anthropic_key
        except anthropic.AuthenticationError:
            console.print(f"  {theme.CHECK_FAIL} Invalid API key — check your key at console.anthropic.com")
            console.print("  [dim]You can add the key to .env later[/dim]")
        except Exception as e:
            # Key is valid but something else failed (billing, rate limit, etc.)
            # Save the key anyway — the issue is not the key itself
            env_vars["ANTHROPIC_API_KEY"] = anthropic_key
            error_msg = str(e)
            if "credit balance" in error_msg.lower() or "billing" in error_msg.lower():
                print_success("API key saved")
                console.print(f"  {theme.CHECK_WARN} Your account has no credits — add credits at [#ff8c00]console.anthropic.com[/#ff8c00] before running AI scoring")
            else:
                print_success("API key saved")
                console.print(f"  {theme.CHECK_WARN} Could not fully validate: {e}")
                console.print("  [dim]The key has been saved. If it's correct, AI scoring will work.[/dim]")
    else:
        show_info(
            "AI scoring skipped",
            "Steps 1-8 work without it. Add the key to .env later to enable AI scoring.",
        )

    # ── Step 3/7: Brand Strategy ─────────────────────────────────────────
    print_wizard_step(3, 7, "Brand Strategy")

    assets_dir.mkdir(parents=True, exist_ok=True)

    console.print("  [dim]The AI reads this to understand YOUR target audience[/dim]")
    console.print()
    console.print("  [dim]Options:[/dim]")
    console.print("  [dim]  path  — Import your own document (paste file path)[/dim]")
    console.print("  [dim]  build — Answer 4 questions, we generate one for you[/dim]")
    console.print("  [dim]  skip  — Start with a template (edit later)[/dim]")
    console.print()
    choice = typer.prompt("  Brand strategy", default="skip")

    if choice.strip().lower() not in ("build", "skip") and Path(choice).expanduser().exists():
        # IMPORT PATH
        src = Path(choice).expanduser()
        shutil.copy2(src, assets_dir / src.name)
        content = src.read_text(encoding="utf-8")
        if len(content) < 100:
            console.print(f"  {theme.CHECK_WARN} File is very short — AI scoring works better with detailed context")
        if "[" in content and "]" in content and "PLACEHOLDER" in content.upper():
            console.print(f"  {theme.CHECK_WARN} Template placeholders detected — edit before running analyze")
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
        # SKIP — copy template with guidance
        template_src = templates_dir / "brand_strategy_example.md"
        if template_src.exists():
            shutil.copy2(template_src, assets_dir / "brand_strategy.md")
            print_success("Created assets/brand_strategy.md from template")
            console.print()
            console.print("  [dim]What to put in this file:[/dim]")
            console.print("  [dim]  - Who your company sells to (industry, size, geography)[/dim]")
            console.print("  [dim]  - What problems you solve for them[/dim]")
            console.print("  [dim]  - The AI uses this to decide if a connection fits your audience[/dim]")
            console.print("  [dim]  - More detail = better scores (aim for 200+ words)[/dim]")

    # ── Step 4/7: ICP & Personas ─────────────────────────────────────────
    print_wizard_step(4, 7, "ICP & Personas")

    console.print("  [dim]The AI uses personas to classify your connections[/dim]")
    console.print()
    console.print("  [dim]Options:[/dim]")
    console.print("  [dim]  path  — Import your own document (paste file path)[/dim]")
    console.print("  [dim]  build — Define personas interactively, we generate the file[/dim]")
    console.print("  [dim]  skip  — Start with a template (edit later)[/dim]")
    console.print()
    choice = typer.prompt("  ICP & Personas", default="skip")

    if choice.strip().lower() not in ("build", "skip") and Path(choice).expanduser().exists():
        # IMPORT PATH
        src = Path(choice).expanduser()
        shutil.copy2(src, assets_dir / src.name)
        content = src.read_text(encoding="utf-8")
        if len(content) < 100:
            console.print(f"  {theme.CHECK_WARN} File is very short — AI scoring works better with detailed context")
        if "[" in content and "]" in content and "PLACEHOLDER" in content.upper():
            console.print(f"  {theme.CHECK_WARN} Template placeholders detected — edit before running analyze")
        print_success(f"Copied {src.name} → assets/ ({len(content.split())} words)")

    elif choice.strip().lower() == "build":
        # INTERACTIVE BUILDER
        personas = []
        persona_num = 1
        console.print()
        console.print("  [dim]Define your target personas one at a time.[/dim]")
        console.print("  [dim]A persona is one type of person you want in your network[/dim]")
        console.print("  [dim](e.g., Decision Maker, Champion, Influencer).[/dim]")
        console.print("  [dim]You'll be able to add more after each one.[/dim]")

        while True:
            console.print()
            console.print(f"  [bold]Persona {persona_num}[/bold]")
            label = typer.prompt("  Label (one persona type, e.g., Decision Maker)")
            titles = typer.prompt("  Job titles (comma-separated)")
            keywords = typer.prompt("  Profile keywords you'd expect (comma-separated)")
            company = typer.prompt("  Company traits (industry, size, geography)")
            personas.append({"label": label, "titles": titles, "keywords": keywords, "company": company})
            print_success(f"Persona {persona_num} added: {label}")
            persona_num += 1
            another = typer.prompt("  Add another persona? [y/N]", default="N")
            if another.strip().lower() != "y":
                break

        # Generate persona markdown
        persona_content = "# Target Personas (ICP)\n\n"
        for i, p in enumerate(personas, 1):
            persona_content += f"## Persona {i}: {p['label']}\n"
            persona_content += f"- **Typical titles**: {p['titles']}\n"
            persona_content += f"- **Profile keywords**: {p['keywords']}\n"
            persona_content += f"- **Company traits**: {p['company']}\n\n"

        persona_path = assets_dir / "Persona_ICP.md"
        persona_path.write_text(persona_content, encoding="utf-8")
        print_success(f"Generated Persona_ICP.md ({len(personas)} persona{'s' if len(personas) != 1 else ''})")

    else:
        # SKIP — copy template with guidance
        template_src = templates_dir / "persona_example.md"
        if template_src.exists():
            shutil.copy2(template_src, assets_dir / "Persona_ICP.md")
            print_success("Created assets/Persona_ICP.md from template")
            console.print()
            console.print("  [dim]What to put in this file:[/dim]")
            console.print("  [dim]  - Define 2-4 personas (e.g., Decision Maker, Champion, Influencer)[/dim]")
            console.print("  [dim]  - For each: job titles, company traits, why they matter to you[/dim]")
            console.print("  [dim]  - The AI classifies every connection against these personas[/dim]")

    # ── Step 5/7: Target Lists ───────────────────────────────────────────
    print_wizard_step(5, 7, "Target Lists (optional)")

    account_count = 0
    prospect_count = 0
    customer_count = 0

    csv_lists = [
        (
            "Account", "Accounts",
            "Companies you're targeting — connections there are kept",
            "CSV with a [bold]Company[/bold] or [bold]Company Name[/bold] column",
        ),
        (
            "Prospect", "Prospects",
            "Specific people you're targeting — matched by LinkedIn ID",
            "CSV with a [bold]Person Linkedin Id[/bold] or [bold]LinkedIn Member ID[/bold] column",
        ),
        (
            "Customer", "Customers",
            "Your current customers — connections there are always kept",
            "CSV with a [bold]company_name[/bold] or [bold]Company Name[/bold] column",
        ),
    ]

    for list_type, subdir, explanation, format_hint in csv_lists:
        console.print(f"\n  [dim]{explanation}[/dim]")
        console.print(f"  [dim]Format: {format_hint}[/dim]")
        csv_paths = typer.prompt(f"  {list_type} CSV paths (comma-separated, Enter to skip)", default="")
        if not csv_paths.strip():
            continue

        target_dir = assets_dir / subdir
        target_dir.mkdir(parents=True, exist_ok=True)
        count = 0

        for p in csv_paths.split(","):
            p = Path(p.strip()).expanduser()
            if not p.exists():
                console.print(f"  {theme.CHECK_WARN} Not found: {p}")
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
    print_wizard_step(6, 7, "Family & VIP Safelist")
    console.print("  [dim]These people will NEVER be removed, regardless of score.[/dim]")
    console.print("  [dim]Think: family, close friends, your boss, key partners.[/dim]")
    console.print()
    console.print("  [dim]You'll need their LinkedIn profile URL.[/dim]")
    console.print("  [dim]Format: https://linkedin.com/in/someone[/dim]")
    console.print()

    add_safelist = typer.prompt("  Add protected profiles? [y/N]", default="N")

    if add_safelist.strip().lower() == "y":
        console.print()
        console.print("  [dim]Paste LinkedIn profile URLs one at a time. Empty line when done.[/dim]")
        console.print("  [dim]You can also edit the safelist later in linkedin-cleaner.toml[/dim]")
        console.print()
        count = 0
        while True:
            url = typer.prompt(f"  Profile URL #{count + 1}", default="")
            if not url.strip():
                break
            entry = url.strip()
            safelist_profiles.append(entry)
            count += 1
            print_success(f"Added: {entry}")
        if safelist_profiles:
            print_success(f"{len(safelist_profiles)} profile{'s' if len(safelist_profiles) != 1 else ''} protected")

    # ── Step 7/7: Review & Generate ──────────────────────────────────────
    print_wizard_step(7, 7, "Review & Generate")

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
            "dm_threshold = 5",
            "keep_likers = true",
            "keep_commenters = true",
            "keep_reposters = true",
            "keep_content_interactions = true",
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

    # Copy CLAUDE.md for Claude Code integration
    claude_md_src = templates_dir / "CLAUDE.md"
    claude_md_dst = config.WORKSPACE_DIR / "CLAUDE.md"
    if claude_md_src.exists() and not claude_md_dst.exists():
        shutil.copy2(claude_md_src, claude_md_dst)
        print_success("CLAUDE.md created (Claude Code integration)")

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

    # ── What's next — explain the full workflow ──────────────────────────
    console.print()
    console.print(f"  [bold]What happens next?[/bold]")
    console.print(f"  [{theme.BRAND_DIM}]{theme.DIVIDER_LIGHT}[/{theme.BRAND_DIM}]")
    console.print()
    console.print(f"  The tool works in 3 phases: [bold]Extract[/bold] {theme.ARROW} [bold]Analyze[/bold] {theme.ARROW} [bold]Clean[/bold]")
    console.print()
    console.print(f"  [bold]1. Extract[/bold]  Pull your LinkedIn data via the Edges API")
    console.print(f"     Connections, followers, messages, post engagement, etc.")
    console.print(f"     Full extraction takes ~1.5-2.5 hours for a large network.")
    console.print()
    console.print(f"  [bold]2. Analyze[/bold]  Score every connection for relevance")
    console.print(f"     9-step pipeline: inbox, engagement, customers, AI scoring.")
    console.print()
    console.print(f"  [bold]3. Clean[/bold]   Preview decisions, then execute")
    console.print(f"     Always dry-run first. You approve every action.")
    console.print()
    console.print(f"  [{theme.BRAND_DIM}]{theme.DIVIDER_LIGHT}[/{theme.BRAND_DIM}]")
    console.print()
    console.print(f"  [{theme.BRAND_AMBER}]Try it out (quick test, ~2 min):[/{theme.BRAND_AMBER}]")
    console.print(f"  [{theme.ACCENT}]  {theme.ARROW}  linkedin-cleaner extract --connections --limit 100[/{theme.ACCENT}]")
    console.print()
    console.print(f"  [{theme.BRAND_AMBER}]Ready for the full run (~1.5-2.5 hours):[/{theme.BRAND_AMBER}]")
    console.print(f"  [{theme.ACCENT}]  {theme.ARROW}  linkedin-cleaner extract --all[/{theme.ACCENT}]")
    console.print()
    console.print(f"  [dim]Or pick what you need:[/dim]")
    console.print(f"  [dim]  linkedin-cleaner extract --connections    Just your connections[/dim]")
    console.print(f"  [dim]  linkedin-cleaner extract --followers      Who follows you[/dim]")
    console.print(f"  [dim]  linkedin-cleaner extract --conversations  Your inbox threads[/dim]")
    console.print(f"  [dim]  linkedin-cleaner extract --help           All options[/dim]")
    console.print()
    console.print(f"  [{theme.BRAND_DIM}]{theme.DIVIDER_LIGHT}[/{theme.BRAND_DIM}]")
    console.print()
    console.print(f"  [{theme.BRAND_GREEN}]{theme.BULLET}[/{theme.BRAND_GREEN}] [bold]Use with Claude Code[/bold] (recommended)")
    console.print(f"     Open Claude Code in this directory and let it handle everything.")
    console.print(f"     Claude can run commands, generate missing files, and explain results.")
    console.print(f"     A CLAUDE.md is already in your workspace — just open Claude Code.")
    console.print()
    console.print(f"  [{theme.BRAND_DIM}]{theme.DIVIDER_LIGHT}[/{theme.BRAND_DIM}]")
    console.print()
    console.print(f"  [dim]Or run it manually:[/dim]")
    console.print(f"  [dim]  linkedin-cleaner --help                   All commands[/dim]")
    console.print(f"  [dim]  linkedin-cleaner <command> --help         Command details[/dim]")
    console.print(f"  [dim]  linkedin-cleaner doctor                   Check your setup[/dim]")
    console.print()
