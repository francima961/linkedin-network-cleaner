"""
Environment configuration and path resolution for LinkedIn Network Cleaner.
Loads credentials from .env, exposes paths for extracts and logs.
"""

import atexit
import logging
import os
import sys
from pathlib import Path
from dotenv import load_dotenv

try:
    import tomllib
except ModuleNotFoundError:
    import tomli as tomllib  # pip install tomli for Python 3.10

logger = logging.getLogger(__name__)

# Package directory (for accessing bundled templates)
PACKAGE_DIR = Path(__file__).resolve().parent.parent

# Workspace directory (where user data lives — defaults to cwd)
WORKSPACE_DIR = Path(os.getenv("LNC_WORKSPACE", "")).resolve() if os.getenv("LNC_WORKSPACE") else Path.cwd()

# Load .env from workspace
load_dotenv(WORKSPACE_DIR / ".env")

# --- Credentials ---
API_KEY = os.getenv("EDGES_API_KEY", "")
IDENTITY_UUID = os.getenv("EDGES_IDENTITY_UUID", "")
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")

# --- Paths ---
EXTRACTS_DIR = WORKSPACE_DIR / "extracts"
ANALYSIS_DIR = WORKSPACE_DIR / "analysis"
ASSETS_DIR = WORKSPACE_DIR / "assets"
CUSTOMERS_DIR = ASSETS_DIR / "Customers"
ACTIONS_LOG_DIR = WORKSPACE_DIR / "logs" / "actions"
DATA_LOG_DIR = WORKSPACE_DIR / "logs" / "data"

# --- Lock ---
LOCK_FILE = WORKSPACE_DIR / ".agent.lock"

# --- API ---
BASE_URL = "https://api.edges.run/v1"


def acquire_lock():
    """
    Acquire an exclusive lock to prevent concurrent script runs.
    If another script is already running, prints an error and exits.
    The lock is automatically released on process exit.
    """
    if LOCK_FILE.exists():
        try:
            pid = int(LOCK_FILE.read_text().strip())
            # Check if the process is still alive
            os.kill(pid, 0)
            print(
                f"ERROR: Another agent script is already running (PID {pid}).\n"
                f"Only one script can run at a time to avoid LinkedIn rate limits.\n"
                f"If the previous run crashed, delete: {LOCK_FILE}",
                file=sys.stderr,
            )
            sys.exit(1)
        except (ProcessLookupError, ValueError):
            # Process is dead or PID is garbage — stale lock, safe to take over
            pass

    LOCK_FILE.write_text(str(os.getpid()))
    atexit.register(_release_lock)


def _release_lock():
    """Remove the lock file on exit."""
    try:
        if LOCK_FILE.exists():
            # Only remove if we own it
            pid = int(LOCK_FILE.read_text().strip())
            if pid == os.getpid():
                LOCK_FILE.unlink()
    except Exception:
        pass


def validate():
    """Validate that required environment variables are set. Raises ValueError on failure."""
    missing = []
    if not API_KEY:
        missing.append("EDGES_API_KEY")
    if not IDENTITY_UUID:
        missing.append("EDGES_IDENTITY_UUID")
    if missing:
        raise ValueError(
            f"Missing required environment variables: {', '.join(missing)}. "
            f"Create .env in your workspace with these values."
        )



def ensure_dirs():
    """Create output directories if they don't exist."""
    for d in (EXTRACTS_DIR, ANALYSIS_DIR, ACTIONS_LOG_DIR, DATA_LOG_DIR):
        d.mkdir(parents=True, exist_ok=True)


# ── Asset file discovery ─────────────────────────────────────────────────────

def _glob_case_insensitive(directory: Path, pattern: str) -> list[Path]:
    """Glob for files matching pattern (case-insensitive on the stem)."""
    results = []
    if not directory.exists():
        return results
    for p in directory.iterdir():
        if p.is_file() and p.suffix.lower() == ".md":
            if pattern.lower() in p.stem.lower():
                results.append(p)
    return results


def find_asset_files(assets_dir: Path) -> tuple[Path | None, Path | None]:
    """
    Find brand strategy and ICP/persona markdown files using convention-based search.

    Returns (brand_path, persona_path) or None for missing files.
    """
    # Brand strategy: exact name first, then keyword search
    brand_path = None
    exact_brand = assets_dir / "brand_strategy.md"
    if exact_brand.exists():
        brand_path = exact_brand
    else:
        candidates = _glob_case_insensitive(assets_dir, "brand")
        if candidates:
            brand_path = candidates[0]

    # ICP/Persona: Persona*.md first, then keyword search
    persona_path = None
    persona_glob = list(assets_dir.glob("Persona*.md"))
    if persona_glob:
        persona_path = persona_glob[0]
    else:
        candidates = _glob_case_insensitive(assets_dir, "persona")
        if not candidates:
            candidates = _glob_case_insensitive(assets_dir, "icp")
        if candidates:
            persona_path = candidates[0]

    return brand_path, persona_path


# ── TOML config loading ─────────────────────────────────────────────────────

def load_config():
    """Load linkedin-cleaner.toml from workspace. Returns dict with defaults."""
    defaults = {
        "extract": {"delay": 1.5, "enrichment_workers": 0},
        "analyze": {"inbox_max": 10, "inbox_min": 5, "ai_model": "claude-sonnet-4-6", "ai_batch_size": 20},
        "clean": {"ai_threshold": 50, "stale_days": 21, "batch_size": 25, "delay": 5},
    }
    config_path = WORKSPACE_DIR / "linkedin-cleaner.toml"
    if not config_path.exists():
        return defaults
    try:
        with open(config_path, "rb") as f:
            user_config = tomllib.load(f)
        # Merge: user values override defaults
        for section, section_defaults in defaults.items():
            if section in user_config:
                section_defaults.update(user_config[section])
        return defaults
    except Exception as e:
        logger.warning("Failed to load config from %s: %s", config_path, e)
        return defaults


def load_safelist():
    """Load the safelist from linkedin-cleaner.toml. Returns set of profile URLs and IDs."""
    config_path = WORKSPACE_DIR / "linkedin-cleaner.toml"
    safelist = set()
    if not config_path.exists():
        return safelist
    try:
        with open(config_path, "rb") as f:
            user_config = tomllib.load(f)
        profiles = user_config.get("safelist", {}).get("profiles", [])
        for p in profiles:
            p = str(p).strip()
            if p:
                safelist.add(p)
                # Also extract profile handle from URL if possible
                # e.g., https://www.linkedin.com/in/handle → "handle"
                import re
                match = re.search(r"linkedin\.com/in/([^/?]+)", p)
                if match:
                    safelist.add(match.group(1).strip("/").lower())
        return safelist
    except Exception:
        return safelist


def load_keep_rules():
    """Load custom keep rules from linkedin-cleaner.toml."""
    config_path = WORKSPACE_DIR / "linkedin-cleaner.toml"
    defaults = {"keep_locations": [], "keep_companies": [], "keep_title_keywords": []}
    if not config_path.exists():
        return defaults
    try:
        with open(config_path, "rb") as f:
            user_config = tomllib.load(f)
        rules = user_config.get("keep_rules", {})
        for key in defaults:
            if key in rules:
                defaults[key] = [str(v).strip().lower() for v in rules[key] if v]
        return defaults
    except Exception:
        return defaults
