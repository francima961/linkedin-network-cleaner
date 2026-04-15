"""Retro-terminal inspired theme for linkedin-network-cleaner."""

# ── Brand colors (retro terminal palette) ─────────────────────────────────
BRAND_GREEN = "#00ff41"      # Matrix/terminal green — keeps, success
BRAND_RED = "#ff3333"        # Bright red — removals, errors
BRAND_AMBER = "#ffb000"      # Amber — warnings, review
BRAND_CYAN = "#00e5ff"       # Cyan — commands, accents, links
BRAND_DIM = "#666666"        # Dim gray — secondary text
BRAND_WHITE = "#e0e0e0"      # Soft white — primary text

# ── Semantic colors ───────────────────────────────────────────────────────
SUCCESS = BRAND_GREEN
ERROR = f"bold {BRAND_RED}"
WARNING = BRAND_AMBER
INFO = BRAND_CYAN
DIM = "dim"
ACCENT = BRAND_CYAN
HIGHLIGHT = f"bold {BRAND_WHITE}"

# ── Decision tags (the signature look) ────────────────────────────────────
TAG_KEEP = f"[bold {BRAND_GREEN}]\\[KEEP][/bold {BRAND_GREEN}]"
TAG_REMOVE = f"[bold {BRAND_RED}]\\[REMOVE][/bold {BRAND_RED}]"
TAG_REVIEW = f"[bold {BRAND_AMBER}]\\[REVIEW][/bold {BRAND_AMBER}]"

# ── Status indicators ────────────────────────────────────────────────────
CHECK_OK = f"[{BRAND_GREEN}]✓[/{BRAND_GREEN}]"
CHECK_FAIL = f"[{BRAND_RED}]✗[/{BRAND_RED}]"
CHECK_WARN = f"[{BRAND_AMBER}]!![/{BRAND_AMBER}]"
CHECK_SKIP = f"[{BRAND_DIM}]--[/{BRAND_DIM}]"

# ── Panel styles ─────────────────────────────────────────────────────────
ERROR_PANEL_STYLE = BRAND_RED
WARNING_PANEL_STYLE = BRAND_AMBER
INFO_PANEL_STYLE = BRAND_CYAN

# ── App branding ─────────────────────────────────────────────────────────
APP_NAME = "linkedin-network-cleaner"
APP_VERSION = "0.1.0"
APP_TAGLINE = "Clean your network. Keep your people."

# ── Personality copy (headers and transitions) ───────────────────────────
COPY = {
    "extract_header": "Pulling your LinkedIn data",
    "extract_complete": "Data loaded. Let's see what we're working with.",
    "analyze_header": "Scoring every connection",
    "analyze_complete": "Every connection scored. Here's the truth about your network.",
    "clean_header": "Network audit results",
    "clean_connections_reveal": "Every connection scored. Here's the truth.",
    "clean_invites_header": "Invitation audit",
    "status_header": "Your network at a glance",
    "status_complete": "Your network is clean.",
    "doctor_header": "Environment check",
}
