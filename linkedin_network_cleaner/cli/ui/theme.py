"""Vintage 70s retro-terminal theme for linkedin-network-cleaner."""

# ── Brand colors (warm retro palette) ────────────────────────────────────
BRAND_GREEN = "#00ff41"      # Terminal green — data, success, keeps
BRAND_RED = "#ff3333"        # Bright red — removals, errors
BRAND_AMBER = "#ffdd00"      # Bright yellow — warnings, review, structure
BRAND_ORANGE = "#ff8c00"     # Orange — commands, actions, CTAs
BRAND_CYAN = "#00e5ff"       # Cyan — reserved (unused currently)
BRAND_BLUE = "#5dadec"       # Bright blue — recommendations, CTAs
BRAND_PURPLE = "#b388ff"     # Bright purple — numbers, data values, dates
BRAND_DIM = "#666666"        # Dim gray — secondary text
BRAND_WHITE = "#ffffff"      # Bright white — primary text

# ── Semantic colors ───────────────────────────────────────────────────────
SUCCESS = BRAND_GREEN
ERROR = f"bold {BRAND_RED}"
WARNING = BRAND_AMBER
INFO = BRAND_ORANGE
DIM = "dim"
ACCENT = BRAND_BLUE
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
INFO_PANEL_STYLE = BRAND_ORANGE

# ── App branding ─────────────────────────────────────────────────────────
APP_NAME = "linkedin-network-cleaner"
APP_VERSION = "0.1.0"
APP_TAGLINE = "Clean your network. Keep your people."

# ── ASCII art banner (compact gradient punch) ────────────────────────────
BANNER_TOP    = "░▒▓█████████████████████████████████████████████████████████▓▒░"
BANNER_SIDE_L = "█"
BANNER_SIDE_R = "█"
BANNER_BOT    = "░▒▓█████████████████████████████████████████████████████████▓▒░"
BANNER_LOGO = [
    "   ██╗     ██╗███╗   ██╗██╗  ██╗███████╗██████╗       ",
    "   ██║     ██║████╗  ██║██║ ██╔╝██╔════╝██╔══██╗      ",
    "   ██║     ██║██╔██╗ ██║█████╔╝ █████╗  ██║  ██║      ",
    "   ██║     ██║██║╚██╗██║██╔═██╗ ██╔══╝  ██║  ██║      ",
    "   ███████╗██║██║ ╚████║██║  ██╗███████╗██████╔╝      ",
    "   ╚══════╝╚═╝╚═╝  ╚═══╝╚═╝  ╚═╝╚══════╝╚═════╝       ",
]
BANNER_WIDTH = len(BANNER_TOP)  # inner width between side borders

# ── Decorative elements ─────────────────────────────────────────────────
DIVIDER_HEAVY = "▓" * 55
DIVIDER_LIGHT = "─" * 55
DIVIDER_DOT = "· " * 27 + "·"
BULLET = "▸"
ARROW = "→"
DIAMOND = "◆"
RADIO_ON = "◉"
RADIO_OFF = "○"

# ── Personality copy (headers and transitions) ───────────────────────────
COPY = {
    # Extract
    "extract_header": "Pulling your LinkedIn data",
    "extract_complete": "Data loaded. Let's see what we're working with.",
    # Analyze
    "analyze_header": "Scoring every connection",
    "analyze_complete": "Every connection scored. Here's the truth about your network.",
    "analyze_limited": "Analysis limited — most steps had insufficient data.",
    "analyze_partial": "Analysis done. Some steps had no data — it's a start.",
    # Clean
    "clean_header": "Network audit results",
    "clean_connections_reveal": "Every connection scored. Here's the truth.",
    "clean_invites_header": "Invitation audit",
    # Status
    "status_header": "Your network at a glance",
    "status_complete": "Your network is clean.",
    # Doctor
    "doctor_header": "Environment check",
    # Init
    "init_header": "Setup Wizard",
    # Misc personality
    "welcome": "Your LinkedIn network deserves a spring clean.",
    "goodbye": "Go forth and network responsibly.",
}

# ── Phase icons for extract command ─────────────────────────────────────
PHASE_ICONS = {
    1: "📡",
    2: "📝",
    3: "🔍",
    4: "🧬",
}
