#!/bin/bash
#
# LinkedIn Network Cleaner — One-line installer
# curl -sSL https://raw.githubusercontent.com/francima961/linkedin-network-cleaner/main/install.sh | bash
#

set -e

GREEN='\033[0;32m'
AMBER='\033[0;33m'
RED='\033[0;31m'
DIM='\033[2m'
BOLD='\033[1m'
NC='\033[0m'

echo ""
echo -e "${GREEN}${BOLD}  LinkedIn Network Cleaner — Installer${NC}"
echo -e "${DIM}  Clean your network. Keep your people.${NC}"
echo ""

# ── Check for uv (fast Python package manager) ──────────────────────────
if command -v uv &> /dev/null; then
    echo -e "  ${GREEN}✓${NC} uv found"
else
    echo -e "  ${DIM}Installing uv (Python package manager)...${NC}"
    curl -LsSf https://astral.sh/uv/install.sh | sh 2>/dev/null

    # Source the uv env so it's available in this session
    if [ -f "$HOME/.local/bin/env" ]; then
        source "$HOME/.local/bin/env"
    elif [ -f "$HOME/.cargo/env" ]; then
        source "$HOME/.cargo/env"
    fi

    # Add to PATH for this session
    export PATH="$HOME/.local/bin:$HOME/.cargo/bin:$PATH"

    if command -v uv &> /dev/null; then
        echo -e "  ${GREEN}✓${NC} uv installed"
    else
        echo -e "  ${RED}✗${NC} Could not install uv automatically."
        echo -e "  ${DIM}Try manually: https://docs.astral.sh/uv/getting-started/installation/${NC}"
        exit 1
    fi
fi

# ── Install linkedin-network-cleaner ─────────────────────────────────────
echo -e "  ${DIM}Installing linkedin-network-cleaner...${NC}"
uv tool install linkedin-network-cleaner 2>/dev/null

if command -v linkedin-cleaner &> /dev/null; then
    echo -e "  ${GREEN}✓${NC} linkedin-cleaner installed"
else
    # uv tool bin might not be in PATH yet
    UV_BIN=$(uv tool dir 2>/dev/null | head -1)
    if [ -z "$UV_BIN" ]; then
        UV_BIN="$HOME/.local/bin"
    fi
    export PATH="$UV_BIN:$HOME/.local/bin:$PATH"

    if command -v linkedin-cleaner &> /dev/null; then
        echo -e "  ${GREEN}✓${NC} linkedin-cleaner installed"
    else
        echo -e "  ${RED}✗${NC} Installation completed but 'linkedin-cleaner' not found in PATH."
        echo -e "  ${DIM}Try closing and reopening your terminal, then run: linkedin-cleaner${NC}"
        exit 1
    fi
fi

# ── Done ─────────────────────────────────────────────────────────────────
echo ""
echo -e "${GREEN}${BOLD}  Installation complete!${NC}"
echo ""
echo -e "  ${BOLD}Get started:${NC}"
echo -e "  ${DIM}  mkdir my-network && cd my-network${NC}"
echo -e "  ${DIM}  linkedin-cleaner init${NC}"
echo ""
echo -e "  ${DIM}If 'linkedin-cleaner' is not found, close and reopen your terminal first.${NC}"
echo ""
