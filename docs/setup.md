# Setup Guide

Get up and running with `linkedin-cleaner` in a few minutes.

---

## Prerequisites

- **Python 3.10+** — Check with `python --version`
- **Edges API key** — Sign up at [https://app.edges.run](https://app.edges.run) and create an API key
- **Anthropic API key** (optional) — Required only for AI scoring. Get one at [https://console.anthropic.com](https://console.anthropic.com)

## Step 1: Install

From the project directory:

```bash
pip install .
```

Once published to PyPI:

```bash
pip install linkedin-network-cleaner
```

Verify the installation:

```bash
linkedin-cleaner --version
```

## Step 2: Initialize Your Workspace

Run the guided setup wizard:

```bash
linkedin-cleaner init
```

The wizard will:

1. Ask for your Edges API key (stored in `.env`, never committed)
2. Optionally ask for your Anthropic API key (for AI scoring)
3. Create the `assets/` directory structure
4. Copy example templates for brand strategy, personas, and CSV files
5. Generate a starter `linkedin-cleaner.toml` configuration file

## Step 3: Configure Your Assets

After `init`, customize the files in your `assets/` directory:

1. **Brand strategy** — Edit `assets/brand_strategy.md` with your company overview, target market, and value proposition. The AI scorer uses this to evaluate audience fit. See [Asset File Formats](asset-formats.md) for details.

2. **Personas** — Edit `assets/persona.md` with your target buyer personas, including role patterns, company characteristics, and fit score ranges.

3. **Customer list** (optional) — Add a CSV of your current customers to `assets/Customers/`. Connections at these companies get flagged as existing customers.

4. **Target accounts** (optional) — Add a CSV of companies you're targeting to `assets/Accounts/`.

5. **Target prospects** (optional) — Add a CSV of specific people (by LinkedIn ID) to `assets/Prospects/`.

## Step 4: Verify Your Setup

Run the diagnostics command to check that everything is configured correctly:

```bash
linkedin-cleaner doctor
```

This checks:

- Python version
- API key validity
- Asset file presence and format
- LinkedIn identity configuration
- Workspace directory structure

Fix any issues the doctor reports before proceeding.

## Step 5: Test Your Connection

Run a small extraction to verify your API key and LinkedIn identity work:

```bash
linkedin-cleaner extract --connections
```

If this succeeds, your setup is complete. See the [Operations Guide](operations.md) for the full workflow.

---

## Configuration Files

| File | Purpose | Committed to git? |
|------|---------|-------------------|
| `.env` | API keys (Edges, Anthropic) | No (gitignored) |
| `linkedin-cleaner.toml` | Thresholds, delays, model settings | Yes |
| `assets/*.md` | Brand strategy, personas | Yes |
| `assets/**/*.csv` | Customer/account/prospect lists | Your choice |

Configuration precedence: **CLI flags > linkedin-cleaner.toml > defaults**
