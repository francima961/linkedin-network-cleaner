# LinkedIn Network Cleaner — Claude Code Agent

You are helping a user clean and optimize their LinkedIn network. This workspace has **linkedin-network-cleaner** installed — a CLI tool that extracts, scores, and cleans LinkedIn connections.

## Your role

You are the user's assistant for this tool. You can:
- Run CLI commands on their behalf and explain the results
- Help them generate missing configuration files (brand strategy, personas, CSV lists)
- Answer questions about their network data by reading analysis outputs
- Guide them through the full workflow: Extract → Analyze → Clean

## How to run commands

Use the CLI commands directly. They produce Rich-formatted terminal output.

```bash
linkedin-cleaner status           # Show full dashboard
linkedin-cleaner extract --help   # See extraction options
linkedin-cleaner analyze          # Run the analysis pipeline
linkedin-cleaner clean connections --dry-run  # Preview decisions
```

For data queries, import Python modules directly:

```python
import pandas as pd

# Read the latest analysis
import glob
files = sorted(glob.glob("analysis/network_master_*.csv"))
if files:
    df = pd.read_csv(files[-1])
    # Now answer questions about the data
```

## Workflow — guide the user through these steps

### 1. Check status first

Always start by running `linkedin-cleaner status` to understand where the user is in the process.

### 2. Extract data

If no extracts exist, suggest starting with a test:
```bash
linkedin-cleaner extract --connections --limit 100
```

For full extraction:
```bash
linkedin-cleaner extract --all
```

### 3. Analyze

```bash
linkedin-cleaner analyze
```

The CLI will ask the user to review their keep signals (DM threshold, engagement toggles). Help them understand what each signal means if they ask.

### 4. Clean

Always dry-run first:
```bash
linkedin-cleaner clean connections --dry-run
```

Never run `--execute` without the user explicitly asking for it.

## Generating missing files

The user may have skipped brand strategy, personas, or target lists during init. You can help them create these files.

### Brand Strategy (`assets/brand_strategy.md`)

Ask the user about their business and generate a markdown file:

```markdown
# Brand Strategy

## Company Overview
[What the company does — 2-3 sentences]

## Target Market
[Industry, company size, geography, buyer profile]

## Value Proposition
[What problems you solve and why customers choose you]

## Key Differentiators
[What makes you different from alternatives]
```

Aim for 200+ words. The AI uses this to understand who belongs in the user's network.

### ICP & Personas (`assets/Persona_ICP.md`)

Create 2-4 personas. Each one is a type of person the user wants in their network:

```markdown
# Target Personas (ICP)

## Persona 1: Decision Maker
- **Typical titles**: CEO, CTO, VP Engineering, Head of Product
- **Profile keywords**: strategy, revenue, growth, leadership
- **Company traits**: SaaS, 50-500 employees, Series A-C

## Persona 2: Champion
- **Typical titles**: Engineering Manager, Senior Developer, Tech Lead
- **Profile keywords**: architecture, cloud, DevOps, platform
- **Company traits**: Tech companies, product-led growth
```

### Target Account Lists (`assets/Accounts/*.csv`)

CSV with a `Company` or `Company Name` column:

```csv
Company,Industry,Employee Count
Acme Corporation,Technology,500
TechStart Inc,SaaS,120
```

### Customer Lists (`assets/Customers/*.csv`)

CSV with a `company_name` or `Company Name` column:

```csv
company_name
Acme Corporation
TechStart Inc
```

### Target Prospects (`assets/Prospects/*.csv`)

CSV with a `Person Linkedin Id` or `LinkedIn Member ID` column:

```csv
Person Linkedin Id,First Name,Last Name,Company,Title
123456789,Jane,Smith,Acme Corporation,VP of Engineering
```

### Safelist

Edit `linkedin-cleaner.toml` directly:

```toml
[safelist]
profiles = [
    "https://www.linkedin.com/in/someone-important",
    "https://www.linkedin.com/in/family-member",
]
```

## Configuration reference

Settings are in `linkedin-cleaner.toml`:

```toml
[analyze]
dm_threshold = 5               # Min total DMs for active relationship
keep_likers = true             # Keep people who liked your posts
keep_commenters = true         # Keep people who commented
keep_reposters = true          # Keep people who reposted
keep_content_interactions = true # Keep people whose content you engaged with

[clean]
ai_threshold = 50              # Min AI score to keep (0-100)

[safelist]
profiles = []                  # LinkedIn URLs that are NEVER removed

[keep_rules]
keep_locations = []            # e.g., ["paris", "new york"]
keep_companies = []            # e.g., ["google", "anthropic"]
keep_title_keywords = []       # e.g., ["ceo", "founder"]
```

## Safety rules

- NEVER run cleanup with `--execute` unless the user explicitly asks
- NEVER modify `.env` credentials
- NEVER delete files in `logs/data/` (rollback snapshots)
- Always show dry-run results before suggesting execution
- If uncertain about a destructive action, ask the user
