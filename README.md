# linkedin-network-cleaner

Extract, score, and clean your LinkedIn network using AI.

Stop posting more. Start cleaning smarter. This tool audits every connection in your LinkedIn network — extracts engagement data, scores audience fit with AI, and helps you remove the dead weight so LinkedIn's algorithm actually shows your content to the right people.

---

## What it does

1. **Extract** — Pull your full LinkedIn data via the [Edges API](https://edges.run): connections, followers, messages, posts, engagement, profile enrichment
2. **Analyze** — Run a 9-step pipeline: inbox activity, post engagement, customer matching, target account matching, and two-tier AI scoring (Haiku triage → Sonnet deep-score)
3. **Clean** — Preview who stays and who goes. Withdraw stale invites. Unfollow low-fit profiles. All with safety controls and dry-run by default.

---

## Quick Start

### Prerequisites

- Python 3.10+
- [Edges API key](https://app.edges.run) — for LinkedIn data extraction
- [Anthropic API key](https://console.anthropic.com) — optional, enables AI scoring (step 9)

### Install

```bash
git clone https://github.com/yourname/linkedin-network-cleaner.git
cd linkedin-network-cleaner
pip install .
```

### Set up your workspace

```bash
linkedin-cleaner init
```

The setup wizard walks you through everything:
- API credentials (validated in real-time)
- Brand strategy & ICP personas (import your docs or build interactively)
- Target account and prospect lists (auto-detects CSV columns)
- Family & VIP safelist (people who are never removed)

### Run it

```bash
# 1. Extract your LinkedIn data
linkedin-cleaner extract --all

# 2. Score every connection
linkedin-cleaner analyze

# 3. See the results
linkedin-cleaner clean connections --dry-run
```

---

## The Three Phases

### Phase 1: Extract

Pull everything LinkedIn knows about your network.

```bash
linkedin-cleaner extract --all              # Everything (1.5-2.5 hours)
linkedin-cleaner extract --connections      # Just connections
linkedin-cleaner extract --enrichment       # Full profile enrichment
linkedin-cleaner extract --messages --resume # Resume from checkpoint
```

Extractions are checkpointed — if you hit a rate limit, resume later with `--resume`.

**What gets extracted:**

| Data | What it is | Why it matters |
|------|-----------|----------------|
| Connections | Your full connection list | The base dataset |
| Followers | Who follows you | Follower overlap signals |
| Conversations + Messages | Full inbox history | Active DM relationship detection |
| Posts + Engagement | Likers, commenters, reposters | Content engagement signals |
| Reaction + Comment Activity | Posts you engaged with | Reciprocal engagement |
| Sent Invitations | Pending connection requests | Stale invite cleanup |
| Profile Enrichment | Full profile data for every connection | Job titles, skills, experience for AI scoring |

### Phase 2: Analyze

A 9-step pipeline that scores every connection.

```bash
linkedin-cleaner analyze                    # Full pipeline
linkedin-cleaner analyze --no-ai            # Steps 1-8 only (no AI cost)
linkedin-cleaner analyze --resume           # Resume from last step
linkedin-cleaner analyze --step 5           # Run a single step
```

**Pipeline steps:**

| Step | What it does | Signal produced |
|------|-------------|----------------|
| 1 | Build base (connections + followers) | Master DataFrame |
| 2 | Analyze inbox activity | `active_dms` flag, message counts |
| 3 | Analyze post engagement | Like/comment/repost counts |
| 4 | Analyze content interactions | Your engagement with their posts |
| 5 | Enrich for matching | Shared schools, shared work experience |
| 6 | Match customers | Current + former customer company flags |
| 7 | Match target accounts | Target account flags |
| 8 | Match target prospects | Target prospect flags |
| 9 | AI scoring (Haiku → Sonnet) | 0-100 audience fit score + ICP tag |

Before step 9, you'll see a cost estimate and can choose to proceed or skip.

### Phase 3: Clean

Preview decisions, then act.

```bash
# Preview what would happen
linkedin-cleaner clean connections --dry-run

# Export decisions to CSV for manual review
linkedin-cleaner clean connections --export

# Withdraw stale invitations
linkedin-cleaner clean invites --execute

# Unfollow removed profiles
linkedin-cleaner clean unfollow --from-file decisions.csv --execute
```

The dry-run shows a full breakdown: who's being kept and why, who's getting cut and why, sample decisions with scores, and network metrics.

**Decision cascade** (first match wins):

| Priority | Signal | Decision |
|----------|--------|----------|
| 0 | Safelist (family, VIPs) | Keep — always |
| 1 | Real network (10+ messages both ways) | Keep |
| 2 | Customer or former customer | Keep |
| 3 | Target account or prospect | Keep |
| 4 | Engaged with your content | Keep |
| 5 | Shared school or work experience | Keep |
| 6 | AI score >= threshold (default 50) | Keep |
| 7 | Two-way messages (below DM threshold) | Review |
| 8 | Everything else | Remove |

**Safety controls:**
- Dry-run is always the default — you must explicitly pass `--execute`
- Execute mode requires typing the count to confirm (e.g., `"withdraw 25"`)
- Every action is logged to `logs/actions/` (audit trail) and `logs/data/` (rollback snapshots)
- Batch size limits (default 25) with configurable delay between actions

---

## Configuration

### `.env` — API credentials (gitignored)

```
EDGES_API_KEY=your_key_here
EDGES_IDENTITY_UUID=your_uuid_here
ANTHROPIC_API_KEY=your_key_here    # optional
```

### `linkedin-cleaner.toml` — Settings

```toml
[extract]
delay = 1.5                    # Seconds between API calls
enrichment_workers = 0         # 0 = auto-detect

[analyze]
dm_threshold = 5               # Min total DMs for active relationship
keep_likers = true             # Keep people who liked your posts
keep_commenters = true         # Keep people who commented on your posts
keep_reposters = true          # Keep people who reposted your content
keep_content_interactions = true # Keep people whose content you engaged with
ai_model = "claude-sonnet-4-6"
ai_batch_size = 20

[clean]
ai_threshold = 50              # Min AI score to keep
batch_size = 25                # Max actions per run
delay = 5                      # Seconds between actions

[safelist]
# These profiles are NEVER removed
profiles = [
    "https://www.linkedin.com/in/your-family-member",
]

[keep_rules]
# Additional keep signals
keep_locations = []            # e.g., ["Lebanon", "France"]
keep_companies = []            # e.g., ["My Company"]
keep_title_keywords = []       # e.g., ["founder", "investor"]
```

CLI flags override toml settings. Toml overrides defaults.

### Asset files

Place these in `assets/` (created by `linkedin-cleaner init`):

| File | Purpose | Format |
|------|---------|--------|
| `brand_strategy.md` | Your company, market, value prop — fed to AI scorer | Markdown, any `.md` with "brand" in name |
| `Persona_ICP.md` | Target personas — who belongs in your network | Markdown, any `.md` with "persona" or "icp" in name |
| `Customers/*.csv` | Your customer companies | CSV with `company_name` column |
| `Accounts/*.csv` | Target companies you're pursuing | CSV with company name column (auto-detected) |
| `Prospects/*.csv` | Target people by LinkedIn ID | CSV with LinkedIn ID column (auto-detected) |

See [docs/asset-formats.md](docs/asset-formats.md) for detailed format specs and examples.

---

## Utility Commands

```bash
# See where you are in the workflow
linkedin-cleaner status

# Validate your setup
linkedin-cleaner doctor
```

---

## How it works under the hood

**Data layer**: The [Edges API](https://edges.run) handles all LinkedIn data extraction. No browser automation, no scraping. API key authentication.

**AI scoring**: Two-tier system using Anthropic's Claude models. Haiku triages all profiles into KEEP/REMOVE/REVIEW (~$0.001 per profile). Sonnet deep-scores the ambiguous REVIEW cases with your brand strategy and ICP context (~$0.004 per profile). Your prompts are generated from YOUR brand strategy and persona files — not hardcoded to any industry.

**Actions**: Invite withdrawal and unfollow use the Edges API natively. Connection removal is coming in a future release.

**Safety**: Every destructive action defaults to dry-run. Execute mode requires explicit confirmation. Dual logging ensures you can audit and roll back.

---

## Cost

Typical costs for a 10,000-connection network:

| Component | Estimated cost |
|-----------|---------------|
| Edges API (extraction + actions) | ~$3-5 |
| Anthropic API (AI scoring) | ~$10-15 |
| **Total** | **~$15-20** |

Actual costs depend on network size and how many profiles need deep scoring.

---

## Troubleshooting

| Problem | Fix |
|---------|-----|
| `LIMIT_REACHED` | LinkedIn rate limit — wait 24 hours, then `--resume` |
| `STATUS_429` | API rate limit — automatic backoff, just wait |
| Empty extracts | Run `linkedin-cleaner doctor` to check credentials |
| AI scoring errors | Check Anthropic key at console.anthropic.com |
| Resume not working | Check `analysis/pipeline_state.json` |

See [docs/operations.md](docs/operations.md) for the full troubleshooting guide.

---

## Project Structure

```
linkedin_network_cleaner/
├── cli/                    # CLI layer (typer + rich)
│   ├── commands/           # init, extract, analyze, clean, status, doctor
│   └── ui/                 # Console, errors, tables, progress, theme
├── core/                   # Engine (no CLI dependencies)
│   ├── edges_client.py     # Edges API client
│   ├── extractors.py       # 11 extraction methods
│   ├── analyzer.py         # 9-step pipeline
│   ├── ai_scorer.py        # Two-tier AI scoring
│   ├── decision_engine.py  # Keep/review/remove cascade
│   └── linkedin_actions.py # Withdraw invites, unfollow
└── templates/              # Example asset files for init wizard
```

---

## License

MIT
