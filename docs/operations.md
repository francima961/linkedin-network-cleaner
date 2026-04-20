# Operations Guide

The `linkedin-cleaner` workflow has three phases: **Extract**, **Analyze**, and **Clean**. Each phase builds on the previous one.

---

## Phase 1 — Extract

Pull data from LinkedIn via the Edges API.

```bash
linkedin-cleaner extract [OPTIONS]
```

### Data options

| Flag | What it extracts |
|------|-----------------|
| `--all` | Everything below |
| `--connections` | Your 1st-degree connections |
| `--followers` | People following you |
| `--profile-viewers` | People who viewed your profile |
| `--conversations` | Inbox conversation list |
| `--messages` | Message content (requires `--conversations` first) |
| `--posts` | Your published posts |
| `--post-engagement` | Reactions/comments on your posts (requires `--posts` first) |
| `--reaction-activity` | Posts you reacted to |
| `--comment-activity` | Posts you commented on |
| `--sent-invites` | Pending sent invitations |
| `--enrichment` | Full profile data for each connection |

### Control options

| Flag | Description | Default |
|------|-------------|---------|
| `--resume` | Resume from checkpoint after interruption | Off |
| `--delay N` | Seconds between API calls | 1.5 |
| `--workers N` | Concurrent enrichment workers | Auto-detect |

### Chained extractions

Some extractions depend on others:

- `--messages` requires `--conversations` to have been run first
- `--post-engagement` requires `--posts` to have been run first

If you use `--all`, dependencies are handled automatically.

### Example

```bash
# Extract connections and enrich profiles
linkedin-cleaner extract --connections --enrichment

# Resume after a rate limit interruption
linkedin-cleaner extract --connections --resume
```

---

## Phase 2 — Analyze

Score and classify your connections through a 9-step pipeline.

```bash
linkedin-cleaner analyze [OPTIONS]
```

### Pipeline steps

| Step | Name | Description |
|------|------|-------------|
| 1 | Build base | Create the master connection table |
| 2 | Inbox activity | Score based on message frequency |
| 3 | Post engagement | Score based on reactions/comments on your content |
| 4 | Content interactions | Score based on your activity on their content |
| 5 | Enrich for matching | Prepare enriched profiles for company/persona matching |
| 6 | Match customers | Flag connections at your customer companies |
| 7 | Match target accounts | Flag connections at companies you're targeting |
| 8 | Match target prospects | Flag specific people by LinkedIn ID |
| 9 | AI scoring | Two-tier AI evaluation (Haiku triage + Sonnet deep scoring) |

### Options

| Flag | Description | Default |
|------|-------------|---------|
| `--resume` | Resume from last completed step | Off |
| `--no-ai` | Run steps 1–8 only, skip AI scoring | Off |
| `--step N` | Run only step N | All steps |
| `--dm-threshold N` | Min total DMs for active relationship | 5 |
| `--ai-batch-size N` | Profiles per AI API call | 20 |
| `--profile-url URL` | Your LinkedIn profile URL (auto-detected if omitted) | Auto |
| `--limit N` | Sample N rows for testing | All rows |

### Example

```bash
# Full analysis pipeline
linkedin-cleaner analyze

# Run without AI scoring (steps 1-8 only)
linkedin-cleaner analyze --no-ai

# Test on a small sample first
linkedin-cleaner analyze --limit 50

# Resume after interruption
linkedin-cleaner analyze --resume
```

---

## Phase 3 — Clean

Take action on your network based on analysis results.

```bash
linkedin-cleaner clean <subcommand> [OPTIONS]
```

### Subcommands

| Subcommand | What it does |
|------------|-------------|
| `invites` | Manage sent invitations — withdraw stale or low-value invites |
| `connections` | Manage connections — preview removal decisions (removal coming soon) |
| `unfollow` | Unfollow profiles you no longer want in your feed |

### Shared options

| Flag | Description | Default |
|------|-------------|---------|
| `--dry-run` | Preview actions without executing | **On** (default) |
| `--export` | Export action plan to CSV for review | Off |
| `--execute` | Actually perform the actions | Off |
| `--ai-threshold N` | Minimum AI score to keep a connection | 50 |
| `--batch-size N` | Maximum actions per run | 25 |
| `--delay N` | Seconds between actions | 5 |
| `--review-file PATH` | Use a pre-reviewed decisions CSV | None |

### Recommended workflow

1. **Preview** what would happen:
   ```bash
   linkedin-cleaner clean connections --dry-run
   ```

2. **Export** the action plan for manual review:
   ```bash
   linkedin-cleaner clean connections --export
   ```

3. **Edit** the exported CSV — change decisions for specific connections.

4. **Execute** with the reviewed file:
   ```bash
   linkedin-cleaner clean connections --execute --review-file reviewed.csv
   ```

---

## Utility Commands

### Status dashboard

See the current state of your workspace — what has been extracted, analyzed, and cleaned.

```bash
linkedin-cleaner status
```

### Environment diagnostics

Check that all prerequisites are met and configuration is valid.

```bash
linkedin-cleaner doctor
```

### Setup wizard

Re-run the initial setup (safe to run multiple times).

```bash
linkedin-cleaner init
```

---

## Troubleshooting

| Problem | Cause | Fix |
|---------|-------|-----|
| `LIMIT_REACHED` error | LinkedIn rate limit (24-hour cooldown) | Wait 24 hours, then re-run with `--resume` |
| `STATUS_429` error | API rate limit | Automatic exponential backoff — just wait |
| Empty extracts | Bad API key or identity UUID | Run `linkedin-cleaner doctor` to check credentials |
| HTTP 424 errors | LinkedIn returned an error | Check `error_label` in the log output, not the HTTP status code |
| Connection removal unavailable | Not yet supported in public release | Use `--dry-run` and `--export` to review decisions; removal coming soon |
| AI scoring errors | Bad Anthropic API key or no model access | Verify your Anthropic API key and check model access at console.anthropic.com |
| Resume not working | Corrupted pipeline state | Check `analysis/pipeline_state.json`; step snapshots are in `analysis/` |
