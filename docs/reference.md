# Technical Reference

Low-level details for developers extending or debugging `linkedin-cleaner`.

---

## Edges API Rules

### Authentication

All requests use the `X-API-Key` header — **not** a Bearer token.

```
X-API-Key: your-api-key-here
```

### Request body format

The body format depends on the execution mode:

- **Live mode** (synchronous): Use `"input"` (singular)
  ```json
  {"input": {"linkedin_url": "https://linkedin.com/in/example"}}
  ```

- **Async mode**: Use `"inputs"` (plural)
  ```json
  {"inputs": [{"linkedin_url": "https://linkedin.com/in/example"}]}
  ```

### Identity modes

| Mode | Description | When to use |
|------|-------------|-------------|
| Managed | API selects an identity automatically | Most extractions |
| Direct | Explicit UUID via `identity_ids` parameter | Required for specific skills |
| Auto | API picks the best available | Fallback |

**Direct mode is required for:** extract-connections, extract-conversations, linkedin-me.

### Pagination

Primary pagination is **cursor-based** via the `X-Pagination-Next` response header. Read the cursor from this header and pass it on the next request.

Falls back to page-number pagination (`&page=N`) when cursors stop advancing.

**Never use `&page=N` as the primary pagination strategy.**

### Error handling

All LinkedIn-originated errors are wrapped in **HTTP 424** responses. Always check the `error_label` field in the response body — do not rely on the HTTP status code alone.

### Rate limits

| Error label | Meaning | Action |
|-------------|---------|--------|
| `LIMIT_REACHED` | LinkedIn hard rate limit | Stop all requests. Wait 24 hours. Resume with `--resume`. |
| `STATUS_429` | API rate limit | Exponential backoff (handled automatically by the client). |

### Other API rules

- Empty results (`[]`) are valid responses, not errors.
- Always store both `linkedin_profile_id` and `sales_navigator_profile_id` when available.

---

## Extending the Tool

### Adding a new extractor

1. Add a new method to `AudienceExtractor` in `core/extractors.py`.
2. Follow the existing pattern: call the Edges API, handle pagination, save to `data/`.
3. Wire the new method into the CLI `extract` command in `cli/commands/extract.py`.
4. Add a `--your-flag` option to the extract command.

### Adding a decision rule

Modify `DecisionEngine._decide_single_connection()` in `core/decision_engine.py`. The method receives the full scored row and returns a keep/review/remove decision with a reason string.

### Adding a network action

1. Add a method to `LinkedInActions` in `core/linkedin_actions.py`.
2. The method **must** default to dry-run mode.
3. Every action must log to **both** `logs/actions/` (audit trail) and `logs/data/` (rollback snapshot).

### Adding an analysis step

1. Add a method to `NetworkAnalyzer` in `core/analyzer.py`.
2. Wire it into the pipeline sequence.
3. Save intermediate results to `analysis/` for resume support.

---

## Configuration Reference

### `.env` — Secrets (gitignored)

```bash
EDGES_API_KEY=your-edges-api-key
ANTHROPIC_API_KEY=your-anthropic-api-key          # Optional, for AI scoring
LINKEDIN_IDENTITY_ID=your-linkedin-identity-uuid   # For direct-mode API calls
```

### `linkedin-cleaner.toml` — Settings (committable)

```toml
[extract]
delay = 1.5           # Seconds between API calls
workers = 4           # Concurrent enrichment workers

[analyze]
dm_threshold = 5      # Min total DMs for active relationship
keep_likers = true    # Keep people who liked your posts
keep_commenters = true
keep_reposters = true
keep_content_interactions = true
ai_batch_size = 20    # Profiles per AI API call

[clean]
ai_threshold = 50     # Minimum AI score to keep
batch_size = 25       # Maximum actions per run
delay = 5             # Seconds between actions
```

### Precedence

**CLI flags > `linkedin-cleaner.toml` > hardcoded defaults**

CLI flags always win. The toml file overrides built-in defaults. If neither is set, the hardcoded default applies.

---

## Further reading

For the full Edges API skill catalog, see the Edges documentation (link TBD).
