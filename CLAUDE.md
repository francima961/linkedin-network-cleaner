# LinkedIn Network Cleaner

Extract, score, and clean your LinkedIn network using AI.

## Package Structure

```
linkedin_network_cleaner/
├── cli/                    # CLI layer (typer + rich)
│   ├── app.py              # Root typer app, command registration
│   ├── commands/            # One file per command
│   │   ├── init_cmd.py     # linkedin-cleaner init (guided wizard)
│   │   ├── extract.py      # linkedin-cleaner extract
│   │   ├── analyze.py      # linkedin-cleaner analyze
│   │   ├── clean.py        # linkedin-cleaner clean invites|connections|unfollow
│   │   ├── status.py       # linkedin-cleaner status (dashboard)
│   │   └── doctor.py       # linkedin-cleaner doctor (diagnostics)
│   └── ui/                 # Reusable Rich display components
│       ├── console.py      # Shared Console instance
│       ├── errors.py       # error/warn/info panel helpers
│       ├── tables.py       # Table formatters
│       ├── progress.py     # Progress bar factories
│       └── theme.py        # Colors, styles
├── core/                   # Engine (no CLI dependencies)
│   ├── config.py           # Env loading, paths, validation, toml config
│   ├── edges_client.py     # Generic Edges API client
│   ├── extractors.py       # LinkedIn data extraction (11 methods)
│   ├── enrich_profiles.py  # Concurrent profile enrichment
│   ├── analyzer.py         # 8-step analysis pipeline
│   ├── ai_scorer.py        # Two-tier AI scoring (Haiku triage + Sonnet deep)
│   ├── decision_engine.py  # Rules-based keep/review/remove decisions
│   ├── invite_analyzer.py  # Sent invitation analysis
│   ├── linkedin_actions.py # Network actions (remove, withdraw, unfollow)
│   └── session_logger.py   # Cross-session markdown log
└── templates/              # Bundled example files for init wizard
```

## Key Rules

- NEVER hardcode credentials, profile URLs, or identity UUIDs. All from .env or runtime.
- NEVER hardcode industry-specific content in AI prompts. All from user's brand strategy + persona files.
- All data paths resolve relative to WORKSPACE_DIR (cwd by default), NOT relative to package install location.
- Dry-run is ALWAYS the default for destructive actions. User must explicitly pass --execute.
- Every network-altering action logs to BOTH logs/actions/ (audit) AND logs/data/ (rollback snapshot).
- Config precedence: CLI flags > linkedin-cleaner.toml > hardcoded defaults.
- Secrets go in .env (gitignored). Settings go in linkedin-cleaner.toml (committable).

## Edges API Rules

- Auth: `X-API-Key` header (NOT Bearer token)
- Body: live mode = `"input"` (singular), async = `"inputs"` (plural)
- Pagination: cursor-based via `X-Pagination-Next` header. NEVER `&page=N` as primary.
- All LinkedIn errors wrapped in HTTP 424. Check `error_label`, not status code.
- `LIMIT_REACHED` = hard stop 24h. `STATUS_429` = exponential backoff.
- Direct mode (`identity_ids`) required for: extract-connections, extract-conversations, linkedin-me.

## CLI Design Principles

- Use `typer` + `rich` for all CLI output
- Every error shows: what went wrong, why it matters, how to fix it
- Progress bars for any operation taking >5 seconds
- Context-aware "Suggested next" after every command
- Destructive confirmations require typing the count ("remove 10"), not just "y"

## Architecture Notes

The `core/` package is the engine layer — pure Python, no CLI dependencies. The `cli/` package wraps it with typer commands and Rich output. This separation means the engine can be used programmatically without the CLI.
