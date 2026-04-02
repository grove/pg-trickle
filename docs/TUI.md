# pg_trickle TUI — User Guide

The `pgtrickle` TUI is a full-featured terminal user interface for managing,
monitoring, and diagnosing pg_trickle stream tables from outside SQL.

## Installation

The TUI binary is built from the `pgtrickle-tui` workspace member:

```bash
cargo build --release -p pgtrickle-tui
# Binary: target/release/pgtrickle
```

## Connecting

```bash
# Via connection URL
pgtrickle --url postgres://user:pass@host:5432/mydb

# Via environment variables (libpq compatible)
export PGHOST=localhost PGPORT=5432 PGDATABASE=mydb PGUSER=postgres
pgtrickle

# Via CLI flags
pgtrickle --host localhost --port 5432 --dbname mydb --user postgres
```

## Interactive Dashboard

Run `pgtrickle` with no subcommand to launch the interactive TUI:

```bash
pgtrickle
```

### Views

Switch between views using number keys or letter keys:

| Key | View | Description |
|-----|------|-------------|
| `1` | Dashboard | Live-updating stream table list with status ribbon |
| `2` | Detail | Properties, refresh stats, error details for selected table |
| `3` | Dependencies | ASCII tree visualization of the DAG |
| `4` | Refresh Log | Color-coded scrollable refresh timeline |
| `5` | Diagnostics | Mode recommendations with confidence levels |
| `6` | CDC Health | Buffer sizes and trigger status |
| `7` | Configuration | pg_trickle GUC parameter browser |
| `8` | Health Checks | Severity-colored system health results |
| `9` | Alerts | Real-time alert feed via LISTEN/NOTIFY |
| `w` | Workers | Parallel worker pool status and job queue |
| `f` | Fuse | Circuit breaker / fuse status per stream table |
| `m` | Watermarks | Watermark groups and source gating status |
| `d` | Delta SQL | Delta SQL inspector (links to CLI explain) |

### Keyboard Shortcuts

| Key | Action |
|-----|--------|
| `j` / `↓` | Move selection down |
| `k` / `↑` | Move selection up |
| `Enter` | Drill into detail (from Dashboard) |
| `Esc` | Go back to Dashboard / clear filter |
| `/` | Open filter input |
| `?` | Toggle help overlay |
| `q` / `Ctrl+C` | Quit |
| `Home` | Jump to first item |
| `End` | Jump to last item |

### Wide Layout

When the terminal is ≥140 columns wide and ≥35 rows tall, the Dashboard
switches to a wide split-pane layout with:

- Issues sidebar (error chains, buffer growth warnings)
- DAG mini-map

## CLI Subcommands

All subcommands support `--format json`, `--format csv`, and table output
(default). They are designed for scripting and CI integration.

```bash
pgtrickle list                    # List all stream tables
pgtrickle status <name>           # Detailed status of one table
pgtrickle refresh <name>          # Trigger manual refresh
pgtrickle create --name <n> --query <q>  # Create stream table
pgtrickle drop <name>             # Drop stream table
pgtrickle alter <name> ...        # Alter mode, schedule, tier
pgtrickle export <name>           # Export definition SQL
pgtrickle diag                    # Refresh mode diagnostics
pgtrickle cdc                     # CDC buffer health
pgtrickle graph                   # Dependency tree (ASCII)
pgtrickle config                  # GUC parameter listing
pgtrickle health                  # System health checks
pgtrickle workers                 # Worker pool & job queue
pgtrickle fuse                    # Fuse / circuit breaker status
pgtrickle watermarks              # Watermark groups & gating
pgtrickle explain <name>          # Show delta SQL
pgtrickle explain <name> --analyze   # EXPLAIN ANALYZE on delta
pgtrickle explain <name> --operators # DVM operator tree
pgtrickle explain <name> --dedup     # Deduplication stats
pgtrickle watch                   # Non-interactive continuous output
pgtrickle watch --compact         # Compact single-line format
pgtrickle watch --no-color        # No ANSI colors
pgtrickle watch --append          # Append mode (don't clear screen)
pgtrickle completions bash        # Generate shell completions
```

### Watch Mode

Watch mode provides non-interactive continuous output, suitable for
CI pipelines, log files, and terminals without full TUI support:

```bash
# Full table, refresh every 5 seconds
pgtrickle watch -n 5

# Compact single-line format for log collection
pgtrickle watch --compact --no-color --append >> /var/log/pgtrickle.log
```

## Shell Completions

Generate and install completions for your shell:

```bash
# Bash
pgtrickle completions bash > /etc/bash_completion.d/pgtrickle

# Zsh
pgtrickle completions zsh > ~/.zfunc/_pgtrickle

# Fish
pgtrickle completions fish > ~/.config/fish/completions/pgtrickle.fish

# PowerShell
pgtrickle completions powershell > pgtrickle.ps1
```

## Architecture

The TUI uses:

- **ratatui** + **crossterm** for terminal rendering
- **tokio-postgres** for async database access
- **clap** for CLI argument parsing
- Background polling every 2 seconds for live data
- Dedicated LISTEN/NOTIFY connection for real-time alerts

All views are read-only visualizations of pg_trickle's SQL API functions.
Mutating operations (refresh, create, drop, alter) are available through
the CLI subcommands.
