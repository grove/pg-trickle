use clap::{Parser, Subcommand, ValueEnum};

use crate::commands;

/// pg_trickle Terminal User Interface — manage and monitor stream tables.
///
/// Launch with no subcommand for the interactive TUI dashboard.
/// Use subcommands for one-shot CLI operations (scriptable, CI-friendly).
#[derive(Parser)]
#[command(name = "pgtrickle", version, about, long_about = None)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Option<Commands>,

    #[command(flatten)]
    pub connection: ConnectionArgs,
}

#[derive(Parser, Clone)]
pub struct ConnectionArgs {
    /// Full PostgreSQL connection URL (overrides PGHOST/PGPORT/etc.)
    #[arg(long = "url", short = 'U', env = "PGTRICKLE_URL")]
    pub url: Option<String>,

    /// Database host
    #[arg(long, env = "PGHOST")]
    pub host: Option<String>,

    /// Database port
    #[arg(long, env = "PGPORT")]
    pub port: Option<u16>,

    /// Database name
    #[arg(long, env = "PGDATABASE")]
    pub dbname: Option<String>,

    /// Database user
    #[arg(long, env = "PGUSER")]
    pub user: Option<String>,

    /// Database password
    #[arg(long, env = "PGPASSWORD", hide_env_values = true)]
    pub password: Option<String>,
}

#[derive(Subcommand)]
pub enum Commands {
    /// List all stream tables
    List(commands::list::ListArgs),

    /// Show detailed status of a stream table
    Status(commands::status::StatusArgs),

    /// Trigger a manual refresh
    Refresh(commands::refresh::RefreshArgs),

    /// Create a new stream table
    Create(commands::create::CreateArgs),

    /// Drop a stream table
    Drop(commands::drop::DropArgs),

    /// Alter a stream table's settings
    Alter(commands::alter::AlterArgs),

    /// Export stream table DDL
    Export(commands::export::ExportArgs),

    /// Show diagnostics and recommendations
    Diag(commands::diag::DiagArgs),

    /// Show CDC health and buffer sizes
    Cdc(commands::cdc::CdcArgs),

    /// Show dependency graph (ASCII)
    Graph(commands::graph::GraphArgs),

    /// Show or set configuration (GUC parameters)
    Config(commands::config::ConfigArgs),

    /// Run health checks
    Health(commands::health::HealthArgs),

    /// Show worker pool and job queue status
    Workers(commands::workers::WorkersArgs),

    /// Show fuse and circuit breaker status
    Fuse(commands::fuse::FuseArgs),

    /// Show watermark groups and source gating
    Watermarks(commands::watermarks::WatermarksArgs),

    /// Inspect delta SQL, operator tree, or dedup stats
    Explain(commands::explain::ExplainArgs),

    /// Watch stream table status (non-interactive, continuous output)
    Watch(commands::watch::WatchArgs),

    /// Generate shell completion scripts
    Completions(commands::completions::CompletionsArgs),
}

#[derive(Clone, Copy, ValueEnum, Default)]
pub enum OutputFormat {
    /// Human-readable table output
    #[default]
    Table,
    /// JSON output
    Json,
    /// CSV output
    Csv,
}
