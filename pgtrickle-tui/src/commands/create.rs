use clap::Parser;
use tokio_postgres::Client;

use crate::error::CliError;
use crate::output;

#[derive(Parser)]
pub struct CreateArgs {
    /// Name of the stream table to create
    pub name: String,

    /// Defining SQL query
    pub query: String,

    /// Refresh schedule (e.g. "10s", "1m", "5m", cron expression)
    #[arg(long)]
    pub schedule: Option<String>,

    /// Refresh mode (full, differential, auto, immediate)
    #[arg(long)]
    pub mode: Option<String>,

    /// Don't populate the stream table immediately
    #[arg(long)]
    pub no_initialize: bool,
}

pub async fn execute(client: &Client, args: &CreateArgs) -> Result<(), CliError> {
    let initialize = !args.no_initialize;

    // Build the SQL call with named parameters.
    let mut sql =
        "SELECT pgtrickle.create_stream_table(name := $1, query := $2, initialize := $3"
            .to_string();
    let mut param_idx = 4u32;

    let schedule_placeholder = if args.schedule.is_some() {
        let p = format!(", schedule := ${param_idx}");
        param_idx += 1;
        Some(p)
    } else {
        None
    };

    let mode_placeholder = if args.mode.is_some() {
        Some(format!(", refresh_mode := ${param_idx}"))
    } else {
        None
    };

    if let Some(ref p) = schedule_placeholder {
        sql.push_str(p);
    }
    if let Some(ref p) = mode_placeholder {
        sql.push_str(p);
    }
    sql.push(')');

    // Build parameter list dynamically.
    let mut params: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> =
        vec![&args.name, &args.query, &initialize];

    if let Some(ref schedule) = args.schedule {
        params.push(schedule);
    }
    if let Some(ref mode) = args.mode {
        params.push(mode);
    }

    client
        .execute(&sql, &params)
        .await
        .map_err(|e| CliError::Query(e.to_string()))?;

    output::print_success(&format!("Created stream table '{}'.", args.name));
    Ok(())
}
