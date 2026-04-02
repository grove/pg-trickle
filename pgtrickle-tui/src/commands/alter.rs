use clap::Parser;
use tokio_postgres::Client;

use crate::error::CliError;
use crate::output;

#[derive(Parser)]
pub struct AlterArgs {
    /// Name of the stream table to alter
    pub name: String,

    /// Change refresh mode (full, differential, auto, immediate)
    #[arg(long)]
    pub mode: Option<String>,

    /// Change schedule (e.g. "10s", "1m", "5m", cron expression)
    #[arg(long)]
    pub schedule: Option<String>,

    /// Change tier (hot, warm, cold)
    #[arg(long)]
    pub tier: Option<String>,

    /// Change status (active, paused, suspended)
    #[arg(long)]
    pub status: Option<String>,

    /// Change defining query
    #[arg(long)]
    pub query: Option<String>,
}

pub async fn execute(client: &Client, args: &AlterArgs) -> Result<(), CliError> {
    // Build ALTER call with only the parameters that were specified.
    let mut sql = String::from("SELECT pgtrickle.alter_stream_table(name := $1");
    let mut param_idx = 2u32;
    let mut params: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> = vec![&args.name];

    macro_rules! add_param {
        ($field:expr, $sql_name:expr) => {
            if let Some(ref val) = $field {
                sql.push_str(&format!(", {} := ${}", $sql_name, param_idx));
                param_idx += 1;
                params.push(val);
            }
        };
    }

    add_param!(args.mode, "refresh_mode");
    add_param!(args.schedule, "schedule");
    add_param!(args.tier, "tier");
    add_param!(args.status, "status");
    add_param!(args.query, "query");

    // Suppress unused variable warning — param_idx is incremented for positional tracking.
    let _ = param_idx;

    sql.push(')');

    if params.len() == 1 {
        return Err(CliError::Other(
            "specify at least one option to alter (--mode, --schedule, --tier, --status, --query)"
                .to_string(),
        ));
    }

    client
        .execute(&sql, &params)
        .await
        .map_err(|e| CliError::Query(e.to_string()))?;

    output::print_success(&format!("Altered stream table '{}'.", args.name));
    Ok(())
}
