use clap::Parser;
use tokio_postgres::Client;

use crate::error::CliError;
use crate::output;

#[derive(Parser)]
pub struct RefreshArgs {
    /// Name of the stream table to refresh
    pub name: Option<String>,

    /// Refresh all stream tables
    #[arg(long)]
    pub all: bool,
}

pub async fn execute(client: &Client, args: &RefreshArgs) -> Result<(), CliError> {
    if args.all {
        client
            .execute("SELECT pgtrickle.refresh_all()", &[])
            .await
            .map_err(|e| CliError::Query(e.to_string()))?;
        output::print_success("All stream tables refreshed.");
        return Ok(());
    }

    let name = args
        .name
        .as_deref()
        .ok_or_else(|| CliError::Other("specify a stream table name or --all".to_string()))?;

    client
        .execute("SELECT pgtrickle.refresh_stream_table($1)", &[&name])
        .await
        .map_err(|e| CliError::Query(e.to_string()))?;

    output::print_success(&format!("Refreshed '{name}'."));
    Ok(())
}
