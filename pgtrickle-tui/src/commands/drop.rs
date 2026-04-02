use clap::Parser;
use tokio_postgres::Client;

use crate::error::CliError;
use crate::output;

#[derive(Parser)]
pub struct DropArgs {
    /// Name of the stream table to drop
    pub name: String,
}

pub async fn execute(client: &Client, args: &DropArgs) -> Result<(), CliError> {
    client
        .execute("SELECT pgtrickle.drop_stream_table($1)", &[&args.name])
        .await
        .map_err(|e| CliError::Query(e.to_string()))?;

    output::print_success(&format!("Dropped stream table '{}'.", args.name));
    Ok(())
}
