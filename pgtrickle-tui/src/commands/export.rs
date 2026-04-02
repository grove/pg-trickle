use clap::Parser;
use tokio_postgres::Client;

use crate::error::CliError;

#[derive(Parser)]
pub struct ExportArgs {
    /// Name of the stream table to export
    pub name: String,
}

pub async fn execute(client: &Client, args: &ExportArgs) -> Result<(), CliError> {
    let rows = client
        .query("SELECT pgtrickle.export_definition($1)", &[&args.name])
        .await
        .map_err(|e| CliError::Query(e.to_string()))?;

    let row = rows
        .first()
        .ok_or_else(|| CliError::NotFound(format!("stream table '{}' not found", args.name)))?;

    let ddl: String = row.get(0);
    println!("{ddl}");
    Ok(())
}
