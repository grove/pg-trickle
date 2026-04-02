use clap::Parser;
use serde::Serialize;
use tokio_postgres::Client;

use crate::cli::OutputFormat;
use crate::error::CliError;
use crate::output;

#[derive(Parser)]
pub struct DiagArgs {
    /// Specific stream table to diagnose (all if omitted)
    pub name: Option<String>,

    /// Output format
    #[arg(long, short, value_enum, default_value_t = OutputFormat::Table)]
    pub format: OutputFormat,
}

#[derive(Serialize)]
pub struct DiagRow {
    pub schema: String,
    pub name: String,
    pub current_mode: String,
    pub recommended_mode: String,
    pub confidence: String,
    pub reason: String,
}

pub async fn execute(client: &Client, args: &DiagArgs) -> Result<(), CliError> {
    let query = if args.name.is_some() {
        "SELECT
            pgt_schema::text,
            pgt_name::text,
            current_mode::text,
            recommended_mode::text,
            confidence::text,
            reason::text
         FROM pgtrickle.recommend_refresh_mode()
         WHERE pgt_name = $1
         ORDER BY pgt_schema, pgt_name"
    } else {
        "SELECT
            pgt_schema::text,
            pgt_name::text,
            current_mode::text,
            recommended_mode::text,
            confidence::text,
            reason::text
         FROM pgtrickle.recommend_refresh_mode()
         ORDER BY pgt_schema, pgt_name"
    };

    let rows = if let Some(ref name) = args.name {
        client
            .query(query, &[name])
            .await
            .map_err(|e| CliError::Query(e.to_string()))?
    } else {
        client
            .query(query, &[])
            .await
            .map_err(|e| CliError::Query(e.to_string()))?
    };

    let items: Vec<DiagRow> = rows
        .iter()
        .map(|row| DiagRow {
            schema: row.get(0),
            name: row.get(1),
            current_mode: row.get(2),
            recommended_mode: row.get(3),
            confidence: row.get(4),
            reason: row.get(5),
        })
        .collect();

    output::print_output(
        args.format,
        &items,
        &[
            "Schema",
            "Name",
            "Current",
            "Recommended",
            "Confidence",
            "Reason",
        ],
        |row| {
            vec![
                row.schema.clone(),
                row.name.clone(),
                row.current_mode.clone(),
                row.recommended_mode.clone(),
                row.confidence.clone(),
                row.reason.clone(),
            ]
        },
    )
}
