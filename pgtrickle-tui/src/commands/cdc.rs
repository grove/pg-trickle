use clap::Parser;
use serde::Serialize;
use tokio_postgres::Client;

use crate::cli::OutputFormat;
use crate::error::CliError;
use crate::output;

#[derive(Parser)]
pub struct CdcArgs {
    /// Output format
    #[arg(long, short, value_enum, default_value_t = OutputFormat::Table)]
    pub format: OutputFormat,
}

#[derive(Serialize)]
pub struct CdcRow {
    pub stream_table: String,
    pub source_table: String,
    pub cdc_mode: String,
    pub pending_rows: i64,
    pub buffer_bytes: i64,
}

pub async fn execute(client: &Client, args: &CdcArgs) -> Result<(), CliError> {
    let rows = client
        .query(
            "SELECT
                stream_table::text,
                source_table::text,
                cdc_mode::text,
                pending_rows,
                buffer_bytes
             FROM pgtrickle.change_buffer_sizes()
             ORDER BY buffer_bytes DESC",
            &[],
        )
        .await
        .map_err(|e| CliError::Query(e.to_string()))?;

    let items: Vec<CdcRow> = rows
        .iter()
        .map(|row| CdcRow {
            stream_table: row.get(0),
            source_table: row.get(1),
            cdc_mode: row.get(2),
            pending_rows: row.get(3),
            buffer_bytes: row.get(4),
        })
        .collect();

    output::print_output(
        args.format,
        &items,
        &[
            "Stream Table",
            "Source",
            "CDC Mode",
            "Pending Rows",
            "Buffer Bytes",
        ],
        |row| {
            vec![
                row.stream_table.clone(),
                row.source_table.clone(),
                row.cdc_mode.clone(),
                row.pending_rows.to_string(),
                format_bytes(row.buffer_bytes),
            ]
        },
    )
}

fn format_bytes(bytes: i64) -> String {
    if bytes < 1024 {
        format!("{bytes} B")
    } else if bytes < 1024 * 1024 {
        format!("{:.1} KB", bytes as f64 / 1024.0)
    } else if bytes < 1024 * 1024 * 1024 {
        format!("{:.1} MB", bytes as f64 / (1024.0 * 1024.0))
    } else {
        format!("{:.1} GB", bytes as f64 / (1024.0 * 1024.0 * 1024.0))
    }
}
