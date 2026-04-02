use clap::Parser;
use serde::Serialize;
use tokio_postgres::Client;

use crate::cli::OutputFormat;
use crate::error::CliError;
use crate::output;

#[derive(Parser)]
pub struct WatermarksArgs {
    /// Output format
    #[arg(long, short, value_enum, default_value_t)]
    pub format: OutputFormat,
}

#[derive(Serialize)]
struct WatermarkRow {
    group_name: String,
    source_count: i32,
    tolerance_secs: f64,
    created_at: String,
}

pub async fn execute(client: &Client, args: &WatermarksArgs) -> Result<(), CliError> {
    let rows = client
        .query(
            "SELECT group_name::text, source_count, tolerance_secs,
                    created_at::text
             FROM pgtrickle.watermark_groups()
             ORDER BY group_name",
            &[],
        )
        .await
        .map_err(|e| CliError::Query(e.to_string()))?;

    let items: Vec<WatermarkRow> = rows
        .iter()
        .map(|row| WatermarkRow {
            group_name: row.get(0),
            source_count: row.get(1),
            tolerance_secs: row.get(2),
            created_at: row
                .get::<_, Option<String>>(3)
                .unwrap_or_else(|| "-".into()),
        })
        .collect();

    output::print_output(
        args.format,
        &items,
        &["Group", "Sources", "Tolerance (s)", "Created At"],
        |r| {
            vec![
                r.group_name.clone(),
                format!("{}", r.source_count),
                format!("{:.1}", r.tolerance_secs),
                r.created_at.clone(),
            ]
        },
    )?;
    Ok(())
}
