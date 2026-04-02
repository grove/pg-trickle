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
    group: String,
    members: i64,
    min_watermark: String,
    max_watermark: String,
    gated: bool,
}

pub async fn execute(client: &Client, args: &WatermarksArgs) -> Result<(), CliError> {
    let rows = client
        .query(
            "SELECT group_name::text, member_count, min_watermark::text,
                    max_watermark::text, gated
             FROM pgtrickle.watermark_groups()
             ORDER BY group_name",
            &[],
        )
        .await
        .map_err(|e| CliError::Query(e.to_string()))?;

    let items: Vec<WatermarkRow> = rows
        .iter()
        .map(|row| WatermarkRow {
            group: row.get(0),
            members: row.get(1),
            min_watermark: row
                .get::<_, Option<String>>(2)
                .unwrap_or_else(|| "-".into()),
            max_watermark: row
                .get::<_, Option<String>>(3)
                .unwrap_or_else(|| "-".into()),
            gated: row.get(4),
        })
        .collect();

    output::print_output(
        args.format,
        &items,
        &[
            "Group",
            "Members",
            "Min Watermark",
            "Max Watermark",
            "Gated",
        ],
        |r| {
            vec![
                r.group.clone(),
                format!("{}", r.members),
                r.min_watermark.clone(),
                r.max_watermark.clone(),
                if r.gated { "Yes" } else { "No" }.to_string(),
            ]
        },
    )?;
    Ok(())
}
