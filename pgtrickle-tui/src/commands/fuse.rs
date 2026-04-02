use clap::Parser;
use serde::Serialize;
use tokio_postgres::Client;

use crate::cli::OutputFormat;
use crate::error::CliError;
use crate::output;

#[derive(Parser)]
pub struct FuseArgs {
    /// Output format
    #[arg(long, short, value_enum, default_value_t)]
    pub format: OutputFormat,
}

#[derive(Serialize)]
struct FuseRow {
    stream_table: String,
    fuse_state: String,
    consecutive_errors: i64,
    last_error: String,
    blown_at: String,
}

pub async fn execute(client: &Client, args: &FuseArgs) -> Result<(), CliError> {
    let rows = client
        .query(
            "SELECT pgt_name::text, fuse_state::text,
                    consecutive_errors, last_error_message::text,
                    blown_at::text
             FROM pgtrickle.fuse_status()
             ORDER BY CASE fuse_state WHEN 'BLOWN' THEN 1 WHEN 'TRIPPED' THEN 2 ELSE 3 END",
            &[],
        )
        .await
        .map_err(|e| CliError::Query(e.to_string()))?;

    let items: Vec<FuseRow> = rows
        .iter()
        .map(|row| FuseRow {
            stream_table: row.get(0),
            fuse_state: row.get(1),
            consecutive_errors: row.get(2),
            last_error: row
                .get::<_, Option<String>>(3)
                .unwrap_or_else(|| "-".into()),
            blown_at: row
                .get::<_, Option<String>>(4)
                .unwrap_or_else(|| "-".into()),
        })
        .collect();

    output::print_output(
        args.format,
        &items,
        &[
            "Stream Table",
            "Fuse State",
            "Errors",
            "Last Error",
            "Blown At",
        ],
        |r| {
            vec![
                r.stream_table.clone(),
                r.fuse_state.clone(),
                format!("{}", r.consecutive_errors),
                r.last_error.clone(),
                r.blown_at.clone(),
            ]
        },
    )?;
    Ok(())
}
