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

// fuse_status() columns: stream_table, fuse_mode, fuse_state, fuse_ceiling,
//   effective_ceiling, fuse_sensitivity, blown_at, blow_reason
#[derive(Serialize)]
struct FuseRow {
    stream_table: String,
    fuse_mode: String,
    fuse_state: String,
    blown_at: String,
    blow_reason: String,
}

pub async fn execute(client: &Client, args: &FuseArgs) -> Result<(), CliError> {
    let rows = client
        .query(
            "SELECT stream_table::text, fuse_mode::text, fuse_state::text,
                    blown_at::text, blow_reason::text
             FROM pgtrickle.fuse_status()
             ORDER BY CASE fuse_state WHEN 'BLOWN' THEN 1 WHEN 'TRIPPED' THEN 2 ELSE 3 END,
                      stream_table",
            &[],
        )
        .await
        .map_err(|e| CliError::Query(e.to_string()))?;

    let items: Vec<FuseRow> = rows
        .iter()
        .map(|row| FuseRow {
            stream_table: row.get(0),
            fuse_mode: row.get(1),
            fuse_state: row.get(2),
            blown_at: row
                .get::<_, Option<String>>(3)
                .unwrap_or_else(|| "-".into()),
            blow_reason: row
                .get::<_, Option<String>>(4)
                .unwrap_or_else(|| "-".into()),
        })
        .collect();

    output::print_output(
        args.format,
        &items,
        &["Stream Table", "Mode", "State", "Blown At", "Reason"],
        |r| {
            vec![
                r.stream_table.clone(),
                r.fuse_mode.clone(),
                r.fuse_state.clone(),
                r.blown_at.clone(),
                r.blow_reason.clone(),
            ]
        },
    )?;
    Ok(())
}
