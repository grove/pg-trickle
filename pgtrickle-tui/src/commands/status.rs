use clap::Parser;
use serde::Serialize;
use tokio_postgres::Client;

use crate::cli::OutputFormat;
use crate::error::CliError;
use crate::output;

#[derive(Parser)]
pub struct StatusArgs {
    /// Name of the stream table
    pub name: String,

    /// Output format
    #[arg(long, short, value_enum, default_value_t = OutputFormat::Table)]
    pub format: OutputFormat,
}

#[derive(Serialize)]
pub struct StreamTableDetail {
    pub name: String,
    pub schema: String,
    pub status: String,
    pub refresh_mode: String,
    pub is_populated: bool,
    pub consecutive_errors: i64,
    pub schedule: Option<String>,
    pub staleness: Option<String>,
    pub total_refreshes: i64,
    pub successful_refreshes: i64,
    pub failed_refreshes: i64,
    pub avg_duration_ms: Option<f64>,
    pub last_refresh_at: Option<String>,
    pub stale: bool,
}

pub async fn execute(client: &Client, args: &StatusArgs) -> Result<(), CliError> {
    let rows = client
        .query(
            "SELECT
                pgt_name::text,
                pgt_schema::text,
                status::text,
                refresh_mode::text,
                is_populated,
                0::bigint as consecutive_errors,
                total_refreshes,
                successful_refreshes,
                failed_refreshes,
                avg_duration_ms,
                last_refresh_at::text,
                staleness_secs,
                stale
             FROM pgtrickle.st_refresh_stats()
             WHERE pgt_name = $1",
            &[&args.name],
        )
        .await
        .map_err(|e| CliError::Query(e.to_string()))?;

    let row = rows
        .first()
        .ok_or_else(|| CliError::NotFound(format!("stream table '{}' not found", args.name)))?;

    let staleness_secs: Option<f64> = row.get(11);

    let detail = StreamTableDetail {
        name: row.get(0),
        schema: row.get(1),
        status: row.get(2),
        refresh_mode: row.get(3),
        is_populated: row.get(4),
        consecutive_errors: row.get(5),
        schedule: None,
        staleness: staleness_secs.map(|s| format!("{s:.1}s")),
        total_refreshes: row.get(6),
        successful_refreshes: row.get(7),
        failed_refreshes: row.get(8),
        avg_duration_ms: row.get(9),
        last_refresh_at: row.get(10),
        stale: row.get(12),
    };

    output::print_detail(args.format, &detail, |d| {
        println!("Stream Table: {}.{}", d.schema, d.name);
        println!("Status:       {}", d.status);
        println!("Mode:         {}", d.refresh_mode);
        println!(
            "Populated:    {}",
            if d.is_populated { "yes" } else { "no" }
        );
        println!("Stale:        {}", if d.stale { "yes" } else { "no" });
        if let Some(ref st) = d.staleness {
            println!("Staleness:    {st}");
        }
        println!();
        println!("Refresh Statistics:");
        println!("  Total:      {}", d.total_refreshes);
        println!("  Successful: {}", d.successful_refreshes);
        println!("  Failed:     {}", d.failed_refreshes);
        if let Some(avg) = d.avg_duration_ms {
            println!("  Avg ms:     {avg:.1}");
        }
        if let Some(ref last) = d.last_refresh_at {
            println!("  Last:       {last}");
        }
        if d.consecutive_errors > 0 {
            println!("  Consec err: {}", d.consecutive_errors);
        }
    })
}
