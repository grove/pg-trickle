use std::time::Duration;

use clap::Parser;
use tokio_postgres::Client;

use crate::error::CliError;

#[derive(Parser)]
pub struct WatchArgs {
    /// Refresh interval in seconds
    #[arg(long, short = 'n', default_value_t = 2)]
    pub interval: u64,

    /// Compact single-line-per-table output
    #[arg(long)]
    pub compact: bool,

    /// Disable color output
    #[arg(long)]
    pub no_color: bool,

    /// Append mode: don't clear screen between updates
    #[arg(long)]
    pub append: bool,

    /// Filter tables by name (case-insensitive substring match)
    #[arg(long, short = 'f')]
    pub filter: Option<String>,
}

pub async fn execute(client: &Client, args: &WatchArgs) -> Result<(), CliError> {
    let interval = Duration::from_secs(args.interval);

    loop {
        if !args.append {
            // Clear screen (ANSI escape)
            print!("\x1b[2J\x1b[H");
        }

        let now = chrono::Utc::now().format("%H:%M:%S");
        println!("pg_trickle watch — {now}");
        println!();

        let rows = client
            .query(
                "SELECT
                    s.pgt_name::text,
                    s.pgt_schema::text,
                    s.status::text,
                    s.refresh_mode::text,
                    s.stale,
                    s.avg_duration_ms,
                    COALESCE(s.total_refreshes, 0)::bigint,
                    s.last_refresh_at::text
                 FROM pgtrickle.st_refresh_stats() s
                 ORDER BY s.pgt_schema, s.pgt_name",
                &[],
            )
            .await
            .map_err(|e| CliError::Query(e.to_string()))?;

        if args.compact {
            for row in &rows {
                let name: String = row.get(0);
                let schema: String = row.get(1);

                if let Some(ref f) = args.filter {
                    let f_lower = f.to_lowercase();
                    let full = format!("{schema}.{name}").to_lowercase();
                    if !full.contains(&f_lower) && !name.to_lowercase().contains(&f_lower) {
                        continue;
                    }
                }

                let status: String = row.get(2);
                let mode: String = row.get(3);
                let stale: bool = row.get(4);
                let avg_ms: Option<f64> = row.get(5);
                let total: i64 = row.get(6);

                let stale_flag = if stale { " STALE" } else { "" };
                let avg = avg_ms
                    .map(|d| format!("{d:.0}ms"))
                    .unwrap_or_else(|| "-".to_string());

                let line =
                    format!("{schema}.{name}  {status}  {mode}  avg={avg}  n={total}{stale_flag}");

                if args.no_color {
                    println!("{line}");
                } else {
                    let color = match status.as_str() {
                        "ACTIVE" => "\x1b[32m",
                        "ERROR" | "SUSPENDED" => "\x1b[31m",
                        _ => "\x1b[33m",
                    };
                    println!("{color}{line}\x1b[0m");
                }
            }
        } else {
            // Table format
            println!(
                "{:<30} {:<10} {:<12} {:<8} {:<10} {:<8}",
                "Name", "Status", "Mode", "Stale", "Avg (ms)", "Total"
            );
            println!("{}", "-".repeat(80));
            for row in &rows {
                let name: String = row.get(0);
                let schema: String = row.get(1);

                if let Some(ref f) = args.filter {
                    let f_lower = f.to_lowercase();
                    let full = format!("{schema}.{name}").to_lowercase();
                    if !full.contains(&f_lower) && !name.to_lowercase().contains(&f_lower) {
                        continue;
                    }
                }

                let status: String = row.get(2);
                let mode: String = row.get(3);
                let stale: bool = row.get(4);
                let avg_ms: Option<f64> = row.get(5);
                let total: i64 = row.get(6);

                let full_name = format!("{schema}.{name}");
                let avg = avg_ms
                    .map(|d| format!("{d:.1}"))
                    .unwrap_or_else(|| "-".to_string());
                let stale_str = if stale { "Yes" } else { "No" };

                if args.no_color {
                    println!(
                        "{:<30} {:<10} {:<12} {:<8} {:<10} {:<8}",
                        full_name, status, mode, stale_str, avg, total
                    );
                } else {
                    let color = match status.as_str() {
                        "ACTIVE" => "\x1b[32m",
                        "ERROR" | "SUSPENDED" => "\x1b[31m",
                        _ => "\x1b[33m",
                    };
                    println!(
                        "{color}{:<30} {:<10} {:<12} {:<8} {:<10} {:<8}\x1b[0m",
                        full_name, status, mode, stale_str, avg, total
                    );
                }
            }
        }

        if args.append {
            println!();
        }

        tokio::time::sleep(interval).await;
    }
}
