use clap::Parser;
use serde::Serialize;
use tokio_postgres::Client;

use crate::cli::OutputFormat;
use crate::error::CliError;
use crate::output;

#[derive(Parser)]
pub struct HealthArgs {
    /// Output format
    #[arg(long, short, value_enum, default_value_t = OutputFormat::Table)]
    pub format: OutputFormat,
}

#[derive(Serialize)]
pub struct HealthRow {
    pub check_name: String,
    pub severity: String,
    pub detail: String,
}

pub async fn execute(client: &Client, args: &HealthArgs) -> Result<(), CliError> {
    let rows = client
        .query(
            "SELECT
                check_name::text,
                severity::text,
                detail::text
             FROM pgtrickle.health_check()
             ORDER BY
                CASE severity
                    WHEN 'critical' THEN 1
                    WHEN 'warning' THEN 2
                    WHEN 'ok' THEN 3
                    ELSE 4
                END,
                check_name",
            &[],
        )
        .await
        .map_err(|e| CliError::Query(e.to_string()))?;

    let items: Vec<HealthRow> = rows
        .iter()
        .map(|row| HealthRow {
            check_name: row.get(0),
            severity: row.get(1),
            detail: row.get(2),
        })
        .collect();

    // CI-friendly exit: if any critical check, exit code 1.
    let has_critical = items.iter().any(|r| r.severity == "critical");

    output::print_output(
        args.format,
        &items,
        &["Check", "Severity", "Detail"],
        |row| {
            vec![
                row.check_name.clone(),
                row.severity.clone(),
                row.detail.clone(),
            ]
        },
    )?;

    if has_critical {
        Err(CliError::Other(
            "health check failed: critical issues found".to_string(),
        ))
    } else {
        Ok(())
    }
}
