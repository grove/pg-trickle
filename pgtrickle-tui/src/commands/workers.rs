use clap::Parser;
use serde::Serialize;
use tokio_postgres::Client;

use crate::cli::OutputFormat;
use crate::error::CliError;
use crate::output;

#[derive(Parser)]
pub struct WorkersArgs {
    /// Output format
    #[arg(long, short, value_enum, default_value_t)]
    pub format: OutputFormat,
}

#[derive(Serialize)]
struct WorkerRow {
    worker_id: i32,
    state: String,
    table_name: String,
    started_at: String,
    duration_ms: String,
}

#[derive(Serialize)]
struct JobRow {
    position: i32,
    table_name: String,
    priority: i32,
    queued_at: String,
    wait_ms: String,
}

pub async fn execute(client: &Client, args: &WorkersArgs) -> Result<(), CliError> {
    let rows = client
        .query(
            "SELECT worker_id, state::text, table_name::text,
                    started_at::text, duration_ms
             FROM pgtrickle.worker_pool_status()
             ORDER BY worker_id",
            &[],
        )
        .await
        .map_err(|e| CliError::Query(e.to_string()))?;

    let items: Vec<WorkerRow> = rows
        .iter()
        .map(|row| {
            let dur: Option<f64> = row.get(4);
            WorkerRow {
                worker_id: row.get(0),
                state: row.get(1),
                table_name: row
                    .get::<_, Option<String>>(2)
                    .unwrap_or_else(|| "-".into()),
                started_at: row
                    .get::<_, Option<String>>(3)
                    .unwrap_or_else(|| "-".into()),
                duration_ms: dur.map(|d| format!("{d:.1}")).unwrap_or_else(|| "-".into()),
            }
        })
        .collect();

    output::print_output(
        args.format,
        &items,
        &["Worker", "State", "Table", "Started At", "Duration (ms)"],
        |r| {
            vec![
                format!("#{}", r.worker_id),
                r.state.clone(),
                r.table_name.clone(),
                r.started_at.clone(),
                r.duration_ms.clone(),
            ]
        },
    )?;

    // Also show job queue
    let queue_rows = client
        .query(
            "SELECT position, table_name::text, priority, queued_at::text, wait_ms
             FROM pgtrickle.parallel_job_status()
             ORDER BY position",
            &[],
        )
        .await
        .map_err(|e| CliError::Query(e.to_string()))?;

    if !queue_rows.is_empty() {
        eprintln!(); // separator
        let q_items: Vec<JobRow> = queue_rows
            .iter()
            .map(|row| {
                let wait: Option<f64> = row.get(4);
                JobRow {
                    position: row.get(0),
                    table_name: row.get(1),
                    priority: row.get(2),
                    queued_at: row.get(3),
                    wait_ms: wait
                        .map(|w| format!("{w:.0}"))
                        .unwrap_or_else(|| "-".into()),
                }
            })
            .collect();

        output::print_output(
            args.format,
            &q_items,
            &["#", "Table", "Priority", "Queued At", "Wait (ms)"],
            |r| {
                vec![
                    format!("{}", r.position),
                    r.table_name.clone(),
                    format!("{}", r.priority),
                    r.queued_at.clone(),
                    r.wait_ms.clone(),
                ]
            },
        )?;
    }

    Ok(())
}
