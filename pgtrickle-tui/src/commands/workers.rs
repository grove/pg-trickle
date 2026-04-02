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

// worker_pool_status() columns: active_workers, max_workers, per_db_cap, parallel_mode
#[derive(Serialize)]
struct WorkerPoolRow {
    active_workers: i32,
    max_workers: i32,
    per_db_cap: i32,
    parallel_mode: String,
}

// parallel_job_status() columns: job_id, unit_key, unit_kind, status, member_count,
//   attempt_no, scheduler_pid, worker_pid, enqueued_at, started_at, finished_at, duration_ms
#[derive(Serialize)]
struct JobRow {
    job_id: i64,
    unit_key: String,
    unit_kind: String,
    status: String,
    enqueued_at: String,
    started_at: String,
    duration_ms: String,
}

pub async fn execute(client: &Client, args: &WorkersArgs) -> Result<(), CliError> {
    // Pool summary
    let pool_rows = client
        .query(
            "SELECT active_workers, max_workers, per_db_cap, parallel_mode::text
             FROM pgtrickle.worker_pool_status()",
            &[],
        )
        .await
        .map_err(|e| CliError::Query(e.to_string()))?;

    let pool: Vec<WorkerPoolRow> = pool_rows
        .iter()
        .map(|row| WorkerPoolRow {
            active_workers: row.get(0),
            max_workers: row.get(1),
            per_db_cap: row.get(2),
            parallel_mode: row.get(3),
        })
        .collect();

    output::print_output(
        args.format,
        &pool,
        &["Active Workers", "Max Workers", "Per-DB Cap", "Mode"],
        |r| {
            vec![
                format!("{}", r.active_workers),
                format!("{}", r.max_workers),
                format!("{}", r.per_db_cap),
                r.parallel_mode.clone(),
            ]
        },
    )?;

    // Active/recent jobs
    let job_rows = client
        .query(
            "SELECT job_id, unit_key::text, unit_kind::text, status::text,
                    enqueued_at::text, started_at::text, duration_ms
             FROM pgtrickle.parallel_job_status()
             ORDER BY enqueued_at DESC",
            &[],
        )
        .await
        .map_err(|e| CliError::Query(e.to_string()))?;

    if !job_rows.is_empty() {
        println!();
        let jobs: Vec<JobRow> = job_rows
            .iter()
            .map(|row| {
                let dur: Option<f64> = row.get(6);
                JobRow {
                    job_id: row.get(0),
                    unit_key: row.get(1),
                    unit_kind: row.get(2),
                    status: row.get(3),
                    enqueued_at: row
                        .get::<_, Option<String>>(4)
                        .unwrap_or_else(|| "-".into()),
                    started_at: row
                        .get::<_, Option<String>>(5)
                        .unwrap_or_else(|| "-".into()),
                    duration_ms: dur.map(|d| format!("{d:.1}")).unwrap_or_else(|| "-".into()),
                }
            })
            .collect();

        output::print_output(
            args.format,
            &jobs,
            &[
                "Job",
                "Table / Key",
                "Kind",
                "Status",
                "Enqueued At",
                "Started At",
                "Duration (ms)",
            ],
            |r| {
                vec![
                    format!("{}", r.job_id),
                    r.unit_key.clone(),
                    r.unit_kind.clone(),
                    r.status.clone(),
                    r.enqueued_at.clone(),
                    r.started_at.clone(),
                    r.duration_ms.clone(),
                ]
            },
        )?;
    }

    Ok(())
}
