use clap::Parser;
use serde::Serialize;
use tokio_postgres::Client;

use crate::cli::OutputFormat;
use crate::error::CliError;
use crate::output;

#[derive(Parser)]
pub struct ExplainArgs {
    /// Stream table name
    pub name: String,

    /// Show EXPLAIN ANALYZE output
    #[arg(long)]
    pub analyze: bool,

    /// Show DVM operator tree
    #[arg(long)]
    pub operators: bool,

    /// Show deduplication stats
    #[arg(long)]
    pub dedup: bool,

    /// Output format
    #[arg(long, short, value_enum, default_value_t)]
    pub format: OutputFormat,
}

pub async fn execute(client: &Client, args: &ExplainArgs) -> Result<(), CliError> {
    if args.operators {
        return show_operators(client, args).await;
    }
    if args.dedup {
        return show_dedup(client, args).await;
    }

    // Default: show delta SQL (optionally with EXPLAIN ANALYZE)
    let format_param = if args.analyze { "ANALYZE" } else { "TEXT" };

    let rows = client
        .query(
            "SELECT line::text FROM pgtrickle.explain_delta($1, $2)",
            &[&args.name, &format_param],
        )
        .await
        .map_err(|e| CliError::Query(e.to_string()))?;

    if rows.is_empty() {
        return Err(CliError::NotFound(format!(
            "stream table '{}' not found or has no delta SQL",
            args.name
        )));
    }

    match args.format {
        OutputFormat::Json => {
            let lines: Vec<String> = rows.iter().map(|r| r.get::<_, String>(0)).collect();
            println!("{}", serde_json::to_string_pretty(&lines)?);
        }
        _ => {
            for row in &rows {
                let line: String = row.get(0);
                println!("{line}");
            }
        }
    }

    Ok(())
}

#[derive(Serialize)]
struct OpRow {
    operator: String,
    detail: String,
}

async fn show_operators(client: &Client, args: &ExplainArgs) -> Result<(), CliError> {
    let rows = client
        .query(
            "SELECT operator::text, detail::text FROM pgtrickle.explain_st($1)",
            &[&args.name],
        )
        .await
        .map_err(|e| CliError::Query(e.to_string()))?;

    let items: Vec<OpRow> = rows
        .iter()
        .map(|row| OpRow {
            operator: row.get(0),
            detail: row.get(1),
        })
        .collect();

    output::print_output(args.format, &items, &["Operator", "Detail"], |r| {
        vec![r.operator.clone(), r.detail.clone()]
    })?;
    Ok(())
}

#[derive(Serialize)]
struct DedupRow {
    source: String,
    dedup_ratio: f64,
    rows_coalesced: i64,
}

async fn show_dedup(client: &Client, args: &ExplainArgs) -> Result<(), CliError> {
    let rows = client
        .query(
            "SELECT source::text, dedup_ratio, rows_coalesced
             FROM pgtrickle.dedup_stats($1)",
            &[&args.name],
        )
        .await
        .map_err(|e| CliError::Query(e.to_string()))?;

    let items: Vec<DedupRow> = rows
        .iter()
        .map(|row| DedupRow {
            source: row.get(0),
            dedup_ratio: row.get(1),
            rows_coalesced: row.get(2),
        })
        .collect();

    output::print_output(
        args.format,
        &items,
        &["Source", "Dedup Ratio", "Rows Coalesced"],
        |r| {
            vec![
                r.source.clone(),
                format!("{:.2}", r.dedup_ratio),
                format!("{}", r.rows_coalesced),
            ]
        },
    )?;
    Ok(())
}
