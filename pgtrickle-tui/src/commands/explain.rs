use clap::Parser;
use tokio_postgres::Client;

use crate::cli::OutputFormat;
use crate::error::CliError;

#[derive(Parser)]
pub struct ExplainArgs {
    /// Stream table name
    pub name: String,

    /// Show EXPLAIN ANALYZE output
    #[arg(long)]
    pub analyze: bool,

    /// Output format
    #[arg(long, short, value_enum, default_value_t)]
    pub format: OutputFormat,
}

pub async fn execute(client: &Client, args: &ExplainArgs) -> Result<(), CliError> {
    // explain_delta() returns SetOf text — each row is one line of output
    let format_param = if args.analyze { "ANALYZE" } else { "TEXT" };

    let rows = client
        .query(
            "SELECT * FROM pgtrickle.explain_delta($1, $2)",
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
