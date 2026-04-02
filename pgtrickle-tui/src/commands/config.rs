use clap::Parser;
use tokio_postgres::Client;

use crate::cli::OutputFormat;
use crate::error::CliError;
use crate::output;

#[derive(Parser)]
pub struct ConfigArgs {
    /// Set a GUC parameter (format: param=value)
    #[arg(long)]
    pub set: Option<String>,

    /// Output format
    #[arg(long, short, value_enum, default_value_t = OutputFormat::Table)]
    pub format: OutputFormat,
}

#[derive(serde::Serialize)]
pub struct GucRow {
    pub name: String,
    pub setting: String,
    pub unit: Option<String>,
    pub short_desc: String,
}

pub async fn execute(client: &Client, args: &ConfigArgs) -> Result<(), CliError> {
    if let Some(ref kv) = args.set {
        let (param, value) = kv
            .split_once('=')
            .ok_or_else(|| CliError::Other("expected format: param=value".to_string()))?;

        let sql = format!("ALTER SYSTEM SET {} = '{}'", param.trim(), value.trim());
        client
            .execute(&sql, &[])
            .await
            .map_err(|e| CliError::Query(e.to_string()))?;

        client
            .execute("SELECT pg_reload_conf()", &[])
            .await
            .map_err(|e| CliError::Query(e.to_string()))?;

        output::print_success(&format!(
            "Set {} = {} (reloaded configuration).",
            param.trim(),
            value.trim()
        ));
        return Ok(());
    }

    // List all pg_trickle GUC parameters.
    let rows = client
        .query(
            "SELECT name, setting, unit, short_desc
             FROM pg_settings
             WHERE name LIKE 'pg_trickle.%'
             ORDER BY name",
            &[],
        )
        .await
        .map_err(|e| CliError::Query(e.to_string()))?;

    let items: Vec<GucRow> = rows
        .iter()
        .map(|row| GucRow {
            name: row.get(0),
            setting: row.get(1),
            unit: row.get(2),
            short_desc: row.get(3),
        })
        .collect();

    output::print_output(
        args.format,
        &items,
        &["Parameter", "Value", "Unit", "Description"],
        |row| {
            vec![
                row.name.clone(),
                row.setting.clone(),
                row.unit.clone().unwrap_or_default(),
                row.short_desc.clone(),
            ]
        },
    )
}
