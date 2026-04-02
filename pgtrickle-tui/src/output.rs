use comfy_table::{ContentArrangement, Table, presets::UTF8_FULL_CONDENSED};
use serde::Serialize;

use crate::cli::OutputFormat;
use crate::error::CliError;

/// Format and print output in the requested format.
pub fn print_output<T: Serialize>(
    format: OutputFormat,
    rows: &[T],
    headers: &[&str],
    row_fn: impl Fn(&T) -> Vec<String>,
) -> Result<(), CliError> {
    match format {
        OutputFormat::Table => {
            let mut table = Table::new();
            table
                .load_preset(UTF8_FULL_CONDENSED)
                .set_content_arrangement(ContentArrangement::Dynamic);
            table.set_header(headers);
            for row in rows {
                table.add_row(row_fn(row));
            }
            println!("{table}");
        }
        OutputFormat::Json => {
            let json = serde_json::to_string_pretty(rows)?;
            println!("{json}");
        }
        OutputFormat::Csv => {
            println!("{}", headers.join(","));
            for row in rows {
                let values = row_fn(row);
                let escaped: Vec<String> = values
                    .iter()
                    .map(|v| {
                        if v.contains(',') || v.contains('"') || v.contains('\n') {
                            format!("\"{}\"", v.replace('"', "\"\""))
                        } else {
                            v.clone()
                        }
                    })
                    .collect();
                println!("{}", escaped.join(","));
            }
        }
    }
    Ok(())
}

/// Print a single-item detail output.
pub fn print_detail<T: Serialize>(
    format: OutputFormat,
    item: &T,
    display_fn: impl FnOnce(&T),
) -> Result<(), CliError> {
    match format {
        OutputFormat::Table => {
            display_fn(item);
        }
        OutputFormat::Json | OutputFormat::Csv => {
            let json = serde_json::to_string_pretty(item)?;
            println!("{json}");
        }
    }
    Ok(())
}

/// Print a simple success message.
pub fn print_success(message: &str) {
    println!("{message}");
}
