use clap::Parser;
use tokio_postgres::Client;

use crate::cli::OutputFormat;
use crate::error::CliError;

#[derive(Parser)]
pub struct GraphArgs {
    /// Output format
    #[arg(long, short, value_enum, default_value_t = OutputFormat::Table)]
    pub format: OutputFormat,
}

pub async fn execute(client: &Client, args: &GraphArgs) -> Result<(), CliError> {
    let rows = client
        .query(
            "SELECT
                tree_line::text,
                node::text,
                node_type::text,
                depth,
                status::text,
                refresh_mode::text
             FROM pgtrickle.dependency_tree()",
            &[],
        )
        .await
        .map_err(|e| CliError::Query(e.to_string()))?;

    match args.format {
        OutputFormat::Table => {
            if rows.is_empty() {
                println!("No stream tables found.");
                return Ok(());
            }
            for row in &rows {
                let tree_line: String = row.get(0);
                println!("{tree_line}");
            }
        }
        OutputFormat::Json => {
            let items: Vec<serde_json::Value> = rows
                .iter()
                .map(|row| {
                    serde_json::json!({
                        "tree_line": row.get::<_, String>(0),
                        "node": row.get::<_, String>(1),
                        "node_type": row.get::<_, String>(2),
                        "depth": row.get::<_, i32>(3),
                        "status": row.get::<_, Option<String>>(4),
                        "refresh_mode": row.get::<_, Option<String>>(5),
                    })
                })
                .collect();
            println!("{}", serde_json::to_string_pretty(&items)?);
        }
        OutputFormat::Csv => {
            println!("tree_line,node,node_type,depth,status,refresh_mode");
            for row in &rows {
                let tree_line: String = row.get(0);
                let node: String = row.get(1);
                let node_type: String = row.get(2);
                let depth: i32 = row.get(3);
                let status: Option<String> = row.get(4);
                let mode: Option<String> = row.get(5);
                println!(
                    "\"{}\",\"{}\",{},{},{},{}",
                    tree_line.replace('"', "\"\""),
                    node,
                    node_type,
                    depth,
                    status.unwrap_or_default(),
                    mode.unwrap_or_default()
                );
            }
        }
    }

    Ok(())
}
