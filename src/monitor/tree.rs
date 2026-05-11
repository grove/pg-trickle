//! Dependency tree rendering (v0.55.0 decomposition).
// Extracted from src/monitor.rs in v0.55.0 module decomposition.
// All shared helpers and types are in monitor/mod.rs (use super::*).

use super::*;

/// Return a visual ASCII tree of all stream table dependencies.
///
/// Each row represents one stream table node. The `tree_line` column contains
/// the indented tree rendering (using `├──` / `└──` / `│` box-drawing
/// characters). Roots (stream tables with no stream-table parents) are shown
/// at depth 0; each dependent is indented beneath its parent.
///
/// Source tables (ordinary tables, views) that feed into a stream table are
/// shown as leaf nodes with a `[src]` tag so the full data lineage is visible.
///
/// Exposed as `pgtrickle.dependency_tree()`.
#[pg_extern(schema = "pgtrickle", name = "dependency_tree")]
#[allow(clippy::type_complexity)]
fn dependency_tree() -> TableIterator<
    'static,
    (
        name!(tree_line, String),
        name!(node, String),
        name!(node_type, String),
        name!(depth, i32),
        name!(status, Option<String>),
        name!(refresh_mode, Option<String>),
    ),
> {
    // ── 1. Load all stream tables ───────────────────────────────────────────
    // Map qualified_name -> (status, refresh_mode)
    let st_info: std::collections::HashMap<String, (String, String)> = Spi::connect(|client| {
        let result = client
            .select(
                "SELECT
                    pgt_schema || '.' || pgt_name,
                    status::text,
                    refresh_mode::text
                 FROM pgtrickle.pgt_stream_tables",
                None,
                &[],
            )
            .unwrap_or_else(|e| {
                pgrx::error!(
                    "{}",
                    crate::error::PgTrickleError::DiagnosticError(format!(
                        "dependency_tree: failed to load stream tables: {e}"
                    ))
                )
            });

        let mut m = std::collections::HashMap::new();
        for row in result {
            let name = row.get::<String>(1).unwrap_or(None).unwrap_or_default();
            let status = row.get::<String>(2).unwrap_or(None).unwrap_or_default();
            let mode = row.get::<String>(3).unwrap_or(None).unwrap_or_default();
            if !name.is_empty() {
                m.insert(name, (status, mode));
            }
        }
        m
    });

    // ── 2. Load all dependency edges ───────────────────────────────────────
    // Each ST may depend on: (a) other stream tables, or (b) plain source tables.
    // We collect both kinds. For plain sources we only store them as leaf
    // display nodes attached to their consumer ST.
    //
    // st_children:  parent_st -> Vec<child_st>   (ST-to-ST edges)
    // st_sources:   consumer_st -> Vec<source_qualified_name>  (ST-to-plain-table)
    let mut st_children: std::collections::HashMap<String, Vec<String>> =
        std::collections::HashMap::new();
    let mut st_sources: std::collections::HashMap<String, Vec<String>> =
        std::collections::HashMap::new();

    // Initialise child lists for all known STs so roots are discoverable.
    for name in st_info.keys() {
        st_children.entry(name.clone()).or_default();
        st_sources.entry(name.clone()).or_default();
    }

    Spi::connect(|client| {
        let result = client
            .select(
                "SELECT
                    st.pgt_schema || '.' || st.pgt_name   AS consumer,
                    n.nspname::text || '.' || c.relname::text AS source,
                    -- is the source itself a stream table?
                    EXISTS (
                        SELECT 1 FROM pgtrickle.pgt_stream_tables st2
                        WHERE st2.pgt_schema = n.nspname::text
                          AND st2.pgt_name   = c.relname::text
                    ) AS is_st
                 FROM pgtrickle.pgt_dependencies d
                 JOIN pgtrickle.pgt_stream_tables st ON st.pgt_id = d.pgt_id
                 JOIN pg_class     c ON c.oid = d.source_relid
                 JOIN pg_namespace n ON n.oid = c.relnamespace
                 ORDER BY consumer, source",
                None,
                &[],
            )
            .unwrap_or_else(|e| {
                pgrx::error!(
                    "{}",
                    crate::error::PgTrickleError::DiagnosticError(format!(
                        "dependency_tree: failed to load dependencies: {e}"
                    ))
                )
            });

        for row in result {
            let consumer = row.get::<String>(1).unwrap_or(None).unwrap_or_default();
            let source = row.get::<String>(2).unwrap_or(None).unwrap_or_default();
            let is_st = row.get::<bool>(3).unwrap_or(None).unwrap_or(false);
            if consumer.is_empty() || source.is_empty() {
                continue;
            }
            if is_st {
                // source is itself a stream table → ST-to-ST edge
                // Record: source_st has consumer_st as a child
                st_children
                    .entry(source.clone())
                    .or_default()
                    .push(consumer.clone());
            } else {
                // plain source table / view
                st_sources
                    .entry(consumer.clone())
                    .or_default()
                    .push(source.clone());
            }
        }
    });

    // ── 3. Find roots (stream tables that are not the child of any other ST) ─
    let output = render_dependency_tree(&st_info, &st_children, &st_sources);

    TableIterator::new(output)
}

/// Context for the dependency tree DFS traversal.
struct DagCtx<'a> {
    st_info: &'a std::collections::HashMap<String, (String, String)>,
    st_children: &'a std::collections::HashMap<String, Vec<String>>,
    st_sources: &'a std::collections::HashMap<String, Vec<String>>,
}

/// Render a dependency tree from pre-loaded ST metadata.
///
/// Pure function: takes three `HashMap`s describing the graph topology
/// and returns formatted tree rows with box-drawing prefixes.
#[allow(clippy::type_complexity)]
pub(crate) fn render_dependency_tree(
    st_info: &std::collections::HashMap<String, (String, String)>,
    st_children: &std::collections::HashMap<String, Vec<String>>,
    st_sources: &std::collections::HashMap<String, Vec<String>>,
) -> Vec<(String, String, String, i32, Option<String>, Option<String>)> {
    let all_children: std::collections::HashSet<String> = st_children
        .values()
        .flat_map(|v| v.iter().cloned())
        .collect();

    let mut roots: Vec<String> = st_info
        .keys()
        .filter(|name| !all_children.contains(*name))
        .cloned()
        .collect();
    roots.sort();

    // DFS to emit rows with proper tree-drawing prefixes.
    let mut output: Vec<(String, String, String, i32, Option<String>, Option<String>)> = Vec::new();

    let ctx = DagCtx {
        st_info,
        st_children,
        st_sources,
    };

    for (i, root) in roots.iter().enumerate() {
        let is_last_root = i == roots.len() - 1;
        let root_connector = if roots.len() == 1 {
            ""
        } else if is_last_root {
            "└── "
        } else {
            "├── "
        };
        dfs(root, 0, root_connector, "", &ctx, &mut output);
    }

    output
}

#[allow(clippy::type_complexity)]
fn dfs(
    node: &str,
    depth: i32,
    connector: &str,    // "├── " | "└── " | ""
    continuation: &str, // prefix inherited from parent
    ctx: &DagCtx<'_>,
    output: &mut Vec<(String, String, String, i32, Option<String>, Option<String>)>,
) {
    let tree_line = format!("{}{}{}", continuation, connector, node);

    let (status, mode, node_type) = if let Some((s, m)) = ctx.st_info.get(node) {
        (Some(s.clone()), Some(m.clone()), "stream_table".to_string())
    } else {
        (None, None, "source_table".to_string())
    };

    output.push((tree_line, node.to_string(), node_type, depth, status, mode));

    // Children of this node: ST dependents + plain source tables
    let mut st_kids = ctx.st_children.get(node).cloned().unwrap_or_default();
    st_kids.sort();

    let mut src_kids = ctx.st_sources.get(node).cloned().unwrap_or_default();
    src_kids.sort();

    // Plain source nodes come after ST children so the ST sub-tree
    // is rendered contiguously.
    let all_kids: Vec<(String, bool)> = st_kids
        .iter()
        .map(|n| (n.clone(), true))
        .chain(src_kids.iter().map(|n| (n.clone(), false)))
        .collect();

    let child_continuation = format!(
        "{}{}",
        continuation,
        if connector == "└── " || connector.is_empty() {
            "    "
        } else {
            "│   "
        }
    );

    let total = all_kids.len();
    for (i, (child, is_st_child)) in all_kids.iter().enumerate() {
        let is_last = i == total - 1;
        let child_connector = if is_last { "└── " } else { "├── " };

        if *is_st_child {
            dfs(
                child,
                depth + 1,
                child_connector,
                &child_continuation,
                ctx,
                output,
            );
        } else {
            // Leaf source node — emit directly without recursing into
            // its own dependencies (those are tracked by its own ST entry
            // if it is also a stream table).
            let src_label = format!("{} [src]", child);
            let src_line = format!("{}{}{}", child_continuation, child_connector, src_label);
            output.push((
                src_line,
                child.clone(),
                "source_table".to_string(),
                depth + 1,
                None,
                None,
            ));
        }
    }
}
