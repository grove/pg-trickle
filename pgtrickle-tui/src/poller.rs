use tokio_postgres::Client;

use crate::state::*;

/// Poll all data from the database and update the state store.
pub async fn poll_all(client: &Client, state: &mut AppState) {
    state.connected = true;
    state.reconnecting = false;
    state.last_poll = Some(chrono::Utc::now());

    // Poll in order of importance — fail gracefully per query.
    poll_stream_tables(client, state).await;
    poll_health(client, state).await;
    poll_cdc(client, state).await;
    poll_dag(client, state).await;
    poll_diagnostics(client, state).await;
    poll_efficiency(client, state).await;
    poll_gucs(client, state).await;
    poll_refresh_log(client, state).await;
}

async fn poll_stream_tables(client: &Client, state: &mut AppState) {
    let result = client
        .query(
            "SELECT
                s.pgt_name::text,
                s.pgt_schema::text,
                s.status::text,
                s.refresh_mode::text,
                s.is_populated,
                COALESCE(s.total_refreshes, 0)::bigint,
                COALESCE(s.failed_refreshes, 0)::bigint,
                s.avg_duration_ms,
                s.last_refresh_at::text,
                s.staleness_secs,
                s.stale
             FROM pgtrickle.st_refresh_stats() s
             ORDER BY s.pgt_schema, s.pgt_name",
            &[],
        )
        .await;

    if let Ok(rows) = result {
        let mut tables = Vec::with_capacity(rows.len());
        for row in &rows {
            let staleness_secs: Option<f64> = row.get(9);
            let avg_ms: Option<f64> = row.get(7);
            // Build sparkline entry
            let name: String = row.get(0);
            if let Some(ms) = avg_ms {
                let entry = state.sparkline_data.entry(name.clone()).or_default();
                entry.push(ms);
                if entry.len() > 20 {
                    entry.remove(0);
                }
            }
            tables.push(StreamTableInfo {
                name,
                schema: row.get(1),
                status: row.get(2),
                refresh_mode: row.get(3),
                is_populated: row.get(4),
                consecutive_errors: 0,
                schedule: None,
                staleness: staleness_secs.map(|s| format!("{s:.0}s")),
                tier: None,
                last_refresh_at: row.get(8),
                total_refreshes: row.get(5),
                failed_refreshes: row.get(6),
                avg_duration_ms: avg_ms,
                stale: row.get(10),
                last_error_message: None,
            });
        }
        state.stream_tables = tables;
    }
}

async fn poll_health(client: &Client, state: &mut AppState) {
    let result = client
        .query(
            "SELECT check_name::text, severity::text, detail::text
             FROM pgtrickle.health_check()
             ORDER BY CASE severity WHEN 'critical' THEN 1 WHEN 'warning' THEN 2 ELSE 3 END",
            &[],
        )
        .await;

    if let Ok(rows) = result {
        state.health_checks = rows
            .iter()
            .map(|row| HealthCheck {
                check_name: row.get(0),
                severity: row.get(1),
                detail: row.get(2),
            })
            .collect();
    }
}

async fn poll_cdc(client: &Client, state: &mut AppState) {
    let result = client
        .query(
            "SELECT stream_table::text, source_table::text, cdc_mode::text,
                    pending_rows, buffer_bytes
             FROM pgtrickle.change_buffer_sizes()
             ORDER BY buffer_bytes DESC",
            &[],
        )
        .await;

    if let Ok(rows) = result {
        state.cdc_buffers = rows
            .iter()
            .map(|row| CdcBuffer {
                stream_table: row.get(0),
                source_table: row.get(1),
                cdc_mode: row.get(2),
                pending_rows: row.get(3),
                buffer_bytes: row.get(4),
            })
            .collect();
    }
}

async fn poll_dag(client: &Client, state: &mut AppState) {
    let result = client
        .query(
            "SELECT tree_line::text, node::text, node_type::text, depth,
                    status::text, refresh_mode::text
             FROM pgtrickle.dependency_tree()",
            &[],
        )
        .await;

    if let Ok(rows) = result {
        state.dag_edges = rows
            .iter()
            .map(|row| DagEdge {
                tree_line: row.get(0),
                node: row.get(1),
                node_type: row.get(2),
                depth: row.get(3),
                status: row.get(4),
                refresh_mode: row.get(5),
            })
            .collect();
    }
}

async fn poll_diagnostics(client: &Client, state: &mut AppState) {
    let result = client
        .query(
            "SELECT pgt_schema::text, pgt_name::text, current_mode::text,
                    recommended_mode::text, confidence::text, reason::text
             FROM pgtrickle.recommend_refresh_mode()
             ORDER BY pgt_schema, pgt_name",
            &[],
        )
        .await;

    if let Ok(rows) = result {
        state.diagnostics = rows
            .iter()
            .map(|row| DiagRecommendation {
                schema: row.get(0),
                name: row.get(1),
                current_mode: row.get(2),
                recommended_mode: row.get(3),
                confidence: row.get(4),
                reason: row.get(5),
            })
            .collect();
    }
}

async fn poll_efficiency(client: &Client, state: &mut AppState) {
    let result = client
        .query(
            "SELECT pgt_schema::text, pgt_name::text, refresh_mode::text,
                    total_refreshes, diff_count, full_count,
                    avg_diff_ms, avg_full_ms, diff_speedup::text
             FROM pgtrickle.refresh_efficiency()
             ORDER BY total_refreshes DESC",
            &[],
        )
        .await;

    if let Ok(rows) = result {
        state.efficiency = rows
            .iter()
            .map(|row| RefreshEfficiency {
                schema: row.get(0),
                name: row.get(1),
                refresh_mode: row.get(2),
                total_refreshes: row.get(3),
                diff_count: row.get(4),
                full_count: row.get(5),
                avg_diff_ms: row.get(6),
                avg_full_ms: row.get(7),
                diff_speedup: row.get(8),
            })
            .collect();
    }
}

async fn poll_gucs(client: &Client, state: &mut AppState) {
    let result = client
        .query(
            "SELECT name, setting, unit, short_desc, category
             FROM pg_settings WHERE name LIKE 'pg_trickle.%' ORDER BY name",
            &[],
        )
        .await;

    if let Ok(rows) = result {
        state.guc_params = rows
            .iter()
            .map(|row| GucParam {
                name: row.get(0),
                setting: row.get(1),
                unit: row.get(2),
                short_desc: row.get(3),
                category: row.get(4),
            })
            .collect();
    }
}

async fn poll_refresh_log(client: &Client, state: &mut AppState) {
    let result = client
        .query(
            "SELECT refreshed_at::text, pgt_name::text, action::text,
                    status::text, duration_ms, rows_affected
             FROM pgtrickle.refresh_timeline()
             ORDER BY refreshed_at DESC
             LIMIT 200",
            &[],
        )
        .await;

    if let Ok(rows) = result {
        state.refresh_log = rows
            .iter()
            .map(|row| RefreshLogEntry {
                timestamp: row.get(0),
                st_name: row.get(1),
                action: row.get(2),
                status: row.get(3),
                duration_ms: row.get(4),
                rows_affected: row.get(5),
            })
            .collect();
    }
}
