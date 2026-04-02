use std::collections::HashMap;

use chrono::{DateTime, Utc};
use serde::Serialize;

/// Central state store for the TUI — updated by async pollers.
#[derive(Default)]
pub struct AppState {
    pub stream_tables: Vec<StreamTableInfo>,
    pub health_checks: Vec<HealthCheck>,
    pub cdc_buffers: Vec<CdcBuffer>,
    pub dag_edges: Vec<DagEdge>,
    pub alerts: Vec<AlertEvent>,
    pub guc_params: Vec<GucParam>,
    pub diagnostics: Vec<DiagRecommendation>,
    pub efficiency: Vec<RefreshEfficiency>,
    pub refresh_log: Vec<RefreshLogEntry>,
    pub last_poll: Option<DateTime<Utc>>,
    pub connected: bool,
    pub reconnecting: bool,
    #[allow(dead_code)]
    pub poll_interval_ms: u64,
    pub error_message: Option<String>,
    /// Sparkline data: st_name -> last N refresh durations
    pub sparkline_data: HashMap<String, Vec<f64>>,
}

#[derive(Clone, Serialize)]
pub struct StreamTableInfo {
    pub name: String,
    pub schema: String,
    pub status: String,
    pub refresh_mode: String,
    pub is_populated: bool,
    pub consecutive_errors: i64,
    pub schedule: Option<String>,
    pub staleness: Option<String>,
    pub tier: Option<String>,
    pub last_refresh_at: Option<String>,
    pub total_refreshes: i64,
    pub failed_refreshes: i64,
    pub avg_duration_ms: Option<f64>,
    pub stale: bool,
    pub last_error_message: Option<String>,
}

#[derive(Clone, Serialize)]
pub struct HealthCheck {
    pub check_name: String,
    pub severity: String,
    pub detail: String,
}

#[derive(Clone, Serialize)]
pub struct CdcBuffer {
    pub stream_table: String,
    pub source_table: String,
    pub cdc_mode: String,
    pub pending_rows: i64,
    pub buffer_bytes: i64,
}

#[derive(Clone)]
#[allow(dead_code)]
pub struct DagEdge {
    pub tree_line: String,
    pub node: String,
    pub node_type: String,
    pub depth: i32,
    pub status: Option<String>,
    pub refresh_mode: Option<String>,
}

#[derive(Clone)]
pub struct AlertEvent {
    pub timestamp: DateTime<Utc>,
    pub severity: String,
    pub message: String,
}

#[derive(Clone, Serialize)]
pub struct GucParam {
    pub name: String,
    pub setting: String,
    pub unit: Option<String>,
    pub short_desc: String,
    pub category: String,
}

#[derive(Clone, Serialize)]
pub struct DiagRecommendation {
    pub schema: String,
    pub name: String,
    pub current_mode: String,
    pub recommended_mode: String,
    pub confidence: String,
    pub reason: String,
}

#[derive(Clone, Serialize)]
pub struct RefreshEfficiency {
    pub schema: String,
    pub name: String,
    pub refresh_mode: String,
    pub total_refreshes: i64,
    pub diff_count: i64,
    pub full_count: i64,
    pub avg_diff_ms: Option<f64>,
    pub avg_full_ms: Option<f64>,
    pub diff_speedup: Option<String>,
}

#[derive(Clone)]
pub struct RefreshLogEntry {
    pub timestamp: String,
    pub st_name: String,
    pub action: String,
    pub status: String,
    pub duration_ms: Option<f64>,
    #[allow(dead_code)]
    pub rows_affected: Option<i64>,
}

impl AppState {
    pub fn active_count(&self) -> usize {
        self.stream_tables
            .iter()
            .filter(|st| st.status == "ACTIVE")
            .count()
    }

    pub fn error_count(&self) -> usize {
        self.stream_tables
            .iter()
            .filter(|st| st.status == "ERROR" || st.status == "SUSPENDED")
            .count()
    }

    pub fn stale_count(&self) -> usize {
        self.stream_tables.iter().filter(|st| st.stale).count()
    }

    pub fn critical_health_count(&self) -> usize {
        self.health_checks
            .iter()
            .filter(|h| h.severity == "critical")
            .count()
    }
}
