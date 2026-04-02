use ratatui::Frame;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::Modifier;
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Paragraph, Wrap};

use crate::state::{AppState, StreamTableInfo};
use crate::theme::Theme;

pub fn render(
    frame: &mut Frame,
    area: Rect,
    state: &AppState,
    theme: &Theme,
    st_index: Option<usize>,
) {
    let st = match st_index.and_then(|idx| state.stream_tables.get(idx)) {
        Some(st) => st,
        None => {
            let block = Block::default()
                .borders(Borders::ALL)
                .title(" Detail — no stream table selected ");
            frame.render_widget(block, area);
            return;
        }
    };

    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(10), // Properties
            Constraint::Length(10), // Refresh stats + efficiency
            Constraint::Length(8),  // Recent refreshes
            Constraint::Min(4),     // Error details / upstream health
        ])
        .split(area);

    render_properties(frame, chunks[0], st, theme);
    render_stats(frame, chunks[1], st, state, theme);
    render_recent_refreshes(frame, chunks[2], st, state, theme);
    render_details(frame, chunks[3], st, state, theme);
}

fn render_properties(frame: &mut Frame, area: Rect, st: &StreamTableInfo, theme: &Theme) {
    let status_style = theme.status_style(&st.status);
    let stale_style = if st.stale { theme.warning } else { theme.ok };

    let eff_str = if st.status == "ERROR" || st.status == "SUSPENDED" {
        "✗ error"
    } else if st.cascade_stale {
        "⚠ cascade-stale"
    } else {
        "✓ ok"
    };
    let eff_style = if st.cascade_stale {
        theme.warning
    } else {
        theme.ok
    };

    let lines = vec![
        Line::from(vec![
            Span::styled(" Name:     ", theme.header),
            Span::raw(format!("{}.{}", st.schema, st.name)),
        ]),
        Line::from(vec![
            Span::styled(" Status:   ", theme.header),
            Span::styled(&st.status, status_style),
            Span::raw("    "),
            Span::styled("Effective: ", theme.header),
            Span::styled(eff_str, eff_style),
        ]),
        Line::from(vec![
            Span::styled(" Mode:     ", theme.header),
            Span::raw(&st.refresh_mode),
        ]),
        Line::from(vec![
            Span::styled(" Schedule: ", theme.header),
            Span::raw(st.schedule.as_deref().unwrap_or("-")),
        ]),
        Line::from(vec![
            Span::styled(" Stale:    ", theme.header),
            Span::styled(if st.stale { "yes" } else { "no" }, stale_style),
            Span::raw("  "),
            Span::styled("Staleness: ", theme.header),
            Span::raw(st.staleness.as_deref().unwrap_or("-")),
        ]),
        Line::from(vec![
            Span::styled(" Tier:     ", theme.header),
            Span::raw(st.tier.as_deref().unwrap_or("hot")),
            Span::raw("  "),
            Span::styled("Populated: ", theme.header),
            Span::raw(if st.is_populated { "yes" } else { "no" }),
        ]),
    ];

    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(theme.border)
        .title(Span::styled(
            format!(" {} — Properties ", st.name),
            theme.title,
        ));

    let paragraph = Paragraph::new(lines).block(block);
    frame.render_widget(paragraph, area);
}

fn render_stats(
    frame: &mut Frame,
    area: Rect,
    st: &StreamTableInfo,
    state: &AppState,
    theme: &Theme,
) {
    // Find efficiency data for this ST
    let eff = state
        .efficiency
        .iter()
        .find(|e| e.name == st.name && e.schema == st.schema);

    let mut lines = vec![
        Line::from(vec![
            Span::styled(" Total refreshes:  ", theme.header),
            Span::raw(st.total_refreshes.to_string()),
        ]),
        Line::from(vec![
            Span::styled(" Failed refreshes: ", theme.header),
            Span::styled(
                st.failed_refreshes.to_string(),
                if st.failed_refreshes > 0 {
                    theme.error
                } else {
                    theme.ok
                },
            ),
        ]),
        Line::from(vec![
            Span::styled(" Avg duration:     ", theme.header),
            Span::raw(
                st.avg_duration_ms
                    .map(|ms| format!("{ms:.1} ms"))
                    .unwrap_or_else(|| "-".to_string()),
            ),
        ]),
        Line::from(vec![
            Span::styled(" Last refresh:     ", theme.header),
            Span::raw(st.last_refresh_at.as_deref().unwrap_or("-")),
        ]),
    ];

    if let Some(e) = eff {
        lines.push(Line::from(vec![
            Span::styled(" Differential:     ", theme.header),
            Span::raw(format!("{}", e.diff_count)),
            Span::raw("  "),
            Span::styled("Full: ", theme.header),
            Span::raw(format!("{}", e.full_count)),
        ]));
        if let Some(ref speedup) = e.diff_speedup {
            lines.push(Line::from(vec![
                Span::styled(" Speedup (D→F):    ", theme.header),
                Span::styled(format!("{speedup}×"), theme.active),
            ]));
        }
    }

    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(theme.border)
        .title(Span::styled(" Refresh Statistics ", theme.title));

    let paragraph = Paragraph::new(lines).block(block);
    frame.render_widget(paragraph, area);
}

fn render_recent_refreshes(
    frame: &mut Frame,
    area: Rect,
    st: &StreamTableInfo,
    state: &AppState,
    theme: &Theme,
) {
    let entries: Vec<Line> = state
        .refresh_log
        .iter()
        .filter(|e| e.st_name == st.name)
        .take(5)
        .map(|e| {
            let status_style = match e.status.as_str() {
                "success" | "ok" => theme.ok,
                "error" | "failed" => theme.error,
                _ => theme.dim,
            };
            let dur = e
                .duration_ms
                .map(|ms| format!("{ms:.0}ms"))
                .unwrap_or_default();
            Line::from(vec![
                Span::styled(&e.timestamp, theme.dim),
                Span::raw("  "),
                Span::styled(&e.action, status_style),
                Span::raw("  "),
                Span::styled(&e.status, status_style),
                Span::raw("  "),
                Span::raw(dur),
            ])
        })
        .collect();

    let display = if entries.is_empty() {
        vec![Line::styled(" No recent refreshes", theme.dim)]
    } else {
        entries
    };

    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(theme.border)
        .title(Span::styled(" Recent Refreshes ", theme.title));

    let paragraph = Paragraph::new(display).block(block);
    frame.render_widget(paragraph, area);
}

fn render_details(
    frame: &mut Frame,
    area: Rect,
    st: &StreamTableInfo,
    state: &AppState,
    theme: &Theme,
) {
    let mut lines = Vec::new();

    if let Some(ref err) = st.last_error_message {
        lines.push(Line::from(Span::styled(" Last Error:", theme.error)));
        lines.push(Line::from(Span::raw(format!(" {err}"))));
        lines.push(Line::raw(""));
    }

    if st.consecutive_errors > 0 {
        lines.push(Line::from(vec![
            Span::styled(" Consecutive errors: ", theme.header),
            Span::styled(st.consecutive_errors.to_string(), theme.error),
        ]));
    }

    // Upstream health (F21)
    if st.cascade_stale {
        lines.push(Line::raw(""));
        lines.push(Line::from(Span::styled(
            " ⚠ Upstream Health:",
            theme.warning,
        )));
        // Find error tables upstream via DAG
        for other in &state.stream_tables {
            if (other.status == "ERROR" || other.status == "SUSPENDED") && other.name != st.name {
                lines.push(Line::from(vec![
                    Span::raw("   "),
                    Span::styled("✗ ", theme.error),
                    Span::styled(&other.name, theme.error.add_modifier(Modifier::BOLD)),
                    Span::raw(" — "),
                    Span::raw(other.last_error_message.as_deref().unwrap_or("(error)")),
                ]));
            }
        }
    }

    if lines.is_empty() {
        lines.push(Line::styled(" No errors", theme.ok));
    }

    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(theme.border)
        .title(Span::styled(
            " Error Details & Upstream Health ",
            theme.title,
        ));

    let paragraph = Paragraph::new(lines)
        .block(block)
        .wrap(Wrap { trim: false });
    frame.render_widget(paragraph, area);
}
