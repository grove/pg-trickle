use ratatui::Frame;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Cell, Paragraph, Row, Table};

use crate::state::AppState;
use crate::theme::Theme;

pub fn render(frame: &mut Frame, area: Rect, state: &AppState, theme: &Theme, selected: usize) {
    let wide = area.width >= 140;
    let tall = area.height >= 35;

    if wide && tall {
        render_wide(frame, area, state, theme, selected);
    } else {
        render_standard(frame, area, state, theme, selected);
    }
}

fn render_wide(frame: &mut Frame, area: Rect, state: &AppState, theme: &Theme, selected: usize) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3), // Status ribbon
            Constraint::Min(10),   // Main content
            Constraint::Length(3), // DAG mini-map
        ])
        .split(area);

    render_status_ribbon(frame, chunks[0], state, theme);

    // Split main into table + issues sidebar.
    let main = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Min(60), Constraint::Length(35)])
        .split(chunks[1]);

    render_table(frame, main[0], state, theme, selected, true);
    render_issues_sidebar(frame, main[1], state, theme);
    render_dag_minimap(frame, chunks[2], state, theme);
}

fn render_standard(
    frame: &mut Frame,
    area: Rect,
    state: &AppState,
    theme: &Theme,
    selected: usize,
) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(3), Constraint::Min(5)])
        .split(area);

    render_status_ribbon(frame, chunks[0], state, theme);
    render_table(frame, chunks[1], state, theme, selected, false);
}

fn render_status_ribbon(frame: &mut Frame, area: Rect, state: &AppState, theme: &Theme) {
    let total = state.stream_tables.len();
    let active = state.active_count();
    let errors = state.error_count();
    let stale = state.stale_count();
    let critical = state.critical_health_count();

    let mut spans = vec![
        Span::styled(format!(" {total} total"), theme.header),
        Span::raw("  "),
        Span::styled(format!("● {active} active"), theme.active),
    ];

    if errors > 0 {
        spans.push(Span::raw("  "));
        spans.push(Span::styled(format!("✗ {errors} error"), theme.error));
    }
    if stale > 0 {
        spans.push(Span::raw("  "));
        spans.push(Span::styled(format!("⚠ {stale} stale"), theme.warning));
    }
    if critical > 0 {
        spans.push(Span::raw("  "));
        spans.push(Span::styled(format!("✗ {critical} critical"), theme.error));
    }

    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(theme.border)
        .title(Span::styled(" Stream Tables ", theme.title));

    let paragraph = Paragraph::new(Line::from(spans)).block(block);
    frame.render_widget(paragraph, area);
}

fn render_table(
    frame: &mut Frame,
    area: Rect,
    state: &AppState,
    theme: &Theme,
    selected: usize,
    show_eff: bool,
) {
    let mut header_cells = vec!["Name", "Schema", "Status", "Mode", "Stale", "Last Refresh"];
    if show_eff {
        header_cells.push("Avg ms");
        header_cells.push("Refreshes");
    }

    let header = Row::new(
        header_cells
            .iter()
            .map(|h| Cell::from(*h).style(theme.header)),
    )
    .height(1);

    let rows: Vec<Row> = state
        .stream_tables
        .iter()
        .enumerate()
        .map(|(i, st)| {
            let status_style = theme.status_style(&st.status);
            let stale_str = if st.stale { "yes" } else { "no" };
            let stale_style = if st.stale { theme.warning } else { theme.ok };

            let mut cells = vec![
                Cell::from(st.name.as_str()),
                Cell::from(st.schema.as_str()),
                Cell::from(st.status.as_str()).style(status_style),
                Cell::from(st.refresh_mode.as_str()),
                Cell::from(stale_str).style(stale_style),
                Cell::from(st.last_refresh_at.as_deref().unwrap_or("-").to_string()),
            ];

            if show_eff {
                cells.push(Cell::from(
                    st.avg_duration_ms
                        .map(|ms| format!("{ms:.1}"))
                        .unwrap_or_default(),
                ));
                cells.push(Cell::from(st.total_refreshes.to_string()));
            }

            let style = if i == selected {
                theme.selected
            } else {
                Style::default()
            };
            Row::new(cells).style(style)
        })
        .collect();

    let widths = if show_eff {
        vec![
            Constraint::Min(20),
            Constraint::Length(12),
            Constraint::Length(10),
            Constraint::Length(14),
            Constraint::Length(6),
            Constraint::Length(22),
            Constraint::Length(8),
            Constraint::Length(10),
        ]
    } else {
        vec![
            Constraint::Min(20),
            Constraint::Length(12),
            Constraint::Length(10),
            Constraint::Length(14),
            Constraint::Length(6),
            Constraint::Length(22),
        ]
    };

    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(theme.border);

    let table = Table::new(rows, widths)
        .header(header)
        .block(block)
        .row_highlight_style(theme.selected)
        .highlight_symbol("▸ ");

    frame.render_widget(table, area);
}

fn render_issues_sidebar(frame: &mut Frame, area: Rect, state: &AppState, theme: &Theme) {
    let mut lines: Vec<Line> = Vec::new();

    // Error chain issues
    let errors: Vec<_> = state
        .stream_tables
        .iter()
        .filter(|st| st.status == "ERROR" || st.status == "SUSPENDED")
        .collect();

    for st in &errors {
        lines.push(Line::from(vec![
            Span::styled("✗ ", theme.error),
            Span::styled(&st.name, Style::default().add_modifier(Modifier::BOLD)),
        ]));
        lines.push(Line::from(vec![
            Span::raw("  "),
            Span::styled(st.status.as_str(), theme.status_style(&st.status)),
        ]));
        if let Some(ref err) = st.last_error_message {
            let truncated = if err.len() > 30 { &err[..30] } else { err };
            lines.push(Line::from(vec![
                Span::raw("  "),
                Span::styled(truncated, theme.dim),
            ]));
        }
        lines.push(Line::raw(""));
    }

    // Buffer growth warnings
    let big_buffers: Vec<_> = state
        .cdc_buffers
        .iter()
        .filter(|b| b.buffer_bytes > 1_000_000)
        .collect();
    for buf in &big_buffers {
        lines.push(Line::from(vec![
            Span::styled("⚠ ", theme.warning),
            Span::raw(format!(
                "Buffer: {} ({} rows)",
                buf.source_table, buf.pending_rows
            )),
        ]));
    }

    if lines.is_empty() {
        lines.push(Line::styled("No issues detected", theme.ok));
    }

    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(theme.border)
        .title(Span::styled(
            format!(" Issues ({}) ", errors.len() + big_buffers.len()),
            theme.title,
        ));

    let paragraph = Paragraph::new(lines).block(block);
    frame.render_widget(paragraph, area);
}

fn render_dag_minimap(frame: &mut Frame, area: Rect, state: &AppState, theme: &Theme) {
    let lines: Vec<Line> = state
        .dag_edges
        .iter()
        .filter(|e| e.depth <= 2)
        .take(area.height.saturating_sub(2) as usize)
        .map(|e| {
            let status_style = e
                .status
                .as_deref()
                .map(|s| theme.status_style(s))
                .unwrap_or_default();
            Line::from(Span::styled(e.tree_line.as_str(), status_style))
        })
        .collect();

    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(theme.border)
        .title(Span::styled(" DAG Mini-Map ", theme.title));

    let paragraph = Paragraph::new(lines).block(block);
    frame.render_widget(paragraph, area);
}
