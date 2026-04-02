use ratatui::Frame;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Paragraph, Wrap};

use crate::state::{AppState, StreamTableInfo};
use crate::theme::Theme;

pub fn render(frame: &mut Frame, area: Rect, state: &AppState, theme: &Theme, st_index: usize) {
    let st = match state.stream_tables.get(st_index) {
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
            Constraint::Length(8),  // Refresh stats
            Constraint::Min(5),     // Defining query / error details
        ])
        .split(area);

    render_properties(frame, chunks[0], st, theme);
    render_stats(frame, chunks[1], st, state, theme);
    render_details(frame, chunks[2], st, theme);
}

fn render_properties(frame: &mut Frame, area: Rect, st: &StreamTableInfo, theme: &Theme) {
    let status_style = theme.status_style(&st.status);
    let stale_style = if st.stale { theme.warning } else { theme.ok };

    let lines = vec![
        Line::from(vec![
            Span::styled(" Name:     ", theme.header),
            Span::raw(format!("{}.{}", st.schema, st.name)),
        ]),
        Line::from(vec![
            Span::styled(" Status:   ", theme.header),
            Span::styled(&st.status, status_style),
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
    _state: &AppState,
    theme: &Theme,
) {
    let lines = vec![
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

    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(theme.border)
        .title(Span::styled(" Refresh Statistics ", theme.title));

    let paragraph = Paragraph::new(lines).block(block);
    frame.render_widget(paragraph, area);
}

fn render_details(frame: &mut Frame, area: Rect, st: &StreamTableInfo, theme: &Theme) {
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

    if lines.is_empty() {
        lines.push(Line::styled(" No errors", theme.ok));
    }

    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(theme.border)
        .title(Span::styled(" Error Details ", theme.title));

    let paragraph = Paragraph::new(lines)
        .block(block)
        .wrap(Wrap { trim: false });
    frame.render_widget(paragraph, area);
}
