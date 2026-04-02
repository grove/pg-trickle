use ratatui::Frame;
use ratatui::layout::Rect;
use ratatui::text::Span;
use ratatui::widgets::{Block, Borders, Cell, Row, Table};

use crate::state::AppState;
use crate::theme::Theme;

pub fn render(frame: &mut Frame, area: Rect, state: &AppState, theme: &Theme, selected: usize) {
    let header = Row::new(vec!["Check", "Severity", "Detail"])
        .style(theme.header)
        .bottom_margin(1);

    let rows: Vec<Row> = state
        .health_checks
        .iter()
        .enumerate()
        .map(|(i, h)| {
            let sev_style = theme.severity_style(&h.severity);
            let icon = match h.severity.as_str() {
                "critical" => "✗",
                "warning" => "⚠",
                "ok" => "✓",
                _ => "?",
            };
            let row = Row::new(vec![
                Cell::from(h.check_name.as_str()),
                Cell::from(format!("{icon} {}", h.severity)).style(sev_style),
                Cell::from(h.detail.as_str()),
            ]);
            if i == selected {
                row.style(theme.selected)
            } else {
                row
            }
        })
        .collect();

    let widths = [
        ratatui::layout::Constraint::Percentage(25),
        ratatui::layout::Constraint::Percentage(15),
        ratatui::layout::Constraint::Percentage(60),
    ];

    let ok_count = state
        .health_checks
        .iter()
        .filter(|h| h.severity == "ok")
        .count();
    let crit_count = state
        .health_checks
        .iter()
        .filter(|h| h.severity == "critical")
        .count();

    let table = Table::new(rows, widths).header(header).block(
        Block::default()
            .borders(Borders::ALL)
            .border_style(theme.border)
            .title(Span::styled(
                format!(" Health Checks ({ok_count} ok, {crit_count} critical) "),
                theme.title,
            )),
    );

    frame.render_widget(table, area);
}
