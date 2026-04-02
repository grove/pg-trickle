use ratatui::Frame;
use ratatui::layout::{Constraint, Rect};
use ratatui::text::Span;
use ratatui::widgets::{Block, Borders, Cell, Row, Table};

use crate::state::AppState;
use crate::theme::Theme;

pub fn render(frame: &mut Frame, area: Rect, state: &AppState, theme: &Theme, selected: usize) {
    let header = Row::new(
        [
            "Schema",
            "Name",
            "Current",
            "Recommended",
            "Confidence",
            "Reason",
        ]
        .iter()
        .map(|h| Cell::from(*h).style(theme.header)),
    )
    .height(1);

    let rows: Vec<Row> = state
        .diagnostics
        .iter()
        .enumerate()
        .map(|(i, d)| {
            let rec_style = match d.recommended_mode.as_str() {
                "KEEP" => theme.ok,
                _ => theme.warning,
            };
            let style = if i == selected {
                theme.selected
            } else {
                ratatui::style::Style::default()
            };
            Row::new(vec![
                Cell::from(d.schema.as_str()),
                Cell::from(d.name.as_str()),
                Cell::from(d.current_mode.as_str()),
                Cell::from(d.recommended_mode.as_str()).style(rec_style),
                Cell::from(d.confidence.as_str()),
                Cell::from(d.reason.as_str()),
            ])
            .style(style)
        })
        .collect();

    let widths = [
        Constraint::Length(12),
        Constraint::Length(20),
        Constraint::Length(14),
        Constraint::Length(14),
        Constraint::Length(12),
        Constraint::Min(30),
    ];

    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(theme.border)
        .title(Span::styled(
            format!(" Diagnostics ({}) ", state.diagnostics.len()),
            theme.title,
        ));

    let table = Table::new(rows, widths).header(header).block(block);
    frame.render_widget(table, area);
}
