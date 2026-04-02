use ratatui::Frame;
use ratatui::layout::Rect;
use ratatui::text::Span;
use ratatui::widgets::{Block, Borders, Cell, Row, Table};

use ratatui::layout::Constraint;

use crate::state::AppState;
use crate::theme::Theme;

pub fn render(frame: &mut Frame, area: Rect, state: &AppState, theme: &Theme, selected: usize) {
    let header = Row::new(vec![
        "Stream Table",
        "Fuse State",
        "Errors",
        "Last Error",
        "Blown At",
    ])
    .style(theme.header)
    .bottom_margin(1);

    let rows: Vec<Row> = state
        .fuses
        .iter()
        .enumerate()
        .map(|(i, f)| {
            let state_style = match f.fuse_state.as_str() {
                "BLOWN" => theme.error,
                "TRIPPED" => theme.warning,
                "OK" | "HEALTHY" => theme.active,
                _ => theme.dim,
            };
            let icon = match f.fuse_state.as_str() {
                "BLOWN" => "✗",
                "TRIPPED" => "⚠",
                _ => "✓",
            };
            let row = Row::new(vec![
                Cell::from(f.stream_table.as_str()),
                Cell::from(format!("{icon} {}", f.fuse_state)).style(state_style),
                Cell::from(format!("{}", f.consecutive_errors)),
                Cell::from(f.last_error.as_deref().unwrap_or("-")),
                Cell::from(f.blown_at.as_deref().unwrap_or("-")),
            ]);
            if i == selected {
                row.style(theme.selected)
            } else {
                row
            }
        })
        .collect();

    let blown = state
        .fuses
        .iter()
        .filter(|f| f.fuse_state == "BLOWN")
        .count();

    let widths = [
        Constraint::Percentage(22),
        Constraint::Percentage(15),
        Constraint::Percentage(10),
        Constraint::Percentage(33),
        Constraint::Percentage(20),
    ];

    let table = Table::new(rows, widths).header(header).block(
        Block::default()
            .borders(Borders::ALL)
            .border_style(theme.border)
            .title(Span::styled(
                format!(
                    " Fuse & Circuit Breaker ({} blown, {} total) ",
                    blown,
                    state.fuses.len()
                ),
                theme.title,
            )),
    );

    frame.render_widget(table, area);
}
