use ratatui::Frame;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::text::Span;
use ratatui::widgets::{Block, Borders, Cell, Row, Table};

use crate::state::AppState;
use crate::theme::Theme;

pub fn render(frame: &mut Frame, area: Rect, state: &AppState, theme: &Theme, selected: usize) {
    // Split into buffers table (top) and trigger inventory (bottom)
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Percentage(60), Constraint::Percentage(40)])
        .split(area);

    render_buffers(frame, chunks[0], state, theme, selected);
    render_triggers(frame, chunks[1], state, theme);
}

fn render_buffers(frame: &mut Frame, area: Rect, state: &AppState, theme: &Theme, selected: usize) {
    let header = Row::new(
        [
            "Stream Table",
            "Source",
            "CDC Mode",
            "Pending Rows",
            "Buffer Size",
        ]
        .iter()
        .map(|h| Cell::from(*h).style(theme.header)),
    )
    .height(1);

    let rows: Vec<Row> = state
        .cdc_buffers
        .iter()
        .enumerate()
        .map(|(i, buf)| {
            let size_style = if buf.buffer_bytes > 10_000_000 {
                theme.error
            } else if buf.buffer_bytes > 1_000_000 {
                theme.warning
            } else {
                theme.ok
            };

            let style = if i == selected {
                theme.selected
            } else {
                ratatui::style::Style::default()
            };

            Row::new(vec![
                Cell::from(buf.stream_table.as_str()),
                Cell::from(buf.source_table.as_str()),
                Cell::from(buf.cdc_mode.as_str()),
                Cell::from(buf.pending_rows.to_string()),
                Cell::from(format_bytes(buf.buffer_bytes)).style(size_style),
            ])
            .style(style)
        })
        .collect();

    let widths = [
        Constraint::Min(20),
        Constraint::Min(20),
        Constraint::Length(10),
        Constraint::Length(14),
        Constraint::Length(14),
    ];

    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(theme.border)
        .title(Span::styled(
            format!(" Change Buffers ({}) ", state.cdc_buffers.len()),
            theme.title,
        ));

    let table = Table::new(rows, widths).header(header).block(block);
    frame.render_widget(table, area);
}

fn render_triggers(frame: &mut Frame, area: Rect, state: &AppState, theme: &Theme) {
    let header = Row::new(
        ["Source Table", "Trigger Name", "Events"]
            .iter()
            .map(|h| Cell::from(*h).style(theme.header)),
    )
    .height(1);

    let rows: Vec<Row> = state
        .trigger_inventory
        .iter()
        .map(|t| {
            Row::new(vec![
                Cell::from(t.source_table.as_str()),
                Cell::from(t.trigger_name.as_str()),
                Cell::from(t.firing_events.as_str()),
            ])
        })
        .collect();

    let widths = [
        Constraint::Percentage(35),
        Constraint::Percentage(40),
        Constraint::Percentage(25),
    ];

    let ok_count = state.trigger_inventory.len();
    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(theme.border)
        .title(Span::styled(
            format!(" Trigger Inventory ({ok_count} triggers) "),
            theme.title,
        ));

    let table = Table::new(rows, widths).header(header).block(block);
    frame.render_widget(table, area);
}

fn format_bytes(bytes: i64) -> String {
    if bytes < 1024 {
        format!("{bytes} B")
    } else if bytes < 1024 * 1024 {
        format!("{:.1} KB", bytes as f64 / 1024.0)
    } else {
        format!("{:.1} MB", bytes as f64 / (1024.0 * 1024.0))
    }
}
