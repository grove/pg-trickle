use ratatui::Frame;
use ratatui::layout::Rect;
use ratatui::text::Span;
use ratatui::widgets::{Block, Borders, Cell, Row, Table};

use ratatui::layout::Constraint;

use crate::state::AppState;
use crate::theme::Theme;

pub fn render(frame: &mut Frame, area: Rect, state: &AppState, theme: &Theme, selected: usize) {
    let header = Row::new(vec![
        "Group",
        "Members",
        "Min Watermark",
        "Max Watermark",
        "Gated",
    ])
    .style(theme.header)
    .bottom_margin(1);

    let rows: Vec<Row> = state
        .watermark_groups
        .iter()
        .enumerate()
        .map(|(i, wg)| {
            let gate_style = if wg.gated {
                theme.warning
            } else {
                theme.active
            };
            let gate_icon = if wg.gated { "⚠ Yes" } else { "✓ No" };
            let row = Row::new(vec![
                Cell::from(wg.group_name.as_str()),
                Cell::from(format!("{}", wg.member_count)),
                Cell::from(wg.min_watermark.as_deref().unwrap_or("-")),
                Cell::from(wg.max_watermark.as_deref().unwrap_or("-")),
                Cell::from(gate_icon).style(gate_style),
            ]);
            if i == selected {
                row.style(theme.selected)
            } else {
                row
            }
        })
        .collect();

    let gated = state.watermark_groups.iter().filter(|w| w.gated).count();

    let widths = [
        Constraint::Percentage(25),
        Constraint::Percentage(12),
        Constraint::Percentage(23),
        Constraint::Percentage(23),
        Constraint::Percentage(17),
    ];

    let table = Table::new(rows, widths).header(header).block(
        Block::default()
            .borders(Borders::ALL)
            .border_style(theme.border)
            .title(Span::styled(
                format!(
                    " Watermarks & Source Gating ({} groups, {} gated) ",
                    state.watermark_groups.len(),
                    gated
                ),
                theme.title,
            )),
    );

    frame.render_widget(table, area);
}
