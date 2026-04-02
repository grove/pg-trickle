use ratatui::Frame;
use ratatui::layout::Rect;
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Paragraph};

use crate::state::AppState;
use crate::theme::Theme;

pub fn render(frame: &mut Frame, area: Rect, state: &AppState, theme: &Theme, selected: usize) {
    let lines: Vec<Line> = state
        .dag_edges
        .iter()
        .enumerate()
        .map(|(i, edge)| {
            let style = if i == selected {
                theme.selected
            } else {
                edge.status
                    .as_deref()
                    .map(|s| theme.status_style(s))
                    .unwrap_or_default()
            };
            Line::from(Span::styled(edge.tree_line.as_str(), style))
        })
        .collect();

    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(theme.border)
        .title(Span::styled(
            format!(" Dependency Graph ({} nodes) ", state.dag_edges.len()),
            theme.title,
        ));

    let paragraph = Paragraph::new(lines).block(block);
    frame.render_widget(paragraph, area);
}
