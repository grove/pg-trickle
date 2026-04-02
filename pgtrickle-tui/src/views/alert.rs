use ratatui::Frame;
use ratatui::layout::Rect;
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Paragraph};

use crate::state::AppState;
use crate::theme::Theme;

pub fn render(frame: &mut Frame, area: Rect, state: &AppState, theme: &Theme) {
    let lines: Vec<Line> = state
        .alerts
        .iter()
        .map(|alert| {
            let sev_style = theme.severity_style(&alert.severity);
            let icon = match alert.severity.as_str() {
                "critical" => "✗",
                "warning" => "⚠",
                _ => "●",
            };
            Line::from(vec![
                Span::styled(icon, sev_style),
                Span::raw(" "),
                Span::styled(alert.timestamp.format("%H:%M:%S").to_string(), theme.dim),
                Span::raw(" "),
                Span::raw(&alert.message),
            ])
        })
        .collect();

    let display = if lines.is_empty() {
        vec![Line::styled(" No alerts", theme.dim)]
    } else {
        lines
    };

    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(theme.border)
        .title(Span::styled(
            format!(" Alerts ({}) ", state.alerts.len()),
            theme.title,
        ));

    let paragraph = Paragraph::new(display).block(block);
    frame.render_widget(paragraph, area);
}
