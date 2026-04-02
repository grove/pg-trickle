use ratatui::Frame;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Paragraph};

use crate::theme::Theme;

pub fn render(frame: &mut Frame, area: Rect, theme: &Theme) {
    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::Yellow))
        .title(Span::styled(
            " Help — press ? or Esc to close ",
            theme.title,
        ));

    let inner = block.inner(area);
    frame.render_widget(block, area);

    let cols = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(50), Constraint::Percentage(50)])
        .split(inner);

    let navigation = Paragraph::new(vec![
        Line::styled(" Navigation", theme.header),
        Line::raw(""),
        Line::raw("  1        Dashboard"),
        Line::raw("  2        Detail view"),
        Line::raw("  3        Dependency graph"),
        Line::raw("  4        Refresh log"),
        Line::raw("  5        Diagnostics"),
        Line::raw("  6        CDC health"),
        Line::raw("  7        Configuration"),
        Line::raw("  8        Health checks"),
        Line::raw("  9        Alerts"),
        Line::raw(""),
        Line::raw("  j / ↓    Move down"),
        Line::raw("  k / ↑    Move up"),
        Line::raw("  Enter    Drill into detail"),
        Line::raw("  Esc      Back / close overlay"),
        Line::raw("  Tab      Next pane"),
    ]);

    let actions = Paragraph::new(vec![
        Line::styled(" Actions", theme.header),
        Line::raw(""),
        Line::raw("  r        Refresh selected"),
        Line::raw("  R        Refresh all"),
        Line::raw("  Ctrl+R   Force poll now"),
        Line::raw("  /        Filter tables"),
        Line::raw("  :        Command palette"),
        Line::raw("  ?        Toggle help"),
        Line::raw("  q        Quit"),
        Line::raw("  Ctrl+C   Quit"),
    ]);

    frame.render_widget(navigation, cols[0]);
    frame.render_widget(actions, cols[1]);
}
