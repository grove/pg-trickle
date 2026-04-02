use ratatui::Frame;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Paragraph, Wrap};

use crate::state::AppState;
use crate::theme::Theme;

pub fn render(
    frame: &mut Frame,
    area: Rect,
    state: &AppState,
    theme: &Theme,
    selected: Option<usize>,
) {
    // Show delta SQL for the selected stream table (if any).
    let st = selected.and_then(|idx| state.stream_tables.get(idx));

    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(3), Constraint::Min(5)])
        .split(area);

    // Header: which table is selected
    let header_text = match st {
        Some(st) => format!(" Delta Inspector — {}.{} ", st.schema, st.name),
        None => " Delta Inspector — select a stream table on Dashboard ".to_string(),
    };

    let header = Paragraph::new(Line::from(vec![Span::styled(header_text, theme.title)])).block(
        Block::default()
            .borders(Borders::ALL)
            .border_style(theme.border),
    );
    frame.render_widget(header, chunks[0]);

    // Body: instructions (actual SQL requires a live query per-table)
    let body_lines = if st.is_some() {
        vec![
            Line::raw(""),
            Line::styled(" Use the CLI to inspect delta SQL:", theme.header),
            Line::raw(""),
            Line::raw(format!(
                "   pgtrickle explain {}",
                st.map(|s| s.name.as_str()).unwrap_or("...")
            )),
            Line::raw(""),
            Line::styled(" Available inspections:", theme.header),
            Line::raw(""),
            Line::raw("   pgtrickle explain <name>              Show generated delta SQL"),
            Line::raw("   pgtrickle explain <name> --analyze     Run EXPLAIN ANALYZE on delta"),
            Line::raw("   pgtrickle explain <name> --operators   Show DVM operator tree"),
            Line::raw("   pgtrickle explain <name> --dedup       Show deduplication stats"),
            Line::raw(""),
            Line::styled(
                " The delta SQL can be copied and run in psql for further analysis.",
                theme.dim,
            ),
        ]
    } else {
        vec![
            Line::raw(""),
            Line::styled(
                " No stream table selected. Press 1 to go to Dashboard, select a table, then press 0.",
                theme.dim,
            ),
        ]
    };

    let body = Paragraph::new(body_lines)
        .block(
            Block::default()
                .borders(Borders::ALL)
                .border_style(theme.border)
                .title(Span::styled(" Delta SQL ", theme.title)),
        )
        .wrap(Wrap { trim: false });

    frame.render_widget(body, chunks[1]);
}
