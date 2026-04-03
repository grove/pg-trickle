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

    // Body: show cached delta SQL or instructions to fetch
    let body_lines = if let Some(st) = st {
        if let Some(sql) = state.delta_sql_cache.get(&st.name) {
            // Render the actual delta SQL
            let mut lines = vec![
                Line::styled(
                    " Generated Delta SQL (press Ctrl+R to reload):",
                    theme.header,
                ),
                Line::raw(""),
            ];
            for sql_line in sql.lines() {
                lines.push(Line::raw(format!("  {sql_line}")));
            }
            lines
        } else {
            // SQL not yet fetched — show instructions
            vec![
                Line::raw(""),
                Line::styled(" Delta SQL not yet loaded. Fetch it with:", theme.header),
                Line::raw(""),
                Line::raw(format!("   :explain {}        (command palette)", st.name,)),
                Line::raw(""),
                Line::styled(" Or use the CLI:", theme.header),
                Line::raw(""),
                Line::raw(format!("   pgtrickle explain {}", st.name)),
                Line::raw("   pgtrickle explain --analyze"),
                Line::raw("   pgtrickle explain --operators"),
                Line::raw("   pgtrickle explain --dedup"),
                Line::raw(""),
                Line::styled(
                    " The delta SQL can be copied and run in psql for further analysis.",
                    theme.dim,
                ),
            ]
        }
    } else {
        vec![
            Line::raw(""),
            Line::styled(
                " No stream table selected. Press 1 to go to Dashboard, select a table, then press d.",
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
