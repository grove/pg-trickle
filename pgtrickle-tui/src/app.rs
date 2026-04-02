use std::io;
use std::time::Duration;

use crossterm::event::{self, Event, KeyCode, KeyEvent, KeyModifiers};
use crossterm::execute;
use crossterm::terminal::{
    EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode,
};
use ratatui::Terminal;
use ratatui::backend::CrosstermBackend;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::Modifier;
use ratatui::text::{Line, Span};
use ratatui::widgets::Paragraph;
use tokio::sync::mpsc;
use tokio::time::interval;

use crate::cli::ConnectionArgs;
use crate::error::CliError;
use crate::state::AppState;
use crate::theme::Theme;
use crate::views;

/// Views the user can switch between.
#[derive(Clone, Copy, PartialEq, Eq)]
enum View {
    Dashboard,
    Detail,
    Graph,
    RefreshLog,
    Diagnostics,
    Cdc,
    Config,
    Health,
    Alerts,
}

impl View {
    fn label(self) -> &'static str {
        match self {
            Self::Dashboard => "Dashboard",
            Self::Detail => "Detail",
            Self::Graph => "Dependencies",
            Self::RefreshLog => "Refresh Log",
            Self::Diagnostics => "Diagnostics",
            Self::Cdc => "CDC Health",
            Self::Config => "Configuration",
            Self::Health => "Health Checks",
            Self::Alerts => "Alerts",
        }
    }

    fn key_hint(self) -> &'static str {
        match self {
            Self::Dashboard => "1",
            Self::Detail => "2",
            Self::Graph => "3",
            Self::RefreshLog => "4",
            Self::Diagnostics => "5",
            Self::Cdc => "6",
            Self::Config => "7",
            Self::Health => "8",
            Self::Alerts => "9",
        }
    }
}

const ALL_VIEWS: [View; 9] = [
    View::Dashboard,
    View::Detail,
    View::Graph,
    View::RefreshLog,
    View::Diagnostics,
    View::Cdc,
    View::Config,
    View::Health,
    View::Alerts,
];

/// Messages from the background poller to the UI thread.
enum PollMsg {
    StateUpdate(Box<AppState>),
    Error(String),
}

struct App {
    state: AppState,
    theme: Theme,
    current_view: View,
    selected: usize,
    show_help: bool,
    filter: Option<String>,
    filter_input: String,
    entering_filter: bool,
    should_quit: bool,
}

impl App {
    fn new() -> Self {
        Self {
            state: AppState::default(),
            theme: Theme::default_dark(),
            current_view: View::Dashboard,
            selected: 0,
            show_help: false,
            filter: None,
            filter_input: String::new(),
            entering_filter: false,
            should_quit: false,
        }
    }

    fn list_len(&self) -> usize {
        match self.current_view {
            View::Dashboard => self.state.stream_tables.len(),
            View::Detail => self.state.stream_tables.len(),
            View::Graph => self.state.dag_edges.len(),
            View::RefreshLog => self.state.refresh_log.len(),
            View::Diagnostics => self.state.diagnostics.len(),
            View::Cdc => self.state.cdc_buffers.len(),
            View::Config => self.state.guc_params.len(),
            View::Health => self.state.health_checks.len(),
            View::Alerts => self.state.alerts.len(),
        }
    }

    fn move_down(&mut self) {
        let len = self.list_len();
        if len > 0 {
            self.selected = (self.selected + 1).min(len - 1);
        }
    }

    fn move_up(&mut self) {
        self.selected = self.selected.saturating_sub(1);
    }
}

/// Launch the interactive TUI. Blocks until the user quits.
pub async fn run(connection: &ConnectionArgs) -> Result<(), CliError> {
    // Set up terminal
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    let result = run_app(&mut terminal, connection).await;

    // Restore terminal
    disable_raw_mode().ok();
    execute!(terminal.backend_mut(), LeaveAlternateScreen).ok();
    terminal.show_cursor().ok();

    result
}

async fn run_app(
    terminal: &mut Terminal<CrosstermBackend<io::Stdout>>,
    connection: &ConnectionArgs,
) -> Result<(), CliError> {
    let mut app = App::new();

    // Channel for state updates from poller
    let (tx, mut rx) = mpsc::channel::<PollMsg>(4);

    // Spawn background poller
    let conn_args = connection.clone();
    tokio::spawn(async move {
        poller_task(conn_args, tx).await;
    });

    loop {
        // Draw
        terminal.draw(|frame| draw_ui(frame, &app))?;

        // Handle events with a short poll timeout so we pick up async state updates
        if event::poll(Duration::from_millis(50))?
            && let Event::Key(key) = event::read()?
        {
            handle_key(&mut app, key);
        }

        // Drain state updates from poller
        while let Ok(msg) = rx.try_recv() {
            match msg {
                PollMsg::StateUpdate(new_state) => {
                    app.state = *new_state;
                }
                PollMsg::Error(e) => {
                    app.state.error_message = Some(e);
                    app.state.connected = false;
                }
            }
        }

        if app.should_quit {
            return Ok(());
        }
    }
}

async fn poller_task(conn_args: ConnectionArgs, tx: mpsc::Sender<PollMsg>) {
    // Try to connect; retry on failure
    let client = loop {
        match crate::connection::connect(&conn_args).await {
            Ok(c) => break c,
            Err(e) => {
                let _ = tx.send(PollMsg::Error(format!("Connecting: {e}"))).await;
                tokio::time::sleep(Duration::from_secs(3)).await;
                if tx.is_closed() {
                    return;
                }
            }
        }
    };

    let mut tick = interval(Duration::from_secs(2));

    loop {
        tick.tick().await;

        if tx.is_closed() {
            return;
        }

        let mut state = AppState {
            poll_interval_ms: 2000,
            ..AppState::default()
        };
        crate::poller::poll_all(&client, &mut state).await;

        if tx
            .send(PollMsg::StateUpdate(Box::new(state)))
            .await
            .is_err()
        {
            return;
        }
    }
}

fn handle_key(app: &mut App, key: KeyEvent) {
    // Filter input mode
    if app.entering_filter {
        match key.code {
            KeyCode::Esc => {
                app.entering_filter = false;
                app.filter_input.clear();
            }
            KeyCode::Enter => {
                app.entering_filter = false;
                if app.filter_input.is_empty() {
                    app.filter = None;
                } else {
                    app.filter = Some(app.filter_input.clone());
                }
                app.filter_input.clear();
            }
            KeyCode::Backspace => {
                app.filter_input.pop();
            }
            KeyCode::Char(c) => {
                app.filter_input.push(c);
            }
            _ => {}
        }
        return;
    }

    // Help overlay
    if app.show_help {
        match key.code {
            KeyCode::Esc | KeyCode::Char('?') | KeyCode::Char('q') => {
                app.show_help = false;
            }
            _ => {}
        }
        return;
    }

    // Global keys
    match key.code {
        KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => {
            app.should_quit = true;
        }
        KeyCode::Char('q') => {
            app.should_quit = true;
        }
        KeyCode::Char('?') => {
            app.show_help = true;
        }
        KeyCode::Char('/') => {
            app.entering_filter = true;
            app.filter_input.clear();
        }
        // View switching via number keys
        KeyCode::Char('1') => switch_view(app, View::Dashboard),
        KeyCode::Char('2') => switch_view(app, View::Detail),
        KeyCode::Char('3') => switch_view(app, View::Graph),
        KeyCode::Char('4') => switch_view(app, View::RefreshLog),
        KeyCode::Char('5') => switch_view(app, View::Diagnostics),
        KeyCode::Char('6') => switch_view(app, View::Cdc),
        KeyCode::Char('7') => switch_view(app, View::Config),
        KeyCode::Char('8') => switch_view(app, View::Health),
        KeyCode::Char('9') => switch_view(app, View::Alerts),
        // Navigation
        KeyCode::Char('j') | KeyCode::Down => app.move_down(),
        KeyCode::Char('k') | KeyCode::Up => app.move_up(),
        KeyCode::Enter => {
            // Drill from dashboard to detail
            if app.current_view == View::Dashboard {
                app.current_view = View::Detail;
            }
        }
        KeyCode::Esc => {
            // Back to dashboard
            if app.current_view != View::Dashboard {
                app.current_view = View::Dashboard;
            }
            app.filter = None;
        }
        KeyCode::Home => app.selected = 0,
        KeyCode::End => {
            let len = app.list_len();
            if len > 0 {
                app.selected = len - 1;
            }
        }
        _ => {}
    }
}

fn switch_view(app: &mut App, view: View) {
    if app.current_view != view {
        app.current_view = view;
        app.selected = 0;
    }
}

fn draw_ui(frame: &mut ratatui::Frame, app: &App) {
    let size = frame.area();

    // Layout: header (1) + body + footer (1)
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(1),
            Constraint::Min(3),
            Constraint::Length(1),
        ])
        .split(size);

    draw_header(frame, chunks[0], app);
    draw_body(frame, chunks[1], app);
    draw_footer(frame, chunks[2], app);

    // Help overlay
    if app.show_help {
        let overlay = centered_rect(70, 70, size);
        frame.render_widget(ratatui::widgets::Clear, overlay);
        views::help::render(frame, overlay, &app.theme);
    }
}

fn draw_header(frame: &mut ratatui::Frame, area: Rect, app: &App) {
    let conn_status = if app.state.connected {
        Span::styled("● connected", app.theme.active)
    } else if app.state.reconnecting {
        Span::styled("◌ reconnecting…", app.theme.warning)
    } else {
        Span::styled("✗ disconnected", app.theme.error)
    };

    let view_label = Span::styled(
        format!(" {} ", app.current_view.label()),
        app.theme.title.add_modifier(Modifier::REVERSED),
    );

    let poll_info = app
        .state
        .last_poll
        .map(|t| {
            let ago = chrono::Utc::now().signed_duration_since(t);
            format!("polled {}s ago", ago.num_seconds())
        })
        .unwrap_or_else(|| "no poll yet".to_string());

    let right = Span::styled(format!(" {poll_info} "), app.theme.dim);

    let header = Line::from(vec![
        Span::styled(" pg_trickle ", app.theme.title),
        view_label,
        Span::raw(" "),
        conn_status,
        Span::raw("  "),
        right,
    ]);

    frame.render_widget(Paragraph::new(header), area);
}

fn draw_body(frame: &mut ratatui::Frame, area: Rect, app: &App) {
    // Show error banner if present
    if let Some(ref err) = app.state.error_message
        && !app.state.connected
    {
        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([Constraint::Length(1), Constraint::Min(1)])
            .split(area);

        let banner = Line::from(vec![
            Span::styled(" ⚠ ", app.theme.error),
            Span::styled(err.as_str(), app.theme.error),
        ]);
        frame.render_widget(Paragraph::new(banner), chunks[0]);

        render_view(frame, chunks[1], app);
        return;
    }

    render_view(frame, area, app);
}

fn render_view(frame: &mut ratatui::Frame, area: Rect, app: &App) {
    match app.current_view {
        View::Dashboard => {
            views::dashboard::render(frame, area, &app.state, &app.theme, app.selected)
        }
        View::Detail => views::detail::render(frame, area, &app.state, &app.theme, app.selected),
        View::Graph => views::graph::render(frame, area, &app.state, &app.theme, app.selected),
        View::RefreshLog => {
            views::refresh_log::render(frame, area, &app.state, &app.theme, app.selected)
        }
        View::Diagnostics => {
            views::diagnostics::render(frame, area, &app.state, &app.theme, app.selected)
        }
        View::Cdc => views::cdc::render(frame, area, &app.state, &app.theme, app.selected),
        View::Config => views::config::render(frame, area, &app.state, &app.theme, app.selected),
        View::Health => views::health::render(frame, area, &app.state, &app.theme, app.selected),
        View::Alerts => views::alert::render(frame, area, &app.state, &app.theme),
    }
}

fn draw_footer(frame: &mut ratatui::Frame, area: Rect, app: &App) {
    let mut spans = vec![];

    if app.entering_filter {
        spans.push(Span::styled(" /", app.theme.title));
        spans.push(Span::raw(&app.filter_input));
        spans.push(Span::styled("█", app.theme.title));
    } else {
        // Context-sensitive key hints
        for view in &ALL_VIEWS {
            if *view == app.current_view {
                spans.push(Span::styled(
                    format!(" [{}]{} ", view.key_hint(), view.label()),
                    app.theme.title,
                ));
            } else {
                spans.push(Span::styled(
                    format!(" {}-{} ", view.key_hint(), view.label()),
                    app.theme.dim,
                ));
            }
        }
        spans.push(Span::styled(" ?-Help q-Quit ", app.theme.dim));

        if let Some(ref f) = app.filter {
            spans.push(Span::styled(format!(" /{f}"), app.theme.warning));
        }
    }

    let footer = Line::from(spans);
    frame.render_widget(Paragraph::new(footer), area);
}

/// Centered overlay rectangle.
fn centered_rect(percent_x: u16, percent_y: u16, r: Rect) -> Rect {
    let popup_layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Percentage((100 - percent_y) / 2),
            Constraint::Percentage(percent_y),
            Constraint::Percentage((100 - percent_y) / 2),
        ])
        .split(r);

    Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Percentage((100 - percent_x) / 2),
            Constraint::Percentage(percent_x),
            Constraint::Percentage((100 - percent_x) / 2),
        ])
        .split(popup_layout[1])[1]
}
