use ratatui::style::{Color, Modifier, Style};

#[derive(Clone)]
#[allow(dead_code)]
pub struct Theme {
    pub name: &'static str,
    pub active: Style,
    pub error: Style,
    pub suspended: Style,
    pub paused: Style,
    pub selected: Style,
    pub header: Style,
    pub border: Style,
    pub warning: Style,
    pub ok: Style,
    pub title: Style,
    pub footer: Style,
    pub dim: Style,
}

impl Theme {
    pub fn default_dark() -> Self {
        Self {
            name: "default",
            active: Style::default().fg(Color::Green),
            error: Style::default().fg(Color::Red),
            suspended: Style::default().fg(Color::Yellow),
            paused: Style::default().fg(Color::DarkGray),
            selected: Style::default()
                .bg(Color::DarkGray)
                .add_modifier(Modifier::BOLD),
            header: Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
            border: Style::default().fg(Color::DarkGray),
            warning: Style::default().fg(Color::Yellow),
            ok: Style::default().fg(Color::Green),
            title: Style::default()
                .fg(Color::White)
                .add_modifier(Modifier::BOLD),
            footer: Style::default().fg(Color::DarkGray),
            dim: Style::default().fg(Color::DarkGray),
        }
    }

    pub fn status_style(&self, status: &str) -> Style {
        match status {
            "ACTIVE" => self.active,
            "ERROR" => self.error,
            "SUSPENDED" => self.suspended,
            "PAUSED" | "FROZEN" => self.paused,
            _ => Style::default(),
        }
    }

    pub fn severity_style(&self, severity: &str) -> Style {
        match severity {
            "critical" => self.error,
            "warning" => self.warning,
            "ok" => self.ok,
            _ => Style::default(),
        }
    }
}

impl Default for Theme {
    fn default() -> Self {
        Self::default_dark()
    }
}
