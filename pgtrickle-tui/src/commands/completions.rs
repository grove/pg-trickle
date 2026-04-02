use clap::{Parser, ValueEnum};
use clap_complete::{Shell, generate};

use crate::cli::Cli;

#[derive(Parser)]
pub struct CompletionsArgs {
    /// Shell to generate completions for
    #[arg(value_enum)]
    pub shell: ShellArg,
}

#[derive(Clone, ValueEnum)]
pub enum ShellArg {
    Bash,
    Zsh,
    Fish,
    #[value(name = "powershell")]
    PowerShell,
}

pub fn execute(args: &CompletionsArgs) {
    let shell = match args.shell {
        ShellArg::Bash => Shell::Bash,
        ShellArg::Zsh => Shell::Zsh,
        ShellArg::Fish => Shell::Fish,
        ShellArg::PowerShell => Shell::PowerShell,
    };

    let mut cmd = <Cli as clap::CommandFactory>::command();
    generate(shell, &mut cmd, "pgtrickle", &mut std::io::stdout());
}
