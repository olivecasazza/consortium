//! Shared output policy for consortium CLI binaries.
//!
//! Mirrors nh's setup so all our bins (claw, molt, pinch, cast)
//! present a consistent UI: green `>` INFO prefix, `!` yellow WARN
//! prefix, etc. Plus resolves color and verbosity from --verbose /
//! --color / --format flags.

use std::io::IsTerminal;

use clap::{Args, ValueEnum};

#[derive(Args, Debug, Clone, Default)]
pub struct OutputArgs {
    /// Increase verbosity: -v info → -vv debug → -vvv trace
    #[arg(short = 'v', long = "verbose", global = true, action = clap::ArgAction::Count)]
    pub verbose: u8,

    /// Color mode. `auto` (default) uses NO_COLOR env + stdout TTY detection.
    #[arg(long = "color", global = true, value_enum, default_value = "auto")]
    pub color: ColorMode,

    /// Output format for cascade visualization (where applicable).
    /// tree (default) | json | yaml | toml | jsonl
    #[arg(short = 'F', long = "format", global = true, default_value = "tree")]
    pub format: String,
}

#[derive(ValueEnum, Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum ColorMode {
    #[default]
    Auto,
    Always,
    Never,
}

pub struct CliOutput {
    pub verbosity: Verbosity,
    /// Resolved: true if color should be emitted.
    pub color: bool,
    pub format: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Verbosity {
    /// Reserve for future -q flag; unused today.
    Quiet,
    /// 0 -v flags → INFO-level messages only.
    Default,
    /// -v → DEBUG-level messages.
    Verbose,
    /// -vv+ → TRACE-level messages.
    Trace,
}

impl CliOutput {
    pub fn from_args(args: &OutputArgs) -> Self {
        let color = match args.color {
            ColorMode::Always => true,
            ColorMode::Never => false,
            ColorMode::Auto => {
                std::io::stdout().is_terminal() && std::env::var("NO_COLOR").is_err()
            }
        };
        let verbosity = match args.verbose {
            0 => Verbosity::Default,
            1 => Verbosity::Verbose,
            _ => Verbosity::Trace,
        };
        Self {
            verbosity,
            color,
            format: args.format.clone(),
        }
    }

    /// Format a status line with the green `>` prefix nh uses.
    ///
    /// Returns the formatted string — useful in tests to assert the prefix
    /// without capturing stderr.
    pub fn format_info(&self, msg: impl AsRef<str>) -> String {
        use console::Style;
        let prefix = if self.color {
            Style::new().green().bold().apply_to(">").to_string()
        } else {
            ">".to_string()
        };
        format!("{} {}", prefix, msg.as_ref())
    }

    /// Format a warning line with the yellow `!` prefix nh uses.
    pub fn format_warn(&self, msg: impl AsRef<str>) -> String {
        use console::Style;
        let prefix = if self.color {
            Style::new().yellow().bold().apply_to("!").to_string()
        } else {
            "!".to_string()
        };
        format!("{} {}", prefix, msg.as_ref())
    }

    /// Format an error line with the red `ERROR` prefix.
    pub fn format_error(&self, msg: impl AsRef<str>) -> String {
        use console::Style;
        let prefix = if self.color {
            Style::new().red().bold().apply_to("ERROR").to_string()
        } else {
            "ERROR".to_string()
        };
        format!("{} {}", prefix, msg.as_ref())
    }

    /// Print a status message with the green `>` prefix to stderr.
    ///
    /// Always written to stderr so it doesn't pollute stdout pipes.
    pub fn info(&self, msg: impl AsRef<str>) {
        eprintln!("{}", self.format_info(msg));
    }

    /// Print a warning with the yellow `!` prefix to stderr.
    pub fn warn(&self, msg: impl AsRef<str>) {
        eprintln!("{}", self.format_warn(msg));
    }

    /// Print an error with red `ERROR` prefix to stderr.
    pub fn error(&self, msg: impl AsRef<str>) {
        eprintln!("{}", self.format_error(msg));
    }

    /// Returns true if -v / --verbose was passed at least once.
    pub fn is_verbose(&self) -> bool {
        matches!(self.verbosity, Verbosity::Verbose | Verbosity::Trace)
    }

    /// Returns true if -vv / --verbose --verbose was passed (trace level).
    pub fn is_trace(&self) -> bool {
        matches!(self.verbosity, Verbosity::Trace)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args_with(verbose: u8, color: ColorMode, format: &str) -> OutputArgs {
        OutputArgs {
            verbose,
            color,
            format: format.to_string(),
        }
    }

    // ── Color resolution ──────────────────────────────────────────────────────

    #[test]
    fn color_always_forces_color() {
        let out = CliOutput::from_args(&args_with(0, ColorMode::Always, "tree"));
        assert!(out.color);
    }

    #[test]
    fn color_never_forces_no_color() {
        let out = CliOutput::from_args(&args_with(0, ColorMode::Never, "tree"));
        assert!(!out.color);
    }

    #[test]
    fn color_auto_respects_no_color_env() {
        // In CI / test harness stdout is not a TTY, so Auto → false already.
        // Force the NO_COLOR path explicitly.
        std::env::set_var("NO_COLOR", "1");
        let out = CliOutput::from_args(&args_with(0, ColorMode::Auto, "tree"));
        std::env::remove_var("NO_COLOR");
        assert!(!out.color);
    }

    // ── Verbosity mapping ─────────────────────────────────────────────────────

    #[test]
    fn verbosity_zero_is_default() {
        let out = CliOutput::from_args(&args_with(0, ColorMode::Never, "tree"));
        assert_eq!(out.verbosity, Verbosity::Default);
        assert!(!out.is_verbose());
    }

    #[test]
    fn verbosity_one_is_verbose() {
        let out = CliOutput::from_args(&args_with(1, ColorMode::Never, "tree"));
        assert_eq!(out.verbosity, Verbosity::Verbose);
        assert!(out.is_verbose());
        assert!(!out.is_trace());
    }

    #[test]
    fn verbosity_two_is_trace() {
        let out = CliOutput::from_args(&args_with(2, ColorMode::Never, "tree"));
        assert_eq!(out.verbosity, Verbosity::Trace);
        assert!(out.is_verbose());
        assert!(out.is_trace());
    }

    // ── Prefix formatting ─────────────────────────────────────────────────────

    fn no_color_out() -> CliOutput {
        CliOutput::from_args(&args_with(0, ColorMode::Never, "tree"))
    }

    #[test]
    fn info_prefix_no_color() {
        let out = no_color_out();
        assert_eq!(out.format_info("hello"), "> hello");
    }

    #[test]
    fn warn_prefix_no_color() {
        let out = no_color_out();
        assert_eq!(out.format_warn("careful"), "! careful");
    }

    #[test]
    fn error_prefix_no_color() {
        let out = no_color_out();
        assert_eq!(out.format_error("boom"), "ERROR boom");
    }

    #[test]
    fn format_field_passed_through() {
        let out = CliOutput::from_args(&args_with(0, ColorMode::Never, "json"));
        assert_eq!(out.format, "json");
    }
}
