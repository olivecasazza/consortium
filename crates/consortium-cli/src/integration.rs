//! CLI generation and dispatch from the adapters' runtime grammar.

use std::sync::Arc;

use clap::{Arg, ArgAction, ArgMatches, Command, FromArgMatches, Subcommand};
use consortium_integration::fleet::FleetConfig;
use consortium_integration::grammar::{Integration, ParameterKind, Request};
use consortium_integration::Executor;

// Compiled from package.metadata.consortium.integrations; enrollment is gated by Nix.
include!(concat!(env!("OUT_DIR"), "/integration_registry.rs"));

/// Arguments parsed from an integration's declared vocabulary.
#[derive(Debug, Clone)]
pub struct IntegrationCommand {
    pub name: String,
    pub request: Request,
    pub plan_only: bool,
    pub on: Option<String>,
    pub tags: Vec<String>,
}

fn positive_integer(value: &str) -> Result<String, String> {
    match value.parse::<usize>() {
        Ok(n) if n > 0 => Ok(value.into()),
        _ => Err("expected a positive integer".into()),
    }
}

impl Subcommand for IntegrationCommand {
    fn augment_subcommands(mut command: Command) -> Command {
        for integration in integrations() {
            let grammar = integration.grammar();
            let mut adapter = Command::new(grammar.name).about(grammar.about).arg(
                Arg::new("plan")
                    .long("plan")
                    .help("Print the logical pipeline without executing it")
                    .action(ArgAction::SetTrue),
            );
            if grammar.targets {
                adapter = adapter
                    .arg(
                        Arg::new("on")
                            .long("on")
                            .short('w')
                            .help("Target node set or @group")
                            .value_parser(clap::builder::NonEmptyStringValueParser::new()),
                    )
                    .arg(
                        Arg::new("tag")
                            .long("tag")
                            .short('g')
                            .help("Select hosts by tag")
                            .value_parser(clap::builder::NonEmptyStringValueParser::new())
                            .action(ArgAction::Append),
                    );
            }
            for parameter in grammar.parameters {
                let mut arg = Arg::new(parameter.name)
                    .long(parameter.name)
                    .help(parameter.help)
                    .required(parameter.required);
                arg = match parameter.kind {
                    ParameterKind::Choice(values) => arg.value_parser(
                        clap::builder::PossibleValuesParser::new(values.iter().copied()),
                    ),
                    ParameterKind::Boolean => arg.value_parser(["true", "false"]),
                    ParameterKind::PositiveInteger => arg.value_parser(positive_integer),
                    ParameterKind::PositiveDuration => arg.value_parser(|value: &str| {
                        consortium_integration::grammar::parse_duration(value)
                            .map(|_| value.to_owned())
                            .map_err(|error| error.to_string())
                    }),
                    ParameterKind::Text => {
                        arg.value_parser(clap::builder::NonEmptyStringValueParser::new())
                    }
                };
                if let Some(default) = parameter.default {
                    arg = arg.default_value(default);
                }
                adapter = adapter.arg(arg);
            }
            command = command.subcommand(adapter);
        }
        command
            .subcommand_required(true)
            .arg_required_else_help(true)
    }

    fn augment_subcommands_for_update(command: Command) -> Command {
        Self::augment_subcommands(command)
    }

    fn has_subcommand(name: &str) -> bool {
        integrations().iter().any(|i| i.grammar().name == name)
    }
}

impl FromArgMatches for IntegrationCommand {
    fn from_arg_matches(matches: &ArgMatches) -> Result<Self, clap::Error> {
        let (name, args) = matches.subcommand().ok_or_else(|| {
            clap::Error::raw(
                clap::error::ErrorKind::MissingSubcommand,
                "missing integration",
            )
        })?;
        let adapter = integrations()
            .into_iter()
            .find(|i| i.grammar().name == name)
            .ok_or_else(|| {
                clap::Error::raw(
                    clap::error::ErrorKind::InvalidSubcommand,
                    "unknown integration",
                )
            })?;
        let grammar = adapter.grammar();
        let parameters = grammar
            .parameters
            .iter()
            .filter_map(|p| {
                args.get_one::<String>(p.name)
                    .map(|v| (p.name.into(), v.clone()))
            })
            .collect();
        Ok(Self {
            name: name.into(),
            request: Request {
                parameters,
                ..Request::default()
            },
            plan_only: args.get_flag("plan"),
            on: grammar
                .targets
                .then(|| args.get_one::<String>("on").cloned())
                .flatten(),
            tags: if grammar.targets {
                args.get_many::<String>("tag")
                    .map(|v| v.cloned().collect())
                    .unwrap_or_default()
            } else {
                vec![]
            },
        })
    }

    fn update_from_arg_matches(&mut self, matches: &ArgMatches) -> Result<(), clap::Error> {
        *self = Self::from_arg_matches(matches)?;
        Ok(())
    }
}

/// Execute or describe an integration after fleet loading and target selection.
pub fn dispatch(
    mut command: IntegrationCommand,
    config: &FleetConfig,
    exec: Arc<dyn Executor>,
    format: &str,
) -> anyhow::Result<()> {
    if !matches!(format, "tree" | "text" | "json" | "jsonl" | "yaml" | "toml") {
        anyhow::bail!("unsupported integration output format: {format}");
    }
    let adapter = integrations()
        .into_iter()
        .find(|i| i.grammar().name == command.name)
        .ok_or_else(|| anyhow::anyhow!("unknown integration: {}", command.name))?;
    if adapter.grammar().targets {
        let explicit_selection = command.on.is_some() || !command.tags.is_empty();
        let on_matches = match command.on.as_deref() {
            Some(pattern) => {
                !crate::groups::expand_pattern(pattern, &crate::groups::group_nodes)?.is_empty()
            }
            None => false,
        };
        if explicit_selection && !on_matches && config.nodes_by_tags(&command.tags).is_empty() {
            anyhow::bail!("no hosts match the requested selectors");
        }
        command.request.targets =
            crate::cast::resolve_targets(config, command.on.as_deref(), &command.tags)?;
    }
    if command.plan_only {
        let plan = adapter.plan(config, &command.request)?;
        render(&plan, format)?;
    } else {
        let report = adapter.run(exec, config, &command.request)?;
        render(&report, format)?;
        if !report.success {
            anyhow::bail!("{}: integration run failed", report.integration);
        }
    }
    Ok(())
}

fn render(value: &impl serde::Serialize, format: &str) -> anyhow::Result<()> {
    let text = match format {
        "json" | "jsonl" => serde_json::to_string(value)?,
        "yaml" => serde_yaml::to_string(value)?,
        "toml" => toml::to_string(value)?,
        _ => serde_json::to_string_pretty(value)?,
    };
    println!("{text}");
    Ok(())
}
