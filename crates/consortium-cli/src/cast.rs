//! cast — NixOS / nix-darwin deployment orchestration CLI.
//!
//! Usage:
//!   cast --flake . deploy --on @darwin switch   # build, copy, activate a group
//!   cast deploy --on hp[01-03] switch           # bracket notation
//!   cast build --on contra                      # build only
//!   cast eval --on mm[01-05]                    # show what would be built
//!   cast health                                 # probe all builders
//!   cast status --on hp[01-03]                  # show current system versions
//!
//! Fleet source, in order of precedence: `--config FILE`, `./fleet.json`
//! when it exists, the flake's `fleet` output (an `mkFleet` result), and
//! finally nodes derived from the flake's `darwinConfigurations` /
//! `nixosConfigurations` names. `--on` accepts `@group` references
//! resolved through ClusterShell `groups.conf` / `groups.d`. Before a
//! deploy, every remote target's ssh endpoint is resolved live (see
//! [`consortium_nix::endpoint`]); a target that is this machine is
//! activated locally without copy or ssh.

use std::path::{Path, PathBuf};
use std::process;
use std::sync::Arc;

use clap::{Parser, Subcommand};

use crate::groups;
use crate::output::{CliOutput, OutputArgs};
use consortium_integration::exec::{Executor, ProcessExecutor};
use consortium_nix::config::{DeployAction, FleetConfig};
use consortium_nix::endpoint::{self, EndpointResolver};
use consortium_nix::fleet_source;
use consortium_nix::health;
use consortium_nix::{DeployOptions, NixArgs};

/// cast — NixOS / nix-darwin deployment orchestration powered by consortium.
#[derive(Parser)]
#[command(name = "cast", version, about)]
pub struct Args {
    /// Fleet configuration JSON file. Default: ./fleet.json when it exists,
    /// otherwise the fleet is discovered from --flake (its `fleet` output,
    /// else its darwinConfigurations / nixosConfigurations).
    #[arg(short, long)]
    config: Option<PathBuf>,

    /// Flake to deploy (e.g. /home/user/nixlab or github:user/repo);
    /// overrides the fleet config's flakeUri. Default: the current directory.
    #[arg(long)]
    flake: Option<String>,

    /// SSH user for every target, overriding the fleet's targetUser.
    /// Flake-derived nodes default to $USER.
    #[arg(long)]
    user: Option<String>,

    /// Extra words for every `nix eval` / `nix build` (whitespace-split,
    /// repeatable), e.g. --nix-args '--option builders ""'.
    #[arg(long = "nix-args", value_name = "WORDS")]
    nix_args: Vec<String>,

    /// Extra words for nix-darwin hosts' `nix eval` / `nix build` only
    /// (whitespace-split, repeatable), e.g. '--override-input foo path:./stub'.
    #[arg(long = "darwin-nix-args", value_name = "WORDS")]
    darwin_nix_args: Vec<String>,

    /// Extra words for NixOS hosts' `nix eval` / `nix build` only
    /// (whitespace-split, repeatable).
    #[arg(long = "nixos-nix-args", value_name = "WORDS")]
    nixos_nix_args: Vec<String>,

    #[command(flatten)]
    output: OutputArgs,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Evaluate which hosts need deployment.
    Eval {
        /// Target nodes (bracket notation, e.g. hp[01-03]; @group references).
        #[arg(short = 'w', long = "on")]
        on: Option<String>,

        /// Target nodes by tag.
        #[arg(short = 'g', long = "tag")]
        tag: Vec<String>,
    },

    /// Build system closures.
    Build {
        /// Target nodes (bracket notation; @group references).
        #[arg(short = 'w', long = "on")]
        on: Option<String>,

        /// Target nodes by tag.
        #[arg(short = 'g', long = "tag")]
        tag: Vec<String>,

        /// Use distributed builders.
        #[arg(long)]
        builders: bool,

        /// Maximum parallel builds.
        #[arg(short = 'f', long = "fanout", default_value = "4")]
        fanout: usize,
    },

    /// Deploy to targets (build + copy + activate).
    Deploy {
        /// Target nodes (bracket notation; @group references).
        #[arg(short = 'w', long = "on")]
        on: Option<String>,

        /// Target nodes by tag.
        #[arg(short = 'g', long = "tag")]
        tag: Vec<String>,

        /// Deployment action.
        #[arg(default_value = "switch")]
        action: String,

        /// Use distributed builders.
        #[arg(long)]
        builders: bool,

        /// Maximum parallel operations.
        #[arg(short = 'f', long = "fanout", default_value = "4")]
        fanout: usize,

        /// Use the cascade primitive for the copy stage — peer-to-peer
        /// fan-out instead of per-host serial. Each host that has the
        /// closure joins the source pool for the next round, dropping
        /// copy time from O(N) to O(log N) for hosts sharing a toplevel.
        #[arg(long = "cascade")]
        cascade: bool,

        /// Cascade tree fanout (children per node). Only meaningful with
        /// --cascade. 2 = binary tree (default); higher values trade
        /// per-source bandwidth contention for fewer rounds.
        #[arg(long = "cascade-fanout", default_value = "2")]
        cascade_fanout: u32,
    },

    /// Probe builder health.
    Health,

    /// Show current system versions on targets.
    Status {
        /// Target nodes (bracket notation; @group references).
        #[arg(short = 'w', long = "on")]
        on: Option<String>,

        /// Target nodes by tag.
        #[arg(short = 'g', long = "tag")]
        tag: Vec<String>,
    },
}

pub fn run() {
    let args = Args::parse();
    let out = CliOutput::from_args(&args.output);
    let exec = Arc::new(ProcessExecutor::new());

    let request = FleetRequest {
        config: args.config.as_deref(),
        flake: args.flake.as_deref(),
        user: args.user.as_deref(),
        cwd: Path::new("."),
    };
    let (config, source) = match load_fleet(&request, &*exec) {
        Ok(loaded) => loaded,
        Err(e) => {
            out.error(format!("{}", e));
            eprintln!(
                "hint: pass --config FILE, or run inside a flake (or pass --flake REF) \
                 that has a `fleet` output or darwinConfigurations / nixosConfigurations"
            );
            process::exit(1);
        }
    };
    if args.output.verbose > 0 {
        eprintln!("fleet: {} ({} node(s))", source, config.nodes.len());
    }

    let nix_args = nix_args_from_flags(&args.nix_args, &args.darwin_nix_args, &args.nixos_nix_args);

    let result = match args.command {
        Commands::Eval { on, tag } => cmd_eval(&config, on.as_deref(), &tag),
        Commands::Build {
            on,
            tag,
            builders,
            fanout,
        } => cmd_deploy(
            exec,
            &config,
            on.as_deref(),
            &tag,
            "build",
            builders,
            fanout,
            false,
            2,
            nix_args,
        ),
        Commands::Deploy {
            on,
            tag,
            action,
            builders,
            fanout,
            cascade,
            cascade_fanout,
        } => cmd_deploy(
            exec,
            &config,
            on.as_deref(),
            &tag,
            &action,
            builders,
            fanout,
            cascade,
            cascade_fanout,
            nix_args,
        ),
        Commands::Health => cmd_health(&config),
        Commands::Status { on, tag } => cmd_status(&config, on.as_deref(), &tag),
    };

    if let Err(e) = result {
        eprintln!("error: {}", e);
        process::exit(1);
    }
}

/// Inputs deciding where the fleet comes from.
struct FleetRequest<'a> {
    /// `--config FILE`.
    config: Option<&'a Path>,
    /// `--flake REF`.
    flake: Option<&'a str>,
    /// `--user USER`.
    user: Option<&'a str>,
    /// Directory holding the implicit `fleet.json`.
    cwd: &'a Path,
}

/// Load the fleet by precedence: `--config`, `<cwd>/fleet.json`, then
/// discovery from the flake (`fleet` output, else configuration names).
/// Applies the `--flake` and `--user` overrides. Returns the config and a
/// description of its source.
fn load_fleet(
    req: &FleetRequest<'_>,
    exec: &dyn Executor,
) -> anyhow::Result<(FleetConfig, String)> {
    let default_user = req
        .user
        .map(str::to_string)
        .or_else(|| std::env::var("USER").ok())
        .unwrap_or_else(|| "root".to_string());

    let (mut config, source) = if let Some(path) = req.config {
        (FleetConfig::from_file(path)?, path.display().to_string())
    } else {
        let implicit = req.cwd.join("fleet.json");
        if implicit.is_file() {
            (
                FleetConfig::from_file(&implicit)?,
                implicit.display().to_string(),
            )
        } else {
            let flake = req.flake.unwrap_or(".");
            let (config, origin) = fleet_source::discover_fleet(exec, flake, &default_user)?;
            (config, format!("{} of {}", origin, flake))
        }
    };

    if let Some(flake) = req.flake {
        config.flake_uri = flake.to_string();
    }
    if let Some(user) = req.user {
        for node in config.nodes.values_mut() {
            node.target_user = user.to_string();
        }
    }
    Ok((config, source))
}

/// Whitespace-split the repeatable `--*-nix-args` flags into [`NixArgs`];
/// `--nix-args` words precede the platform-specific ones.
fn nix_args_from_flags(all: &[String], darwin: &[String], nixos: &[String]) -> NixArgs {
    let words = |groups: &[&[String]]| -> Vec<String> {
        groups
            .iter()
            .flat_map(|g| g.iter())
            .flat_map(|s| s.split_whitespace())
            .map(str::to_string)
            .collect()
    };
    NixArgs {
        darwin: words(&[all, darwin]),
        nixos: words(&[all, nixos]),
    }
}

/// Resolve target nodes from --on and --tag flags, falling back to all nodes.
///
/// `--on` tokens may be `@group` references; expanded names not found in
/// the fleet fall back to their bare form (`host.local` → `host`).
fn resolve_targets(
    config: &FleetConfig,
    on: Option<&str>,
    tags: &[String],
) -> anyhow::Result<Vec<String>> {
    let mut targets = Vec::new();

    if let Some(pattern) = on {
        let ns = groups::expand_pattern(pattern, &groups::group_nodes)?;
        for name in ns.iter() {
            let bare = endpoint::bare_name(&name).to_string();
            let resolved = if config.nodes.contains_key(&name) {
                name
            } else if config.nodes.contains_key(&bare) {
                bare
            } else {
                name
            };
            if !targets.contains(&resolved) {
                targets.push(resolved);
            }
        }
    }

    if !tags.is_empty() {
        let tag_nodes = config.nodes_by_tags(tags);
        for node in tag_nodes {
            if !targets.contains(&node.name) {
                targets.push(node.name.clone());
            }
        }
    }

    // Default to all nodes if nothing specified
    if targets.is_empty() {
        targets = config.node_names();
    }

    // Validate all targets exist in config
    for name in &targets {
        if !config.nodes.contains_key(name) {
            anyhow::bail!(
                "unknown node '{}' (available: {})",
                name,
                config.node_names().join(", ")
            );
        }
    }

    Ok(targets)
}

fn cmd_eval(config: &FleetConfig, on: Option<&str>, tags: &[String]) -> anyhow::Result<()> {
    let targets = resolve_targets(config, on, tags)?;

    println!("Evaluating {} host(s):", targets.len());
    for name in &targets {
        let node = &config.nodes[name];
        println!(
            "  {} → {} ({}@{}, tags: [{}])",
            name,
            node.system,
            node.target_user,
            node.target_host,
            node.tags.join(", ")
        );
        if let Some(drv) = &node.drv_path {
            println!("    drv: {}", drv);
        }
    }

    Ok(())
}

/// Live endpoint resolution for a deploy: hosts that are this machine are
/// activated locally; every other host's `target_host` is replaced by the
/// first endpoint accepting ssh (candidates extended through `lookup`).
/// Returns the local hosts and the hosts that could not be reached (with
/// the error to report).
fn resolve_endpoints(
    exec: &dyn Executor,
    lookup: endpoint::Lookup<'_>,
    config: &mut FleetConfig,
    targets: &[String],
) -> (Vec<String>, Vec<(String, String)>) {
    let local_hostname = endpoint::local_hostname(exec);
    let mut local_hosts = Vec::new();
    let mut remote: Vec<String> = Vec::new();
    for name in targets {
        match &local_hostname {
            Some(local) if endpoint::is_local_node(name, local) => local_hosts.push(name.clone()),
            _ => remote.push(name.clone()),
        }
    }

    let resolver = EndpointResolver::new(exec, lookup);
    let results: Vec<(String, consortium_nix::Result<String>)> = std::thread::scope(|scope| {
        let handles: Vec<_> = remote
            .iter()
            .map(|name| {
                let node = &config.nodes[name];
                let resolver = &resolver;
                scope.spawn(move || (name.clone(), resolver.resolve(node)))
            })
            .collect();
        handles
            .into_iter()
            .map(|h| h.join().expect("endpoint resolution thread panicked"))
            .collect()
    });

    let mut unreachable = Vec::new();
    for (name, result) in results {
        match result {
            Ok(host) => {
                config
                    .nodes
                    .get_mut(&name)
                    .expect("validated target")
                    .target_host = host
            }
            Err(e) => unreachable.push((name, e.to_string())),
        }
    }
    (local_hosts, unreachable)
}

#[allow(clippy::too_many_arguments)]
fn cmd_deploy(
    exec: Arc<ProcessExecutor>,
    config: &FleetConfig,
    on: Option<&str>,
    tags: &[String],
    action_str: &str,
    use_builders: bool,
    fanout: usize,
    cascade: bool,
    cascade_fanout: u32,
    nix_args: NixArgs,
) -> anyhow::Result<()> {
    let mut targets = resolve_targets(config, on, tags)?;
    let action: DeployAction = action_str.parse().map_err(|_| {
        anyhow::anyhow!(
            "invalid action '{}' (try: switch, boot, test, dry-activate, build)",
            action_str
        )
    })?;

    let mut config = config.clone();
    let mut options = DeployOptions::new().nix_args(nix_args);
    let mut unreachable = Vec::new();
    if action != DeployAction::Build {
        println!("Resolving ssh endpoints for {} host(s)...", targets.len());
        let (local_hosts, failed) =
            resolve_endpoints(&*exec, &endpoint::system_lookup, &mut config, &targets);
        unreachable = failed;
        targets.retain(|t| !unreachable.iter().any(|(u, _)| u == t));
        options = options.local_hosts(local_hosts);
    }

    println!(
        "Deploying {} host(s) with action '{}'{}:",
        targets.len(),
        action,
        if cascade {
            format!(
                " [cascade copy fanout={} — peer-to-peer fan-out]",
                cascade_fanout
            )
        } else {
            String::new()
        }
    );
    for name in &targets {
        let node = &config.nodes[name];
        if options.is_local(name) {
            println!("  {} → local (this machine)", name);
        } else if action == DeployAction::Build {
            println!("  {}", name);
        } else {
            println!("  {} → {}@{}", name, node.target_user, node.target_host);
        }
    }
    for (name, err) in &unreachable {
        eprintln!("  skipping {}: {}", name, err);
    }

    let report = if targets.is_empty() {
        None
    } else if cascade && action != DeployAction::Build {
        // Determine seed addr — the host running cast IS the seed
        // (closure was built locally, or fetched here). The display
        // name is just for the live UI; NixCopyExecutor uses local
        // `nix copy` regardless.
        let seed_addr = std::env::var("USER")
            .map(|u| format!("{}@localhost", u))
            .unwrap_or_else(|_| "localhost".into());
        Some(consortium_nix::deploy_with_cascade_options(
            exec,
            &config,
            &targets,
            action,
            fanout,
            use_builders,
            cascade_fanout,
            &seed_addr,
            None, // event sink — deferred until LiveTreeRenderer wiring
            &options,
        )?)
    } else {
        Some(consortium_nix::deploy_with_options(
            exec,
            &config,
            &targets,
            action,
            fanout,
            use_builders,
            &options,
        )?)
    };

    println!();
    let mut failed = unreachable.len();
    if let Some(report) = report {
        failed += report.failure_count();
        if report.is_success() {
            println!("Deployment successful!");
            println!("  Built: {}", report.built.len());
            if action != DeployAction::Build {
                println!("  Copied: {}", report.copied.len());
                println!("  Activated: {}", report.activated.len());
            }
        } else {
            println!(
                "Deployment completed with {} failure(s):",
                report.failure_count()
            );
            for (name, err) in &report.eval_failures {
                eprintln!("  eval failed on {}: {}", name, err);
            }
            for (name, err) in &report.build_failures {
                eprintln!("  build failed on {}: {}", name, err);
            }
            for (name, err) in &report.copy_failures {
                eprintln!("  copy failed to {}: {}", name, err);
            }
            for (name, err) in &report.activation_failures {
                eprintln!("  activation failed on {}: {}", name, err);
            }
        }
    }
    if !unreachable.is_empty() {
        println!("Unreachable ({} host(s)):", unreachable.len());
        for (name, err) in &unreachable {
            eprintln!("  {}: {}", name, err);
        }
    }
    if failed > 0 {
        process::exit(1);
    }

    Ok(())
}

fn cmd_health(config: &FleetConfig) -> anyhow::Result<()> {
    if config.builders.is_empty() {
        println!("No builders configured.");
        return Ok(());
    }

    println!("Probing {} builder(s):", config.builders.len());

    let statuses = health::check_builders(config);
    let healthy_count = statuses.iter().filter(|s| s.healthy).count();

    for status in &statuses {
        let icon = if status.healthy { "ok" } else { "FAIL" };
        let latency = status
            .latency_ms
            .map(|ms| format!(" ({}ms)", ms))
            .unwrap_or_default();
        let error = status
            .error
            .as_ref()
            .map(|e| format!(" - {}", e))
            .unwrap_or_default();

        println!(
            "  [{}] {}@{}{}{} (jobs:{}, speed:{}x)",
            icon,
            status.builder.user,
            status.builder.host,
            latency,
            error,
            status.builder.max_jobs,
            status.builder.speed_factor,
        );
    }

    println!("\n{}/{} builders healthy", healthy_count, statuses.len());

    Ok(())
}

fn cmd_status(config: &FleetConfig, on: Option<&str>, tags: &[String]) -> anyhow::Result<()> {
    let targets = resolve_targets(config, on, tags)?;

    println!("Querying {} host(s):", targets.len());

    for name in &targets {
        let node = &config.nodes[name];
        match consortium_nix::eval::query_current_system(
            &ProcessExecutor::new(),
            &node.target_host,
            &node.target_user,
        ) {
            Ok(Some(path)) => println!("  {} → {}", name, path),
            Ok(None) => println!("  {} → (unknown)", name),
            Err(e) => println!("  {} → error: {}", name, e),
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use consortium_integration::exec::{ExecOutput, ScriptedExecutor};
    use consortium_nix::ProfileType;

    const FLEET_JSON: &str = r#"{"nodes":{"filehost":{"name":"filehost","targetHost":"192.0.2.1",
        "targetUser":"root","targetPort":null,"system":"x86_64-linux","profileType":"nixos",
        "buildOnTarget":false,"tags":[]}},"builders":{},"flakeUri":"."}"#;

    const MISSING: &str = "error: flake 'git+file:///cfg' does not provide attribute 'fleet'";

    /// A flake without a `fleet` output but with one darwin and one nixos
    /// configuration.
    fn flake_exec() -> ScriptedExecutor {
        ScriptedExecutor::new()
            .on("#fleet --apply", ExecOutput::new(1, "", MISSING))
            .on("#darwinConfigurations", ExecOutput::ok(r#"["mac01"]"#))
            .on("#nixosConfigurations", ExecOutput::ok(r#"["box01"]"#))
    }

    #[test]
    fn explicit_config_beats_implicit_fleet_json_and_flake() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("fleet.json"), FLEET_JSON).unwrap();
        let explicit = dir.path().join("other.json");
        std::fs::write(&explicit, FLEET_JSON.replace("filehost", "explicit")).unwrap();
        let exec = flake_exec();

        let req = FleetRequest {
            config: Some(&explicit),
            flake: None,
            user: None,
            cwd: dir.path(),
        };
        let (config, source) = load_fleet(&req, &exec).unwrap();
        assert_eq!(config.node_names(), vec!["explicit"]);
        assert_eq!(source, explicit.display().to_string());
        assert_eq!(exec.invocation_count(), 0, "no nix when a file is given");
    }

    #[test]
    fn implicit_fleet_json_in_cwd_beats_flake_discovery() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("fleet.json"), FLEET_JSON).unwrap();
        let exec = flake_exec();

        let req = FleetRequest {
            config: None,
            flake: Some("github:me/cfg"),
            user: None,
            cwd: dir.path(),
        };
        let (config, _) = load_fleet(&req, &exec).unwrap();
        assert_eq!(config.node_names(), vec!["filehost"]);
        // --flake still overrides the file's flakeUri.
        assert_eq!(config.flake_uri, "github:me/cfg");
        assert_eq!(exec.invocation_count(), 0);
    }

    #[test]
    fn without_any_file_the_fleet_is_derived_from_flake_configurations() {
        let dir = tempfile::tempdir().unwrap();
        let exec = flake_exec();

        let req = FleetRequest {
            config: None,
            flake: Some("github:me/cfg"),
            user: Some("olive"),
            cwd: dir.path(),
        };
        let (config, source) = load_fleet(&req, &exec).unwrap();
        assert_eq!(config.node_names(), vec!["box01", "mac01"]);
        assert_eq!(config.nodes["mac01"].profile_type, ProfileType::NixDarwin);
        assert_eq!(config.nodes["mac01"].target_user, "olive");
        assert_eq!(config.flake_uri, "github:me/cfg");
        assert!(source.contains("nixosConfigurations"), "{source}");
        exec.assert_invoked_containing(
            "nix eval --json github:me/cfg#nixosConfigurations --apply builtins.attrNames",
        );
    }

    #[test]
    fn user_flag_overrides_every_node() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("fleet.json"), FLEET_JSON).unwrap();
        let req = FleetRequest {
            config: None,
            flake: None,
            user: Some("deploy"),
            cwd: dir.path(),
        };
        let (config, _) = load_fleet(&req, &ScriptedExecutor::new()).unwrap();
        assert_eq!(config.nodes["filehost"].target_user, "deploy");
    }

    #[test]
    fn nix_args_flags_split_words_and_layer_platforms() {
        let args = nix_args_from_flags(
            &["--option builders ''".to_string()],
            &[
                "--override-input foo path:./stub".to_string(),
                "-L".to_string(),
            ],
            &[],
        );
        assert_eq!(
            args.darwin,
            vec![
                "--option",
                "builders",
                "''",
                "--override-input",
                "foo",
                "path:./stub",
                "-L"
            ]
        );
        assert_eq!(args.nixos, vec!["--option", "builders", "''"]);
    }

    #[test]
    fn resolve_targets_maps_group_entries_to_bare_node_names() {
        let config = FleetConfig::from_json(FLEET_JSON).unwrap();
        let targets = resolve_targets(&config, Some("filehost.local"), &[]).unwrap();
        assert_eq!(targets, vec!["filehost"]);
        let err = resolve_targets(&config, Some("ghost.local"), &[]).unwrap_err();
        assert!(
            err.to_string().contains("unknown node 'ghost.local'"),
            "{err}"
        );
    }

    #[test]
    fn resolve_endpoints_splits_local_and_rewrites_remote_target_host() {
        let mut config = FleetConfig::from_json(FLEET_JSON).unwrap();
        // Add a node whose name is this machine.
        let mut me = config.nodes["filehost"].clone();
        me.name = "Workstation".into();
        me.target_host = "Workstation".into();
        config.nodes.insert("Workstation".into(), me);
        let mut dead = config.nodes["filehost"].clone();
        dead.name = "dead".into();
        dead.target_host = "198.51.100.7".into();
        config.nodes.insert("dead".into(), dead);

        let exec = ScriptedExecutor::new()
            .on("hostname", ExecOutput::ok("workstation.lan\n"))
            .on("198.51.100.7", ExecOutput::new(255, "", "refused"))
            .on("dead.local", ExecOutput::new(255, "", "refused"))
            .on(" dead 'true'", ExecOutput::new(255, "", "refused"))
            .on("192.0.2.1", ExecOutput::new(255, "", "stale"))
            .on("filehost.local", ExecOutput::ok(""));
        let targets = vec![
            "filehost".to_string(),
            "Workstation".to_string(),
            "dead".to_string(),
        ];
        let (local, unreachable) =
            resolve_endpoints(&exec, &|_: &str| vec![], &mut config, &targets);

        assert_eq!(local, vec!["Workstation"]);
        assert_eq!(config.nodes["filehost"].target_host, "filehost.local");
        assert_eq!(unreachable.len(), 1);
        assert_eq!(unreachable[0].0, "dead");
        assert!(
            unreachable[0]
                .1
                .contains("tried: 198.51.100.7, dead.local, dead"),
            "{}",
            unreachable[0].1
        );
        // The local host was never probed over ssh.
        exec.assert_not_invoked_containing("Workstation");
    }
}
