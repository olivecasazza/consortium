use clap::Parser;
use consortium_cli::cast::Args;

#[test]
fn every_integration_accepts_the_common_plan_grammar() {
    for (name, extra) in [
        ("nix", vec![]),
        (
            "ansible",
            vec!["--playbook", "site.yml", "--environment", "ansible"],
        ),
        ("slurm", vec!["--name", "job", "--script", "true"]),
        ("ray", vec!["--name", "job", "--entrypoint", "true"]),
        ("skypilot", vec!["--name", "cluster", "--task", "run: true"]),
    ] {
        let mut argv = vec!["cast", "integration", name, "--plan"];
        argv.extend(extra);
        assert!(Args::try_parse_from(argv).is_ok(), "{name}");
    }
}

#[test]
fn integration_grammar_rejects_unsupported_options_and_invalid_values() {
    for args in [
        vec!["nix", "--wait", "false"],
        vec!["nix", "--action", "invalid"],
        vec!["nix", "--fanout", "0"],
        vec![
            "ray",
            "--name",
            "job",
            "--entrypoint",
            "true",
            "--on",
            "node01",
        ],
        vec!["ansible", "--plan"],
        vec!["unknown", "--plan"],
    ] {
        let mut argv = vec!["cast", "integration"];
        argv.extend(args);
        assert!(Args::try_parse_from(argv).is_err());
    }
}

#[test]
fn plans_are_printed_without_external_tools() {
    let temp = tempfile::tempdir().unwrap();
    let config = temp.path().join("fleet.json");
    std::fs::write(
        &config,
        include_str!("../../../examples/inventories/fleet.json"),
    )
    .unwrap();
    for (name, extra) in [
        ("nix", vec![]),
        (
            "ansible",
            vec!["--playbook", "site.yml", "--environment", "ansible"],
        ),
        ("slurm", vec!["--name", "job", "--script", "true"]),
        ("ray", vec!["--name", "job", "--entrypoint", "true"]),
        ("skypilot", vec!["--name", "cluster", "--task", "run: true"]),
    ] {
        let output = assert_cmd::Command::cargo_bin("cast")
            .unwrap()
            .arg("--config")
            .arg(&config)
            .args(["--format", "json", "integration", name, "--plan"])
            .args(extra)
            .env("PATH", temp.path())
            .assert()
            .success()
            .get_output()
            .stdout
            .clone();
        let plan: serde_json::Value = serde_json::from_slice(&output).unwrap();
        assert_eq!(plan["integration"], name);
        assert!(!plan["phases"].as_array().unwrap().is_empty());
    }
}

#[test]
fn unmatched_tags_never_widen_selection_to_the_whole_fleet() {
    let temp = tempfile::tempdir().unwrap();
    let config = temp.path().join("fleet.json");
    std::fs::write(
        &config,
        include_str!("../../../examples/inventories/fleet.json"),
    )
    .unwrap();
    assert_cmd::Command::cargo_bin("cast")
        .unwrap()
        .arg("--config")
        .arg(config)
        .args(["integration", "nix", "--plan", "--tag", "absent-tag"])
        .env("PATH", temp.path())
        .assert()
        .failure()
        .stderr(predicates::str::contains("no hosts match"));
}

#[test]
fn root_nix_arguments_reach_the_integration_plan() {
    let temp = tempfile::tempdir().unwrap();
    let config = temp.path().join("fleet.json");
    std::fs::write(
        &config,
        include_str!("../../../examples/inventories/fleet.json"),
    )
    .unwrap();
    let output = assert_cmd::Command::cargo_bin("cast")
        .unwrap()
        .arg("--config")
        .arg(&config)
        .args([
            "--format",
            "json",
            "--nix-args",
            "--option sandbox false",
            "integration",
            "nix",
            "--plan",
        ])
        .env("PATH", temp.path())
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let plan: serde_json::Value = serde_json::from_slice(&output).unwrap();
    assert_eq!(
        plan["parameters"]["nixos-nix-args"],
        "--option sandbox false"
    );
    assert_eq!(
        plan["parameters"]["darwin-nix-args"],
        "--option sandbox false"
    );
    assert_cmd::Command::cargo_bin("cast")
        .unwrap()
        .arg("--config")
        .arg(&config)
        .args([
            "--nix-args",
            "--option sandbox false",
            "integration",
            "ray",
            "--name",
            "job",
            "--entrypoint",
            "true",
            "--plan",
        ])
        .env("PATH", temp.path())
        .assert()
        .failure()
        .stderr(predicates::str::contains("root Nix arguments apply only"));
}

#[derive(clap::Parser)]
struct IntegrationOnly {
    #[command(subcommand)]
    command: consortium_cli::integration::IntegrationCommand,
}

fn required_parameters(name: &str) -> Vec<(&'static str, &'static str)> {
    match name {
        "nix" => vec![],
        "ansible" => vec![("playbook", "site.yml"), ("environment", "ansible")],
        "slurm" => vec![("name", "job"), ("script", "true")],
        "ray" => vec![("name", "job"), ("entrypoint", "true")],
        "skypilot" => vec![("name", "cluster"), ("task", "run: true")],
        other => panic!("missing CLI fixture for {other}"),
    }
}

fn integration_argv(name: &str, overrides: &[(&str, &str)]) -> Vec<String> {
    let mut values = std::collections::BTreeMap::new();
    for (name, value) in required_parameters(name)
        .into_iter()
        .chain(overrides.iter().copied())
    {
        values.insert(name, value);
    }
    let mut argv = vec!["cast".into(), "integration".into(), name.into()];
    for (name, value) in values {
        argv.extend([format!("--{name}"), value.into()]);
    }
    argv
}

fn write_fleet(temp: &tempfile::TempDir) -> std::path::PathBuf {
    let path = temp.path().join("fleet.json");
    std::fs::write(
        &path,
        include_str!("../../../examples/inventories/fleet.json"),
    )
    .unwrap();
    path
}

#[test]
fn every_declared_parameter_has_cli_value_validation() {
    use consortium_integration::grammar::ParameterKind;
    for adapter in consortium_cli::integration::integrations() {
        let name = adapter.grammar().name;
        for parameter in adapter.grammar().parameters {
            let (valid, invalid): (Vec<&str>, Vec<&str>) = match parameter.kind {
                ParameterKind::Text => {
                    (vec!["value", "value with spaces", "quote';$HOME"], vec![""])
                }
                ParameterKind::Boolean => (vec!["true", "false"], vec!["1", "yes", "True", ""]),
                ParameterKind::PositiveInteger => (
                    vec!["1", "2"],
                    vec!["0", "-1", "1.5", "18446744073709551616", ""],
                ),
                ParameterKind::PositiveDuration => (
                    vec!["1", "0.000000001", "1.25"],
                    vec!["0", "-1", "1e3", "0.0000000001", "18446744073709551616", ""],
                ),
                ParameterKind::Choice(values) => (values.to_vec(), vec!["invalid", ""]),
            };
            for value in valid {
                assert!(
                    Args::try_parse_from(integration_argv(name, &[(parameter.name, value)]))
                        .is_ok(),
                    "{name} --{}={value:?}",
                    parameter.name
                );
            }
            for value in invalid {
                assert!(
                    Args::try_parse_from(integration_argv(name, &[(parameter.name, value)]))
                        .is_err(),
                    "{name} --{}={value:?}",
                    parameter.name
                );
            }
        }
    }
}

#[test]
fn adapters_reject_each_others_unsupported_options() {
    let adapters = consortium_cli::integration::integrations();
    let vocabulary: std::collections::BTreeSet<_> = adapters
        .iter()
        .flat_map(|a| a.grammar().parameters.iter().map(|p| p.name))
        .chain(["on", "tag"])
        .collect();
    for adapter in &adapters {
        let grammar = adapter.grammar();
        for parameter in &vocabulary {
            let supported = grammar.parameters.iter().any(|p| p.name == *parameter)
                || (grammar.targets && matches!(*parameter, "on" | "tag"));
            if !supported {
                assert!(
                    Args::try_parse_from(integration_argv(grammar.name, &[(parameter, "value")]))
                        .is_err(),
                    "{} accepted --{parameter}",
                    grammar.name
                );
            }
        }
    }
}

#[test]
fn cli_defaults_match_programmatic_request_defaults() {
    use consortium_integration::grammar::Request;
    let fleet =
        serde_json::from_str(include_str!("../../../examples/inventories/fleet.json")).unwrap();
    for adapter in consortium_cli::integration::integrations() {
        let name = adapter.grammar().name;
        let required: std::collections::BTreeSet<_> = adapter
            .grammar()
            .parameters
            .iter()
            .filter(|p| p.required)
            .map(|p| p.name)
            .collect();
        let expected: std::collections::BTreeSet<_> = required_parameters(name)
            .iter()
            .map(|(name, _)| *name)
            .collect();
        assert_eq!(
            required, expected,
            "{name}: required parameter contract changed"
        );
        let argv = integration_argv(name, &[]);
        let mut only = vec![argv[0].clone()];
        only.extend_from_slice(&argv[2..]);
        let parsed = IntegrationOnly::try_parse_from(only).unwrap();
        let mut request = Request::default();
        for (key, value) in required_parameters(name) {
            request = request.with(key, value);
        }
        let from_cli = adapter.plan(&fleet, &parsed.command.request).unwrap();
        let from_api = adapter.plan(&fleet, &request).unwrap();
        assert_eq!(
            serde_json::to_value(from_cli).unwrap(),
            serde_json::to_value(from_api).unwrap(),
            "{name}"
        );
    }
}

#[test]
fn all_supported_output_formats_preserve_the_plan() {
    let temp = tempfile::tempdir().unwrap();
    let config = write_fleet(&temp);
    for adapter in consortium_cli::integration::integrations() {
        let name = adapter.grammar().name;
        let argv = integration_argv(name, &[]);
        let mut expected = None;
        for format in ["json", "jsonl", "yaml", "toml", "tree", "text"] {
            let output = assert_cmd::Command::cargo_bin("cast")
                .unwrap()
                .arg("--config")
                .arg(&config)
                .args(["--format", format])
                .args(&argv[1..])
                .arg("--plan")
                .env("PATH", temp.path())
                .assert()
                .success()
                .get_output()
                .stdout
                .clone();
            let text = String::from_utf8(output).unwrap();
            let value: serde_json::Value = match format {
                "yaml" => serde_yaml::from_str(&text).unwrap(),
                "toml" => toml::from_str(&text).unwrap(),
                _ => serde_json::from_str(&text).unwrap(),
            };
            assert_eq!(value["integration"], name);
            if format == "jsonl" {
                assert_eq!(text.lines().count(), 1);
            }
            if let Some(expected) = &expected {
                assert_eq!(&value, expected, "{name} {format}");
            } else {
                expected = Some(value);
            }
        }
    }
}

#[test]
fn selector_combinations_choose_exact_hosts() {
    let temp = tempfile::tempdir().unwrap();
    let config = write_fleet(&temp);
    for adapter in consortium_cli::integration::integrations()
        .into_iter()
        .filter(|a| a.grammar().targets)
    {
        let name = adapter.grammar().name;
        for (selectors, expected) in [
            (vec!["--on", "node[01-02]"], vec!["node01", "node02"]),
            (vec!["--tag", "compute"], vec!["node02", "node03"]),
            (
                vec!["--on", "node01", "--tag", "compute"],
                vec!["node01", "node02", "node03"],
            ),
            (
                vec!["--tag", "web", "--tag", "web"],
                vec!["node01", "node02"],
            ),
        ] {
            let argv = integration_argv(name, &[]);
            let output = assert_cmd::Command::cargo_bin("cast")
                .unwrap()
                .arg("--config")
                .arg(&config)
                .args(["--format", "json"])
                .args(&argv[1..])
                .arg("--plan")
                .args(selectors)
                .env("PATH", temp.path())
                .assert()
                .success()
                .get_output()
                .stdout
                .clone();
            let value: serde_json::Value = serde_json::from_slice(&output).unwrap();
            assert_eq!(value["targets"], serde_json::json!(expected), "{name}");
        }
        for selectors in [
            vec!["--on", "absent"],
            vec!["--on", ""],
            vec!["--tag", "absent"],
            vec!["--on", ","],
        ] {
            let argv = integration_argv(name, &[]);
            assert_cmd::Command::cargo_bin("cast")
                .unwrap()
                .arg("--config")
                .arg(&config)
                .args(&argv[1..])
                .arg("--plan")
                .args(selectors)
                .env("PATH", temp.path())
                .assert()
                .failure();
        }
    }
}

#[cfg(unix)]
fn fake_tools(temp: &tempfile::TempDir) -> std::path::PathBuf {
    use std::os::unix::fs::PermissionsExt;
    let bin = temp.path().join("bin");
    std::fs::create_dir(&bin).unwrap();
    let shell = std::env::var("SHELL").unwrap_or_else(|_| "/bin/sh".into());
    let script = format!("#!{shell}\n")
        + r#"
set -eu
tool=${0##*/}
printf '%s:%s\n' "$tool" "$*" >> "$CALL_LOG"
if [ "$tool" = nix ] && [ "${1-}" = build ] && [ "$FAIL_BUILD" = 1 ]; then
    printf 'injected build failure\n' >&2
    exit 1
fi
case "$tool:${1-}:${2-}" in
    nix:eval:*|nix:build:*) printf '/nix/store/test-system\n' ;;
    ssh:*) case "$*" in
        *sbatch*) printf 'Submitted batch job 12345\n' ;;
        *sacct*) printf 'COMPLETED\n' ;;
    esac ;;
    ray:job:submit) printf 'Job submitted successfully\nraysubmit_123\n' ;;
    ray:job:status) printf 'Status: SUCCEEDED\n' ;;
esac
"#;
    for tool in ["nix", "ssh", "ray", "sky"] {
        let file = bin.join(tool);
        std::fs::write(&file, &script).unwrap();
        std::fs::set_permissions(file, std::fs::Permissions::from_mode(0o700)).unwrap();
    }
    bin
}

#[cfg(unix)]
#[test]
fn cli_execution_reports_outcomes_from_real_child_processes() {
    let temp = tempfile::tempdir().unwrap();
    let config = write_fleet(&temp);
    let bin = fake_tools(&temp);
    for adapter in consortium_cli::integration::integrations() {
        let name = adapter.grammar().name;
        for fail in [false, true] {
            let log = temp.path().join(format!("{name}-{fail}.log"));
            let argv = integration_argv(name, &[]);
            let output = assert_cmd::Command::cargo_bin("cast")
                .unwrap()
                .arg("--config")
                .arg(&config)
                .args(["--format", "json"])
                .args(&argv[1..])
                .env("PATH", &bin)
                .env("CALL_LOG", &log)
                .env("FAIL_BUILD", if fail { "1" } else { "0" })
                .output()
                .unwrap();
            assert_eq!(
                output.status.success(),
                !fail,
                "{name}: {}",
                String::from_utf8_lossy(&output.stderr)
            );
            let report: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
            assert_eq!(report["integration"], name);
            assert_eq!(report["success"], !fail);
            if fail {
                assert!(report["failed"].as_u64().unwrap() > 0);
                let trace = std::fs::read_to_string(log).unwrap();
                for blocked in [
                    "sbatch",
                    "ansible-playbook",
                    "job submit",
                    "sky:launch",
                    "switch-to-configuration",
                ] {
                    assert!(!trace.contains(blocked), "{name}: {trace}");
                }
            } else {
                assert!(report["succeeded"].as_u64().unwrap() > 0);
            }
        }
    }
}

#[cfg(unix)]
#[test]
fn invalid_fleet_input_never_starts_external_tools() {
    let temp = tempfile::tempdir().unwrap();
    let bin = fake_tools(&temp);
    let missing = temp.path().join("missing.json");
    for adapter in consortium_cli::integration::integrations() {
        let name = adapter.grammar().name;
        let argv = integration_argv(name, &[]);
        for (index, contents) in [None, Some("{broken-json"), Some("{}")]
            .into_iter()
            .enumerate()
        {
            let config = if let Some(contents) = contents {
                let file = temp.path().join(format!("invalid-{index}.json"));
                std::fs::write(&file, contents).unwrap();
                file
            } else {
                missing.clone()
            };
            let log = temp.path().join(format!("{name}-invalid-{index}.log"));
            assert_cmd::Command::cargo_bin("cast")
                .unwrap()
                .arg("--config")
                .arg(config)
                .args(&argv[1..])
                .env("PATH", &bin)
                .env("CALL_LOG", &log)
                .env("FAIL_BUILD", "0")
                .assert()
                .failure();
            assert!(!log.exists(), "{name}: invalid fleet invoked a tool");
        }
    }
}
