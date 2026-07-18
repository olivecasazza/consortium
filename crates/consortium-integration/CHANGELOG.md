# Changelog

## Features

- add executor abstraction (`Executor`, `ProcessExecutor`, `ScriptedExecutor`) for testable command execution
- add shared fleet configuration module (`fleet`), moved from `consortium-nix`
- add executor-based nix staging helpers (`staging::build_flake_attr`, `staging::copy_closure`)
- add `IntegrationReport` trait with an impl for `consortium::dag::DagReport`
