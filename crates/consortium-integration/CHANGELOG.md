# Changelog


## Features

- native fleet deploys — flake discovery, @groups, live ssh endpoints (replaces cast-on) ([#9](https://github.com/olivecasazza/consortium/pull/9))
- per-host deploy progress and --activate-timeout ([#18](https://github.com/olivecasazza/consortium/pull/18))


## Features

- add executor abstraction (`Executor`, `ProcessExecutor`, `ScriptedExecutor`) for testable command execution
- add shared fleet configuration module (`fleet`), moved from `consortium-nix`
- add executor-based nix staging helpers (`staging::build_flake_attr`, `staging::copy_closure`)
- add `IntegrationReport` trait with an impl for `consortium::dag::DagReport`
