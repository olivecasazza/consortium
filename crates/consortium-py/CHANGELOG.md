# Changelog


## Bug Fixes

- make cargo build --workspace link on macOS
- resolve unported modules from oracle tree under rust backend

## Features

- expose upstream 1.10.1 APIs via PyO3 and shim ports

## Refactoring

- migrate test infrastructure to consortium-tests repo

## style

- apply cargo fmt across workspace crates


## Bug Fixes

- PyO3 bindings use contains_int/intiter/i64, auto-create venv in nix shell

## Features

- RangeSet Rust implementation with 43 passing tests
- dual-backend switching via CONSORTIUM_BACKEND env var

