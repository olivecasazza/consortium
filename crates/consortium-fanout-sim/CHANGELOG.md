# Changelog

## Bug Fixes

- seed-aware fuzz oracles + checked-in regression corpus

## Documentation

- fix rustdoc errors under RUSTDOCFLAGS="-D warnings"

## Features

- add SimExecutor — network/failure-aware Executor for integration sim tests
- harden SimExecutor determinism comparison, status visibility, and ergonomics

## style

- factor NormalizeFn alias, reword doc to satisfy clippy
- apply cargo fmt --all


## Bug Fixes

- transient-vs-permanent error semantic + summary alignment

## Features

- add log-N closure-distribution primitive + sim testbed
- builder + contention + event protocol + cli viz
- random failures + orphan re-routing in level-tree

## Testing

- tighten loose strategy assertions
- tighten loose assertions across sim test suite

