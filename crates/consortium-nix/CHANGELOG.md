# Changelog

## Bug Fixes

- qualify Log2FanOut intra-doc link in MaxBottleneckSpanning docs
- move expected_rounds off the trait to inherent methods

## CI

- let workspace-internal dev-deps be stripped at publish

## Documentation

- fix rustdoc errors under RUSTDOCFLAGS="-D warnings"
- fix broken intra-doc links
- drop redundant explicit intra-doc link targets

## Features

- add executor abstraction and shared integration contract
- adopt integration contract suite; fix eval-failure accounting and target validation
- per-strategy expected_rounds on CascadeStrategy
- fold cascade-viz into `cast cascade`; relay assertion in core
- measure verify against the strategy's own round rule
- declarative strategy registry with aliases
- swarm strategy as the relay assertion's negative control
- fanout traversal orders over the log2 pairing

## Refactoring

- route all command execution through the Executor abstraction
- drop deprecated pre-executor staging shims

## Testing

- add SimExecutor-based deploy pipeline sim tests

## style

- apply cargo fmt --all


## Bug Fixes

- qualify Log2FanOut intra-doc link in MaxBottleneckSpanning docs
- move expected_rounds off the trait to inherent methods

## Documentation

- fix rustdoc errors under RUSTDOCFLAGS="-D warnings"
- fix broken intra-doc links
- drop redundant explicit intra-doc link targets

## Features

- add executor abstraction and shared integration contract
- adopt integration contract suite; fix eval-failure accounting and target validation
- per-strategy expected_rounds on CascadeStrategy
- fold cascade-viz into `cast cascade`; relay assertion in core
- measure verify against the strategy's own round rule
- declarative strategy registry with aliases
- swarm strategy as the relay assertion's negative control
- fanout traversal orders over the log2 pairing

## Refactoring

- route all command execution through the Executor abstraction
- drop deprecated pre-executor staging shims

## Testing

- add SimExecutor-based deploy pipeline sim tests

## style

- apply cargo fmt --all


## Bug Fixes

- transient-vs-permanent error semantic + summary alignment
- pre-populate pending nodes + allow transient retries

## Features

- parallelize builds with DAG executor ([#2](https://github.com/olivecasazza/consortium/pull/2))
- add log-N closure-distribution primitive + sim testbed
- builder + contention + event protocol + cli viz
- LevelTreeFanOut strategy + claw default
- random failures + orphan re-routing in level-tree
- production wiring — NixCopyExecutor + cascade-copy bin
- wire cascade primitive into deploy — peer-to-peer copy fan-out

## Testing

- tighten loose strategy assertions


## Features

- add NixOS deployment (cast) with generic DAG executor
- add tool integrations (ansible, slurm, ray, skypilot) and test improvements
- add versioned documentation with GitHub Pages publishing
- add docs.rs metadata, fix semantic-release success comments

