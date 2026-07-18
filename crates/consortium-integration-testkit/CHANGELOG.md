# Changelog

## Features

- add abstract integration contract test harness: `Contract` trait, `Phase` / `OptionVariant` / `PartialFailure` fixture types, seven generic semantic checks, and the `integration_contract_tests!` macro
- add self-test dummy pipeline integration (stage → execute → collect) with negative meta-tests proving the checks catch broken implementations
