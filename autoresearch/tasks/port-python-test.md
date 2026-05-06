# Task template: port-python-test

Used for migrating one Python ClusterShell test into Rust.

## Frontmatter

```yaml
type: port-python-test
target_file: tests/<X>Test.py
target_line: <N>            # the def test_<name> line
test_name: <test_name>
description: <one-line copied from the docstring or assert message>
acceptance:
  - new test exists under crates/<appropriate-crate>/tests/
  - new test asserts the same behavior as the Python original
  - cargo nextest run --workspace passes (and the count goes up by 1)
  - the original Python test still passes (we keep both during migration)
```

## Body

Rules:
- One test per task. If the Python test class has setup/teardown that
  the Rust crate doesn't have helpers for, abandon with
  `needs-helper: <name>`.
- Match assertion semantics, not Python idioms. `assertEqual` → rust
  `assert_eq!`; `assertRaises` → `should_panic` or `Result::is_err`
  depending on the API surface.
- Do not delete the Python test. We keep both until the migration
  scorecard says otherwise.
