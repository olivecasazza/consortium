# Task template: resolve-rust-todo

Used for `todo!()` and `unimplemented!()` macros in `crates/`.

## Frontmatter

```yaml
type: resolve-rust-todo
target_file: crates/<crate>/src/<file>.rs
target_line: <N>
macro: todo!() | unimplemented!()
description: <surrounding fn signature + 1-line context>
acceptance:
  - macro replaced with real implementation OR unreachable!() with reason
  - if real implementation: at least one new test that hits this path
  - cargo nextest run --workspace passes
  - cargo clippy --workspace -- -D warnings passes
```

## Body

Two valid resolutions:

1. **Real implementation.** Read the function signature, surrounding
   module, and any callers. Implement the smallest correct behavior.
   Add a test in the crate's `tests/` directory (or a `#[cfg(test)]`
   module if private items are needed) that exercises it.

2. **`unreachable!()` with reason.** Only if you can prove the case is
   genuinely unreachable from the current call graph. Replace with
   `unreachable!("reason: {}", why)` and add a comment explaining the
   invariant that makes it unreachable.

If you cannot determine which applies in your time budget, abandon
with `scope: needs-design`.
