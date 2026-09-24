AI-ONLY: true
path: .memories/cli-grammar-gate.md
scope: project
topic: consortium-cli-grammar-gate
summary: consortium-grammar checks a Cargo-derived binary registry and clap-introspected claw/molt/pinch/cast signatures; cascade binaries and the checker itself are registry placeholders because their clap Args are private.
contract: Root long-option spellings, per-command selectors (strict cast subcommands), legacy short flags, and argument help are checked against reviewed rules. Violation keys include bin and subcommand. build.rs snapshots Cargo [[bin]] names/paths and source/test presence into the binary: installed checks never read the vanished build directory. src/grammar/baseline.json embeds an empty reviewed violation set; check fails on new, stale, or duplicate keys, and an explicit unreadable --baseline fails closed.
ci: .github/workflows/ci.yml invokes cargo run -p consortium-cli --bin consortium-grammar -- check as a strict unit step and checks the Nix-installed binary after nix build; existing nextest reporting remains unchanged.
orchestration: Grammar implementation was tracked in local-only City/Beads project tasks on the isolated grammar-gate worktree; City metadata and the Beads ledger are ignored and are not published.
links: [README.md, crates/consortium-cli/src/grammar.rs, crates/consortium-cli/src/bin/consortium_grammar.rs]
