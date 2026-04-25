# consortium-autoresearch

Karpathy-style overnight agent loop for the consortium repo. Picks one
pending task at a time from a flock-protected queue, branches off
`master` in a fresh worktree, runs an opencode/Claude-Code agent against
[program.md](./program.md), gates with `cargo nextest + clippy + fmt`
(and `pytest` for Python-touching diffs), commits and opens a PR if
green, abandons with a log if not.

## Layout

```
autoresearch/
├── program.md          # the agent's standing orders (rules + per-task guidance)
├── city.toml           # gascity orchestration config (optional)
├── tasks/              # task-type templates
├── queue/
│   ├── pending/        # *.task.toml files waiting for an agent
│   ├── in-progress/    # claimed by an agent (atomic mv via flock)
│   ├── done/           # successful PRs
│   └── abandoned/      # failed runs (with appended failure log)
├── scripts/
│   ├── score.sh        # fitness gate
│   ├── pick-task.sh    # atomic dequeue
│   ├── new-worktree.sh # .claude/worktrees/agent-<id>/ on agent/<id>/<topic> branch
│   ├── finalize.sh     # commit, push, open PR
│   ├── agent-opencode.sh # invokes opencode against LiteLLM
│   ├── seed-queue.sh   # walk repo for TODOs/FIXMEs → queue/pending/
│   ├── upstream-diff.sh# cea-hpc/clustershell sync tasks
│   ├── run-once.sh     # single iteration (pick → worktree → agent → score → finalize)
│   └── run-loop.sh     # overnight driver
├── results.tsv         # karpathy-style log: task_id, branch, status, score, ...
└── logs/               # per-agent stdout/stderr
```

## One-time setup

```sh
cp autoresearch/.env.example autoresearch/.env
$EDITOR autoresearch/.env                       # fill LITELLM_API_KEY

# Get LiteLLM reachable. Either port-forward or use the tunnel hostname.
kubectl port-forward -n apps svc/litellm 4000:4000 &

# Install opencode (coding agent). Once available in nixlab's overlay we
# pull from there; for now use the upstream installer.
curl -fsSL https://opencode.ai/install | bash

# Seed the queue from current TODOs/FIXMEs.
bash autoresearch/scripts/seed-queue.sh
```

## Smoke test (no agent — just plumbing)

```sh
set -a; source autoresearch/.env; set +a
bash autoresearch/scripts/run-once.sh --no-agent
```

This claims a task, creates a worktree, runs `score.sh` against an
unmodified worktree (which should pass — no diff), and abandons because
the agent didn't change anything. Confirms the dispatch wiring works.

## Real run

```sh
set -a; source autoresearch/.env; set +a
bash autoresearch/scripts/run-loop.sh --max-tasks 5
```

Tail the logs:

```sh
tail -f autoresearch/logs/*.log
```

In the morning:

```sh
column -t -s $'\t' autoresearch/results.tsv | less
gh pr list --state open --search 'head:agent/'
```

## Multi-agent run with gascity (later)

When gascity is set up:

```sh
gc init .
gc start
```

`city.toml` declares two formulas: `drain-queue` (run-once.sh, parallelism
2) and `refill-queue` (seed + upstream-diff hourly).

## Hard rules (mirrored in program.md)

1. Never commit to `master` directly. All work goes to `agent/<id>/<topic>`.
2. Never bypass pre-commit hooks (`--no-verify`).
3. Never touch `modules/k8s/` from agent worktrees (Flux serializes K8s).
4. Never delete another agent's worktree.
5. Conventional commits, ≤72 char subject.
6. PRs go to **this fork's** master, not consortium upstream. Cherry-pick
   into consortium by hand after review.
