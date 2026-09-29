# .specs — consortium specs

Design documents, tasks, and the reasoning behind them, following the
[context-engineering-kit](https://github.com/NeoLabHQ/context-engineering-kit)
`.specs/` convention. This directory is the planning surface for the
project; `doc/` holds reference material for users of the tool.

## Layout

| Path            | Holds                                                          |
| --------------- | -------------------------------------------------------------- |
| `tasks/`        | Task specs, bucketed by status (see below) plus `roadmap.md`    |
| `analysis/`     | Cross-cutting analysis that informs several tasks               |
| `research/`     | Investigations and collected evidence, one file per question    |
| `reports/`      | Finished reports produced by a task                             |
| `draft/`        | Top-level design documents not yet broken into tasks            |

## Task status buckets

A task file lives in exactly one bucket, and moves between them as it is
worked. The bucket *is* the status — do not encode it in the filename or
in a frontmatter field.

| Bucket         | Meaning                                                            |
| -------------- | ------------------------------------------------------------------ |
| `todo/`        | Committed to, not started                                            |
| `draft/`       | Being written or reviewed; scope not settled                         |
| `in-progress/` | Actively being implemented right now                                |
| `done/`        | Shipped. Keep these — they are the record of why the code is shaped |
|               | the way it is                                                       |

`tasks/roadmap.md` is the checkbox list of everything, and is the
fastest way to see what the project has committed to. A task is done
when its file is in `done/`, and its roadmap checkbox is ticked.

## Design documents

A design document that has not been broken into tasks is a `draft/`.
`draft/testing-microvms.md` is the current one: the design for the
declarative microVM test fleet in `nix/vms/`. See
[`nix/vms/README.md`](../nix/vms/README.md) for the module contract it
describes.
