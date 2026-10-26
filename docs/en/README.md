# YangBase Documentation (English)

[English](README.md) | [简体中文](../zh/README.md)

| Document | What it covers |
|---|---|
| [getting-started.md](getting-started.md) | Build it, run it against MinIO or local files, execute your first SQL |
| [architecture.md](architecture.md) | How the storage layout, write path, read path, transactions and metadata service fit together |
| [compatibility.md](compatibility.md) | What MySQL features are supported, what is not, and the traps in between — **read this before migrating** |

The complete design document (3000+ lines, §0–§17 plus four appendices) is maintained in Chinese at [docs/zh/yangbase-design.md](../zh/yangbase-design.md). The English architecture overview summarizes it and cites the relevant sections; where the two disagree, the Chinese document is authoritative.

Also available (Chinese only for now):

- [User guide](../zh/user-guide.md) — DDL, queries, deployment, operations in depth
- [Index benchmarks](../zh/index-bench.md) — SQL-level performance gates for every index type
- [Development process](../zh/development-process.md) — branching model and PR rules
- [Code review regression list](../zh/code-review-2026-09.md) — 59 findings from a systematic audit, each with "why it happened" and "which test guards it"

## Design principles in one screen

1. **Object storage is the only source of truth.** Compute nodes hold no durable state and can be replaced at any time.
2. **Request count is a first-class metric.** Every design decision is judged by how many object-store requests it costs, not only by latency.
3. **Objects are immutable.** Updates are new objects plus an atomic manifest pointer switch.
4. **Metadata lives in memory, statistics live in `.idx`.** Point lookups and `COUNT(*)` must not read data objects.
5. **Correctness before speed.** A faster wrong answer is a regression, not an optimization.
