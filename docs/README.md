---
---

# YangBase Documentation

[English](en/README.md) | [简体中文](zh/README.md)

YangBase is a transactional (TP) database that keeps **all durable state in object storage**. The compute layer is stateless and can be destroyed and rebuilt at any time; the object store is the single source of truth.

All documentation is maintained in two languages:

| Language | Entry point | What's there |
|---|---|---|
| **English** | [docs/en/](en/README.md) | Getting started, architecture overview, compatibility and limitations |
| **简体中文** | [docs/zh/](zh/README.md) | Full design document (3000+ lines, §0–§17), user guide, compatibility list, index benchmarks, development process |

> The Chinese design document is the authoritative design record. The English architecture overview is a faithful summary of it, with section references back to the source (`> Source: §x.y`). If they ever disagree, the Chinese document wins — and that is a bug worth reporting.

## Where to start

- **Just want to run it?** → [Getting Started](en/getting-started.md)
- **Want to know how it works?** → [Architecture Overview](en/architecture.md)
- **Migrating an existing MySQL app?** → [Compatibility and Limitations](en/compatibility.md)
- **Choosing or verifying an object store?** → [Object Storage Backends](en/backends.md)
- **Want to contribute?** → [CONTRIBUTING.md](https://github.com/clouddea/yang-base/blob/main/CONTRIBUTING.md), [Versioning & release policy](en/versioning.md), and [Development process](zh/development-process.md) (Chinese)

## Documentation status

| Document | English | 简体中文 |
|---|---|---|
| Getting started | ✅ | ✅ [user-guide.md](zh/user-guide.md) (much more detailed) |
| Architecture overview | ✅ | ✅ [yangbase-design.md](zh/yangbase-design.md) (full design) |
| Compatibility & limits | ✅ | ✅ [compatibility.md](zh/compatibility.md) |
| Object storage backends | ✅ [backends.md](en/backends.md) | ✅ [backends.md](zh/backends.md) |
| Index benchmarks | — | ✅ [index-bench.md](zh/index-bench.md) |
| Development process | — | ✅ [development-process.md](zh/development-process.md) |
| Versioning & release policy | ✅ [versioning.md](en/versioning.md) | ✅ [versioning.md](zh/versioning.md) |
| Code review regression list | — | ✅ [code-review-2026-09.md](zh/code-review-2026-09.md) |
| SQL test manual | — | ✅ [sql-manual/](sql-manual/CASES.md) |

Rows marked `—` are Chinese-only for now; English versions are being written. See the roadmap in [PROJECT_GUIDE.md](https://github.com/clouddea/yang-base/blob/main/PROJECT_GUIDE.md).
