# Contributing to YangBase

[English](CONTRIBUTING.md) | [简体中文](docs/zh/CONTRIBUTING.md)

Thanks for taking the time to contribute. This document describes what we expect, how the project is built and tested, and how a change gets merged.

YangBase is a **transactional database that keeps all durable state in object storage**. That single design decision shapes almost every rule below: object storage bills per request, has no in-place update, and has high tail latency. Contributions that ignore those constraints tend to be rejected not because they are wrong, but because they are expensive.

---

## 1. Before you start

| You want to… | Do this |
|---|---|
| Report a bug | Open a [bug report](.github/ISSUE_TEMPLATE/bug_report.yml). Include the version/commit, the object-store backend, the exact SQL, and what you expected vs. what happened. |
| Propose a feature | Open a [feature request](.github/ISSUE_TEMPLATE/feature_request.yml) **before** writing code. Large PRs that were never discussed are hard to merge. |
| Ask a question | Use [Discussions](https://github.com/clouddea/yang-base/discussions) rather than an issue. |
| Report a security problem | **Do not open a public issue.** Follow [SECURITY.md](SECURITY.md). |

If you are looking for a first task, issues labelled `good first issue` are scoped to be self-contained.

---

## 2. Development environment

Requirements:

- **JDK 21** (the build uses `jvmToolchain(21)`)
- A POSIX shell (Linux or macOS). Windows works through WSL.
- Optional: MySQL 8 client tools (`mysql`, `mysqldump`) — a few compatibility tests skip themselves when the tools are absent.
- Optional: Docker if you want MinIO instead of the embedded S3 test server.

```bash
git clone https://github.com/clouddea/yang-base.git
cd yang-base
./gradlew build              # compile + unit tests + coverage gate
./gradlew integrationTest    # integration tests (embedded S3, no Docker required)
```

Do **not** put machine-specific absolute paths (JDK home, local S3 credentials) into tracked files; the CI build breaks and so does everyone else's checkout.

Running a local instance against MinIO:

```bash
./scripts/minio.sh start
./gradlew :yb-server:run --args="--s3-bucket yangbase \
  --s3-endpoint http://127.0.0.1:9000 --s3-access-key yangbase --s3-secret-key yangbase123 \
  --port 13306"
```

---

## 3. Branching and pull requests

The full model lives in [docs/zh/development-process.md](docs/zh/development-process.md). Short version:

- `main` is always releasable and is **never pushed to directly**.
- Work happens on `feature/<version>/<slug>` or `bugfix/<version>/<slug>`, branched from `dev/<version>`, PR'd **into `dev/<version>`**, and merged with **squash merge**.
- `dev/<version>` is merged into `main` with a **merge commit** when the release is cut.
- Emergency fixes for a released version use `hotfix/<patch>/<slug>` branched from `main`.

A PR is expected to contain **one logical change**. If you find yourself writing "and also…" in the description, split it.

### Commit messages

We use a light Conventional Commits style with a module scope:

```text
fix(yb-sql): keep DECIMAL precision when mixing with integers
test(yb-server): print client output when mysqldump restore times out
docs: add branching model and development process
```

Allowed types: `feat`, `fix`, `perf`, `refactor`, `test`, `docs`, `build`, `ci`, `chore`.
Scopes are module names (`yb-sql`, `yb-storage`, `yb-mysql`, `yb-server`, …) or omitted for repository-wide changes.

### Pull request checklist

- [ ] `./gradlew build` passes locally
- [ ] `./gradlew integrationTest` passes if the change touches storage, transactions, indexes, or the MySQL protocol
- [ ] New behaviour has tests; bug fixes have a test that **fails before** the fix
- [ ] Branch coverage stays at or above the gate (50% on `yb-server`; do not lower it to make a PR pass)
- [ ] No absolute paths, no hardcoded credentials, no committed build output
- [ ] Documentation updated when user-visible behaviour changes
- [ ] PR description explains **why**, not just what

---

## 4. Non-negotiable project rules

These come from the design and from production constraints. A PR that violates them will be asked to change, even if the code is otherwise good.

1. **Request count is a first-class metric.** Object storage bills per request. Features that read the whole table to answer a small question (e.g. `COUNT(*)` doing a full scan) are not acceptable.
2. **All object keys go through `ObjectKeys`.** No string concatenation of key paths anywhere else; the key layout is part of the on-storage format and changing it silently breaks recovery.
3. **Immutable objects + manifest pointer switch.** Never mutate a stored object in place. Updating means writing a new object and switching the pointer atomically.
4. **Statistics and metadata are read from `.idx`, not from data objects.** Keep the `.idx`/`.dat` separation intact.
5. **The engine does not know about SQL dialects.** Dialect differences live in the dialect/parser layer. No `if (mysqlMode)` inside the executor.
6. **Correctness beats performance.** A faster wrong answer is always a regression.
7. **New code comments are written in English** (from 2026-11 onward). Existing Chinese comments are being migrated module by module.

---

## 5. Tests

- Unit tests live in `src/test`; integration tests live in `src/integrationTest` and are **not** part of `build`.
- Tests must never block indefinitely: any subprocess or IO wait needs a bounded timeout, and a timeout must fail with the buffered output attached. A test that hangs costs 45 minutes of CI and tells you nothing.
- Tests must not depend on filesystem iteration order or on timing assumptions that only hold on a fast local machine.
- When a test asserts an IO budget, assert on the **exact** counter (`get`, `get_range`, `put`), not on wall-clock time.

---

## 6. Review and merge

- Every change needs at least one maintainer approval and a green CI.
- Reviewers look for: correctness under crash/restart, request-cost impact, new failure modes, and whether the tests would actually catch the bug they describe.
- We aim to respond to PRs within a few days. If a PR goes quiet for two weeks it may be closed with a note — re-open it whenever you want to continue.

---

## 7. License

YangBase is licensed under [Apache-2.0](LICENSE). By submitting a pull request you agree that your contribution is licensed under the same terms. No CLA is required.
