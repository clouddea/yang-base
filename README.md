# YangBase

[English](README.md) | [简体中文](README.zh-CN.md)

[![CI](https://github.com/clouddea/yang-base/actions/workflows/ci.yml/badge.svg)](https://github.com/clouddea/yang-base/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)
[![JDK](https://img.shields.io/badge/JDK-21-orange.svg)](https://adoptium.net/)
[![Kotlin](https://img.shields.io/badge/Kotlin-2.1-7F52FF.svg)](https://kotlinlang.org/)

**A distributed TP database built entirely on object storage** — written in Kotlin, MySQL protocol compatible.

> 📖 Full design in [docs/zh/yangbase-design.md](docs/zh/yangbase-design.md) (3007 lines / §0–§17 + 4 appendices, Chinese).
> Implementation guidance and project conventions in [CLAUDE.md](CLAUDE.md).

---

## What problem does it solve?

Cloud providers offer steep discounts on ECS during holidays, then snap back to list price at renewal. An "ECS + database on local disk" architecture is therefore **held hostage by the billing cycle**: when renewal gets expensive you have to buy new machines, migrate everything, and take downtime.

Object storage is extremely cheap per byte, fully decoupled from compute, and comes with built-in multi-replica durability and strongly consistent reads. Put persistent state there, and the compute layer can be destroyed and rebuilt at any time.

**The price is three physical constraints of object storage**, and the entire design revolves around them:

| Constraint | Response |
|---|---|
| Object-level API, no in-place updates | Immutable objects + manifest pointer swap |
| High RTT (~10–60ms within a region, hundreds of ms at the tail) | Group commit amortization, multi-level caching, metadata resident in memory |
| Billed per request | **Request count is a first-class performance metric**, not just latency |

---

## Current status

**Implemented and tested** (M0 plus the core of M1/M2/M3):

| Capability | Status | How it's verified |
|---|---|---|
| `ObjectStore` SPI + capability probing | ✅ | 5 operations (`get`/`put`/`delete`/`list`/`head`) + 3 backends (in-memory / local FS / S3) + embedded S3 test server + 3 decorators (counting / retry / instrumentation) |
| S3 backend (conditional writes, Range, paginated LIST) | ✅ | Integration tests against the embedded S3 server + **real MinIO** |
| **Alibaba Cloud OSS backend** (`--s3-oss`) | ✅ | Uses OSS's own primitives: atomic create via `x-oss-forbid-overwrite` (409), three-level domain addressing; **no ETag CAS**, so pointer swap requires the explicit `--i-accept-nonatomic-pointer-switch` downgrade. **Measured against real OSS in 2026-09**: single CN (`S3IntegrationTest` 8/8, `IoBudgetIntegrationTest` 15/15) and **multi-CN + 3-node MS** (`MultiCnCoordinationTest` 4/4, with the MS Raft log also landing in OSS). ⚠️ The `objectstore` coordination mode cannot be used on OSS |
| LSM engine (MemTable / WAL / SST / Manifest / recovery) | ✅ | Unit tests + crash recovery tests |
| **`.idx`/`.dat` split for SST** | ✅ | Point-query request-count assertions (steady state 0.004 requests/query) |
| **SST data block compression** | ✅ | Measured on 20k rows: `.dat` shrinks by 56.6%; `.idx` is deliberately left uncompressed (it must stay resident in memory) |
| Group commit WAL | ✅ | Commit latency and PUT amortization (16 concurrent writers → 14.5 transactions/PUT) |
| MVCC snapshot isolation | ✅ | Cross-connection visibility tests, long-running read transactions vs GC |
| **COUNT optimization (0 data GETs)** | ✅ | Integration test asserts `get_range == 0`; 300× faster than a full table scan |
| Partitioning (`HASH` / `KEY` / **`RANGE` / `LIST`**) | ✅ | Value-based routing, out-of-range rejection, DDL validation |
| **Partition pruning + composite primary key range pushdown** | ✅ | Out-of-range queries assert **0 GETs** |
| **Shard writer lease and fencing** (§4.3) | ✅ | Epoch never resets, renewal, `LeaseLost`, stale writers ignored |
| **Two-phase commit for cross-shard transactions** | ✅ | Pending WAL + commit marker; after a crash the whole transaction vanishes atomically |
| **Secondary indexes** (ordered / hash / unique / composite) | ✅ | Maintained on the write path, covering index avoids table lookups, atomic uniqueness checks, NULLs excluded |
| **Online index build** (§8.7) | ✅ | BUILDING → snapshot backfill → incremental catch-up → ACTIVE; writes during the build window are not lost |
| **Skip indexes** (minmax / set / bloom / ngram) | ✅ | `.skp` written alongside SSTs; scans **skip entire blocks with 0 GETs** |
| **Full-text search** (`MATCH ... AGAINST`) | ✅ | Hand-rolled inverted index (FST + postings + WAND + BM25); `WITH PARSER ngram` |
| **Garbage collection and time travel** | ✅ | GC watermark · `AS OF TIMESTAMP` · **PITR** · backup/restore · recoverable `DROP TABLE` tombstones |
| **`ALTER TABLE`** | ✅ | Add column / drop trailing column / add or drop index / rename / `TRUNCATE`, all without rewriting data (O(1) metadata) |
| **CDC change streams** | ✅ | Reads the existing WAL directly (no second change log); cursors are persistable |
| **Segmented allocation of auto-increment primary keys** (§11.4) | ✅ | No duplicate IDs across restarts or nodes; 1000 allocations write the watermark object only once |
| **Optimizer and statistics** (§10.3) | ✅ | `ANALYZE TABLE` (reads only `.idx`, 0 `get_range`) + histograms + HLL cardinality + cost model + `EXPLAIN` |
| **SQL surface area** | ✅ | Aggregates (`SUM`/`AVG`/`MIN`/`MAX`/`GROUP_CONCAT`) · `GROUP BY`/`HAVING` (with `ONLY_FULL_GROUP_BY` enforced) · `JOIN` (inner/left/right/cross, equi-joins via hash) · `UNION` · `SELECT DISTINCT` · **window functions** (ranking/value/aggregate windows + `PARTITION BY`/frames) · subqueries (non-correlated · correlated in predicate position and in **scalar position**) · `JSON` function family |
| **Metadata service MS** (§4) | ✅ | Raft (log/snapshot in object storage, cold start <3s) + state machine (timestamp watermark / leases / catalog version) + row lock table + CN client; deploy standalone with `--role ms`, or in-process with the CN via `--run-meta-service` |
| **Dual-mode coordination** (§4.7) | ✅ | `single` / `objectstore` / `ms` implementations all share **the same consistency test suite** (timestamps never overlap, epoch never resets, exclusive locks really are mutually exclusive) |
| **`SELECT ... FOR UPDATE`** | ✅ | Row locks go through the coordination service; pre-commit self-check (an MS leader change loses the lock → error 1213 and retry, instead of silently losing an update) |
| **L1 adaptation layer** (§2.2) | ✅ | Retry/backoff/rate limiting: transient failures are retried, `412`/`404` never are; the local FS backend requires an explicit `--i-know-this-is-not-durable` |
| **Observability** (§12.5) | ✅ | `/metrics` + `/healthz`, slow query log, object storage audit log; scraping **generates no object storage requests** |
| MySQL protocol (handshake / COM_QUERY / prepared statements / cursors) | ✅ | **Real JDBC driver** end to end; server-side cursors via `useCursorFetch`, multi-statement, character sets |
| SQL types | ✅ | Integers / floats / `DECIMAL` / strings / `BLOB` / temporal / `YEAR` / `ENUM` / `SET` / `JSON` |
| `information_schema` + `SHOW` family | ✅ | Introspection works for clients such as DataGrip |
| Stateless compute nodes (recover from object storage after restart) | ✅ | Recovery test that destroys a CN and rebuilds it |
| **Disk (NVMe) second-level cache** | ✅ | After a CN restart, reading the same data issues **0 object storage requests**; CRC validation, LRU eviction |
| **`yb-cli` operations tool** | ✅ | `ls` / `shards` / `gc` / `recover-check` / `cost` / `backup` / `backups` / `restore` / `pitr` / `cdc` / `analyze-sql`; operates directly on object storage and **does not require a CN to be online**; capacity accounting only spends LIST requests (no per-object `head`) |
| Auth / permissions / TLS | ❌ | Accepts any username and password; intended for trusted networks only |
| Storage tiers and lifecycle rules (§13.3) | ❌ | The design doc warns explicitly about this (archive tiers almost always lose money), see [docs/en/compatibility.md](docs/en/compatibility.md) |

> **Compatibility and limitations** (which MySQL features are unsupported, where semantics differ) are
> collected in [docs/en/compatibility.md](docs/en/compatibility.md) — **read it before migrating**,
> especially the two entries on "the default isolation level does not prevent write skew" and
> "auto-increment is not guaranteed to be contiguous".

---

## Quick start

### Build and test

```bash
./gradlew build              # compile + all checks
./gradlew test               # unit tests
./gradlew integrationTest    # integration tests (embedded S3 server, no Docker needed)
./gradlew koverVerify        # branch coverage gate (≥ 50%)
./gradlew :yb-bench:bench    # benchmarks
```

> Integration tests use the project's built-in **S3-compatible server** (`EmbeddedS3Server`), which speaks
> real HTTP and implements real conditional-write semantics, so no Docker or external binaries are needed.
> For production validation, MinIO or real object storage is still recommended — see "Verifying with MinIO" below.
>
> ⚠️ **Neither `build` nor `check` includes `integrationTest`** — it lives in a separate source set (see
> "Build and test" in [PROJECT_GUIDE.md](PROJECT_GUIDE.md)). Before committing, run both:
> `./gradlew build integrationTest`.

### Starting the server

```bash
# Local filesystem backend (development only)
# ⚠️ --i-know-this-is-not-durable is **required**: local FS has no multi-replica durability and no
#    cross-process conditional-write guarantee, while writer election, manifest pointer swap, fencing
#    and crash recovery all depend on object storage semantics. Without this flag the server refuses
#    to start (YangBaseServer.requireDurableStorageUnlessAllowed).
./gradlew :yb-server:run --args="--data-dir /tmp/yangbase --i-know-this-is-not-durable --port 3306"

# S3-compatible object storage (production)
./gradlew :yb-server:run --args="\
  --s3-bucket my-yangbase-bucket \
  --s3-endpoint https://oss-cn-hangzhou.aliyuncs.com \
  --s3-access-key $AK --s3-secret-key $SK \
  --port 3306"
```

### Connecting over JDBC

```kotlin
val conn = DriverManager.getConnection(
    "jdbc:mysql://127.0.0.1:3306/mydb?useSSL=false&allowPublicKeyRetrieval=true",
    "root", ""
)
conn.createStatement().use { st ->
    st.execute("CREATE TABLE users (id BIGINT NOT NULL, name VARCHAR(64), PRIMARY KEY (id))")
    st.execute("INSERT INTO users VALUES (1, 'alice'), (2, 'bob')")
    st.executeQuery("SELECT name FROM users WHERE id = 2").use { rs ->
        while (rs.next()) println(rs.getString(1))   // bob
    }
}
```

### Verifying with MinIO

The integration tests **use the project's built-in S3-compatible server by default** (real HTTP + real
conditional writes) and need no external dependencies. But the same assertions also run unchanged against a
real MinIO (`minio.sh test` points both `:yb-server:integrationTest` and `:yb-ms:integrationTest` at
it — the latter being the one that proves "the MS log really lands in object storage"):

```bash
# One command (binary mode, zero dependencies)
./scripts/minio.sh test

# One command (container mode)
./scripts/minio.sh test docker

# Just start / stop / clean data
./scripts/minio.sh start          # binary
./scripts/minio.sh start docker   # container
./scripts/minio.sh stop
./scripts/minio.sh clean          # use when data is corrupted or permissions are scrambled
```

> If your current shell hasn't activated the `docker` group (`usermod` only affects new login sessions),
> the script automatically wraps itself in `sg docker`.

Benchmark numbers from the two modes are nearly identical (same machine, container network overhead is negligible):

| | Commit p50 | 16-writer TPS | Group commit amortization |
|---|---|---|---|
| Bare binary | 4006µs | 3376 | 14.48 |
| Container | 4005µs | 3333 | 14.61 |

Starting the server against MinIO:

```bash
./gradlew :yb-server:run --args="--s3-bucket yangbase --s3-endpoint http://127.0.0.1:9000 \
  --s3-access-key yangbase --s3-secret-key yangbase123"
```

> You can also run MinIO in Docker (`docker run -d -p 9000:9000 ... minio/minio server /data`),
> but the current user must be in the `docker` group (`sudo usermod -aG docker $USER`, then log in again).
> `scripts/minio.sh` uses a single static binary and needs no privileges at all.

> ⚠️ **Two known issues with Docker on this machine**
>
> **(1) The daemon is configured to go through a local proxy**
> (`HTTP_PROXY=http://127.0.0.1:7890` in `/lib/systemd/system/docker.service`),
> and that proxy is currently unavailable, so **every `docker pull` fails with EOF** — even when the
> registry itself is directly reachable. `scripts/minio.Dockerfile` therefore uses `FROM scratch` to
> **build offline** (MinIO is a static binary and needs no base image), sidestepping the problem entirely.
> To actually fix the proxy, move that `Environment="*_PROXY=..."` stanza out of the service file into
> `/etc/systemd/system/docker.service.d/proxy.conf`, change it to a working address, then
> `sudo systemctl daemon-reload && sudo systemctl restart docker`.
>
> **(2) scratch images run as root by default**, so the data files they write are owned by root,
> which makes them unreadable even for the host's binary mode. The script adds
> `--user "$(id -u):$(id -g)"`, and the two modes use **separate data directories** so they can't pollute each other.
>
> ⚠️ **A gotcha when downloading MinIO**: `dl.min.io/aistor/...` now serves **MinIO AIStor (the commercial
> edition)**, which without a license rejects **all S3 operations** (the log says
> `All S3 operations are denied`). The open-source edition lives on **GitHub Releases**:
> ```
> https://github.com/minio/minio/releases/download/RELEASE.2025-09-07T16-13-09Z/minio.linux-amd64.RELEASE.2025-09-07T16-13-09Z
> ```
> How to tell: run `./minio --version` and start it — if the log says `License: GNU AGPLv3`, it's the open-source edition.

---

## Skip indexes

A skip index never returns rows; it answers exactly one question: **might this data block contain the value
I'm looking for?** Blocks that answer "no" generate **zero GETs** — that is the entire mechanism by which
it saves IO.

```sql
CREATE TABLE events (
    id BIGINT NOT NULL,
    created_at BIGINT NOT NULL,
    payload VARCHAR(64),
    PRIMARY KEY (id),
    KEY idx_created (created_at) USING MINMAX     -- range pruning
);
-- or set/bloom (equality pruning), ngram (LIKE prefix)
CREATE INDEX idx_payload ON events(payload) USING BLOOM;
```

The write path is **incidental**: while `SstWriter` writes a data block it feeds each row's column values to
`SkipIndexBuilder`, and `.skp` is written out along with it. Tables without skip indexes don't decode a
single extra row.

The read path asks a `BlockPruner` **before reading the block** — order matters; reading first and then
deciding is no pruning at all. Decisions prefer the block-level min/max already present in `.idx`
(**0 GETs, 0 bytes transferred**, since that data is resident in memory anyway); `.skp` only supplements
the set / bloom / ngram information that `.idx` doesn't have.

> **One precondition for use**: the effectiveness of min/max pruning depends on whether the data is
> **clustered** on that column. With fully random writes, every block's min/max spans the entire domain and
> the index degrades to "can never prune". This system doesn't yet offer a clustering hint mechanism
> (mentioned in design doc §9.2), so monotonically time-ordered writes are the most effective case.

---

## Full-text search

The inverted index is hand-rolled: FST dictionary + block-partitioned postings + WAND early termination +
BM25 scoring, and it is itself "immutable files + manifest pointer", stored in object storage (§8.6,
principle P2).

```sql
CREATE TABLE articles (id BIGINT NOT NULL, body TEXT, PRIMARY KEY (id));

-- English: the standard tokenizer (lowercasing + stopword removal)
CREATE FULLTEXT INDEX ft_body ON articles(body) WITH PARSER standard;

-- Chinese: you must specify ngram explicitly — standard treats all CJK as separators
CREATE FULLTEXT INDEX ft_body ON articles(body) WITH PARSER ngram;

-- Building the index indexes **existing data** too; no table rebuild required

SELECT id FROM articles WHERE MATCH(body) AGAINST('storage engine');
SELECT id FROM articles WHERE MATCH(body) AGAINST('+quick -rabbit' IN BOOLEAN MODE);
SELECT id FROM articles WHERE MATCH(body) AGAINST('data*' IN BOOLEAN MODE);
SELECT id FROM articles WHERE MATCH(body) AGAINST('"quick brown"' IN BOOLEAN MODE);
```

Two design points worth noting:

- **`MATCH` is an access path, not a per-row predicate.** BM25 scoring depends on
  **corpus-wide statistics** such as document frequency and average document length, which live in no
  single row. So the executor first pulls candidate primary keys from the inverted index, then does the
  table lookup, and finally applies the remaining `WHERE` filters. Pushing it into per-row evaluation is
  semantically wrong (the code reports an explicit error if you try).
- **The index is maintained together with the data, and only updated when the transaction commits.**
  Indexing immediately would make uncommitted rows searchable — that's a dirty read, and in fuzzy
  matching a few extra rows are very hard to notice.

### Second-level cache (local NVMe)

`BlockCache` is an **in-memory** cache; it is empty the moment a CN restarts, so for a while after a rebuild
every read goes back to object storage. Local NVMe turns that "getting warm again" period from hundreds of
network round trips into hundreds of local reads:

```bash
./gradlew :yb-server:run --args="--data-dir /var/lib/yangbase --i-know-this-is-not-durable \
    --disk-cache-dir /mnt/nvme/yangbase-cache --disk-cache-mb 51200"
```

> `--data-dir` must be paired with `--i-know-this-is-not-durable` (see "Starting the server" above);
> in production use `--s3-bucket` instead, in which case this flag is neither needed nor appropriate.

It **saves request count, not latency** (latency only improves as a side effect), which is why the assertion
is on request count: in the integration test, `get_range` for "read the same data after a restart" must be 0.

It is safe because it **is nothing** — not state, just a cache. Delete the whole directory and the system
keeps working. That is the precondition for its existence (principle P1: everything on local disk must be
disposable).

Two design notes: only **immutable** objects are cached (caching a mutable key such as `manifest/CURRENT`
by key would serve stale content, which is a silent misread); each block is prefixed with a 4-byte CRC32C
that is verified on read, and a mismatch is treated as a miss — **the cache may be slow, but it must never lie**.

---

## Operations

A database on object storage has one particular difficulty: **the things you can't see are the problem**.
Which shard holds the data, how big the manifest is, which objects are no longer referenced — things that
`du` and `ls` settle in a traditional database need dedicated tooling on object storage. `yb-cli` does that,
and it **operates directly on object storage without needing a CN online** (the compute layer can be
destroyed at any time; operational actions shouldn't depend on it).

```bash
# What's in storage (grouped by prefix)
./gradlew :yb-cli:cli -Pargs="ls --endpoint http://127.0.0.1:9000 --bucket yangbase"

# How big each shard is, and how many rows
./gradlew :yb-cli:cli -Pargs="shards --endpoint ..."

# Garbage collection. By default it **marks only, never deletes**; add --delete to actually delete
./gradlew :yb-cli:cli -Pargs="gc --endpoint ..."
./gradlew :yb-cli:cli -Pargs="gc --delete --endpoint ..."

# Check recovery-path health: any objects referenced by the manifest but missing
./gradlew :yb-cli:cli -Pargs="recover-check --endpoint ..."

# Estimate monthly cost using the cost model from design doc §13
./gradlew :yb-cli:cli -Pargs="cost --endpoint ..."
```

A few more commands (also operating directly on object storage):

```bash
# Backup (writes a manifest only, cost independent of data size) / list backups / restore (--yes to actually do it)
./gradlew :yb-cli:cli -Pargs="backup --note pre-upgrade --endpoint ..."
./gradlew :yb-cli:cli -Pargs="backups --endpoint ..."
./gradlew :yb-cli:cli -Pargs="restore --yes --endpoint ..."

# Point-in-time recovery: roll a table back to a moment in time
# ⚠️ Use T rather than a space in the timestamp: -Pargs splits arguments on whitespace, so writing
#    "2026-09-25 10:00:00" gets split into two arguments and only the first half is consumed by --to
#    (silently becoming 00:00:00 that day)
./gradlew :yb-cli:cli -Pargs="pitr --table users --to 2026-09-25T10:00:00 --yes --endpoint ..."

# Change streams: read the existing WAL directly, no second change log
./gradlew :yb-cli:cli -Pargs="cdc --since 2026-09-25T10:00:00 --limit 100 --endpoint ..."

# Migration risk scan: which parts of the target SQL this system doesn't support, or where semantics differ from MySQL
./gradlew :yb-cli:cli -Pargs="analyze-sql --path ./migrations"
```

GC is **two-phase** (design doc §6.9): mark first and write the list, then actually delete **only after a
safety window (24 hours by default)**. The window isn't there to "wait for readers to finish" — it downgrades
the cost of a misjudgment from "silently reading wrong data" to "paying for one extra day of storage". The
`gc` report counts **fenced-out stale writer WALs** and **compaction leftovers** separately — the former is a
normal cost of fencing (§4.3), and lumping them together makes leaks look far worse than they are.

---

## Architecture

```
 App / JDBC ──MySQL protocol──▶ Compute Node (CN) × N      stateless, add or remove at will
                               ├ SQL parse / optimize / execute
                               ├ transaction layer (MVCC)
                               ├ LSM storage engine + index layer
                               └ in-memory BlockCache (disposable)
                                        │
                                        ▼
        ┌───────────────────────────────────────────────────────┐
        │  Object storage (S3 / OSS / COS / MinIO) ★single source of truth★ │
        │  catalog/  shard/.../wal|sst|manifest  stats/          │
        └───────────────────────────────────────────────────────┘
```

### Four core mechanisms

**1. Point query = 1 GET** (`yb-storage`)

SSTs are split into two objects: `.idx` (small, resident in memory) and `.dat` (large, read by byte range).
Because the index is in memory, a point query only needs the final hop to actually fetch the data block from
object storage.

```
manifest (resident) → bloom check (memory) → binary search .idx for the block (memory) → 1 Range GET
```

**2. COUNT = 0 GETs** (§8.9)

`SELECT COUNT(*)` is both the most expensive and the most common query on object storage. This system makes
it **read no data at all**:

- **Exact row counts in the manifest** — maintained by the single writer (which serializes all commits and
  therefore knows each transaction's net row delta)
- **Block-level cumulative `commit_ts` counts** — enables exact counts for historical snapshots
- **Block-level "value → count"** — makes `COUNT(*) WHERE status='x'` also 0 GETs

**3. Group commit** (§6.3.4)

On object storage a PUT costs 12.5× a GET (AWS) and has high latency. The writer batches up and persists a
batch of transactions with a single PUT. In the benchmark, 16 concurrent writers amortized 3200 transactions
down to 221 PUTs (**14.5 transactions/PUT**); single-threaded serial writes have no batching headroom
(1.0 transactions/PUT) — the benefit of group commit only shows up under concurrency.

**4. Partitions are write shards** (§9.3)

MySQL "partitions" and this system's "shards" align naturally: `PARTITION BY HASH(user_id) PARTITIONS 8`
directly yields 8 independent writers and 8× write parallelism. The partition function is deterministic, so
**no data movement ever occurs**.

---

## Modules

| Module | Responsibility |
|---|---|
| `yb-common` | Types, order-preserving byte encoding, row/value codecs, schema |
| `yb-objectstore` | `ObjectStore` SPI + in-memory/local FS/S3 implementations + **embedded S3 test server** + request counters |
| `yb-storage` | LSM engine: MemTable, WAL, SST, Manifest, caches, counters |
| `yb-txn` | TSO, Catalog, DatabaseEngine, partition routing and pruning, transaction buffering, GC and time travel, `CoordinationService` (single / objectstore / ms implementations share one consistency test suite) |
| `yb-ms` | Metadata service (Raft log/snapshot in object storage + state machine + row lock table + CN client) |
| `yb-index` | Skip indexes (minmax / set / bloom / ngram) and the hand-rolled segmented inverted index (FST / postings / WAND / BM25) |
| `yb-sql` | Lexer/parser, executor, `information_schema` virtual tables |
| `yb-mysql` | MySQL protocol server |
| `yb-server` | Compute node main program |
| `yb-bench` | Benchmarks |
| `yb-cli` | Operations CLI (object inventory, shards, GC, recovery check, cost estimation) |

---

## Performance (measured)

`./gradlew :yb-bench:bench`, with the embedded S3 server as the backend (real HTTP). **Measured data**
(numbers come from one particular benchmark run; methodology in [docs/en/compatibility.md](docs/en/compatibility.md)
§4.9; latency is highly environment-dependent, **request count is the column that is deterministic**):

| Scenario | Magnitude |
|---|---|
| Point query (steady state, cache hit) | p50 **9µs**, mean **0.004** object storage requests/query |
| Point query (cold cache) | p50 **124µs**, **1** Range GET |
| `COUNT(*)` | **0** requests, about 0.37ms (300× faster than a full table scan) |
| `COUNT(*) WHERE status='x'` | **0** requests |
| Group commit amortization | 3200 transactions → **220–221 PUTs** (**14.5 transactions/PUT**) |

Running the same benchmark suite **against a different backend** (`./gradlew :yb-bench:bench` with
`-PYB_S3_ENDPOINT=http://127.0.0.1:9000 ...` hits real MinIO) — the differences are all in latency and
throughput; **request counts do not vary with the backend**:

| Scenario | Embedded S3 server | **Real MinIO** |
|---|---|---|
| Range scan of 1000 rows | 1 GET, 350k rows/s | 1 GET, **170k rows/s** |
| Writes (single-threaded) | 1451 TPS, commit p50 689µs | **249 TPS, commit p50 4.0ms** |
| **Writes (16 concurrent)** | 18605 TPS | **3376 TPS** |
| `.idx` metadata overhead | 6.9% of data | 6.9% of data |

**The gap between the two sets of numbers is itself the conclusion**: the embedded server stores objects in a
HashMap, so a PUT is an in-process call; real MinIO goes over HTTP plus a disk fsync, so a single PUT costs
about **4ms**. On cloud object storage the RTT is another order of magnitude higher (the measured reference
in design doc §14.1 is S3 PUT p99 of 400–600ms).

But **all three core design claims hold on real object storage and are independent of the backend**: COUNT is
0 requests, steady-state point queries are 0.004 requests, and group commit amortizes 3200 transactions into
221 PUTs. These are architectural properties, not artifacts of a particular backend.

> Two key conclusions:
> 1. **Object storage request count is worth asserting on more than latency** — it is deterministic and it
>    directly determines the bill. Every read-path optimization aims to bring the request count down to 0 or 1.
> 2. **The benefit of group commit only shows up under concurrency**. Single-threaded serial writes have no
>    batching headroom (1.0 transactions/PUT); 16 concurrent writers amortize the PUT count by 14.5×. That's
>    also why the benchmark has a dedicated concurrent-write section — it's what revealed, during
>    implementation, that "group commit wasn't actually taking effect".

---

## Known limitations

Design docs §11.3 and §16.3 list the complete set. The ones to watch most:

| Limitation | Notes |
|---|---|
| **The default snapshot isolation does not prevent write skew** | InnoDB's RR avoids it with next-key locks; this system doesn't provide that. `SHOW VARIABLES LIKE 'yangbase_isolation_detail'` reports this honestly |
| **Auto-increment primary keys are not guaranteed to be contiguous** | Matches `innodb_autoinc_lock_mode=2` behavior |
| **String comparison is binary order** | Equivalent to `utf8mb4_bin`: `WHERE name = 'ABC'` does **not** match `'abc'`, and `ORDER BY` / `DISTINCT` / `GROUP BY` group by byte order too. ⚠️ The collation name reported in the handshake packet and by `SHOW VARIABLES` is still `utf8mb4_0900_ai_ci` (the MySQL default, case-**insensitive**), which doesn't match the actual semantics — **trust the actual semantics**, see [docs/en/compatibility.md](docs/en/compatibility.md) §4.3.1 |
| **Commit latency p50 around 30–50ms** | The lower bound is the RTT of one object storage PUT; this is a physical constraint |
| **Cross-shard transactions may briefly expose some rows if the commit fails** | A crash doesn't affect correctness (recovery discards the whole transaction), but if the commit path fails, some rows may linger in this process's memory and are cleared on restart. **Splitting transactions along the partition key avoids this entirely** (§9.3) |
| **Full-text search requires choosing a tokenizer explicitly** | `standard` only splits ASCII; Chinese text yields 0 tokens (in which case it errors out explicitly rather than returning an empty set). Use `WITH PARSER ngram` for Chinese |
| **Full-text indexes are single-column** | One index covers one field; multiple fields need multiple indexes |
| **Cold vs. warm query latency differs by ~60×** | Object storage as the data layer is a physical lower bound that optimization cannot remove |
| Not supported | Stored procedures, triggers, updatable views, foreign key cascades, `LOAD DATA INFILE`, binlog |

---

## Development conventions

See [CLAUDE.md](CLAUDE.md). The ones easiest to trip over:

- **Object storage is the single source of truth** — everything local must be disposable
- **Never call `LIST` on a hot path** — it's a slow operation
- **Don't merge `.idx` and `.dat`** — it would make point queries download the entire data file
- **L1 and below must be leveled** — this is the semantic precondition for COUNT exactness
- **When adding a read path, write its request-count assertion first**

---

## Contributing

Contributions are welcome. Start with [CONTRIBUTING.md](CONTRIBUTING.md) — it covers the build, the test expectations, and the project rules that are not negotiable (request-count budgets, immutable objects, `.idx`/`.dat` separation). All participants are expected to follow the [Code of Conduct](CODE_OF_CONDUCT.md).

Branching model: `feature/*` and `bugfix/*` are branched from `dev/<version>` and squash-merged back into it; `main` is only updated when a release is cut. Details in [Development process](docs/zh/development-process.md) (Chinese).

---

## License

Apache-2.0
