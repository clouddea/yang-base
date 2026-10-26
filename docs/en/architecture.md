# Architecture Overview

[English](architecture.md) | [简体中文](../zh/yangbase-design.md)

A 15-minute tour of how YangBase works: what it is, the three physical facts that shaped every
decision, and how a query actually reaches bytes. Everything below comes from
`docs/zh/yangbase-design.md`, with section numbers cited so each claim can be verified.

---

## 1. What YangBase is (and is not)

YangBase is a distributed OLTP database whose **only durable state lives in S3-compatible object
storage**. Compute nodes are stateless and can be created or destroyed at any time with no data
migration. It speaks the MySQL wire protocol and supports snapshot-isolated transactions
(cross-shard included), partitioning, and five kinds of index. Kotlin 2.x, JDK 21+.

It is **not** a replacement for a high-concurrency OLTP core: a shard's write floor is one
object-storage PUT, so the target is read-heavy workloads with write QPS in the hundreds to low
thousands (SaaS, content sites, IoT metadata). Nor is it the whole MySQL ecosystem, cross-region
strong consistency, a columnar engine, or a P99 match for local disk.

> Source: docs/zh/yangbase-design.md §0.3

## 2. Three physical constraints, three design answers

| Physical fact | Consequence | Design answer |
|---|---|---|
| API is **object-level, not block-level** — no in-place update | every write creates a new object | immutable objects + pointer switch (the "manifest" pattern); LSM fits naturally |
| **High RTT** (≈10–60 ms in-region, tail in the hundreds) | each synchronous write may pay a full RTT | group commit to amortize, multi-tier cache, small-and-hot metadata, prefetch |
| **Billed per request** | small-object traffic destroys the cost model | request count becomes a first-class metric (§14.2), gated in CI |

Three underrated advantages are used deliberately: immutability makes MVCC and time travel nearly
free (history manifests are kept for GC safety anyway, so PITR costs ~0); aggregate read bandwidth
reaches tens of GB/s, so wide scans trade bandwidth for latency; and consistency plus replicas come
from the provider, so RPO=0 needs no replication protocol of your own.

> Source: docs/zh/yangbase-design.md §1.3, §3.4, §12.2

## 3. Architecture at a glance

```
   app / JDBC / MyBatis / mysqldump / Navicat
                    │ MySQL wire protocol
     ┌──────────────┼──────────────┐
     ▼              ▼              ▼
  CN cn-1        CN cn-2        CN cn-3   stateless: SQL · txn/MVCC · LSM · indexes · caches
     └──────┬───────┴──────┬───────┘
            │ TSO / leases / locks / catalog
            ▼
      ┌───────────────────────────┐
      │ MS ms-1 / ms-2 / ms-3     │  optional 3-node Raft; reviveable,
      │ TSO · lease · lock · cat  │  no local durable state
      └────────────┬──────────────┘
                   ▼
  ┌─────────────────────────────────────────────────────┐
  │ object storage — the single source of truth          │
  │ catalog/  meta/ms/  shard/…/{manifest,wal,sst}  …    │
  └─────────────────────────────────────────────────────┘
```

The asymmetry is intentional: the data plane is fully decentralized and elastic; the control plane
is a deliberately *weak* 3-node cluster holding only derived or short-lived state (TSO watermarks,
leases, locks), snapshotted to object storage. If MS loses its majority, **read-only transactions
keep working** on prefetched TSO batches — only writes and DDL wait.

> Source: docs/zh/yangbase-design.md §2.1, §4.1

## 4. Storage layout

- `catalog/` — database/table/schema definitions: `schema.{version}.json` plus a `CURRENT` pointer
- `meta/ms/` — the metadata service's own Raft snapshot and log segments
- `shard/{table_id}/{partition_id}/` — one LSM instance per `(table, partition)`: `manifest/`
  (`CURRENT`, `M-{seq}.mf` snapshots, `d-{seq}.mf` deltas), `wal/e{epoch}/` (one object per
  group-commit batch, plus merged `s-{from}-{to}.log` segments), `sst/L{level}/f_{id}.dat` and
  `.idx`, and `state.json`
- `index/{index_id}/` — standalone indexes, shaped exactly like a data shard
- `stats/`, `tx/`, `ops/` — optimizer statistics, `gc_watermark.json`, GC manifests, audit log

Three rules carry the weight. **Prefix carries the shard, suffix spreads the load**: one shard's
objects share a prefix (cheap LIST and GC); many shards spread requests across prefixes. **Hot and
cold objects are separated**: `CURRENT` <1 KB, `M-*.mf` 10 KB–2 MB, `f_*.idx` 1–5% of data size,
`f_*.dat` 8–64 MB. **Versions only increase**, so caches validate by integer compare. Numbers are
zero-padded because LIST is byte-lexicographic (`10.log` sorts before `9.log`).

The `.idx`/`.dat` split is a hard requirement: sharing one object would make a point lookup
download 8–64 MB, whereas the split keeps the index resident and fetches 4–64 KB.

> Source: docs/zh/yangbase-design.md §5.1, §5.2, §5.3

## 5. Write path

1. **Group commit** — buffer briefly, then emit one WAL object per batch. `sync` (factory default,
   window `min(5 ms, 256 KB)`, RPO=0), `batch` (`min(50 ms, 4 MB)`), `async` (100 ms, ACKs before
   the PUT lands, **RPO>0**, declared explicitly per table/session and reported truthfully by `SHOW
   VARIABLES`). The window self-tunes: near zero when idle, at the cap under load.
2. **WAL objects** — `wal/e{epoch}/{seq}.log`, "YBWL" header, per-record CRC so a corrupt byte
   loses one transaction rather than the batch; >128 objects trigger a background merge into
   ~8 MiB segments.
3. **MemTable** — skiplist keyed by `user_key || ~commit_ts`, so the newest version sorts first and
   a read short-circuits at the first visible version.
4. **Flush to L0** — write a fresh `.dat` + `.idx` pair; nothing is rewritten in place.
5. **Manifest pointer switch** — write the new manifest (or delta), then swap `CURRENT` with
   `If-Match`; a stale writer loses the race. Old objects go through two-phase GC: mark
   reachable-minus-existing into `ops/garbage/`, delete after a 24 h window.

The obvious design — one appendable WAL object — is impossible: PUT overwrites wholesale, and
Multipart Upload needs every part but the last to be ≥5 MiB, i.e. waiting for 5 MiB before
durability. S3 Express One Zone does support append, but lacks conditional `PutObject`, caps
objects at 10,000 parts, and its ETag is not an MD5 — parked as an M5 evaluation item.

Recovery is that same path run by a new writer: take the lease, read the manifest, replay WAL
beyond `effective_from`, write one L0 file, swap the pointer — roughly 3–5 s, which is precisely
why lease TTL is 10–15 s. A 5 s TTL would livelock: the lease would expire mid-recovery.

> Source: docs/zh/yangbase-design.md §6.2, §6.3, §6.8, §6.9, §4.3

## 6. Read path

Three cache tiers, all disposable — no tier may hold authoritative state: a heap block cache (~60%
of heap, weighted by bytes, `.idx` > manifest > data); an NVMe KV cache (80% of disk, keyed by
`object_key#etag`, and since objects are immutable, entries need eviction, never invalidation); and
a resident metadata area holding every open shard's manifest view plus each `.idx`'s block index and
column stats — ~3.5 KB per MiB at 10 columns, so 1 TiB needs ~3.5 GiB. Concurrent misses on one key
coalesce into one in-flight GET; else a compaction swapping `.idx` objects triggers a storm.

A steady-state point lookup is **1 GET** of 4–64 KB: manifest from cache (otherwise a rate-limited
ETag check every 200 ms), bloom filter narrows candidates, the resident `.idx` yields a byte range,
one range GET, binary search, `read_ts` visibility check. Cold cache costs ~3 GET; index point
lookup 1–2 GET; read transaction start 0 requests because `start_ts` is prefetched. All of this is
gated by `CountingObjectStore` in tests: request counts are deterministic, latency is not.

> Source: docs/zh/yangbase-design.md §2.3, §5.4, §6.7, §14.2

## 7. Transactions

Every row version carries `commit_ts`; a reader takes `read_ts` at start and sees
`commit_ts ≤ read_ts`, deletions being tombstones. Ordering by `commit_ts` rather than `start_ts`
makes commit order the only visibility order (as in Percolator/TiKV).
`gc_watermark = min(active read_ts) - margin`, which is what lets compaction drop old versions.

Inside one shard exactly **one writer exists at a time**, so writes queue serially: no row locks
for same-shard conflicts, no conflict detection, no retry, and a naturally ordered WAL. Cross-shard
transactions use Percolator-style 2PC with a **primary record**: prepare records land in each
shard's WAL (already the writer's own append, so prepare is cheap), the commit decision is the
primary record, and that instant is the point of no return. Recovery reads the primary shard's WAL
(commit present → replay, absent → roll back), and since uncommitted versions are invisible,
rollback only clears locks. Cost is 2 × RTT (~100–200 ms), which is why partition keys should keep
transactions in one shard (M2: single-shard share > 90%).

Writers are fenced four ways, none of which must be perfect alone: epoch embedded in the WAL key
prefix; manifests carry `writer_epoch` and swap `CURRENT` under `If-Match`; MS rejects any epoch
already superseded; readers verify epoch consistency. Default isolation is REPEATABLE READ
implemented as snapshot isolation — no next-key locks, so write skew is possible; `SERIALIZABLE` is
SI plus write-skew detection. TSO prefetches `start_ts` in batches but allocates `commit_ts`
strictly in real time, since holes in timestamp space would leave some snapshots undefined.

> Source: docs/zh/yangbase-design.md §4.2, §4.3, §7.1, §7.2, §7.3, §7.4

## 8. Indexes

Every index has the same shape: **immutable files + a manifest pointer + a query interface** (D8),
which is what lets caching, GC, backup/PITR, and online build be written once. The abstraction
unifies the container, not the file format.

- **Ordered secondary index** (B+tree semantics) is a separate LSM instance. In-place B+trees
  cannot exist on object storage; a COW B+tree would cost 3–4 GETs per point lookup against 1–2
  for LSM with bloom + block index.
- **Hash index** is extendible hashing: static `base/{version}/bkt_*.bin`, small `delta/{seq}/`
  buckets, a global bloom. Equality only, opt-in via `USING HASH`.
- **Skip index** lives *inside* `.idx` — per-column min/max, value sets, bloom, n-gram bloom next
  to the block index — so `WHERE age > 60` is decided in memory. Costs 3–8% more `.idx` bytes,
  turns a 100-GET scan into 3 GETs, and can only ever prove absence.
- **Full-text index** is self-written and segmented: `terms.fst`, `postings.bin` with block-level
  `max_score` bounds in `postings.idx`, `positions.bin`, `norms.bin`, `stats.json`,
  `deletes.roar`. With block-max WAND a top-10 query on a term matching 800k of 1M documents reads
  a few KB, not 10 MB. Since segments are immutable, the delete bitmap is a list of
  `(commit_ts range, bitmap)` pairs — the only correct way to do MVCC over immutable segments, and
  impossible to retrofit.
- **Count index** is the headline optimization: `COUNT(*)` costs **0 GETs**, via exact row counts
  in the manifest, per-block `ts → cumulative count` tables, value→count maps for low-cardinality
  predicates, and partition rollups. It requires L1 and below to be leveled: `Σ file.row_count` is
  exact only if a key appears at most once in L1+ — which is why tiered compaction is forbidden
  (D14).

Indexes share the writer's WAL batch: no extra PUT, only 5–15% more latency and more compaction bytes.

> Source: docs/zh/yangbase-design.md §6.6, §8.1, §8.2, §8.3, §8.4, §8.5, §8.6, §8.8, §8.9

## 9. Metadata service

MS does four things, all derived or short-lived: allocate TSO, arbitrate shard leases, hold
row/table locks, serialize catalog changes. The authoritative catalog is `catalog/` in object
storage — MS is its write serializer and read cache, hence fully disposable; Raft exists only to
put concurrent DDL on one order. Its log and snapshots live in `meta/ms/`, which is why **cold
start targets <3 s** and MS nodes can be preempted or migrated.

Coordination sits behind `CoordinationService`, with three shipped implementations:
`SingleProcessCoordination` (factory default `--coord-mode single`, correct with one CN),
`MetaServiceClient` (3-node MS), and `ObjectStoreCoordination` (CAS election via
`If-None-Match`/`If-Match`; zero resident cost, +1–2 RTT per commit). This is a first-class
deployment choice, not a fallback — "dual mode" means both ship. One hard rule:
`ObjectStoreCoordination` must fail at startup on a backend without conditional writes, never
degrade silently, since its correctness rests entirely on CAS.

> Source: docs/zh/yangbase-design.md §0.4, §4.1, §4.5, §4.6, §4.7

## 10. Cost model: why request count is first-class

On S3 a PUT costs 12.5× a GET ($0.005 vs $0.0004 per 1,000). That asymmetry, more than latency, is
why group commit exists: even if PUT latency matched a local disk, per-request billing would still
force batching. Chinese providers price reads and writes symmetrically (¥0.01 per 10,000) but still
per request, so the conclusion holds.

The structural claim is narrower than "10× cheaper". In the reference case (50 GB, 20 QPS reads at
95% hit rate, 2 TPS writes) object storage costs ~¥13/month against ¥450–800 for an equivalent RDS
setup — 30–50× on the *data-bound* half. The compute-bound half does not get cheaper per hour; it
becomes *interruptible*, which turns renewal-price shock from a forced migration into starting a
new instance and destroying the old one.

Two traps are named. Archive tiers are essentially never right: Glacier Instant Retrieval charges
$0.03/GB per retrieval against $0.004/GB-month storage, so one read in 7.5 months loses money — the
floor is the infrequent-access tier, and `DROP PARTITION` is the real archival mechanism.
Intelligent-Tiering must be disabled too: its per-object monitoring fee ($0.0025 per 1,000
objects-month) dwarfs the storage cost of an LSM's many small objects.

> Source: docs/zh/yangbase-design.md §6.3.4, §13.1, §13.2, §13.3

## 11. Known limits

Out of scope by design: stored procedures, functions, triggers, event scheduler, cursors; updatable
views (views are read-only); `ON DELETE CASCADE` (foreign keys are declarable and validated, cascade
is left to the app, M4 evaluation); `LOAD DATA INFILE` (a client-streaming variant exists); `SELECT
... INTO OUTFILE/DUMPFILE`; spatial indexes and `GEOMETRY` (M5); X Protocol; and all replication
surface (`CHANGE MASTER TO`, `START SLAVE`, binlog) — CDC comes from the WAL instead.

Semantic differences that must be surfaced, not hidden: snapshot isolation does not prevent write
skew the way InnoDB's next-key locks do; auto-increment is handed out in segments of 1,000 per CN,
so values are neither contiguous nor gap-free after rollbacks (equivalent to
`innodb_autoinc_lock_mode=2`); unique constraints need a cross-shard validation lock.

Beyond that, unresolved or explicitly declined: multi-tenancy and quotas; the full permission model
(`GRANT`/`REVOKE`, column privileges); cross-database JOIN and transactions (undefined);
subpartitions and `EXCHANGE PARTITION`; the full charset matrix (only `utf8mb4`, `utf8`, `latin1`,
`binary` are listed); credential rotation; type-changing migrations; Serverless/edge deployment.
Security (TLS, auth, IAM), storage tiering, and the cross-region read-only replica are *decided not
to do* for now, not merely unscheduled.

> Source: docs/zh/yangbase-design.md §7.4, §11.3, §11.4, §16.3, §17 (M5)

---

## Where to read next

The document's advice: §1 and §16 first (they bound the design and its uncertainty), then §5, the most
central and least changeable part. Nine decisions were ratified on 2026-09-23 (§16.1); of the premises
to defend during implementation, manifest consistency (§6.5), MVCC inverted-index integration (§8.6.5),
and count de-duplication with the leveled constraint (§8.9.3) are likeliest to cause silent data errors.

> Source: docs/zh/yangbase-design.md §0.1, §16.1, §16.2, 评审关注点速查
