---
layout: default
title: Object storage is the database
lang: en
---

# YangBase

**A transactional database built entirely on object storage — MySQL protocol compatible, written in Kotlin.**

Compute nodes hold no durable state. Destroy them, rebuild them, move them to whichever cloud is cheapest
this month: the data stays in the object store, and a fresh node comes back in seconds.

## Why

Cloud compute is priced by the hour and re-priced every renewal. A database pinned to a local disk turns
that pricing cycle into an operational emergency. Object storage is cheap per byte, replicated by the
provider, and decoupled from compute — put the durable state there and the compute layer becomes
disposable.

The trade is three physical constraints, and the whole design follows from them:

| Constraint | Response |
|---|---|
| No in-place update, object-level API | Immutable objects + manifest pointer swap |
| High round-trip latency (10–60 ms, tail worse) | Group commit, multi-level caching, metadata resident in memory |
| Billed per request | **Request count is a first-class metric**, asserted in tests |

## Three numbers

- **0** data reads for `COUNT(*)` — exact, not an estimate, and asserted at `get_range == 0`
- **56.6%** smaller data blocks from compression (20k-row measurement), with `.idx` deliberately left
  uncompressed so it can stay resident
- **2 requests** to delete 1001 objects (batch `DeleteObjects`), down from 2002

## 60 seconds

```bash
docker compose up -d --wait      # YangBase + an S3-compatible object store
mysql -h 127.0.0.1 -P 3306 -uroot
```

```sql
CREATE DATABASE demo;
USE demo;
CREATE TABLE orders (id BIGINT PRIMARY KEY, item VARCHAR(32), amount DECIMAL(10,2));
INSERT INTO orders VALUES (1,'apple',3.50),(2,'banana',1.20),(3,'apple',4.25);
SELECT item, COUNT(*), SUM(amount) FROM orders GROUP BY item;
```

Then point it at real object storage — AWS S3, Alibaba Cloud OSS, Tencent COS, Huawei OBS, Cloudflare R2,
Backblaze B2, MinIO, or anything S3-compatible — with `--s3-provider`.

## Docs

| | |
|---|---|
| [Getting started](en/getting-started.md) | Clone to first query, then to production-shaped deployment |
| [Architecture](en/architecture.md) | Storage layout, write path, read path, transactions, metadata service |
| [Compatibility & limits](en/compatibility.md) | What MySQL features exist, what does not, and the traps between them — **read before migrating** |
| [Object storage backends](en/backends.md) | Backend contract, capability probing, and the conformance suite |
| [Versioning & releases](en/versioning.md) | What the numbers mean, the weekly train, the freeze rule, the release gate |
| [完整设计文档](zh/yangbase-design.md) | 3000+ 行，§0–§17 + 4 附录（中文，设计意图的权威记录） |
| [兼容性与限制](zh/compatibility.md) | 中文版，迁移前必读 |

## What it is not

Honesty is cheaper than a support ticket, so:

- **No authentication, permissions or TLS** in this version. Deploy inside a trusted network.
- The default `REPEATABLE READ` becomes snapshot isolation, which **does not prevent write skew**. InnoDB's
  next-key locking largely does; applications that rely on it can see new anomalies after migrating.
- **Auto-increment values are not contiguous** (segmented allocation, same class of behaviour as
  `innodb_autoinc_lock_mode=2`).
- Cold reads are ~60× slower than warm ones. That gap is a property of the medium, not a bug to be fixed.
- Storage tiers, stored procedures and triggers are not implemented.

The full list, with the reasoning, is in [Compatibility & limits](en/compatibility.md).
