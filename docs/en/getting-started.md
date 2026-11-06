---
---

# Getting Started

[English](getting-started.md) | [简体中文](../zh/user-guide.md)

YangBase keeps **all persistent state in object storage** and speaks the MySQL protocol. Compute nodes are
stateless: destroy and rebuild them at any time without losing a row. This page takes you from a clone to a
first query in about ten minutes; for the full capability surface read the
[user guide](../zh/user-guide.md) (Chinese) and [compatibility.md](compatibility.md).

## 1. Prerequisites

**Option A — Docker only.** No JDK, no Gradle, no object storage to install:

```bash
docker compose up -d --wait     # YangBase + an S3-compatible object store
mysql -h 127.0.0.1 -P 3306 -uroot
```

That is the whole demo: a stateless compute node, an S3-compatible store, and the engine on top.
Compose reports `healthy` only once the server actually accepts MySQL connections. The credentials in
`docker-compose.yml` are development-only. Continue with §5 ("First query") from there.

**Option B — build from source.** You need:

| Need | Why |
|---|---|
| **JDK 21** | The build pins a Java 21 toolchain (`jvmToolchain(21)`); nothing older will do. |
| A POSIX shell | Every command below is bash. |
| *Optional:* a MySQL client (`mysql`) | Only for poking at the server by hand; JDBC works as well. |
| *Optional:* MinIO, or Docker for its container mode | Only for the object-storage run in §4; the local-filesystem run in §3 needs neither. |

No Gradle install is needed — the wrapper (`./gradlew`) fetches what it needs.

> ⚠️ **Point `JAVA_HOME` at a JDK 21** if your default `java` is older: `JAVA_HOME=/path/to/jdk-21 ./gradlew build`.
> Do **not** commit an absolute JDK path into the repo's `gradle.properties` (`org.gradle.java.home=...`): the
> repo is public, CI has no such directory, and the build dies with `Value '/home/you/app/jdk21' given for
> org.gradle.java.home Gradle property is invalid`. Keep machine-local paths in `~/.gradle/gradle.properties`
> instead (`org.gradle.java.installations.paths=/path/to/jdk-21`).

This version has **no authentication** (any username/password is accepted, no permission model): deploy only
inside a trusted network, never expose the port to the internet.

## 2. Build

```bash
./gradlew build            # compile + all checks (unit tests, static analysis)
./gradlew assemble         # compile only, if that is all you want to know
./gradlew integrationTest  # integration tests, embedded S3 server, no Docker needed
```

> ⚠️ **Neither `build` nor `check` includes `integrationTest`** — it lives in a separate source set. To run
> everything, ask for both: `./gradlew build integrationTest`.

## 3. Run it against the local filesystem (development only)

The fastest path to a running server — no object storage involved:

```bash
./gradlew :yb-server:run --args="--data-dir /tmp/yangbase --i-know-this-is-not-durable --port 3306"
```

> ⚠️ **`--i-know-this-is-not-durable` is not optional.** The local filesystem backend has no multi-replica
> durability and no cross-process conditional writes, while writer election, manifest pointer swap, fencing and
> crash recovery all rely on object-storage semantics. Without the flag the server **refuses to start**
> (`拒绝启动：--data-dir 使用的是本地文件系统后端，它不是持久存储。`). That refusal is intended: better a
> failed start than data everyone believes is safe. Production must use §4.

> ⚠️ **Port 3306 is a local MySQL/MariaDB's default** and is often already taken on dev machines (a
> containerised MySQL counts). Startup then fails with `MySQL 端口 3306 已被占用，无法启动。` and prints how to
> find the culprit (`ss -ltnp | grep :3306`). Pick another port with `--port 3307`, then connect with `-P 3307`.

## 4. Run it against MinIO (production-shaped)

```bash
./scripts/minio.sh start
```

It prints the endpoints and waits for the health check: API at `http://127.0.0.1:9000`, console at
`http://127.0.0.1:9001` (login `yangbase` / `yangbase123`).

> ⚠️ **The bucket must already exist.** The server does **not** create buckets — the first write would fail with
> `NoSuchBucket` (404), which is exactly what the startup capability probe reports. Create a bucket named
> `yangbase` in the console, or point `--s3-bucket` at one you already have.

```bash
./gradlew :yb-server:run --args="\
  --s3-bucket yangbase \
  --s3-endpoint http://127.0.0.1:9000 \
  --s3-access-key yangbase --s3-secret-key yangbase123 \
  --port 3306"
```

Add `--metrics-port 9104` for `/metrics` and `/healthz` (off by default), and
`--disk-cache-dir /mnt/nvme/yangbase-cache --disk-cache-mb 51200` to use a local NVMe as second-level cache.
`--s3-region` defaults to `us-east-1`, which is what MinIO wants.

At startup the server prints a **capability probe** (conditional writes, strongly consistent reads, Range
reads, LIST consistency); if something essential is missing it refuses to start rather than degrade silently —
read those lines first whenever a start fails.

> Docker instead of the static binary: `./scripts/minio.sh start docker`; stop with `./scripts/minio.sh stop`,
> wipe data with `./scripts/minio.sh clean`. If you download MinIO yourself, note that `dl.min.io/aistor/...`
> serves the **commercial AIStor** edition, which without a license rejects every S3 operation
> (`All S3 operations are denied`); the open-source build is on GitHub Releases, and
> `./minio --version` printing `License: GNU AGPLv3` is how you tell.

## 5. Your first SQL

```bash
mysql -h 127.0.0.1 -P 3306 -u root
```

```sql
CREATE DATABASE app;
USE app;
CREATE TABLE users (
    id     BIGINT NOT NULL AUTO_INCREMENT,
    name   VARCHAR(64) NOT NULL,
    email  VARCHAR(128),
    city   VARCHAR(32),
    amount DECIMAL(12,2),
    created_at DATETIME,
    PRIMARY KEY (id),
    UNIQUE KEY uq_email (email),
    KEY idx_city (city)
);
INSERT INTO users (name, email, city, amount, created_at)
VALUES ('alice', 'a@x.com', 'bj', 12.34, NOW());
INSERT INTO users (name, email, city, amount, created_at)
VALUES ('bob', 'b@x.com', 'sh', 7.50, NOW());
SELECT id, UPPER(name), DATE_FORMAT(created_at, '%Y-%m-%d')
  FROM users
 WHERE city = 'bj' AND amount > 10
 ORDER BY created_at DESC
 LIMIT 10;
```

```sql
-- how is a count planned and executed?
EXPLAIN SELECT COUNT(*) FROM users;
SELECT COUNT(*) FROM users;
```

`EXPLAIN` reports `type: METADATA_COUNT` (`Using metadata count (manifest, exact, 0 object reads)`). That is not
a cached number: the manifest carries exact row counts maintained by the single writer, so `COUNT(*)` reads
**zero** data objects — no GET, no scan. `COUNT(*) WHERE status='x'` is 0 GETs too, via per-block value counts.
It is the main reason this database is cheap to run.

Two semantics worth knowing on day one:

- **Time is UTC** — literals are parsed and rendered as UTC regardless of server timezone. Over JDBC use
  `jdbc:mysql://127.0.0.1:3306/app?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC`;
  `SET time_zone` is accepted but changes nothing.
- **String comparison is binary** (like `utf8mb4_bin`): `WHERE name = 'ABC'` does **not** match `'abc'`.

## 6. Look inside with `yb-cli`

`yb-cli` talks to object storage **directly — no compute node needed** (a CN can be destroyed at any time, so
operational tooling must not depend on one). Its storage flags are
`--endpoint / --bucket / --access-key / --secret-key`:

```bash
./gradlew :yb-cli:cli -Pargs="ls --endpoint http://127.0.0.1:9000 --bucket yangbase --access-key yangbase --secret-key yangbase123"
```

```bash
./gradlew :yb-cli:cli -Pargs="cost --endpoint http://127.0.0.1:9000 --bucket yangbase --access-key yangbase --secret-key yangbase123"
```

`ls` groups objects by prefix; `cost` estimates the monthly bill from object count and total bytes. Neither
issues a `head` per object — sizes come from the LIST response, so capacity accounting costs LIST requests
only (if a backend omits sizes there, it samples and labels the output an estimate).

> ⚠️ **`-Pargs` splits on whitespace.** A timestamp written as `"2026-02-01 10:00:00"` becomes two arguments and
> only the first half is consumed — silently becoming midnight. Write `2026-02-01T10:00:00` with a `T` (this
> bites `pitr --to` and `cdc --since`). Local-filesystem mode needs none of the four flags:
> `-Pargs="ls --fs /tmp/yangbase"`. `./gradlew :yb-cli:cli -Pargs="--help"` lists every command.

## 7. Shut down

`Ctrl-C` stops the server. Everything persistent lives in the bucket (or `--data-dir`); the disk cache is
disposable. If you started MinIO for this, stop it with `./scripts/minio.sh stop`.

## 8. Next steps

- **[compatibility.md](compatibility.md)** — missing MySQL features, differing semantics, and how each shows
  up. Read it before pointing a real application here.
- **[User guide](../zh/user-guide.md)** (Chinese) — transactions and row locks, index types, partitioning, time
  travel, CDC, multi-node deployment, observability, and the full `yb-cli` command set.
- **[architecture.md](architecture.md)** — how the storage layout, write path, read path, transactions and
  metadata service fit together (the full design document is Chinese: `../zh/yangbase-design.md`).
- **[`README.md`](../README.md)** — measured performance and current implementation status.
