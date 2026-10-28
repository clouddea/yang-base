---
---

# YangBase User Guide

[English](user-guide.md) | [简体中文](../zh/user-guide.md)

> **Who this is for**: developers and operators putting an application on YangBase.
> To decide "can this be migrated" first → read [compatibility.md](compatibility.md);
> to understand "why is it designed this way" → read [yangbase-design.md](yangbase-design.md);
> this guide only answers **how to use it**.

---

## Table of contents

1. [What it is and when to use it](#1-what-it-is-and-when-to-use-it)
2. [Quick start](#2-quick-start)
3. [Connections and drivers](#3-connections-and-drivers)
4. [Data types](#4-data-types)
5. [SQL surface](#5-sql-surface)
6. [Built-in functions](#6-built-in-functions)
7. [Transactions, isolation levels, and row locks](#7-transactions-isolation-levels-and-row-locks)
8. [Indexes](#8-indexes)
9. [Partitioning](#9-partitioning)
10. [Time travel, backup, and recovery](#10-time-travel-backup-and-recovery)
11. [CDC change streams](#11-cdc-change-streams)
12. [Multi-node deployment](#12-multi-node-deployment)
13. [Observability](#13-observability)
14. [Operations CLI](#14-operations-cli)
15. [Performance and cost](#15-performance-and-cost)
16. [MySQL migration checklist](#16-mysql-migration-checklist)

---

## 1. What it is and when to use it

YangBase is a **relational database that keeps all of its durable state in object storage**, and it
speaks the MySQL protocol. The compute node (CN) is **stateless**: the process can be destroyed at
any time and rebuilt on another machine without losing a single row.

**Use it when**:

- the data set is GB-scale or larger, does not fit on one machine, or you do not want to pay a
  fixed bill for a database that "nobody uses most of the time";
- reads dominate writes (reporting, configuration, order archives, admin backends, cold data in
  multi-tenant SaaS);
- you need transactions and SQL, and do not want to fall back to "a key-value store plus
  home-grown transactions";
- you need to **move the whole instance at any time** (change cloud, change region, rebuild
  following a discount cycle).

**Do not use it when**:

- writes are high-frequency point writes (tens per millisecond) or you need p99 in the
  single-digit milliseconds — object-storage RTT sets the floor;
- you need heavy locking semantics beyond `SELECT ... FOR UPDATE`, or stored procedures /
  triggers;
- you need complex cross-row constraints with strong consistency (such as deferrable foreign
  keys).

**In one sentence**: this is a database for "**low frequency, read-heavy, needs transactions and
SQL**", trading **tens of extra milliseconds per write** for "no bill when there are no customers,
and a compute tier you can throw away at any time".

---

## 2. Quick start

### Build

```bash
./gradlew build           # compile + all checks (unit tests, static analysis)
./gradlew integrationTest # integration tests (embedded S3, no Docker needed)
```

> ⚠️ **Neither `build` nor `check` includes `integrationTest`** (it lives in a separate source
> set), so to run everything you have to write `./gradlew build integrationTest`.

### Start a minimal instance (local filesystem, development only)

```bash
./gradlew :yb-server:run --args="\
  --data-dir /tmp/yangbase \
  --i-know-this-is-not-durable \
  --port 3306"
```

> ⚠️ `--data-dir` uses the local filesystem: **no replication, and no durability guarantee**.
> That is exactly why the server demands it be **declared explicitly**: without
> `--i-know-this-is-not-durable` it **refuses to start** (better a failed start than someone
> believing the data is somewhere safe). Production must switch to object storage.
>
> ⚠️ **3306 is the default port for a local MySQL/MariaDB**, and on a development machine it is
> usually already taken (a MySQL in a container takes it too). In that case startup reports
> explicitly that MySQL port 3306 is already in use and prints a command to find the holder —
> pick another port: `--port 3307`.

### Start a production instance (S3-compatible object storage)

Self-hosted MinIO as the example (AWS S3 / COS / R2 work the same way; change endpoint and
region):

```bash
./gradlew :yb-server:run --args="\
  --s3-bucket my-yangbase \
  --s3-endpoint http://127.0.0.1:9000 \
  --s3-region us-east-1 \
  --s3-access-key $AK --s3-secret-key $SK \
  --port 3306 \
  --metrics-port 9104 \
  --disk-cache-dir /mnt/nvme/yangbase-cache --disk-cache-mb 51200"
```

> ⚠️ **Self-hosted gateways (MinIO and friends) use path-style addressing, which is this
> project's default**; **Alibaba Cloud OSS is the opposite** — its endpoint looks like S3 but the
> primitives differ, and copying the block above will not start. See the next section.

Startup logs print the results of the **capability probes** (conditional write, strongly
consistent read, range read, LIST consistency). Any failing critical capability refuses startup —
that is not a "warning", because a backend without conditional writes would silently break leases
and the manifest pointer.

### Start an Alibaba Cloud OSS instance

```bash
# the bucket name and region below are **examples**, substitute your own (bucket must exist, see below)
export AK=<your AccessKey ID>
export SK=<your AccessKey Secret>

./gradlew :yb-server:run --args="\
  --s3-oss \
  --s3-bucket my-yangbase \
  --s3-endpoint https://oss-cn-hangzhou.aliyuncs.com \
  --s3-region cn-hangzhou \
  --s3-path-style false \
  --i-accept-nonatomic-pointer-switch \
  --s3-access-key $AK --s3-secret-key $SK \
  --port 3306 --metrics-port 9104"
```

**Confirm two things before running** (both are holes actually stepped into; neither is something
the code can guess for you):

1. **The bucket must already exist**: the bucket `--s3-bucket` points at has to be created first
   in the console (or with `ossutil mb`). The server **will not** create it — the first PUT
   returns `NoSuchBucket` (404). (`yb-cli` and the test fixtures call `ensureBucket`; the server
   does not: conjuring a bucket into an account is not its decision to make.)
2. **The keys must actually be passed in**: if `$AK` in `--s3-access-key $AK` was never
   `export`ed, the shell **quietly passes an empty argument** and the command line looks entirely
   normal. Run `echo "len(${AK})=${#AK}"` first (an empty string now fails at startup instead of
   waiting for a signature failure).

Each of the four parameters has a reason, and **startup fails without any one of them**:

| Parameter | Why it is required |
|---|---|
| `--s3-oss` | Makes the client use **OSS's own primitives**: atomic create sends `x-oss-forbid-overwrite` (a conflict is **409 FileAlreadyExists**, not S3's 412). Without it, conditional create is rejected by OSS outright (400) |
| `--s3-path-style false` | OSS **rejects path-style addressing** (`SecondLevelDomainForbidden`: it requires a third-level domain). This project defaults to path-style for self-hosted gateways — MinIO needs that |
| `--s3-region cn-hangzhou` | Enters the SigV4 signing scope; must be the OSS region |
| `--i-accept-nonatomic-pointer-switch` | OSS **has no ETag CAS** (measured: `If-Match` returns 400 `NotImplemented`), and **three** coordination writes in this system need it. Without it the server **refuses to start during capability probing** (principle P6: refuse rather than silently lose mutual exclusion); with it, one prominent warning line is printed |

> **Which three places the switch covers** (behind them sits one fact: this backend has no
> conditional write, and under a single-writer deployment I accept that):
>
> | Who | When CAS is used | After degradation |
> |---|---|---|
> | **manifest pointer switch** | every flush / compaction | unconditional PUT + **read-back verification after write**; **orphan snapshot audit** when taking over a shard (`yb_manifest_orphan_snapshots_total`) |
> | **timestamp watermark** (`meta/tso/<cluster>.json`) | every renewal (once per 64 seconds by default in `single` mode) | same as above; **refuses to issue timestamps when the read-back shows another writer overwrote it** (better to fail this statement than to issue a possibly duplicate timestamp) |
> | **auto-increment segment watermark** (`auto_inc/`) | each time a new segment is claimed (default 1000 ids at a time) | same as above; a failed read-back likewise **refuses** instead of retrying. ⚠️ **Only `single` (single-CN) mode allows degradation**: in `ms` mode the mutual exclusion for claiming auto-increment segments rests entirely on this CAS (it does not go through the MS), so degrading it lets two CNs claim the same segment and issue duplicate primary keys — which is why on an `ms` + no-CAS backend `AUTO_INCREMENT` **errors out explicitly** (a known limitation, see below) |
>
> ⚠️ **The timestamp watermark once was not wired to the switch** (fixed 2026-09-27): `single`-mode
> TSO persists its watermark in object storage (timestamps do not go backwards after a restart)
> and used `If-Match` unconditionally on renewal — so on OSS "the server started, every probe
> passed, but the **first statement** raised `CapabilityNotSupportedException`". It only triggers
> when the watermark object **already exists** in the bucket (that is, **after a restart**, or when
> the bucket has been used before), so running the test suite against an empty bucket never finds
> it.
>
> **What degradation means exactly**: it falls back to "unconditional PUT + read-back verification
> after write". Why that is acceptable (what gets overwritten is an **SST reference**, not data: if
> the watermark moves back, that batch of WAL is not reclaimed and is replayed on recovery) is
> argued in the invariants of design doc §3.3. **It is not CAS**; multi-CN + OSS is a known
> boundary; its **precondition is a single writer** — the point of the read-back check is to turn
> "there actually is a second writer" into one explicit failure.

**Hard limits**:

1. **Coordination mode can only be `single` (single CN) or `ms`**: leases and row locks in
   `objectstore` mode are built on ETag CAS, which OSS cannot do — it **refuses at startup** and
   offers those two alternatives (degrading that mode does not leave "weaker coordination", it
   leaves "no coordination").
2. **The MS election timeout uses production-scale defaults** (500ms–1s): every MS log append on
   OSS is a real PUT, and copying the in-memory backend's tuned values makes the cluster churn
   leaders under load — no error, but every switch loses the lock table.
3. **`AUTO_INCREMENT` only works with a single CN**: in `ms` (multi-CN) mode the mutual exclusion
   for claiming segments depends on ETag CAS, which OSS lacks. Writes **error out explicitly**
   (never silently duplicate) — if you need multi-CN plus auto-increment, use a backend with CAS
   (AWS S3 / MinIO / COS), or supply primary keys explicitly.
   (Handing segment allocation to the MS to serialize is future work in §11.4, not implemented.)

`yb-cli` supports OSS the same way: add `--s3-oss --path-style false` (or the environment
variable `YB_S3_OSS=true`).

**When it will not start, read the "capability probe" lines first**: they report each capability
measured against the backend, and failing items **carry the exception chain** (for example
`read/write=✗ … ObjectStoreException: PUT … 403 InvalidAccessKeyId ← UncheckedIOException: …`).
The four most common, with their fingerprints:

| Fingerprint in the report | Cause | What to do |
|---|---|---|
| `404 NoSuchBucket` | bucket not created | create the bucket in the console first |
| `403 InvalidAccessKeyId` / `SignatureDoesNotMatch` | wrong key, rotated key, or variable not exported | `echo ${#AK}`; switch to a fresh AK/SK |
| `403 SecondLevelDomainForbidden` | missing `--s3-path-style false` | add it (OSS rejects path-style addressing) |
| `403` mentioning endpoint / region | `--s3-region` does not match the bucket's region | set it to the bucket's region, e.g. `cn-hangzhou` |
| `read/write` ✓ but `compare-and-swap If-Match` ✗ | **normal** (OSS has no ETag CAS) | add `--i-accept-nonatomic-pointer-switch` |

Measured (2026-09-25, Hangzhou): a single CN and **multi-CN with a 3-node MS** (the MS Raft log
also lands in OSS) both passed, covering **steady-state point lookup ≤ 1 GET**, COUNT at 0 GET,
index vs. full scan, partition pruning, commit budget, restart recovery, Range/LIST, cross-CN
visibility, and shard takeover.

> ⚠️ **But that round missed one path** (exposed and fixed only on 2026-09-27): the test fixtures
> ran against an **empty bucket** every time, so the timestamp watermark took the "first create"
> path (`If-None-Match`, which OSS supports); only when the watermark object **already exists**
> does it go through `If-Match` — that is, **after any restart**. In other words, that "all
> passed" round did not prove "it still runs after a restart". Both paths are covered by tests
> now (the one in `OssCompatibilityTest` that pre-seeds the watermark object, plus the one that
> claims a second auto-increment segment), and both were verified to go red with the fix removed.

### Connect to it

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
SELECT id, UPPER(name), DATE_FORMAT(created_at, '%Y-%m-%d')
  FROM users
 WHERE city = 'bj' AND amount > 10
 ORDER BY created_at DESC
 LIMIT 10;
```

---

## 3. Connections and drivers

The standard MySQL JDBC driver is enough (Connector/J 8.x is the primary test target):

```java
String url = "jdbc:mysql://127.0.0.1:3306/app"
           + "?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC";
Connection c = DriverManager.getConnection(url, "root", "");
```

| Parameter | Recommendation | Reason |
|---|---|---|
| `serverTimezone` | **`UTC`** | see "Time" below |
| `useSSL` | `false` (internal network) | this version has no TLS (see "not implemented") |
| `useCursorFetch` | enable for large result sets | the server supports read-only cursors (`COM_STMT_FETCH`) |
| `rewriteBatchedStatements` | enable for batch inserts | merges multiple VALUES into one statement |
| connection pool | HikariCP / Druid both work | `COM_PING` and `COM_RESET_CONNECTION` are implemented |

### Time: everything in UTC

**Time literals are interpreted as UTC and rendered as UTC**, regardless of the server's local
time zone:

```sql
INSERT INTO t (dt) VALUES ('2026-02-01 10:00:00');
SELECT dt FROM t;          -- always 2026-02-01 10:00:00
SELECT NOW();              -- current instant (expressed in UTC)
```

So put `serverTimezone=UTC` in the JDBC URL and both sides agree.
`SET time_zone` is currently **a no-op** (accepted, but it does not change behaviour), so do not
rely on it for time-zone conversion — convert in the application, or store UTC timestamps
(`BIGINT`).

`DATE` columns show only the date part (`2026-02-01`); `DATETIME` shows down to seconds or
milliseconds.

### Authentication and privileges

The current version has **no authentication**: any username and password are accepted, and there
is no privilege model. **Deploy only inside a trusted network** (security group / private network
/ VPN); do not expose the port to the public internet.

---

## 4. Data types

| Type | Description | Notes |
|---|---|---|
| `TINYINT` / `SMALLINT` / `MEDIUMINT` / `INT` / `BIGINT` | integers | 64-bit internally; out-of-range writes **raise 1264** (`TINYINT` -128~127, `SMALLINT` ±32767, `INT` ±2147483647); `MEDIUMINT` and `BIGINT` are handled as 64-bit |
| `BOOL` / `BOOLEAN` | alias for `TINYINT(1)` | stores 0/1 |
| `FLOAT` / `DOUBLE` / `REAL` | floating point | 8-byte IEEE754; the precision declaration of `FLOAT(p,s)` is ignored |
| `DECIMAL(p,s)` / `NUMERIC` | **fixed point** | stored and rounded at the declared scale (`DECIMAL(12,2)` stores `1.005` as `1.01`). Use it for money, not `DOUBLE` |
| `VARCHAR(n)` / `CHAR(n)` / `TEXT` / `TINYTEXT` / `MEDIUMTEXT` / `LONGTEXT` | strings (UTF-8) | `CHAR` is treated as `VARCHAR`; over-length **raises 1406** (counted in **characters**, not bytes); `TEXT` without a declared length is not checked |
| `BINARY` / `VARBINARY` / `BLOB` / `TINYBLOB` / `MEDIUMBLOB` / `LONGBLOB` | binary | `BINARY`/`VARBINARY` map to `BLOB` (stored by byte, no fixed-length padding) |
| `DATE` / `DATETIME` / `TIMESTAMP` | time (millisecond precision) | all three are structurally identical internally and all UTC; `TIMESTAMP` does no automatic time-zone conversion |
| `YEAR` | year | an integer in storage; non-numeric writes **raise 22007**, but there is **no 1901–2155 range check** |
| `ENUM(...)` / `SET(...)` | closed sets | a write outside the members **raises an error** (not a silent truncation); comparison and ordering use **string** order (MySQL uses the declared ordinal, see compatibility.md) |
| `JSON` | stored as text plus write-time validation | provides `JSON_EXTRACT` / `->` / `->>` and similar functions; no binary JSON and no JSON indexes |

**Types that do not exist**: `TIME`, `BIT`, `GEOMETRY`, spatial types — putting them in DDL raises
1235.

> **Current state of length and range validation**: `VARCHAR(n)` over-length raises 1406 and
> integer width overflow raises 1264 (matching MySQL strict mode); `DECIMAL(p,s)` precision
> overflow **does not raise**, it rounds at the declared scale. Under `INSERT IGNORE` these errors
> degrade to "skip the row" rather than MySQL's "write the closest value" — **this system will not
> rewrite your data**.
> Affected row counts are reported faithfully, so an application can discover "a few rows did not
> go in" instead of receiving a quietly altered value.

---

## 5. SQL surface

### Supported statements

```sql
-- DDL
CREATE DATABASE [IF NOT EXISTS] db;
DROP DATABASE [IF EXISTS] db;
CREATE TABLE [IF NOT EXISTS] t (...);      -- with PRIMARY KEY / UNIQUE / KEY / partitions / USING <index type>
DROP TABLE [IF EXISTS] a[, b, ...];
ALTER TABLE t ADD|DROP COLUMN, ADD|DROP INDEX, RENAME TO, TRUNCATE PARTITION, ADD|DROP|REORGANIZE PARTITION;
TRUNCATE [TABLE] t;
CREATE [UNIQUE] [FULLTEXT] INDEX i ON t (cols) [USING <type>] [WITH PARSER <analyzer>];
DROP INDEX i ON t;                          -- drops the index only, leaves the data alone
CREATE [OR REPLACE] VIEW v AS SELECT ...;   -- read-only view
DROP VIEW [IF EXISTS] v;

-- DML
INSERT INTO t [(cols)] VALUES (...), (...);
INSERT INTO t [(cols)] SELECT ...;                       -- insert from a query
INSERT ... ON DUPLICATE KEY UPDATE col = VALUES(col), ...; -- upsert
REPLACE INTO t ...;                                      -- delete-then-insert on key conflict
INSERT IGNORE INTO t ...;                                -- on key conflict **or invalid value** the row is skipped (inserted rows reported faithfully)
UPDATE t SET ... [WHERE ...];
DELETE FROM t [WHERE ...];

-- metadata queries (clients and export tools depend on these)
SHOW DATABASES;  SHOW TABLES [FROM db] [LIKE 'x'];
USE information_schema;              -- system databases are entered like real ones (SHOW TABLES / unqualified queries work)
SELECT * FROM information_schema.tables;   -- or always use qualified names
SHOW FULL TABLES [FROM db];         -- one extra column, Table_type (BASE TABLE / VIEW)
SHOW TABLE STATUS [FROM db] [LIKE 'x'];   -- what mysqldump uses to fetch per-table metadata
SHOW TRIGGERS [FROM db] [LIKE 'x']; -- triggers are unsupported, returns an empty set
SHOW CREATE TABLE t;  SHOW INDEX FROM t;  SHOW VARIABLES [LIKE 'x'];

-- DQL
SELECT [DISTINCT] ... FROM ... [JOIN ...] [WHERE] [GROUP BY] [HAVING] [ORDER BY] [LIMIT]
       [FOR UPDATE | LOCK IN SHARE MODE] [AS OF TIMESTAMP '...'];
SELECT ... UNION [ALL] SELECT ... [ORDER BY ...] [LIMIT ...];
EXPLAIN SELECT ...;
ANALYZE TABLE t;

-- transactions
BEGIN | START TRANSACTION;  COMMIT;  ROLLBACK;
SET autocommit = 0|1;

-- administration
SHOW DATABASES | TABLES | COLUMNS | INDEX | CREATE TABLE | VARIABLES | STATUS | WARNINGS | ENGINES;
DESC t;  USE db;  SET NAMES utf8mb4;
```

### Expressions

- comparison: `=` `<=>` `<>` `!=` `<` `<=` `>` `>=`, `BETWEEN`, `IN`, `LIKE`, `IS [NOT] NULL`
- logic: `AND` `OR` `NOT` (**three-valued**: `NULL` is "unknown", not "false")
- arithmetic: `+ - * / %` (integer overflow **raises 1690**, no wraparound; division by zero is
  covered below)
- `CASE WHEN ... THEN ... ELSE ... END` (both searched and simple forms)
- `CAST(x AS type)`, `CONVERT(x, type)`
- JSON: `->`, `->>`
- subqueries: `IN` / `EXISTS` / correlated subqueries in scalar position (correlated subqueries in
  predicate position are rewritten into semi/anti joins)
- window functions: `ROW_NUMBER` `RANK` `DENSE_RANK` `PERCENT_RANK` `CUME_DIST` `NTILE` `LAG`
  `LEAD` `FIRST_VALUE` `LAST_VALUE` `NTH_VALUE` plus aggregate windows, with `PARTITION BY` /
  `ORDER BY` / `ROWS` frames and `RANGE UNBOUNDED PRECEDING` frames
- aggregates: `COUNT` `SUM` `AVG` `MIN` `MAX` `GROUP_CONCAT` (`DISTINCT` allowed), `GROUP BY` /
  `HAVING`, with **`ONLY_FULL_GROUP_BY` enforced**

### Constructs that error out instead of being silently ignored

`INTO OUTFILE`, `USING`/`NATURAL JOIN`, `FOR UPDATE NOWAIT`/`SKIP LOCKED`,
`RANGE n PRECEDING` frames, `GROUPS` frames, `ORDER BY`/`LIMIT` inside a `UNION` branch, derived
tables `FROM (SELECT ...) x`, multi-table `UPDATE`/`DELETE`, `SAVEPOINT`.

> What these have in common: **a literal implementation would produce wrong results** (treating
> `USING` as an ordinary JOIN, for instance, degrades an outer join into a Cartesian product), so
> this system chooses to raise. The full list and the alternatives are in
> [compatibility.md](compatibility.md) section 3.

---

## 6. Built-in functions

> Convention: **almost every function returns `NULL` when given `NULL`** (`UPPER(NULL)`,
> `ROUND(NULL)`, `CONCAT('a', NULL)` are all `NULL`). Rows below that note a NULL exception are
> the exceptions.
> A wrong argument count raises **1210**; a missing function raises **1235**.

### Control flow and NULL

| Function | Description |
|---|---|
| `CASE WHEN c THEN v ... [ELSE e] END` | searched form; the simple form `CASE x WHEN 1 THEN ...` equals `x = 1`. A `NULL` condition does not count as true |
| `IF(c, a, b)` | `a` when `c` is true; `b` when `c` is NULL |
| `IFNULL(a, b)` / `NVL` | `b` when `a` is NULL |
| `NULLIF(a, b)` | returns NULL when `a = b` |
| `COALESCE(a, b, ...)` | first non-NULL |
| `GREATEST(...)` / `LEAST(...)` | maximum/minimum; **NULL if any argument is NULL** |
| `ISNULL(x)` | whether it is NULL |

### Strings

| Function | Description |
|---|---|
| `UPPER`/`UCASE`, `LOWER`/`LCASE` | case conversion |
| `LENGTH` / `OCTET_LENGTH` / `BIT_LENGTH` | **byte count** (`LENGTH('€')` = 3) |
| `CHAR_LENGTH` / `CHARACTER_LENGTH` | **character count** (`CHAR_LENGTH('€')` = 1) |
| `CONCAT(...)` | concatenation; **NULL if any argument is NULL** |
| `CONCAT_WS(sep, ...)` | concatenation with a separator, **skipping NULL**; NULL if the separator is NULL |
| `SUBSTRING(s, pos[, len])` / `SUBSTR` / `MID` | `pos` is 1-based; negative counts from the end; `pos = 0` yields an empty string |
| `LEFT(s, n)` / `RIGHT(s, n)` | first / last n characters |
| `TRIM(s)` / `LTRIM` / `RTRIM` | strip whitespace |
| `TRIM(BOTH \| LEADING \| TRAILING 'x' FROM s)` | strip the given character (syntactic form) |
| `REPLACE(s, from, to)` | replace (an empty `from` returns the input unchanged) |
| `INSTR(s, sub)` / `LOCATE(sub, s[, pos])` / `POSITION(sub, s)` | position, 0 when not found |
| `LPAD(s, n, pad)` / `RPAD(s, n, pad)` | pad or truncate to n |
| `SUBSTRING_INDEX(s, delim, count)` | the count-th segment by delimiter (negative counts from the right) |
| `REVERSE`, `REPEAT`, `SPACE` | reverse / repeat / spaces |
| `ASCII(s)`, `CHAR(n, ...)`, `HEX(v)`, `UNHEX(s)` | encoding conversion |
| `MD5(s)`, `SHA1(s)`, `SHA2(s[, bits])` | digests (lowercase hex) |
| `STRCMP(a, b)` | comparison: returns -1 / 0 / 1 (binary order) |

### Numbers

| Function | Description |
|---|---|
| `ABS` | absolute value (`ABS(-9223372036854775808)` **raises 1690**, no wraparound) |
| `CEIL`/`CEILING`, `FLOOR`, `SIGN` | rounding and sign |
| `ROUND(x[, d])` | rounds **away from zero** (`ROUND(-2.5)` = -3, not -2) |
| `TRUNCATE(x, d)` | truncates toward zero |
| `MOD(a, b)` | modulo; **returns NULL when the divisor is 0** |
| `POW`/`POWER`, `SQRT`, `EXP`, `LN`, `LOG([base,] x)`, `LOG2`, `LOG10` | powers and logarithms (square root or logarithm of a negative returns NULL) |
| `SIN` `COS` `TAN` `ASIN` `ACOS` `ATAN` `ATAN2` `DEGREES` `RADIANS` `PI` | trigonometry |

### Date and time (all UTC)

| Function | Description |
|---|---|
| `NOW()` / `CURRENT_TIMESTAMP` / `LOCALTIME` / `SYSDATE` / `UTC_TIMESTAMP` | current instant. **Constant within one statement** |
| `CURDATE()` / `CURRENT_DATE` / `UTC_DATE` | today at midnight |
| `CURTIME()` / `CURRENT_TIME` | current time (string) |
| `DATE(x)`, `TIME(x)` | date part / time part |
| `YEAR` `MONTH` `DAY`/`DAYOFMONTH` `HOUR` `MINUTE` `SECOND` `MICROSECOND` | field extraction |
| `DAYOFWEEK` (1=Sunday) `WEEKDAY` (0=Monday) `DAYOFYEAR` `WEEKOFYEAR` `QUARTER` | weekday / day-of-year ordinals |
| `MONTHNAME` `DAYNAME` | English names |
| `LAST_DAY(x)` | last day of the month |
| `DATE_FORMAT(x, fmt)` | format by pattern (`%Y %m %d %H %i %s %b %e %W %j %p %T %r …`); unknown specifiers are emitted verbatim |
| `STR_TO_DATE(s, fmt)` | parse by pattern (same specifier table as `DATE_FORMAT`) |
| `DATEDIFF(a, b)` | difference in days (date parts only) |
| `TIMESTAMPDIFF(unit, a, b)` | units below; `TIMESTAMPADD(unit, n, d)` adds or subtracts |
| `DATE_ADD(d, INTERVAL n unit)` / `DATE_SUB(...)`; `ADDDATE(d, n[, unit])` / `SUBDATE(...)` | add/subtract; units: `MICROSECOND SECOND MINUTE HOUR DAY WEEK MONTH QUARTER YEAR` |
| `TIME_FORMAT(x, fmt)` | formats the time part only (same specifier table as `DATE_FORMAT`) |
| `UNIX_TIMESTAMP([d])` / `FROM_UNIXTIME(ts[, fmt])` | convert to and from Unix seconds |
| `TO_DAYS(x)` | day number |

### Type conversion

```sql
CAST(x AS SIGNED | UNSIGNED | CHAR | BINARY | DATE | DATETIME | TIME | DECIMAL(p,s) | DOUBLE | JSON)
CONVERT(x, type)
```

A failed conversion **raises 1292** (MySQL's loose mode would give 0 plus a warning; this system
has no warning channel, and silently returning 0 is "a wrong value that looks normal").

---

## 7. Transactions, isolation levels, and row locks

```sql
SET autocommit = 0;          -- or BEGIN / START TRANSACTION
INSERT INTO t VALUES (...);
UPDATE t SET v = v + 1 WHERE id = 1;
COMMIT;                      -- or ROLLBACK
```

| Item | Behaviour |
|---|---|
| isolation level | **snapshot isolation** (reported externally as `REPEATABLE-READ`) |
| phantom reads | cannot happen (snapshot) |
| **write skew** | ⚠️ **not prevented** — two transactions that each read what the other writes can both commit. Use `FOR UPDATE` when you need it |
| cross-shard transactions | 2PC plus a commit marker (a failed `COMMIT` makes the whole thing vanish atomically, never half of it) |
| commit profile | `sync` (default, RPO=0) / `batch` (high-throughput imports) |
| commit latency | about 0.4–2 ms for `sync` (local) / one PUT RTT on real object storage |

### Row locks

```sql
BEGIN;
SELECT balance FROM acct WHERE id = 1 FOR UPDATE;  -- exclusive lock
UPDATE acct SET balance = balance - 100 WHERE id = 1;
COMMIT;                                            -- lock released with the commit
```

- `LOCK IN SHARE MODE` is a shared lock.
- Locks are **released on transaction commit/rollback**; with `autocommit = 1` they are released
  when the statement ends.
- A wait timeout raises **1205** per `innodb_lock_wait_timeout` (50 seconds by default); deadlock
  detection raises **1213** (`objectstore` mode has no deadlock detection, so you can only wait
  for the timeout).
- Locks carry a TTL (default `2 × the wait limit`, floor 30 seconds): a lock is **not** released
  immediately when a connection dies abruptly. For long transactions, raise
  `innodb_lock_wait_timeout` as well (the TTL follows it).
- `FOR UPDATE` on a JOIN or a view **raises an error** (this system locks by base-table primary
  key; multi-table semantics are undefined).
- `NOWAIT` / `SKIP LOCKED` raise an error (they change locking semantics, and ignoring them
  silently would hand you a transaction that "looks locked").

---

## 8. Indexes

```sql
CREATE INDEX idx_city ON t (city);                   -- ordered index (B+ tree semantics)
CREATE UNIQUE INDEX uq_email ON t (email);           -- unique (NULL does not participate in uniqueness)
CREATE INDEX idx_h ON t (x) USING HASH;              -- hash: serves equality point lookups only
CREATE INDEX idx_ts ON t (created_at) USING MINMAX;  -- skip index: block-level pruning
CREATE INDEX idx_s  ON t (status) USING SET;         -- equality pruning for low-cardinality columns
CREATE INDEX idx_p  ON t (payload) USING BLOOM;
CREATE INDEX idx_n  ON t (name) USING NGRAM;         -- LIKE 'prefix%'
CREATE FULLTEXT INDEX ft ON articles (body) WITH PARSER ngram;  -- full text (Chinese requires ngram)
```

**How to use a skip index correctly**: it does not return rows, it only answers "could this block
contain the value I want", so **the data has to be clustered by that column** to help (appending in
time order is the typical case). With fully random writes, every block's min/max spans the whole
domain and the index degrades to "never prunes anything".

**Full-text search**:

```sql
SELECT id FROM articles WHERE MATCH(body) AGAINST('storage engine');
SELECT id FROM articles WHERE MATCH(body) AGAINST('+quick -rabbit' IN BOOLEAN MODE);
SELECT id FROM articles WHERE MATCH(body) AGAINST('"quick brown"' IN BOOLEAN MODE);
```

- `MATCH` is an **access path**, not a per-row predicate (BM25 needs whole-corpus statistics); the
  executor pulls candidates from the inverted index first.
- The index is updated **at transaction commit** (uncommitted rows are not searchable, on
  purpose).
- Without `LIMIT` at most 1000 candidates are returned. For all hits, write an explicit `LIMIT` or
  fetch in batches.

---

## 9. Partitioning

```sql
CREATE TABLE events (
    id BIGINT NOT NULL, dt DATE NOT NULL, v INT,
    PRIMARY KEY (id, dt)
) PARTITION BY RANGE (TO_DAYS(dt)) (
    PARTITION p2026q1 VALUES LESS THAN (TO_DAYS('2026-04-01')),
    PARTITION p2026q2 VALUES LESS THAN (TO_DAYS('2026-07-01')),
    PARTITION pmax    VALUES LESS THAN MAXVALUE
);
```

`RANGE` / `LIST` / `HASH` / `KEY` are supported. **Partition pruning** makes a query outside the
range read 0 objects — one of the most valuable optimizations on object storage: partition by
time, and cold partitions are never read.

`DROP PARTITION` is a **cooling method with a cost of ≈ 0** (it deletes metadata, it does not move
data), cheaper than DELETE or archiving to low-frequency storage.

---

## 10. Time travel, backup, and recovery

```sql
SELECT * FROM t AS OF TIMESTAMP '2026-02-01 10:00:00';   -- read a historical version
SELECT * FROM t AS OF TSO 471234567890123456;
```

- How far back you can read depends on the **GC watermark** (the default retention window appears
  in the `yb-cli gc` report); going past it **raises an error** rather than returning truncated
  results.
- Backup / restore / PITR are covered in [the operations CLI](#14-operations-cli).

---

## 11. CDC change streams

The change stream **reads the existing WAL directly** (it does not maintain a second log), and the
cursor can be persisted:

```bash
./gradlew :yb-cli:cli -Pargs="cdc --endpoint http://127.0.0.1:9000 --bucket yangbase --from-ts 471234567890123456"
```

---

## 12. Multi-node deployment

### Single CN (simplest, and the default)

```bash
./gradlew :yb-server:run --args="--s3-bucket b --coord-mode single ..."
```

Coordination is guaranteed by the deployment shape: timestamps use the object-storage watermark,
leases and locks degrade to in-process (there is only one writer).
⚠️ The precondition is that **CN clocks are roughly synchronized** (the high bits of `commit_ts`
come from the local millisecond clock).

### Multiple CNs

| Mode | Mechanism | Standing cost | Suitable for |
|---|---|---|---|
| `objectstore` | object-storage conditional writes serve as CAS for timestamping / election / row locks throughout | 0 | multiple CNs, tolerating 2–6 requests per coordination |
| `ms` | 3-node metadata service (Raft) | 3 small machines | multiple CNs, lock-heavy workloads, low latency |

```bash
# three MS nodes (one machine each)
./gradlew :yb-server:run --args="--role ms --ms-node-id ms-1 \
  --ms-peers 10.0.0.1:9000,10.0.0.2:9000,10.0.0.3:9000 \
  --ms-advertise 10.0.0.1:9000 --ms-bind-port 9000 --s3-bucket b ..."

# CNs connect to them
./gradlew :yb-server:run --args="--coord-mode ms \
  --ms-peers 10.0.0.1:9000,10.0.0.2:9000,10.0.0.3:9000 --s3-bucket b ..."
```

- The three modes share **the same contract**; they differ only in latency, cost, and failure
  behaviour (measured numbers are in compatibility.md §4.4.2).
- **Reads can go to any CN; writes must reach the owner**: any CN serves reads for any shard,
  while a write to a non-owner CN raises `1205` (retryable) with the owner node, epoch, and
  remaining time in the error — use `yb-cli shards --owner` to find the current owner. **There is
  no server-side forwarding**; the client or gateway routes by shard itself (issue #14).
- An `ms` leader change **loses row locks** (the lock table lives in MS memory): a lock-holding
  transaction checks before commit and raises 1213 for a retry when its lock is gone — **the worst
  case is a retry, never a silently lost update**.
- When every `ms` node is unavailable: CNs keep serving reads and writes from the timestamp range
  they already hold; once it is exhausted, write transactions fail explicitly while reads
  continue.
- **Do not shrink** `--ms-election-timeout-ms`: MS logs live in object storage, and too small a
  timeout makes the cluster re-elect repeatedly under load.
- **Idle shards are evicted** (15 minutes by default): a partition that has been opened does not
  stay resident forever — every open shard renews its lease periodically (about 480 PUTs per hour
  per shard in `objectstore` mode), so a single pass over a 128-partition table is a permanent
  fixed cost. After eviction the next access reopens it (a few extra requests, no correctness
  impact). Tune with `--shard-idle-evict-min`; `0` disables eviction (only suitable for
  single-machine debugging).

### Deploying on Alibaba Cloud OSS

The startup command and the four required parameters, explained one by one, are in
[Start an Alibaba Cloud OSS instance](#start-an-alibaba-cloud-oss-instance); only the deployment
points are added here:

- **Coordination mode can only be `single` or `ms`** — leases and row locks in `objectstore` mode
  are built on ETag CAS, which OSS does not have (measured: `If-Match` → 400 `NotImplemented`).
- **Do not shrink the MS election timeout**: on OSS every MS log append is a real PUT (tens of
  milliseconds); too small a timeout churns leaders under load, and every switch loses the lock
  table (§4.4).
- **`--i-accept-nonatomic-pointer-switch` is not CAS**: it degrades the atomicity of three
  coordination writes (manifest pointer switch · timestamp watermark · auto-increment segment
  watermark) into "detectable + recoverable" (read-back verification after write, which
  **refuses** when it sees someone else's overwrite; orphan snapshot audit on shard takeover,
  metric `yb_manifest_orphan_snapshots_total`). With a single CN the deployment shape covers this
  risk; for multi-CN treat it as a known boundary, argued in design doc §3.3.

---

## 13. Observability

```bash
./gradlew :yb-server:run --args="--metrics-port 9104 ..."
curl -s localhost:9104/healthz
curl -s localhost:9104/metrics | grep yangbase_
```

- `/metrics` (Prometheus text format) and `/healthz` **generate no object-storage requests**, so
  scrape them aggressively.
- Key metrics: object-storage request count (labelled by `op` and `cached`), query latency, slow
  query count, connection count, lock waits, cache hit rate.
- **Slow query log**: statements above `--slow-query-ms` (500ms by default) print one line with the
  SQL, the duration, the **object-storage read breakdown**, and the row count — answering "why did
  this query read 500 objects".
- **Object-storage audit log**: DDL and GC write `ops/audit/{date}.jsonl` (batched and
  asynchronous; a failure does not affect the main path). Disable with `--no-audit`.

---

## 14. Operations CLI

`yb-cli` **operates object storage directly and does not need a CN online** (the compute tier can be
destroyed at any time, and operations should not depend on it).

```bash
# what is in storage (grouped by prefix)
./gradlew :yb-cli:cli -Pargs="ls --endpoint http://127.0.0.1:9000 --bucket yangbase"

# how large each shard is, and how many rows
./gradlew :yb-cli:cli -Pargs="shards --endpoint ... --bucket ..."

# garbage collection: by default it **only marks, never deletes**; --delete actually removes
./gradlew :yb-cli:cli -Pargs="gc --endpoint ... --bucket ..."
./gradlew :yb-cli:cli -Pargs="gc --delete --endpoint ... --bucket ..."
# ⚠️ --window-hours has a 1-hour floor: the window is the only mechanism protecting
#    objects that were just written but are not yet in the manifest; set it too small and
#    in-flight objects are deleted as garbage. To bypass it you must write
#    --i-know-what-im-doing explicitly (it will not ask a second time)

# recovery-path health check: objects referenced by a manifest but already lost
./gradlew :yb-cli:cli -Pargs="recover-check --endpoint ... --bucket ..."

# monthly cost estimate using the cost model from design doc §13
./gradlew :yb-cli:cli -Pargs="cost --endpoint ... --bucket ..."

# backup (writes only a manifest, cost independent of data size) / list backups / restore (--yes actually does it)
./gradlew :yb-cli:cli -Pargs="backup --note pre-upgrade --endpoint ... --bucket ..."
./gradlew :yb-cli:cli -Pargs="backups --endpoint ... --bucket ..."
./gradlew :yb-cli:cli -Pargs="restore --yes --endpoint ... --bucket ..."

# point-in-time recovery: roll one table back to an instant (--yes actually does it)
# ⚠️ use T rather than a space in the instant: -Pargs splits arguments on spaces, so
#    "2026-02-01 10:00:00" becomes two arguments and only the first half is consumed by
#    --to (silently becoming 00:00:00 that day)
./gradlew :yb-cli:cli -Pargs="pitr --table users --to 2026-02-01T10:00:00 --yes --endpoint ..."

# change stream: reads the existing WAL, no second change log
./gradlew :yb-cli:cli -Pargs="cdc --since 2026-02-01T10:00:00 --limit 100 --endpoint ..."

# migration risk scan: which parts of the target SQL this system does not support,
# or supports with different semantics than MySQL
./gradlew :yb-cli:cli -Pargs="analyze-sql --path ./migrations"
```

> ⚠️ **On backends without ETag CAS, such as Alibaba Cloud OSS, write commands need one extra
> switch**: `restore` / `pitr` switch the manifest pointer, and without it they fail at execution
> time with "conditional write capability required". Under a single-writer deployment you can
> accept the degradation explicitly (same meaning as the identically named switch on `yb-server`):
>
> ```bash
> ./gradlew :yb-cli:cli -Pargs="restore --yes --i-accept-nonatomic-pointer-switch --s3-oss --path-style false --endpoint ... --bucket ..."
> ```
>
> Read-only commands (`ls` / `shards` / `cost` / `backups` / `recover-check` / `cdc`) do not use
> conditional writes and do not need it.

> Run `./gradlew :yb-cli:cli -Pargs="--help"` for the full set of parameters.

**GC has two phases**: mark and write a manifest first, and only delete **after the safety window
(24 hours by default)**. The window is not there to "let readers finish"; it downgrades the cost of
a misjudgement from "silently reading wrong data" to "one extra day of storage".

**`ls` and `cost` do not issue a `head` per object**: object sizes come from the LIST response
itself (0 extra requests). On a bucket with a hundred thousand objects, a `head` per object would
make "one capacity assessment" more expensive than a full day of production traffic — so this
command also respects the P4 budget. When the backend does not return sizes in LIST, it falls back
to **bounded sampling plus extrapolation** and prints the word "estimate" in the output.

---

## 15. Performance and cost

**The main cost line of this database is "request count", not "latency".** Remember three numbers:

| Scenario | Order of magnitude | Object-storage requests |
|---|---|---|
| point lookup (cache hit) | tens of microseconds | **0** |
| point lookup (cold cache) | hundreds of microseconds | **1 GET** |
| `COUNT(*)` / `WHERE col = constant` counting | sub-millisecond | **0** (metadata path) |
| single-row commit (`sync`) | a few milliseconds | 1 PUT (amortized by group commit) |
| range scan | one GET per block | parallel prefetch |

**Three practical ways to save requests**:

1. **Do not write a full scan for `COUNT(*)` or equality counts** — this system goes through
   metadata, 0 GETs, and `EXPLAIN` shows `type: METADATA_COUNT`.
2. **Partition by time**: `DROP PARTITION` on a cold partition costs ≈ 0, while DELETE has to walk
   the rows.
3. **Enable the local NVMe second-level cache** (`--disk-cache-dir`): after a CN restart, "getting
   hot again" reads only the local disk.

**The hot/cold gap is a factor of tens**, and that is the physical lower bound of object storage as
a data tier. State SLOs in two columns, "cache hit" and "cache miss".

---

## 16. MySQL migration checklist

- [ ] **Does the application rely on `AUTO_INCREMENT` being contiguous?** → this system **does not
  guarantee contiguity** (segmented allocation; restarts and multiple nodes skip numbers).
- [ ] **Does it rely on write skew being prevented?** → snapshot isolation **does not prevent it**;
  add `FOR UPDATE`.
- [ ] **Does it rely on case-insensitive string comparison?** → this system uses **binary order**
  (`WHERE name = 'ABC'` does not match `'abc'`).
- [ ] **Does it use `DECIMAL` for money?** → fine, rounded at the declared scale.
- [ ] Does it rely on **`VARCHAR(n)` silently truncating** or **integer overflow wrapping**? → this
  system **raises** (1406 / 1264), stricter than MySQL's non-strict mode.
- [ ] Does it use **stored procedures / triggers / events / foreign-key cascades**? → unsupported
  (foreign-key constraints can be declared and validated; cascades never happen).
- [ ] Does it use **`REPLACE INTO` / `INSERT IGNORE` / `ON DUPLICATE KEY UPDATE`**? → supported.
  Note that `INSERT IGNORE` on a **value error** (too long, unconvertible type) **skips the row**,
  while MySQL writes the "closest value".
- [ ] Does it use **`INSERT ... SELECT`**? → supported.
- [ ] Does it use **`SAVEPOINT` / derived tables / multi-table UPDATE**? → **unsupported**, rewrite
  needed.
- [ ] Is **`serverTimezone=UTC`** set in the connection string?
- [ ] Does it rely on **`NOW()` changing within a statement**? → it is a statement-level constant.
- [ ] Is there SQL wrapped in **`/*! ... */` version comments** (mysqldump output)? → **the
  statements inside are executed** (as in MySQL); ordinary block comments remain comments.
- [ ] Does it use the **`mysqldump` / `mysql`** command line for backups? → ✅ measured working
  (`mysqldump` output loads straight back into this system).
- [ ] Have the backup/restore flows been switched to **`yb-cli backup` / `pitr`**?

> This checklist shares its source with [compatibility.md](compatibility.md), but that document is
> more complete (it includes how each difference was verified and what an error looks like).
> **Read compatibility.md in full before migrating.**
