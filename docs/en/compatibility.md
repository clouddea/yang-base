# YangBase Compatibility and Limitations

[English](compatibility.md) | [简体中文](../zh/compatibility.md)

> **Read this page before you migrate.** The fact that it exists is itself part of the design: design doc §11.3 requires
> that an "explicit list of limitations **must be written into the user documentation**", and the recurring lesson in `docs/README.md` is
> — **"protocol compatibility" is not "semantic compatibility"**. Bufstream passed Jepsen's storage-correctness verification
> and still inherited Kafka's transaction-isolation defects. Writing the differences down clearly matters far more than supporting a few extra syntax forms.
>
> This file corresponds to design doc §11.2 (compatibility matrix) and §11.3 (limitations list);
> every item states **how it was verified** and **what you will see when it goes wrong**.

---

> **2026-09-27 SQL regression fixes**: `DROP PARTITION` preserves physical shard numbering and secondary-index consistency;
> `INSERT` applies `DEFAULT` to omitted columns; `GROUP BY`/`HAVING`/`ORDER BY` in `JOIN` recognize output aliases;
> `LAST_INSERT_ID()` keeps the auto-increment value of the first successful insert in the statement; `VARBINARY` and `HEX` keep the original bytes.

## 1. The One-Sentence Version

**`mysqldump` and Connector/J run fine; single-table queries, aggregates, joins, unions and subqueries all work;
applications that depend on InnoDB locking semantics, `AUTO_INCREMENT` continuity, or foreign-key cascades will behave differently.**

---

## 2. Supported Clients and Tools

| Client / tool | Status | What it depends on |
|---|---|---|
| **MySQL Connector/J 8.x** | ✅ | Primary test target; covered by end-to-end JDBC cases |
| MariaDB Connector/J | ⚠️ not tested | Same authentication-negotiation path |
| HikariCP / Druid connection pools | ✅ | `COM_PING`, `COM_RESET_CONNECTION` |
| MyBatis / MyBatis-Plus | ✅ | `information_schema`, `LAST_INSERT_ID()` |
| `mysql` command-line client | ✅ **tested** (2026-09) | Handshake, `show databases`, create/insert/select, `\G` output; **response packet sequence numbers must be contiguous with the client** — libmysqlclient validates strictly, Connector/J is lenient (this once meant only JDBC worked) |
| `mysqldump` | ✅ **tested** (2026-09) | **Export → load into a brand-new instance → field-by-field comparison** all passed (including Chinese text, `DECIMAL`, `NULL`, `DATE`, `BLOB`, `ENUM`, views, primary and secondary indexes). To get there we filled in: `SHOW TABLE STATUS` (18 columns) · the `Table_type` column of `SHOW FULL TABLES` · `SHOW TRIGGERS` (empty set) · user variables · variables such as `@@collation_database` · `ALTER TABLE … ENABLE/DISABLE KEYS` (no-op) · `SET x = bare_identifier` |
| `mysql` CLI | ✅ | |
| Navicat / DataGrip / DBeaver | ⚠️ **not tested** | Need a complete `information_schema`; Navicat cannot connect when there is **no schema at all** (see below) |
| Spring Data JPA / Hibernate | ⚠️ not tested | Need a more precise `information_schema` and `SHOW` |
| `mysqlbinlog` / replication | ❌ | No binlog; use `yb-cli cdc` for change subscriptions |
| X Protocol / MySQL Shell | ❌ | Not implemented |

### What to Do When a GUI Client Cannot Connect

Tools like Navicat send metadata queries **during connection setup**, and a brand-new instance contains no databases at all,
so they get an empty result and declare the connection failed (this is pitfall T4 recorded in §11.1.1).
Workaround: create a database from the command line first.

```sql
CREATE DATABASE app;   -- afterwards GUI clients connect normally
```

For DataGrip, turn on *introspect using JDBC metadata*.

---

## 3. Unsupported, and **Explicitly Rejected** (no silent failure)

Statements for the following features return an explicit error code (mostly `1235 ER_NOT_SUPPORTED_YET`).
They **never** "look like they succeeded while doing nothing". This distinction matters a great deal: a silent no-op
is far more dangerous than an error — if `SELECT ... INTO OUTFILE` silently succeeded, the user would assume the file had been written.

- Stored procedures, functions, triggers, event scheduler, cursors
- View updatability (views themselves can be queried read-only)
- Foreign keys' `ON DELETE CASCADE` (the constraint can be declared and validated, but **the cascade never happens automatically**)
- `LOAD DATA INFILE` (local file paths)
- `SELECT ... INTO OUTFILE / DUMPFILE`
- Spatial indexes and `GEOMETRY` functions
- `EXPLAIN ANALYZE` (it really executes the statement, so it is explicitly refused rather than pretended to be supported)
- Replication-related: `CHANGE MASTER TO`, `START SLAVE`, `binlog`
- `ALTER TABLE` operations that **require rewriting data**: `MODIFY COLUMN`, inserting a column in the middle, dropping a middle column
  (non-rewriting cases — appending a column, dropping the last column, adding/dropping indexes, renaming — are all supported and are O(1))

### A Few "Successful No-ops"

`LOCK TABLES` / `UNLOCK TABLES` / `FLUSH TABLES` return **success but do nothing**.
This is deliberate, not an oversight: `mysqldump`'s fixed preamble is `LOCK TABLES ... WRITE`,
and rejecting it would make mysqldump completely unusable; meanwhile this system's writer identity is a **shard lease**
(exactly one writer at any moment), and reads are **snapshot reads**, so there is no additional guarantee a "table lock" could provide.
Conversely, pretending to take a lock would be the dangerous option — it would let callers believe they are protected.

---

## 4. Semantic Differences (**this is the important section**)

### 4.0 SQL Surface (**read this first to judge whether you can migrate**)

| Capability | Status | Notes |
|---|---|---|
| Single-table `SELECT` / `WHERE` / `ORDER BY` / `LIMIT` | ✅ | SELECT column aliases accept single quotes (e.g. `id AS 'name'`; `AS` may also be omitted); single quotes elsewhere still denote a string literal |
| Aggregates `COUNT` / `SUM` / `AVG` / `MIN` / `MAX` / `GROUP_CONCAT` | ✅ | `DISTINCT` works; **NULL is ignored** (`COUNT(column)` counts only non-null values) |
| `GROUP BY` / `HAVING` | ✅ | **`ONLY_FULL_GROUP_BY` is enforced** (see below) |
| `JOIN` (`INNER` / `LEFT` / `RIGHT` / `CROSS` / comma) | ✅ | Equi-joins go through hashing, non-equi cases fall back to a nested loop; `USING` / `NATURAL` are explicitly rejected |
| `UNION` / `UNION ALL` | ✅ | A trailing `ORDER BY` / `LIMIT` is allowed (it applies to the whole union); writing them inside a branch is an error (see below); **before deduplication, types are normalized to the left branch's type** (`SELECT 10 UNION SELECT '10'` yields 1 row, matching MySQL); `UNION ALL` does no normalization |
| `ORDER BY <ordinal>` | ✅ | `ORDER BY 1` refers to the 1st output column (not a constant) |
| `SELECT DISTINCT` | ✅ | Deduplication happens **before** `LIMIT`; under `DISTINCT`, ordering by a column absent from the result raises 3065 (matching MySQL) |
| Non-correlated subqueries (scalar / `IN` / `EXISTS`) | ✅ | |
| **Correlated subqueries** (predicate position: `EXISTS` / `NOT EXISTS` / `IN` / `NOT IN`) | ✅ | Rewritten into semi/anti joins; **the inner table is scanned only once**; `NOT IN` NULL semantics match MySQL |
| **Correlated subqueries** (scalar position: `SELECT (SELECT … WHERE i.k = o.k) FROM o`) | ✅ | The inner side is **scanned once** and materialized by join key; the outer side probes that table per row; `ORDER BY` / `LIMIT 1` / `DISTINCT` all work |
| **Window functions** | ✅ | `ROW_NUMBER` / `RANK` / `DENSE_RANK` / `PERCENT_RANK` / `CUME_DIST` / `NTILE` / `LAG` / `LEAD` / `FIRST_VALUE` / `LAST_VALUE` / `NTH_VALUE` + `SUM` / `AVG` / `COUNT` / `MIN` / `MAX`; `PARTITION BY` / `ORDER BY` / `ROWS` / `RANGE` frames as below |
| **Views** | ✅ **read-only** | `CREATE [OR REPLACE] VIEW` / `DROP VIEW` / `SHOW CREATE VIEW` / `information_schema.VIEWS`; **writing to them raises an error** |
| Counting with `COUNT(*)` / `col = constant` | ✅ | **0 data reads** (§8.9) — the single most worthwhile thing to exploit |
| Indexes (secondary / hash / unique / composite / full-text / jump) | ✅ | Unique constraints and `MATCH ... AGAINST` work |
| **Candidate cap for full-text search** | ⚠️ | Without a `LIMIT` at most **1000** candidates are returned (truncated before the table lookup). For all hits, write an explicit `LIMIT` or fetch in batches |
| `ALTER TABLE` (append column / drop last column / add-drop index / rename / `TRUNCATE`) | ✅ | O(1), no rewrite |
| Time travel `AS OF TIMESTAMP/TSO`, `ANALYZE`, `EXPLAIN` | ✅ | |
| **Selecting a non-grouped column under `GROUP BY`** | ❌ **raises 1055** | See below; this is **intentional** |
| Correlated subqueries hidden in an `OR` branch | ❌ explicit error | The two sides of an `OR` cannot be rewritten as a join (split into `UNION` instead) |
| Correlated subqueries with non-equi correlation | ❌ explicit error | Only a nested loop is possible, costing rows × rows |
| Views participating in `JOIN` | ❌ explicit error | This would need a derived table, which is not supported yet |
| `GROUP_CONCAT` as a **window** function | ❌ explicit error | Every other aggregate can be windowed |
| `EXPLAIN` displaying window functions | ❌ | Window functions execute normally; the plan simply does not annotate them |
| `SELECT ... FOR UPDATE`, `LOCK IN SHARE MODE` | ✅ | Row locks go through the coordination service (§4.4); locks are released on **transaction commit/rollback**, and at statement end under autocommit; waiting times out per `innodb_lock_wait_timeout` (default 50 s) with 1205. Locks have a TTL (default 2 × the wait limit, floor 30 s): on an **abnormal connection loss** a lock is not released immediately but expires after at most the TTL — so for long transactions raise `innodb_lock_wait_timeout` as well (the TTL follows it) |
| `FOR UPDATE` modifiers (`NOWAIT` / `SKIP LOCKED` / `OF t`) | ❌ explicit error | All of them change locking semantics; silently ignoring them would hand you a transaction that "looks locked" |
| `FOR UPDATE` on joins / views | ❌ explicit error | Row locks here are taken on **base-table primary keys**, and multi-table semantics are undefined — better an error than an implementation that "locks only half" |

#### Enforced `ONLY_FULL_GROUP_BY` (matching the MySQL 5.7+ default)

`SELECT city, name FROM t GROUP BY city` **raises an error** (1055) instead of "picking an arbitrary row per group".

MySQL's behaviour with this rule turned off is **non-deterministic**: the same query can return a different `name` at different times
while everything still looks normal at the SQL layer. This system reports `ONLY_FULL_GROUP_BY` truthfully in `sql_mode`,
so it really enforces it here — **reporting it without doing it would trade "looks more compatible" for a silent, non-deterministic result**.

#### `UNION` Branches Cannot Carry `ORDER BY` / `LIMIT`

In MySQL, `A UNION B ORDER BY x` applies `ORDER BY` to **the whole union**;
here clause parsing hangs off each individual `SELECT`, so a literal implementation would **sort only the right half and then concatenate**
(wrong ordering, but the result would look plausible). So this case is an **explicit error**; split it into two queries and merge them in the application layer.

#### Window Functions

Evaluation position matches MySQL: `WHERE` → `GROUP BY` → aggregates → `HAVING` → **window functions** → `SELECT`
→ `DISTINCT` → `ORDER BY` → `LIMIT`. Therefore:

- they **do not change the row count** (`SELECT ROW_NUMBER() OVER () FROM t` still yields one row per row);
- with `GROUP BY` they act on the rows **after grouping**, so `SUM(SUM(v)) OVER (ORDER BY x)` is legal;
- they may appear in `ORDER BY` (aliases work too); putting them in `WHERE` / `HAVING` / `GROUP BY` **raises an error**.

| Frame form | Supported |
|---|---|
| No frame, window has `ORDER BY` | ✅ default frame = `RANGE UNBOUNDED PRECEDING → CURRENT ROW` (includes peer rows) |
| No frame, window has no `ORDER BY` | ✅ the whole partition |
| `ROWS BETWEEN … AND …` (`UNBOUNDED` / `CURRENT ROW` / `n PRECEDING` / `n FOLLOWING`) | ✅ |
| `RANGE` + `UNBOUNDED` / `CURRENT ROW` | ✅ |
| `RANGE n PRECEDING` / `n FOLLOWING` | ❌ error (this needs interval arithmetic on **sort-key values**, not row counting; computing it as ROWS would return the wrong rows) |
| `GROUPS` | ❌ error |

`RANK` / `DENSE_RANK` / `LAG` / `LEAD` / `NTILE` raise an error without `ORDER BY` —
which row is "row N" is undefined without an order, and answering by scan order would change with the execution plan.
`LAST_VALUE`'s default frame ends at **the current row** (matching MySQL); to get the last value of the whole partition
you must write `ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING` explicitly.

#### Subqueries

| Form | Status |
|---|---|
| `WHERE id IN (SELECT …)` / `EXISTS (SELECT …)` | ✅ |
| `WHERE EXISTS (SELECT … WHERE inner.k = outer.k)` | ✅ **rewritten into a semi join** (inner table scanned once) |
| `NOT EXISTS` / correlated `NOT IN` | ✅ rewritten into anti joins; `NOT IN` NULL semantics match MySQL |
| `SELECT (SELECT … WHERE inner.k = outer.k) FROM …` (scalar position) | ✅ **the inner side is scanned once and materialized by join key**; the outer side probes it per row; aggregates like `MAX`/`COUNT`, `ORDER BY … LIMIT 1` and `DISTINCT` all work |
| `WHERE x = 1 OR EXISTS (…)` | ❌ error (the two sides of `OR` cannot be rewritten as a join) |
| Non-equi correlation (`ON inner.x > outer.x`) | ❌ error (nested loop only → rows × rows) |

**Why not drive row by row**: correlated subqueries could also be implemented as "execute the subquery once per outer row".
That covers more shapes, but the cost is `outer rows × one inner-table scan` — on object storage,
an outer table of a thousand rows means a thousand inner-table scans. This system prefers an **explicit error** for shapes it cannot rewrite.

#### Views Are **Read-Only**

```sql
CREATE VIEW v AS SELECT id, name FROM users WHERE city = 'bj';
SELECT * FROM v WHERE id = 10;      -- ✅ the outer WHERE stacks on top of the view result
INSERT INTO v VALUES (1, 'x');      -- ✗ 1347 'v' is not BASE TABLE
```

**Outer predicates are not pushed down into the view**: there is no derived-table layer here, so a view is "compute it all, then apply the outer clauses".
`SELECT * FROM v WHERE id = 1` first computes the whole view and then filters — if the view body is a full table scan,
that cost is paid. Still done this way because it never returns wrong results, whereas "expanding into a derived table" would require touching the query planner.
(Views also **cannot** participate in `JOIN`, for the same reason.)

#### `UNION`'s `ORDER BY` / `LIMIT` Go at the End

```sql
SELECT a FROM t1 UNION SELECT b FROM t2 ORDER BY 1;   -- ✅ applies to the whole union
SELECT a FROM t1 ORDER BY a UNION SELECT b FROM t2;   -- ✗ error
```

Writing them **after the last branch** is legal (they apply to the whole union); writing them inside a branch is an error —
in MySQL that is a syntax error too, and a literal implementation here would "sort only the right half and then concatenate".

### 4.1 Isolation Level: by Default **Write Skew Is Not Prevented** (highest migration risk)

| | MySQL / InnoDB default | YangBase |
|---|---|---|
| Isolation level | `REPEATABLE READ` | Snapshot isolation (reported externally as `REPEATABLE-READ` as well) |
| Phantom reads | blocked by next-key locks | snapshot isolation has none to begin with |
| **Write skew** | largely avoided by next-key locks | ⚠️ **not prevented** |

**What write skew is**: two transactions each read data **the other is about to modify**, then each writes,
and the result violates an invariant the application was supposed to maintain. The classic example is "two-person on-call" —
two transactions both check "at least two people are still on duty", then each reassigns itself away, and in the end nobody is on call.

**What to do**: any invariant that depends on a read-write intersection must be guaranteed in the application with an explicit lock
(`SELECT ... FOR UPDATE`) or a unique constraint; do not rely on the isolation level.
`SHOW VARIABLES LIKE 'yangbase_isolation_detail'` reports `snapshot-isolation` truthfully.

**`SET TRANSACTION ISOLATION LEVEL …` is accepted, but downgrades are reported truthfully**:
connection pools and Connector/J's `transactionIsolation` setting both send this statement when establishing a connection,
and rejecting it would prevent clients from connecting, so this system **accepts** it — but does not pretend the level changed.
When `READ COMMITTED` / `READ UNCOMMITTED` are requested (two levels this system does not implement),
`SELECT @@yangbase_isolation_degraded` returns the requested level (e.g. `READ-COMMITTED`)
while the server logs a warning line; when the requested level is exactly the one this system provides (`REPEATABLE-READ` /
`SERIALIZABLE`) it returns an empty string. **Wire this variable into your startup self-check**
and you will catch a misconfigured connection pool immediately, instead of waiting until some reconciliation figure fails to add up.

### 4.2 `AUTO_INCREMENT` Is Not Guaranteed to Be Contiguous (§11.4)

Auto-increment primary keys use **segmented allocation**: each CN claims a segment at a time (1000 by default) and claims another when it runs out.

| Scenario | MySQL | YangBase |
|---|---|---|
| Continuous inserts on one connection | 1, 2, 3, … | 1, 2, 3, … |
| Transaction rollback | leaves a gap | leaves a gap |
| **CN restart / inserts alternating across CNs** | still contiguous | **can jump** (segments may be far apart) |
| `TRUNCATE TABLE` | resets to 1 | resets to 1 |

This matches MySQL 8.0's `innodb_autoinc_lock_mode=2` (interleaved mode);
the MySQL documentation states explicitly that auto-increment values are not guaranteed to be contiguous in that mode.

⚠️ **Businesses that rely on `AUTO_INCREMENT` continuity are affected** — for example using it as the "Nth user" identifier.
If you need contiguous numbering, maintain it yourself in the application layer, or switch to an explicit business-number column.

### 4.3 `ENUM` / `SET` Compare as **Strings**, Not by Declaration Ordinal

```sql
CREATE TABLE t (s ENUM('z','a'));   -- declaration order differs from lexicographic order
```

| | MySQL | YangBase |
|---|---|---|
| `'z' < 'a'`? | **true** (compares ordinals 1 < 2) | **false** (compares strings) |
| `ORDER BY s` | by declaration order | by lexicographic order |
| Storage | 1–2 byte ordinal | string (with compression as a fallback) |
| Display order of `SET` | always declaration order | always declaration order (✅ consistent) |

**Why not use ordinals**: ordinal semantics require the column definition to be available at comparison time, but comparisons happen in the evaluator —
where only `Datum` is available. Two inconsistent paths (index path by ordinal, scan path by string) would give
**different orderings without any error**, which is more dangerous than a single point of difference.

**The server warns proactively**: when creating a table, if the declaration order is not lexicographic, a warning is printed to stderr
pointing out that the column's comparison semantics differ from MySQL. Writing declarations in lexicographic order avoids this difference entirely.

### 4.3.1 String Comparison Is by **Binary Order** (equivalent to `utf8mb4_bin`)

This is the item most likely to **silently change your result sets** after migration, and it affects more than `WHERE`:

| | MySQL 8 default (`utf8mb4_0900_ai_ci`) | YangBase |
|---|---|---|
| `'ABC' = 'abc'` | **true** (case-insensitive) | **false** (byte comparison) |
| `WHERE name = 'ABC'` | matches `'abc'`, `'Abc'`, … | **matches only `'ABC'`** |
| `ORDER BY name` | mixed case (`a` and `A` adjacent) | uppercase letters have smaller byte values, so `'Z' < 'a'` |
| `DISTINCT` / `GROUP BY` / unique index | `'ABC'` and `'abc'` are **the same** value | they are **two** values |
| Trailing spaces | `'a' = 'a '` (equal under PAD SPACE semantics) | not equal (byte-by-byte, no padding) |

**What to do**: when you need case-insensitive equality/grouping semantics, normalize in the application layer (e.g. store everything lowercase,
or keep a separate normalized key column); do not rely on the server's collation. To find out whether a piece of SQL trips over this,
use `yb-cli analyze-sql --path <dir>`: it reports any `COLLATE` usage that is not `_bin` as "semantically different",
and also lists any `SET NAMES` that is not utf8mb4.

> ⚠️ **The reported value does not match the actual semantics; trust the actual semantics.**
> The collation name reported in the handshake packet, column definitions and `SHOW VARIABLES` is `utf8mb4_0900_ai_ci`
> (the default of `SessionCharset.UTF8MB4` and `ColumnSchema.collation`, and also the MySQL 8 default name),
> while **actual comparison always uses binary order** (`Datum.compareTo` compares strings directly; `KeyEncoder` is an order-preserving byte encoding).
> In other words: **the client reads "case-insensitive" from the protocol but gets "case-sensitive" behaviour**.
> The reported value exists so that clients and introspection tools do not choke on an unfamiliar collation name,
> but it genuinely does not match the semantics — follow the tables in this section, not what `SHOW VARIABLES` reports.
> (`utf8mb4_0900_ai_ci` itself is **not implemented**; a `COLLATE` declaration on a column is parsed but does not change comparison behaviour.)

### 4.4 `JSON` Is Stored and Retrieved as Text

- Stored in **text** form; validity is checked on write (invalid JSON raises an error, matching MySQL strict mode).
- **No** binary JSON and **no** JSON path indexes; comparison and ordering are textual
  (MySQL compares normalized JSON, where a different key order may make values unequal).
- Functions: `JSON_EXTRACT` / `->` / `->>` / `JSON_UNQUOTE` / `JSON_VALID` / `JSON_TYPE` /
  `JSON_LENGTH` / `JSON_KEYS` (`JSON_KEYS` orders keys as MySQL does: first by key length, then by the key itself).
  Write functions (`JSON_SET` / `JSON_INSERT` / …) as well as `JSON_TABLE` and `JSON_ARRAYAGG` are not implemented.
- A missing path yields `NULL`, but a path **syntax error raises 3143** — a misspelled path that silently returned NULL
  would look like "this row simply does not have that field". The difference between `->` and `->>` is **type**:
  `->` returns JSON (strings keep their quotes), `->>` returns unquoted text, so filter by value with `->>`.

### 4.4.1 Integers: Overflow **Raises an Error**, It Does Not Wrap

`SELECT 9223372036854775807 + 1` raises **1690** (`BIGINT value is out of range`),
instead of wrapping to `-9223372036854775808`. A wrapped number **looks completely normal**,
only its sign is flipped — the user doing reconciliation would never think to distrust that row. This is consistent with `SUM`'s stance
(no `DECIMAL` accumulator type, so overflow is an error).

- `+` / `-` / `*` check for overflow on integers; `/` always computes in floating point (`5 / 2` = 2.5, matching MySQL),
  so integer division has no overflow to speak of.
- **Integer literals beyond `BIGINT` raise an error directly**: `9223372036854775809` does not degrade into a
  precision-losing float (MySQL treats it as `DECIMAL`; this system has no `DECIMAL` literals).
- `-9223372036854775808` is a legal literal and is **exact** (the minus sign is folded into the literal during parsing).

### 4.4.2 Coordination Modes: Three, Chosen at Deployment Time (§4.7)

Four things — timestamps, the shard writer lease, row locks, and the catalog staleness criterion — **must have a single system-wide arbiter**.
They are provided by one interface (`CoordinationService`), with three implementations selected via `--coord-mode`:

| Mode | Mechanism | Steady-state cost | Suitable for |
|---|---|---|---|
| `single` (default) | In-process: timestamps use an object-storage watermark; leases and locks are guaranteed by the **deployment shape** (single CN) | 0 | Single-CN deployments |
| `objectstore` | Uses conditional writes on object storage for CAS-based time assignment, election and row locks throughout | 0 | Multiple CNs, where +1–2 RTT per commit is acceptable |
| `ms` | 3-node metadata service (Raft); TSO / leases / locks / DDL serialization all go through it | 3 small instances | Multiple CNs, low latency desired |

The two multi-CN modes have **exactly the same contract** (the same consistency test suite runs against each); the differences are only latency, cost,
and failure behaviour:

- **`objectstore` has no deadlock detection**: a wait-for graph needs a central node with a global view, and there is none here. Two transactions
  waiting on each other can only wait until `innodb_lock_wait_timeout` expires (1205). `ms` mode reports 1213 immediately.
- **`ms` loses row locks when the leader changes** (the lock table lives in MS memory; §4.4 decided not to persist it). A lock-holding transaction self-checks before commit,
  and rolls back with 1213 if its lock is gone — **the worst case is a transaction retry, never a silently lost update**.
- **When `ms` is unavailable**: CNs keep serving reads and writes using the timestamp range they already obtained; once the range is exhausted, write transactions fail,
  while **read-only transactions** can continue (§4.2's degradation semantics).
- **`ms`'s timestamp window defaults to 1 second** (64 seconds in `single` mode). The larger the window, the greater the cross-CN visibility
  delay: a `commit_ts` issued by this CN may lead another CN's `read_ts` by up to one window
  (see 4.4.3). Do not increase this value in multi-CN deployments.
- ⚠️ **Multi-CN deployments without MS (`objectstore`) have an unavoidable precondition: the clocks of the CNs must be roughly synchronized.**
  The high bits of `commit_ts` come from the local physical millisecond clock (§4.2 requires `commit_ts` to be allocated in real time and not prefetched, otherwise
  the timestamp set is no longer dense for visibility decisions); object storage only provides a watermark and CAS, and will not synchronize clocks for you.
  With MS, it assigns timestamps uniformly and this assumption disappears — one more reason `ms` is recommended for multi-CN setups.

`--role ms` makes a process run only the metadata service (no engine, no MySQL port),
and `--run-meta-service` makes a CN process additionally run an MS node alongside itself (for single-machine deployments and tests).

> ⚠️ **The backend's conditional-write capability constrains which coordination modes are available** (measured, 2026-09-25):
> **Alibaba Cloud OSS has no ETag CAS** (`If-Match` returns 400 `NotImplemented`),
> and `objectstore` mode's leases and row locks **are built precisely on CAS** — on OSS you can only use
> `single` (single CN) or `ms`. In addition, writing on OSS requires
> `--s3-oss --s3-path-style false --i-accept-nonatomic-pointer-switch`
> (the last one explicitly accepts that "coordinated writes are not atomic" — it covers **three** places: manifest pointer switching ·
> the timestamp watermark · the auto-increment segment watermark; all three degrade to "unconditional PUT + read-back verification after write",
> see chapter 2 of the user guide, "Starting an Alibaba Cloud OSS Instance", and design doc §3.3).
> ⚠️ **The auto-increment segment case can only be degraded in `single` (single-CN) mode**: in `ms` mode its mutual exclusion depends entirely
> on this CAS (auto-increment segment allocation does not go through MS), and degradation ⇒ two CNs claim the same segment ⇒ duplicate primary keys.
> Therefore under OSS + `ms`, `AUTO_INCREMENT` **raises an explicit error** (unusable, but not data corruption).

**Cost of coordination operations (microbenchmark numbers, see `:yb-bench:benchCoordination`)**:

| Operation | `single` | `objectstore` | `ms` |
|---|---|---|---|
| `SELECT ... FOR UPDATE` lock + unlock | 22 µs (0 requests) | **98 ms (6 requests)** | 37 µs (0 requests) |
| lease acquire + release | 5 µs (0 requests) | 49 ms (4 requests) | 11 ms (6 requests) |
| point lookup (steady state) | 370 µs (**0 requests**) | 244 µs (0 requests) | 206 µs (0 requests) |
| single-row commit | 2.1 ms (1 PUT) | 1.3 ms (1 PUT) | 1.2 ms (1 PUT) |
| `SELECT ... FOR UPDATE` (end-to-end SQL) | 535 µs (0 requests) | **133 ms (7 requests)** | 431 µs (0 requests) |

> Methodology: embedded S3 on a single machine, p50. The microsecond figures include the fixed per-sample overhead (about 10–20 µs),
> so read them as "≤ this number"; the millisecond cells are the real cost. **The request-count column is the deterministic one.**

**Conclusion: MS does not make ordinary queries faster — it takes "coordination" off object storage.** Point lookups and commits are
identical in all three modes (coordination is **not** on the hot path: timestamps are prefetched a whole window at a time). The difference is entirely in operations like row locks and leases
that must "ask an authority every time": `ms` replaces `objectstore`'s 2–6
object-storage requests with a single in-memory RPC, making row locks three orders of magnitude faster.

Conversely, `ms`'s **lease** (11 ms) is faster than `objectstore`'s (49 ms) but three orders of magnitude slower than `single`
— because leases and timestamp-window updates both go into the consensus log, and each one is a real PUT (6 requests = 2 log entries × 3 nodes). So the choice depends on **load shape**: row-lock heavy (lots of `FOR UPDATE`)
→ choose `ms`; almost no `FOR UPDATE` but you want zero steady-state cost → choose `objectstore`.

> ⚠️ **MS's election timeout defaults to 500 ms–1 s, an order of magnitude larger than the rule of thumb for "in-memory consensus"** — because its
> log lives on object storage, and each append is a PUT. With an election timeout of only 150 ms, a single slow replication would be enough
> for a follower to start an election: the cluster would repeatedly change leaders under load, without errors, but each change loses the lock table (§4.4).
> The replication wait limit is derived from the election timeout, so tuning `--ms-election-timeout-ms` alone is enough.

### 4.4.3 The Two "Windows" in Multi-CN Deployments: Just-Committed Rows, Just-Completed DDL

Multi-CN deployments have two **inherent** propagation delays. Neither is jitter — retrying fixes them — but you need to know which one you are seeing:

| What you see | Real cause | Maximum lag | How to avoid |
|---|---|---|---|
| A row just committed on CN1 is not visible on CN2 | **Timestamp window**: a `commit_ts` issued by CN1 may lead CN2's `read_ts` by up to one window | one window (`ms` default 1 s, `single` 64 s) | Do not increase `--tso-batch-ms` in multi-CN setups |
| A table just created on CN1 reports "table does not exist" on CN2 | **Asynchronous catalog propagation**: CN2's in-memory catalog learns about it only on the next poll (5 s) | one poll interval (5 s) | Wait 5 s or simply retry |

**The "database" layer does not lag**: when `USE db`, the database name in a connection string, or the protocol-level `COM_INIT_DB`
cannot find the database in the local catalog, the catalog is **refreshed once before deciding** (a real reload happens only if the version actually changed).
So "`CREATE DATABASE d` on CN1, then immediately connect to `d` via a CN2 connection" does not produce
`Unknown database 'd'` — that would be **the worst class of error**: the database does exist, but the user sees
"the database I just created is gone", and a retry fixes it, so it gets written off as occasional jitter.

**The "table" layer is still asynchronous**: a table just created on another CN appears in this node's in-memory catalog only after
at most one poll interval (5 s); querying it during that window reports "table does not exist" —
**just retry**; the DDL did take effect.

> "Databases usable immediately, tables after 5 seconds" looks asymmetric, but it reflects two different costs: judging a **database name**
> is worth refreshing the catalog once (otherwise you get the misdirecting "the database I just created is gone" error),
> whereas every table resolution sits on the query hot path, where a catalog refresh cannot be hung.

### 4.5 Character Sets: 4 Supported, the Rest **Explicitly Rejected**

`utf8mb4` (default), `utf8`, `latin1` and `binary` are supported.

```sql
SET NAMES gbk;   -- ✗ 1115 Unknown character set: 'gbk'
```

**We do not "accept it but treat it as utf8mb4"** — that is exactly the silent error described as pitfall T1 in design doc §11.1.1:
the client believes it is using gbk while the server uses utf8mb4, Chinese text written in looks fine and comes back as mojibake,
and nothing anywhere along the path reports an error. §11.1.1's response column says
"either fully support per-column character sets, **or explicitly reject everywhere else**" — this system chose the latter.

Returning Chinese text on a `latin1` / `binary` connection **raises an error** rather than substituting `?`:
silent substitution would hand the client a string that "looks normal but whose content has already changed".

`SET character_set_results = NULL` (sent by Connector/J during connection setup) is equivalent to utf8mb4:
its semantics are "no conversion, return raw bytes in the column's own character set", and every string column here is utf8mb4,
so the two happen to be equivalent.

### 4.5.1 Object-Storage Retries and Throttling (L1 Adaptation Layer)

Object-storage failures are **not exceptional events but the norm** (the official S3 SLA permits 5xx). This system adds
a retry/backoff/throttling layer on top of object storage:

| Failure | Handling |
|---|---|
| 5xx / 429 / timeout / connection reset | **retry** (exponential backoff + jitter, default max 4 attempts, 5 s total budget) |
| `409 ConditionalRequestConflict` | **retry** (the conditional outcome is unknown; resending yields a definite answer) |
| `412 PreconditionFailed` | **never retry** (the condition is definitively not met) |
| `404` | **never retry** (an explicit business meaning) |

Retrying **does not break CAS**: the caller sees success only if the server accepted the conditional write; if the condition truly does not hold,
the resend gets 412 and the caller still sees "CAS failed".

The default cap on concurrent in-flight requests is 64 (§10.2's parallel prefetch naturally issues many concurrent GETs;
without a cap you would get a flood of 503s, which the retry layer would then turn into even longer tail latency).

**The SDK's built-in retry is explicitly disabled** (`RetryPolicy.none()`): retries must have exactly one owner,
otherwise layer counts and budgets multiply, metrics become misleading, and the total deadline cannot be enforced.

Observability: `yb_objectstore_retries_total{op,reason}` and `yb_objectstore_retries_exhausted_total{op}`.

`yb_objectstore_requests_total` carries a `cached` label whose two values mean **opposite** things — do not misread it:

| Label | Meaning | Is it a cost |
|---|---|---|
| `cached="false"` | **requests that actually reached object storage** (retries included; one retry counts as one request) | ✅ **these are the requests on your bill** |
| `cached="true"` | requests **satisfied by the in-memory block cache / local NVMe second-level cache and therefore never issued** (derived by the `yb-server` collector from cache hit counts, recorded only under `op="get"`) | ❌ zero cost |

So when building cost-scoped alerts and regression assertions, **filter on `cached="false"`**;
to see "how much the cache saved", look at `cached="true"`. Their sum is not the total request count; adding them together is meaningless.
(Retry accounting is as above: `cached="false"` already includes the extra attempts issued by retries — that is real cost.)

### 4.5.2 The Local Filesystem Backend Requires an Explicit Switch

`--data-dir` (local FS) is **for development and testing only**, and must be declared explicitly:

```bash
# Without this flag the server refuses to start
./gradlew :yb-server:run --args="--data-dir /tmp/yb --i-know-this-is-not-durable"
```

Rationale: a local FS provides neither cross-process atomicity for conditional writes nor multi-replica durability. "Tests pass on a local FS
while production depends on object-storage semantics" is the most dangerous class of defect source (design doc §3.3).
With the flag present, a prominent warning is printed and the server continues.

### 4.5.3 Commit Modes (`commit-mode`): `sync` by Default, `batch` and `async` Optional

| Mode | Status | Semantics |
|---|---|---|
| `sync` | ✅ **factory default** | The group-commit writer ACKs only after the PUT is durable. **RPO = 0** (batching window 5 ms / 256 KB) |
| `batch` | ✅ available | For high-throughput imports; **also waits for durability, RPO = 0**, just with a wider window (50 ms / 4 MB) ⇒ more PUT amortization, higher per-transaction latency |
| `async` | ⚠️ available, **must be declared explicitly** | ACKs as soon as the PUT is **issued** (does not wait for it); a background coroutine completes the write. **RPO > 0**: a crash loses writes that were ACKed but not yet durable |

```bash
# Deployment-level setting, chosen only here: sync (default) / batch / async
./gradlew :yb-server:run --args="--s3-bucket my-bucket --commit-mode batch"
```

**What `async` actually gives up** (three things, and all three matter):

1. **RPO > 0**: a process crash loses writes that were ACKed but not yet durable. The bound is "upload queue of 64 batches × 100 ms window",
   not infinity — when the queue fills up the group-commit loop blocks (backpressure), which surfaces as higher latency rather than unbounded memory.
2. **Cross-CN visibility is one round trip late**: at ACK time the object is not on the object store yet, so another CN reading immediately will **not** see it.
   Within the same CN it is visible (the MemTable is applied before the ACK). So this is **not** strong consistency.
3. **A failed write is not rolled back**: the client already got success, so the background path retries indefinitely;
   a batch only counts as lost if it still cannot be written during shutdown, which increments `yb_async_commit_lost_total` (non-zero means real loss).

How to see the current exposure window: `yb_async_commit_inflight` (non-zero = N batches ACKed but not durable) and
`yb_commit_mode_rpo_gt0` (1 = this instance promises RPO > 0). Startup also prints an explicit RPO warning, and
`SHOW VARIABLES LIKE 'commit_mode'` truthfully reports `async`.

> The default is still `sync`: `async` is for workloads that can tolerate a loss window (bulk imports, log-style writes),
> not a generic performance switch. If in doubt, stay on `sync`.

**The session-level `SET commit_mode` can only equal the deployment value**:

```sql
SET commit_mode = 'batch';   -- raises 1235 (ER_NOT_SUPPORTED_YET) when the deployment value is sync
```

This is not a hasty refusal: the group-commit WalWriter is **one per shard and shared by all sessions**,
and if transactions with different durability promises were mixed into one batch, "batching" would have no definition. So `SET` is a **no-op** for the same value,
raises 1235 for a different one, and points you at `--commit-mode`. **Do not put it in your connection pool's init SQL to "standardize the mode".**

To confirm which mode the current instance uses:

```sql
SHOW VARIABLES LIKE 'commit_mode';   -- reports the deployment value truthfully; sync from the factory
```

### 4.6 Protocol Modes

The protocol layer works according to the capabilities negotiated by the client; both modes are fully supported:

| Capability | Notes |
|---|---|
| `CLIENT_DEPRECATE_EOF` | ✅ supported. Once negotiated, the result-set terminator switches from an EOF packet to an OK packet (one fewer round trip per segment); after the column-definition section the terminator is **no longer** sent — that is what this capability requires |
| `CLIENT_MULTI_STATEMENTS` | See 4.7; must be enabled explicitly |
| `CLIENT_MULTI_RESULTS` | ✅ |
| Server-side cursors (`CURSOR_TYPE_READ_ONLY` + `COM_STMT_FETCH`) | ✅ supported. Connector/J's `useCursorFetch=true` takes this path |
| `CLIENT_SESSION_TRACK` | ❌ not implemented (not advertised, so clients fall back to the classic field layout) |

To diagnose "which protocol mode this connection is in":

```sql
SELECT @@yangbase_protocol_flags;   -- e.g. deprecate_eof,multi_statements,multi_results
```

### 4.7 Multiple Statements: **Explicit Negotiation** Required

Multiple statements in a single `COM_QUERY` are executed only when the client has negotiated `CLIENT_MULTI_STATEMENTS`
(`allowMultiQueries=true` in Connector/J). Without negotiation, `a; b` is a syntax error.

MySQL behaves the same way — `CLIENT_MULTI_STATEMENTS` is a switch by which the client declares
"I know this SQL may contain multiple statements", and SQL-injection protection depends on clients not enabling it.
A server treating it as default behaviour would be closing that gate on the client's behalf.

### 4.8 Other Differences at a Glance

| Item | MySQL / InnoDB | YangBase |
|---|---|---|
| `SELECT ... FOR UPDATE` | row locks (gap locks + next-key locks) | **row locks only, no gap locks**: ordinary reads without `FOR UPDATE` are blocked by no lock at all, so an `INSERT` conflicting after a `SELECT ... WHERE` still needs a unique constraint as a backstop. MS mode has deadlock detection (1213); object-storage mode only has wait timeouts (1205) |
| `FOR UPDATE` locks when the MS leader changes | not applicable (no leader concept) | the lock table lives in MS memory and disappears with the leader. A lock-holding transaction **self-checks before commit** and rolls back with 1213 if the lock is gone — worst case a transaction retry, **never** a silently lost update |
| DDL | locks the table in some cases | fully online (metadata operations), including index creation (§8.7) |
| Unique constraint implementation | next-key locks | atomic duplicate detection on the index shard (one extra RTT across shards) |
| **String comparison / collation** | per-column collation, default `utf8mb4_0900_ai_ci` (case-insensitive) | **always binary order** (equivalent to `utf8mb4_bin`); the reported collation name does not match the actual semantics, **see 4.3.1** |
| **Commit mode** | — | deployment-level `--commit-mode`: `sync` (default) / `batch` / `async`. `async` has RPO > 0 and cross-CN visibility one round trip late, **see 4.5.3** |
| NULL position in `ORDER BY` | NULL sorts lowest | matches MySQL |
| Next value after explicitly inserting a large value into an auto-increment column | value + 1 | value + 1 (✅ consistent) |
| `information_schema` coverage | complete | common views are all present; some detail fields may be empty |
| Error codes | the full MySQL set | common error codes match MySQL numerically; a few cases use `1105` |
| Integer arithmetic overflow | raises 1690 | raises 1690 (**no wrapping**, see 4.4.1) |
| Referencing a column by **table name** after the table is aliased (`t.v` in `FROM t AS x`) | raises 1054 | accepted (more permissive; ambiguity in join queries still raises 1052) |
| `CASE WHEN` / `IF()` / `SUBSTRING` / `ROUND` / `CAST` | supported | **raises an error** (`1235` or a syntax error); never silently returns NULL |
| `INSERT ... SELECT` / derived tables (`FROM (SELECT ...) x`) | supported | **raises an error** (syntax error); never silently inserts 0 rows |
| Rows returned by full-text search | all hits | **at most 1000** without a `LIMIT` (top-k candidates, to avoid full table lookups); write a `LIMIT` to specify |

### 4.9 Performance Characteristics (not defects, but physical constraints)

| Scenario | Magnitude |
|---|---|
| Point lookup (cache hit) | p50 **9 µs** (0.004 object-storage requests per query) |
| Point lookup (cold cache) | p50 **124 µs**, 1 GET |
| Write commit (`sync` mode) | p50 ~0.5 ms (local FS) / on real object storage, determined by one PUT's RTT |
| Group-commit amortization (16 concurrent writers) | 3200 transactions → **220–221 PUTs** (14.5 transactions/PUT); **single-threaded serial writes have no batching room** (1.0 transactions/PUT) |
| Range scan of 1000 rows | 1 Range GET (about 350k rows/s on embedded S3; about 170k rows/s on real MinIO) |
| `.idx` metadata overhead | **6.9%** of the data size (deliberately uncompressed — it must stay resident in memory) |
| `COUNT(*)` | **0 GETs**, about 0.37 ms (300× faster than a full table scan) |

> Methodology: `./gradlew :yb-bench:bench`, with an embedded S3 server as the backend (real HTTP), p50.
> In the table above, **only the last column, "request count", is deterministic** — latency and throughput depend heavily on the environment; switching to real
> object storage only changes latency (about 4 ms per PUT; on the cloud, another order of magnitude higher) and **never changes request counts**:
> point lookups at 0.004 requests, `COUNT(*)` at 0, and 14.5× group-commit amortization are architectural properties. For the full backend comparison, see
> the "Performance (measured)" section of [README](../../README.md).

**The hot/cold difference is a few tens of times**, which is a physical lower bound of object storage as a data layer;
no optimization can remove it. Therefore SLOs must be stated in two columns: "cache hit" and "cache miss"
(design doc §14.1). The thing that differs from MySQL users' expectations by 2–3 orders of magnitude is:
**point-lookup latency with a cold cache**. Before concluding "the database got slow", check whether the cache hit.

---

## 5. Known Unimplemented Features (by Priority)

| Item | Impact | Notes |
|---|---|---|
| Authentication / authorization / TLS | any username and password are accepted | for use on trusted networks only; **do not expose it directly to the public internet** |
| Storage tiering and lifecycle rules | cold data cannot sink automatically | **decided not to do it in the short term (the coming months)**; see the note below. The design doc also warns that archival tiers are almost always a loss |
| Cross-region read replicas | a single-region failure means unavailability | relies on object storage's own cross-region replication (CRR) |
| Cross-database JOIN / cross-database transactions | behaviour is undefined | a design gap (§16.3) |
| `GEOMETRY` and spatial functions | not implemented | spatial indexes are out of design scope (§8 has no such indexes) |

### On Storage Tiering: Not in the Short Term (Decision Record)

**Decision of 2026-09-24: storage tiering will not be implemented in the coming months.** This is not an omission, it is a trade-off:

- it saves on **storage unit price**, whereas this system's main cost line is "decoupling compute from storage, migrating on discount cycles"
  — a line already delivered by "stateless CNs + object storage as the single source of truth";
- the cooling mechanism that should actually be used is **`DROP PARTITION` (already implemented, cost ≈ 0)**, not sinking objects;
- and getting it wrong is expensive (see below), not worth doing before the feature surface is filled in.

If it is done later, the boundary should be: **sink only to the infrequent-access tier, only for objects large enough to be worth it (e.g. `.dat` > 8 MB),
never touch `.idx` / `manifest` / WAL, and compute "storage saved vs. the cost of one full retrieval" before executing**.

### The Full Warning on Storage Tiering (§13.3)

**Do not** sink objects to an archival tier (Glacier and the like). Taking AWS as an example: Glacier IR storage costs
$0.004/GB-month, but **retrieval** costs $0.03/GB — **one retrieval equals 7.5 months of storage**.
Data that "will never be read again" barely exists in a database (backup verification, audit queries, and occasional historical look-backs
all touch it). **The lower bound for storage tiering is the "infrequent access tier", not the "archival tier".**

Also **do not** enable Intelligent-Tiering: it charges a monitoring fee per **object** ($0.0025 per 1000 objects-month),
and LSM's defining trait is precisely **many small objects**. 100 million small objects would cost $250 per month in monitoring fees alone, far exceeding the storage cost of those objects.

The mechanism that should actually be used to archive cold data is `DROP PARTITION` (§9.4): cost ≈ 0,
and it incidentally reduces manifest size and read amplification.

---

## 6. How to Verify These Claims

```bash
# Unit tests + integration tests. ⚠️ integrationTest is **not** part of check/build
# (it is a separate source set and a separate task), so it must be named explicitly to run
./gradlew build integrationTest

# The same integration assertions running against real MinIO. ⚠️ Must use -P properties, not environment variables:
# the Gradle daemon lives for a long time and fixes its environment at startup, so variables exported
# in the shell afterwards do not reach the test JVM it launches (this is noted in build.gradle.kts)
./gradlew integrationTest \
  -PYB_S3_ENDPOINT=http://127.0.0.1:9000 -PYB_S3_BUCKET=yangbase \
  -PYB_S3_ACCESS_KEY=yangbase -PYB_S3_SECRET_KEY=yangbase123

# Or use the script to do both at once (start MinIO + run the :yb-server and :yb-ms integration tests)
./scripts/minio.sh test

# Branch coverage gate (≥ 50%)
./gradlew :yb-server:koverVerify

# Benchmarks (point-lookup request counts, group-commit amortization, COUNT's 0 GETs)
./gradlew :yb-bench:bench
```

Tests corresponding to each item in this file (all runnable individually):

| Item | Test |
|---|---|
| 4.1 Isolation level | `TransactionTest`, `IoBudgetIntegrationTest > long read transactions are not disturbed by concurrent writes` |
| 4.2 Non-contiguous auto-increment | `AutoIncrementTest` (no duplicate numbers across restarts, TRUNCATE resets) |
| 4.0 SQL surface | `SqlSurfaceTest` (end-to-end), `AggregateTest` / `JoinTest` (pure logic) |
| 4.3 ENUM/SET ordering | `ColumnTypesTest` |
| 4.3.1 String comparison semantics | no dedicated case (it is a direct consequence of `Datum.compareTo` and `KeyEncoder`); `yb-cli analyze-sql` reports `COLLATE` usage that is not `_bin` as "semantically different" |
| 4.4 JSON | `ColumnTypesTest` |
| 4.5 Character sets | `ProtocolCompatTest` (Chinese round-trip, `SET NAMES gbk` rejected) |
| 4.5.3 Commit modes | `EndToEndTest > SHOW and metadata queries` (asserts the factory default is `sync`); the rejection of `async` lives in `YangBaseServer.parseCommitMode` and fails at startup |
| 4.4.2 Coordination modes | `MultiCnCoordinationTest` (the same consistency suite runs against each mode), `:yb-bench:benchCoordination` |
| 4.6 Protocol modes | `RawProtocolTest` (byte-level assertions), `ProtocolCompatTest` |
| 4.7 Multiple statements | `ProtocolCompatTest` |
| Section 3, no-op statements | `ProtocolCompatTest > LOCK TABLES and SET sequences do not interrupt a dump` |
| 4.5.1 Retry/throttling | `RetryingObjectStoreTest`, `S3FailureClassificationTest` (real HTTP against embedded S3) |
| 4.5.2 Local FS guard | verified manually (two paths: refuses to start without the flag / warns and starts with it) |
| 4.9 Performance characteristics | `:yb-bench:bench` (request counts and latency), `IoBudgetIntegrationTest` (request-count assertions) |

### Index and Partitioning Performance Acceptance

The full acceptance entry point is [Index Performance Acceptance](../zh/index-bench.md) (Chinese). Literal `LIKE` conditions that
a NGRAM jump index can handle are pushed down to block-level pruning; function-wrapped columns, `NOT LIKE`, and OR conditions
from which nothing can be safely extracted continue to fall back to a full scan plus filter.

`HASH(<integer column>)` routes by the declared column and prunes for equality, IN, and safely extractable AND/OR conditions.
Partition columns must be part of the primary key; HASH expressions are currently rejected explicitly, and range predicates still scan all partitions.
KEY currently hashes the full primary key, so an exact full-primary-key lookup can be located directly; pruning on an arbitrary subset of KEY columns is not provided.
