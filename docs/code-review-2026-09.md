# 代码审查报告（2026-09-25）

> **这份文档记录一次系统性审查的结论**：查了什么、修了什么、还剩什么。
>
> 审查维度：SQL 语义 · 存储与事务 · 协议与边界 · 运维与成本 · 文档一致性。
> 方法：逐行读代码走通调用链（不是静态扫描），每一条都给出触发它的具体输入。
>
> **两轮之后的现状：59 条发现项全部有结论**——第一批 35 条在审查当轮修掉，
> 第二批 24 条（编号 O1–O24，原本列在下面"仍然开着"一节）**已全部处置**：
> 23 条修掉并各配一条守护测试，1 条（O23 `compose`）判定为"不该接线"、
> 从 SPI 中移除并把理由与"什么条件下加回来"写进设计文档 §3.2。
>
> **这份文档的用法**：它不是成绩单，而是**回归清单**。下面每一条都写了
> "它当初为什么会发生"与"哪条测试盯着它"——改动相关代码时请连同那条测试一起看。

---

## 一、本轮已修复（按"不修会怎样"排序）

### 静默错误 / 静默丢数据

| # | 缺陷 | 触发 | 后果 | 位置 |
|---|---|---|---|---|
| 1 | **列级 `PRIMARY KEY` 被丢弃** | `CREATE TABLE t (id INT PRIMARY KEY, …)` —— 最常见的建表写法 | 表以**空主键**建成，`KeyEncoder.encodeKey` 返回空 `ByteArray`，**每一行用同一个 key 写入**：LSM 的同 key 覆盖语义下这张表**永远只剩最后一行**，且不报任何错 | `Parser.kt` 列定义解析 |
| 2 | **`DROP INDEX i ON t` 把整张表删了** | 任何人执行这条 DDL | `catalog.dropTable` 真删，返回 affectedRows=1 | `Parser.kt` 的 `DROP` 分支 |
| 3 | **`INSERT` 撞主键静默覆盖** | `INSERT INTO t VALUES (1,…)`，id=1 已存在 | 覆盖别人的行并返回成功（应用拿不到 1062，也就不会走它本该走的更新分支） | `Executor.checkPrimaryKeys`（新增） |
| 4 | **DECIMAL 列写入行错位** | `INSERT INTO t(amount) VALUES (12.34)` | 按浮点编码写、按定点解码：读出垃圾，且只消耗 2 字节——**这一行后续所有列全部错位** | `Executor.coerce` |
| 5 | **DECIMAL 的 `%` 算成了乘积、`*` 截断** | `SELECT 10.00 % 3.00` | 得到 30（应为 1.00）；`2.50*3.00` 得到 6 | `Expr.arith` |
| 6 | **`CEILING`/`FLOOR` 对 DECIMAL 不取整** | `SELECT CEILING(price)`（DECIMAL 列） | 原样返回 2.50 | `Functions.ceilFloor` |
| 7 | **`MOD` 对 DECIMAL 截断** | `SELECT MOD(2.5, 1)` | 返回 0（应为 0.5） | `Functions` |
| 8 | **BLOB 列的等值查询永不匹配** | `WHERE blob_col = 'abc'` | 存成字符串、比较走"异类"分支 → 静默空集 | `Datum.coerceTo` / `Executor.coerce` / 协议层改发原始字节 |
| 9 | **`YEAR` 列被写成字符串** | `INSERT INTO t VALUES (1, 1998)` | 按 zigzag 整数解码 → 读出来是 **2** | `Executor.coerce`（YEAR 不在 `isInteger` 里，曾被 `else` 分支转成字符串） |
| 10 | **`DROP TABLE a, b` 只删第一张** | 多表 DROP | 返回成功，其余表静默留下 | `Parser` + `Executor` |
| 11 | **`UNION` 混合 `ALL` 时每条边共用一个标志** | `A UNION B UNION ALL C` | 行数不对（曾经三条边都用最后一个 `ALL`），不报错 | `Parser.parseUnion` |
| 12 | **时间值往返漂移** | 服务器时区非 UTC 时 `INSERT '2026-02-01 10:00:00'` | 读出来是 `18:00:00`（解析按 UTC、渲染按服务器时区） | `MysqlServer` 两个渲染点 + `Datum.formatDateTime` |
| 13 | **`DATE` 列渲染出多余的时刻** | `SELECT d FROM t`（d 是 DATE） | 返回 `2026-02-01 00:00:00`，且列类型报成 DATETIME | `ResultSet.declaredTypes` + 协议层 |
| 14 | **字符串函数的 NULL 语义** | `UPPER(NULL)` / `CONCAT('a',NULL)` / `LENGTH(NULL)` | 分别返回 `''` / `'a'` / `0`（MySQL 都是 NULL） | `Builtins` 的 strict 机制 |
| 15 | **`LENGTH` 返回字符数而不是字节数** | `LENGTH('中')` | 返回 1（应为 3）——`LENGTH()` 常被用来判断"是否超长" | `Builtins` |
| 16 | **`ROUND` 用银行家舍入** | `ROUND(-2.5)` | 返回 -2（MySQL 是"远离零"→ -3） | `Builtins.round` |
| 17 | **`NOW()` 逐行取时钟** | `WHERE ts > NOW()` 扫多行 | 每一行用不同的基准比较，结果不可复现 | `EvalContext.nowMillis`（语句级常量） |
| 18 | **`CASE` 的 ELSE 被无条件求值** | `CASE WHEN 1=1 THEN 'ok' ELSE CAST('x' AS SIGNED) END` | 报 22007——不该求值的分支把语句搞失败 | `Eval` 的 CASE 分支改短路 |
| 19 | **TSO 把 `commit_ts` 夹回段上界** | 别的 CN 的读时间戳越过本段上界时 | **同一个 commit_ts 发给多笔事务** → 写同一行时后者覆盖前者（已提交事务静默消失）+ 水位漂移 | `WindowTso.next`（改为强制重新预取） |
| 20 | **`walFlushedThroughSeq` 记的是"已分配"而非"已进 SST"** | flush 与写入并发 + 崩溃 | 恢复时跳过一批**既不在 SST 也没被重放**的 WAL → 已确认的写入静默消失 | `LsmStore` 新增 `appliedThroughSeq` |
| 21 | **WAL 序号重启后回退** | 写→flush→重启→再写→崩溃 | 新批次的序号 ≤ 上一轮的 `walFlushedThroughSeq`，被整段跳过 | `LsmStore.startWriter` 取 `max(manifest.seq, 前缀最大序号, appliedThroughSeq)+1` |
| 22 | **`Catalog.load()` 是合并而不是替换** | CN1 `DROP TABLE`，CN2 轮询重载 | 表**仍然留在 CN2 的内存目录里**，继续可读可写 | `Catalog.load` 加 `clear()` |
| 23 | **`restoreTable` 不推进 catalog 版本** | 恢复一张被删的表 | 别的 CN 永远发现不了它回来了（一直报"表不存在"） | `Catalog.restoreTable` 加 `afterDdl()` |
| 24 | **预处理语句的字符串参数可注入** | 参数含 `\'`（Go 驱动默认就走服务端预处理） | 转义只处理了 `'` 没处理 `\` → **引号提前闭合，条件退化成恒真** | `MysqlServer.escapeSqlString` |
| 25 | **绑定参数的负数被当无符号读** | `ps.setInt(1, -5)` | 渲染成 `4294967291` → 查询返回另一批行/空集 | `readBinaryParam` 改用有符号读 |
| 26 | **读侧不做拆包重组** | 客户端发 >16MiB 的语句 | 前 16MiB 被当完整语句执行，**余下字节被当成下一条命令** | `Packets.readPacket` |
| 27 | **畸形包把内部缓冲区偏移回给客户端** | 构造一个长度字段撒谎的包 | `1105 IndexOutOfBoundsException: fromIndex: 20, toIndex: 2147483647` | `Packets.Cursor` 加边界校验 + `ProtocolException` |
| 28 | **连接断开不释放行锁** | 客户端 `BEGIN; SELECT … FOR UPDATE;` 后掉线 | 行锁只能等 TTL（默认 30s），其它事务表现为"莫名很慢" | `MysqlConnection.run` 的 finally |
| 29 | **未预期异常泄漏内部信息** | 任何非 `SqlException` 的失败 | 对象键（`shard/3/0/manifest/CURRENT`）、桶名、endpoint 直接进错误包 | `MysqlServer` 的错误出口 |
| 30 | **`yb-cli` 以写者身份打开分片** | 对**线上桶**执行 `ls`/`recover-check`/`backup` | `single` 模式下 CLI 与线上 CN **同时成为同一分片的写者**（epoch 恒为 1），互相覆盖 `wal/e1/` 下的对象 | `DatabaseEngine(readOnly=)` + `YbCli.engineOf` 默认只读 |
| 31 | **`gc --delete` 的窗口无下限** | `--window-hours 0` 或对在线集群执行 | 删掉刚写出、还没进 manifest 的 SST/WAL → 分片永久不可读 | `YbCli` 加 1 小时下限 + 逃生开关 |
| 32 | **GC 指标撒谎** | 水位从未推进（默认部署就是这样） | `yb_gc_watermark_lag_seconds` 报 **0**（"刚跑过"）而真相是"从没跑过"，于是**存储无限增长而监控显示健康** | 改为 `-1` 表示"尚无水位" |
| 33 | **`yb-cli` 的 6 个命令没接进分发** | `backup`/`backups`/`restore`/`pitr`/`cdc`/`analyze-sql` | 函数写好了但 `when (cmd)` 里没有 → 报"未知命令"，而 README 说支持 | `YbCli` 接线 |
| 34 | **`VARCHAR(n)` 超长静默截断** | `INSERT INTO t (v) VALUES ('200 个字符')`，v 是 `VARCHAR(10)` | 存进去了（MySQL 严格模式报 1406） | `Executor.checkWidth` |
| 35 | **整数宽度溢出静默回绕** | `INSERT INTO t (i) VALUES (999)`，i 是 `TINYINT` | 存进去了（MySQL 报 1264） | 同上 |

### 能力补齐（用户要求的"日常会用到的 SQL 语法、函数、数据类型"）

| 能力 | 说明 |
|---|---|
| **内置函数库约 90 个** | 字符串（`SUBSTRING`/`LEFT`/`RIGHT`/`TRIM` 三种语法/`REPLACE`/`INSTR`/`LOCATE`/`LPAD`/`RPAD`/`CONCAT_WS`/`SUBSTRING_INDEX`/`HEX`/`UNHEX`/`MD5`/`SHA2`…）· 数值（`ROUND`/`TRUNCATE`/`CEIL`/`FLOOR`/`MOD`/`POW`/`SQRT`/`LOG`/三角…）· 日期（`DATE_FORMAT`/`STR_TO_DATE`/`DATEDIFF`/`TIMESTAMPDIFF`/`DATE_ADD`/`LAST_DAY`/`UNIX_TIMESTAMP`…）· 控制流（`IF`/`IFNULL`/`NULLIF`/`COALESCE`/`GREATEST`/`LEAST`/`ISNULL`） |
| **`CASE WHEN … END`** | 搜索式与简单式都支持；三值逻辑正确（`WHEN NULL` 永不匹配）；ELSE 短路 |
| **`CAST(x AS t)` / `CONVERT`** | `SIGNED`/`UNSIGNED`/`CHAR`/`DATE`/`DATETIME`/`DECIMAL(p,s)`/`DOUBLE`/`JSON`/`BINARY`；失败报 1292 而不是给 0 |
| **`INTERVAL` 语法** | `DATE_ADD(d, INTERVAL 1 DAY)` |
| **upsert 三兄弟** | `INSERT … ON DUPLICATE KEY UPDATE`（含 `VALUES(col)`）· `REPLACE INTO`（先删后插，唯一键冲突也认）· `INSERT IGNORE` |
| **`INSERT … SELECT`** | 列数不符报 1136 而不是错位插入 |
| **函数参数个数校验** | 写错报 1210（曾经抛 `IndexOutOfBoundsException` → 1105 + 栈） |

---

## 二、第二轮：上一轮**仍然开着**的缺陷，逐条处理

> 本节保留每一条**当时的触发条件**（那是它值得被记下来的原因），
> 后面接本轮的处置与证据。**24 条全部有结论**：23 条已修，1 条（O23）
> 判定为"不该接线"，并把理由与"什么条件下该回来"写进了设计文档 §3.2。
>
> 处置方式分三类：**改代码**（多数）· **加测试**（每条都加了，因为它们共同的特点是
> "没有测试就一定会复发"）· **写入文档**（O23，以及所有改变了对外行为的部分）。

### 🔴 数据正确性

**O1. 2PC 的提交标记不影响读可见性 —— 读者能看到半笔事务**
`DatabaseEngine.commitBuffer` 在阶段 2 写提交标记，但可见性完全由各分片 WAL 批次的 `commitTs` 决定，阶段 3 是**逐分片**写入的。读者在"`ts` 已分配、阶段 3 还没跑完"这段时间开始事务，就会拿到 `read_ts > ts` → 分片 A 看得见、分片 B 看不见。
窗口是每分片的网络往返（毫秒级）。
**建议**：读路径按提交标记解析未决事务，或让阶段 3 落定后再放开可见性。

> ✅ **已修**（`DatabaseEngine`）。新增在途 2PC 表：`commitBuffer` 在阶段 1 之前登记 `ts`，
> 阶段 3 落定后移除；`currentReadTs()` 取 `min(peeked, oldestInflight)`，**绝不发出一个
> 会跨过在途提交的读时间戳**。代价是那一段窗口里读会被压到提交点之前（多等一次，
> 而不是看到半笔）。守护测试：`ReviewFixesTest > 读不到半笔跨分片事务`
> （200 轮里断言"两行要么都在、要么都不在"）。

**O2. WAL 段对象**（`s-{from}-{to}.log`）**永远无法解码**
`compactWalSegments` 把多个 WAL 对象原样拼接，而 `WalCodec.decode` 要求整个 buffer 末尾是 CRC → 必然 `CorruptDataException`。后果：**在线索引构建的追平阶段整段漏读**（索引缺项 → 走索引的查询静默少行）、CDC 静默缺一段。
段对象还**不在 GC 的覆盖范围内**（键名解析不出序号）→ 单调累积。
**建议**：段对象带自己的索引/CRC 头，或让这些路径显式拒绝段并给出明确错误。

> ✅ **已修**（`Wal.kt` + `Gc.kt`）。两件事：
> 1. 段对象有了自己的帧格式（`SEG_MAGIC` + 对象数 + 总长），所有读 WAL 的路径统一走
>    `WalCodec.decodeAny`——**单对象级别的 CRC 仍然成立**（内嵌对象原样保留）。
> 2. **GC 现在看得见段对象**：`Gc.seqOf` 改为复用 `WalCodec.segmentRangeOfKey`
>    （一种格式只能有一个解析器），并且**按段的上界判断能否删除**而不是起始序号——
>    GC 是唯一真正删数据的地方，判错时它只该少删、绝不该多删。
>    守护测试：`GcWalCoverageTest`（4 条：已 flush 的段要回收、未 flush 的段必须留、
>    普通批次与段一视同仁、未决 `.p` 对象保守保留）。

**O3. manifest 提交不校验 `writerEpoch`**
`ManifestStore.commit` 只用 eTag 做 CAS，从不比较指针里的 epoch。一个**已失去租约但进程还在跑**的旧写者，其 flush 仍可能 CAS 成功 → 把 `CURRENT` 指回自己那份陈旧 manifest（新写者的 SST 变成无人引用，随后被 GC 当真垃圾回收），并把合法写者踢下线。
**建议**：提交时比较 epoch 单调性；`flush`/`compact` 走 `ensureWriter()`。

> ✅ **已修**（`Manifest.kt`）。`ManifestStore.commit` 在 CAS 之前比较
> `existing.writerEpoch > manifest.writerEpoch` → 抛 `ConcurrentModificationException`，
> 旧写者再也无法把 `CURRENT` 指回去。快照键也改成 `manifestSnapshot(shard, seq, epoch)`：
> 带 epoch 之后不同代际的写者不会踩同一个键（P2 不可变优先）。

**O4. `UPDATE` 改主键：不判主键冲突 + 无二级索引时不写旧行墓碑**
```sql
CREATE TABLE t (id INT PRIMARY KEY, v INT);
INSERT INTO t VALUES (1,10),(2,20);
UPDATE t SET id = 2 WHERE id = 1;   -- (2,20) 被静默覆盖
```
无二级索引时，`UPDATE t2 SET id = 2 WHERE id = 1` 还会**留下旧行**（幽灵行，COUNT 也自洽）。
**建议**：UPDATE 走与 INSERT 相同的主键判重；墓碑写入不要被 `table.indexes.isNotEmpty()` 挡住。

> ✅ **已修**（`Executor.update`）。两处：
> 1. 新增 `checkUpdatePrimaryKeys`：新主键撞上**别的行**报 1062；同一批更新后的行之间
>    也判重；但"让位给本语句里其它行的旧主键"是合法的（`UPDATE t SET id = id + 1` 要能跑）。
> 2. 旧行墓碑**不再被 `table.indexes.isNotEmpty()` 挡住**：主键变了就必须写墓碑，
>    否则旧 key 永远留在 LSM 里（幽灵行，`COUNT(*)` 也自洽）。
>    守护测试：`ReviewFixesTest > UPDATE 改主键撞上已有行必须报错` / `UPDATE 改主键不留幽灵行`。

### 🟠 成本与可观测性

**O5. 只读分片每次读放大到 3 + LIST + N 次请求**
非属主 CN 的每一次 `get`/`scan`/`count` 都先 `refreshReadOnlyView()`：`head` + `get(CURRENT)` + `get(manifest)` + LIST WAL 前缀 + 对**每个** WAL 对象一次 GET（已消费的也重新下载）。一个 epoch 下最多 ~128 个 WAL 对象 → 单次点查可达 130+ 次计费请求，而预算是 **1 次**。
**建议**：按 key 里的序号跳过已消费对象（键名里就有 seq，不必先 GET 再过滤）；CURRENT 的 etag 短路。

> ✅ **已修**（`Wal.readWalPrefix` + `ManifestStore.load` + `LsmStore.refreshReadOnlyView`）。
> 三处叠加：WAL 对象**按键名里的序号跳过已消费的**（不必先 GET 再过滤）；
> `CURRENT` 指针按 etag 短路（内容没变就不取 manifest 本体）；只读视图只在
> `walFlushedThroughSeq` 前进时重建。稳态每次读 = **1 次 GET**（CURRENT）。
> 守护测试：`LsmStoreTest > 只读分片重复读不重新下载已消费的 WAL`。

**O6. 打开的 shard 永不驱逐**
`DatabaseEngine.shards` 只增不减。碰过一次的分区永久常驻（MemTable 默认 32MiB + 常驻 `.idx`），并且**永远每 7.5 秒续一次租约**（`objectstore` 模式 480 PUT/小时/分片；`ms` 模式 1440）。分区表扫一遍 128 个分区 = 永久成本。
**建议**：空闲分片淘汰（TTL / LRU）+ 指标告警。

> ✅ **已修**（`DatabaseEngine.startIdleEviction` + `YangBaseServer`）。空闲分片按
> `--shard-idle-evict-min`（默认 15 分钟）淘汰，`yb_shards_evicted_total` 计数；
> 被淘汰的分片下次访问重新打开（多几次请求，不丢数据）。

**O7. `meta/`、`index/`、`stats/`、`ops/` 不在任何 GC 覆盖内**
MS 的 Raft 日志**每条一个对象且永不删除**（快照只截断内存日志）。按 O6 的续约频率，10 分片规模约 1.7 万对象/小时永久留在桶里。
**建议**：MS 实现日志压缩后的对象回收；把这些前缀纳入 `yb-cli gc`。

> ✅ **已修**（`Gc.droppedTableObjects` / `Gc.msLogObjects` + `yb-cli gc`）。
> MS 的 Raft 日志按快照边界（`lastIncludedIndex`）回收——恢复只加载边界之后的条目，
> 边界之下的是确定的可回收物；被删表遗留的分片在 30 天墓碑保留期之后一并进入清单。

**O8. `--role ms` 进程没有 `/metrics` 与 `/healthz`**
`startMetaServiceOnly()` 提前 return，3 个 MS 节点完全不可观测：任期变化、反复重选、日志积压都看不见。

> ✅ **已修**（`YangBaseServer`）。`--role ms` 进程同样起 `/metrics` 与 `/healthz`，
> 并从 MS 节点读角色 / 任期 / 提交位点（`msHealth()`）——反复重选、日志积压现在看得见。

**O9. 后台任务静默死亡**
`LsmStore` 的租约续期协程 `catch (_: Throwable) { leaseLost = true; return@launch }`——异常对象被丢掉，无日志无指标。凭证轮换、代理抖动之后该分片不再续约，直到有人写它才收到 `LeaseLost`，而**归因线索已经断了**。
`YangBaseServer` 的 catalog 轮询同理（失败只 println，且版本号已被记账 → 不再重试）。

> ✅ **已修**（`LsmStore.startLeaseRenewal` + `YangBaseServer` catalog 轮询）。
> 续租失败不再把异常丢掉：打一行含分片与异常类型的告警，并计入
> `yb_lease_renewal_failed_total`——它与 `yb_lease_lost_total` **分开计数**，
> 因为"没问出来"和"被抢走了"的排查方向完全不同。catalog 轮询失败会打印原因
> 并在下一轮**重试**（不再因为"版本号已记账"而永久停在旧目录上）。

**O10. 重试放大**
`RetryingObjectStore`（4 次）之上各家还有 16 次重试 → 单次逻辑操作最坏 **64 次计费请求**。信号量限并发不限总量，后端抖动时请求数随并发线性放大。
**建议**：全局重试预算 / 令牌桶 / 熔断。

> ✅ **已修**（`RetryingObjectStore`）。加**令牌桶**限制重试速率
> （`retryBudgetPerSecond`，默认 50/s，启动时满载）：超预算的重试直接放弃并把错误
> 交给调用方，而不是让请求数随并发线性放大。

**O11. `DiskCache.used` 重启归零**
进程启动时计数器为 0 而磁盘上旧文件还在 → 实际占用可达上限的 ~2 倍；淘汰还会在一次 `put` 里同步做 O(文件数) 全树遍历。

> ✅ **已修**（`DiskCache`）。启动时扫描磁盘把已有文件计入 `used`
> （两级哈希子目录，**递归**——只扫顶层会得到"缓存是空的"，淘汰因此永不触发）；
> 淘汰不再在一次 `put` 里做全树遍历。

**O12. `DROP TABLE`/`DROP DATABASE` 的数据永不回收**
墓碑只被写、被读，没有任何代码删除它；而 `gc` 的遍历集合是 `allTables()`（只含活表）→ 被删表的分片**根本不在 GC 的视野里**，其 SST/WAL 永不回收。
在一个以"消解固定存储成本"为存在理由的系统里，这是最贵的失效模式。

> ✅ **已修**（`Gc.droppedTableObjects` + `yb-cli gc`）。被删表的分片根进入 GC 清单
> （保留期 30 天，与墓碑可恢复窗口一致）。在一个以"消解固定存储成本"为存在理由的
> 系统里，这是最该被堵住的一类失效。

### 🟡 语义细节

| # | 问题 | 影响 |
|---|---|---|
| O13 | `COUNT(*) … LIMIT 0` 返回 1 行（`COUNT(col)` 路径返回 0 行） | ✅ **已修**：两条路径一致（0 行）。`ReviewFixesTest > COUNT 的捷径也要遵守 LIMIT 0` |
| O14 | `truthy('0.0')` 为真（而 `'0.0' = 0` 为真） | ✅ **已修**：按 MySQL 的「前缀数值」规则，与 `=` 的判决一致。`ReviewFixesTest > 字符串在布尔上下文里按 MySQL 规则转数字` |
| O15 | `INSERT IGNORE` 不降级**值**错误（只降级键冲突） | ✅ **已修**：值错误也跳过该行并如实回报行数（本系统不替你写「最接近的值」）。`ReviewFixesTest > INSERT IGNORE 连值错误一起降级` |
| O16 | `UNION`/`DISTINCT` 的去重口径与 `=` 不一致（`'10'` 与 `10` 不去重） | ✅ **已修**：去重前按左侧分支类型归一，`UNION ALL` 刻意不归一。`ReviewFixesTest > UNION 按左侧类型归一之后再合并` |
| O17 | `CURDATE()` 渲染成 `2026-09-25 00:00:00`（列类型 DATETIME） | ✅ **已修**：日期函数的声明类型是 DATE，只发日期部分。`ReviewFixesTest > DATE 列与 CURDATE 只渲染日期部分` |
| O18 | `manifest` 快照键 `M-{seq}.mf` 会被**原地改写**（`seq` 在三处复用不增） | ✅ **已修**：键改为 `manifestSnapshot(shard, seq, epoch)`，不再有原地改写（P2） |
| O19 | `/*!40101 … */` 版本注释被整段丢弃 | ✅ **已修**：其中的语句会被执行（普通块注释仍然只是注释）。`ReviewFixesTest > 版本注释里的语句会被执行` |
| O20 | `SET TRANSACTION ISOLATION LEVEL …` 静默成功（文档说会拒绝） | ✅ **已修**：收下但如实回报降级（`@@yangbase_isolation_degraded`）。`ReviewFixesTest > 隔离级别的降级会被如实回报` |
| O21 | `S3ObjectStore` 的前置条件守卫只查 `conditionalCreate` | ✅ **已修**：`If-Match` 按 `conditionalUpdate` 单独判定，不支持时直接抛 `CapabilityNotSupportedException`，不发出去 |
| O22 | `LocalFileObjectStore.head()` 为算 ETag 把整个对象读进内存 | ✅ **已修**：改为流式计算（64 KiB 缓冲），内存与对象大小无关 |
| O23 | ~~`compose`（服务端拼接）全链路无生产调用者~~ | ✅ **已判定不该接线并从 SPI 移除**：两层 LSM 下没有"可原样保留的区间"，S3 的 5 MiB 最小片长让 KB 级对象的服务端拼接永远走不到（兜底反而多花请求），备份/恢复又都是指针操作。理由与"什么条件下加回来"见设计文档 §3.2 决策记录 |
| O24 | ~~`yb-cli cost` / `ls` 是 N+1 请求风暴（每个对象一次 `head`）~~ | ✅ **已修**：大小改从 LIST 响应本身取（0 次额外请求），后端不给大小时退化为**有上限的采样+外推**并印出"估算值"。守护测试 `YbCliTest > ls 与 cost 不逐对象发 head` |

---

## 二·五、第三轮：**真实使用者报上来的问题**（2026-09-25）

> 第一二轮都是"读代码找问题"，这一轮是**跑起来用**找出来的——起因是有人
> 用标准 `mysql` 命令行客户端连上来，`show databases` 直接
> `ERROR 2013 Lost connection`。三个缺陷叠在一起，全部通过 JDBC 测试漏掉了，
> 因为 **Connector/J 对协议细节比 libmysqlclient 宽容**。

| # | 缺陷 | 触发 | 后果 | 守护测试 |
|---|---|---|---|---|
| S1 | **应答包的序号没有接上客户端的序号** | 任何命令——命令循环把写侧序号**清零**，而协议要求应答从「客户端序号 + 1」开始 | **标准 `mysql` 命令行客户端、`mysqladmin`、`mysqldump`、Python MySQLdb 全部连不上**（libmysqlclient 严格校验序号，直接掐连接）。握手路径早就做对了，命令循环漏了——同一件事只做对了一半 | `RawProtocolTest`（新用例断言应答首包序号）+ 真实 `mysql` CLI 实测 |
| S2 | **列级 `PRIMARY KEY` 又被丢掉了**（第一轮第 1 条的**回归**） | `CREATE TABLE t (id INT PRIMARY KEY, …)`——最常见的建表写法 | 表以**空主键**建成：每行同一个 key，LSM 同 key 覆盖 ⇒ **整张表只剩最后一行**；而 `COUNT(*)` 走元数据计数**照常加 N**。扫描少行、计数多行、两边都不报错 | `PrimaryKeyDdlTest`（4 条，含"列级与表级行为一致"） |
| S3 | **无 FROM 的 `SELECT` 丢掉全部子句** | `SELECT @@version_comment LIMIT 1`（**mysql CLI 每次连接都会发**）、`SELECT 1 LIMIT 0`、`SELECT 1 WHERE 0` | 解析器在"无 FROM"时提前 return，`LIMIT`/`WHERE`/`GROUP BY`/`HAVING` **被静默忽略**（`SELECT 1 LIMIT 0` 返回 1 行）或报 1064；前者正是 CLI 连不上的第二层原因 | `SqlSurfaceTest > 无 FROM 的查询支持 LIMIT 与 WHERE` |

### 第三轮续：把 `mysqldump` 跑通时又挖出四个

> 目标是把标准导出工具跑通（它要求的东西比 `mysql` 客户端更多）。
> 四个缺陷里有**两个会静默给出错误的库/空值**，全部只在这一层才暴露。

| # | 缺陷 | 触发 | 后果 | 守护测试 |
|---|---|---|---|---|
| S4 | **`CREATE DATABASE` 会顺手切换当前库** | 任何"先建库、再建表（不带库名）"的脚本 | 建完库之后**所有的语句都落到新库**：不报错、不提示，数据进错库。真事：`MysqldumpCompatTest` 里 `t_here` 建进了 `other` | `MysqldumpCompatTest > CREATE DATABASE 不改变当前库` |
| S5 | **用户变量被接受但丢弃** | `SET @a = 42` → `SELECT @a` | 永远是 NULL。受影响的是**客户端协议**：mysqldump 用它保存/恢复会话设置，拿到 NULL 后再 `SET character_set_client = @saved_cs_client` 就报 "Unknown character set: ''" | `MysqldumpCompatTest > 用户变量能存能取且不遮蔽系统变量` |
| S6 | **`@@collation_database` 返回 NULL** | mysqldump 连接后必读 | 客户端**不加 NULL 检查**地拷进固定缓冲：`strncpy(dest, NULL, 30)` → **段错误**（core dump 实证 `rsi=0x0 rdx=0x1e`）。一个 NULL 变量换来客户端崩溃 | `MysqldumpCompatTest > 客户端会读的会话变量不能是 NULL` |
| S7 | **工具依赖的一批"必须存在"的语法/元数据** | `SHOW FULL TABLES` 少了 `Table_type` 列 · `SHOW TABLE STATUS` 未实现 · `SHOW TRIGGERS` 报错 · `ALTER TABLE … ENABLE/DISABLE KEYS` 报错 · `SET x = 裸标识符` 求值成 NULL · `:=` 词法不认识 | 导出/导入在**第一张表**就停住；`SHOW FULL TABLES` 少一列还会让 mysqldump 读到 NULL 而崩溃 | `MysqldumpCompatTest`（10 条，含**导出→灌进全新实例→逐字段对照**的往返用例） |

### 第三轮续二：`COM_FIELD_LIST`（2026-09-25，手工试用时暴露）

> 现象：交互式 `mysql` 客户端里 `use <db>` 之后 `show tables` **必掉连接**
> （`ERROR 2013` 之后甚至变成 `ERROR 2027 Malformed packet`）。
> 换成 `-A`（关掉自动补全）就一切正常——这一条线索直接指向了原因。

| # | 缺陷 | 触发 | 后果 |
|---|---|---|---|
| S8 | **`COM_FIELD_LIST` 的应答不合法**：① 协商 `CLIENT_DEPRECATE_EOF` 时**一个收尾包都不发**；② 列定义包**缺少包尾的默认值字段** | `mysql` 客户端的**自动补全**（交互式会话里对每张表发一次，`use db` 之后尤其明显） | 客户端读不到收尾包 → 超时/掐连接；补上收尾包后仍报 `Malformed packet`（列定义比真实 MySQL 少两个字节） |

**为什么 JDBC 全绿而命令行必挂**：`Connector/J` **根本不发** `COM_FIELD_LIST`
（它用 `SHOW COLUMNS` 做元数据），而这个命令是 libmysqlclient 的补全代码在用的。
两条都靠**对照真实 MySQL 8.0 的字节**定位：收尾包是 `fe 00 00 02 00 00 00`（7 字节
OK-as-EOF，与我们发的一致），列定义包尾是 `01 30`／`fb`（默认值的长度编码字符串）。
守护测试：`RawProtocolTest > COM_FIELD_LIST 必须有收尾包`。

**为什么会漏掉**：三个都躲在"测试用的那条路径"之外——
S1 只有严格客户端才校验；S2 的用例全写表级主键；S3 只有命令行工具会那么写。
所以这一轮补的三条断言都刻意写在**真实使用形态**上（真序号、真写法、真语句），
而不是再加几条单元断言。

### 第三轮续三：`information_schema` 只能"远观"（2026-09-25，使用者提问暴露）

> 现象：被问「为什么现在没有 information_schema」——它其实一直在，
> `SELECT * FROM information_schema.tables` 也一直能查，但**没有任何一条
> 真实使用路径进得去它**。这条被记为缺陷，而不是"文档没写清楚"。

| # | 缺陷 | 触发 | 后果 | 守护测试 |
|---|---|---|---|---|
| S9 | **`information_schema` 不是一个能进去的 schema**：不进 `SHOW DATABASES`；`USE information_schema` 静默"成功"但随后 `SHOW TABLES` 是空的、裸表名 `SELECT … FROM tables` 报"表不存在"；连接串里的库名不生效；写它报的是"没有选中数据库" | 真实 MySQL 的常规用法：`mysql information_schema`、`USE information_schema; SHOW TABLES`、靠"看得见的库列表"工作的客户端 | 使用者被告知"没有 information_schema"。**手敲命令的人和工具都按真实 MySQL 的用法来**，而我们的实现只认限定名 | `InformationSchemaTest > 能像 schema 一样使用 information_schema`、`… > 系统库只读且错误指向真正的原因` |
| S10 | **`USE 不存在的库` 曾经静默成功**；改成报错之后，**在多 CN 下会误报**（目录是异步传播的，"CN1 建库 → CN2 立刻 USE"落在窗口里） | 多 CN 部署：一个 CN 上做 DDL，另一个 CN 上立刻用 | 两种错法都是"指错方向"：静默成功 → 后面每条语句都报"表不存在"；严格判本地目录 → 一个**确实存在**的库报 `Unknown database`，看起来像"我刚建的库没了" | `InformationSchemaTest > USE 一个不存在的库要报错而不是静默成功` + `MultiCnCoordinationTest > CN1 新建的库，CN2 的新连接必须立刻能用` |

### 第三轮续四：**只有"重启过"才会踩到的那条路径**（2026-09-27，使用者在真实 OSS 上撞到）

> 现象：在阿里云 OSS 上用 `--coord-mode single`（**不是** `objectstore`）启动，
> 探测六项全过（`比较交换 If-Match` 那条按预期降级了），服务也起来了——
> 然后**第一条语句**就炸：`CapabilityNotSupportedException … precondition=MustMatch(etag="4448…")
> key=meta/tso/default.json`。

| # | 缺陷 | 触发 | 后果 | 守护测试 |
|---|---|---|---|---|
| S11 | **`single` 模式的时间戳水位没有接上降级开关**：它把水位持久化在对象存储上（重启后时间戳不回退），续期时无条件用 `If-Match`。`--i-accept-nonatomic-pointer-switch` 当时只覆盖 manifest 指针切换 | 桶里**已经存在** `meta/tso/<cluster>.json`——也就是**任何一次重启之后**，或桶被用过 | 服务能起来、探测全过，但**每条需要时间戳的语句都失败**。而"服务起得来"这件事本身会让人往错的方向查 | `OssCompatibilityTest > 桶里已有时戳水位时 OSS 模式下也能执行语句` + `NonAtomicTsoTest`（3 条，含"没开开关必须拒绝"与"回读发现别的写者要拒绝发号"） |
| S12 | **同一类的第二处：自增段水位**（`auto_inc/`）。它更晚才触发——本地段用完、需要**再领一段**时 | 插入量超过一个段（默认 1000） | 与 S11 同样的报错，只是来得更晚、更像"偶发" | `OssCompatibilityTest > OSS 模式下自增段可以领第二段` |

| S13 | **降级开关被当成了"部署级"开关，传到了不该生效的部署形态上**：自增段水位的降级只要"用户开了开关"就生效，而 `ms` 模式下自增领段的互斥**完全依赖**这一次 CAS（它没有走 MS）。两个 CN 会读到同一个水位、写入同一个新值、回读**也都能通过**（两边写下的内容一样），然后各自发 1001 ⇒ 重复主键、后写的行静默覆盖先写的 | OSS + 多 CN（`ms`）+ 开降级开关——而这个组合**本来就必须**开那个开关（manifest 指针要用） | 把原来的**明确失败**换成了**静默重复发号**。判据必须是"部署上确实只有一个写者"，而不是"用户开了开关" | `AutoIncCasModeTest`（3 条：ms 不许降级 · single 仍可降级 · 没开关两种模式都拒绝） |
| S14 | **CLI 的恢复开关只传到了引擎**：真正切 `manifest/CURRENT` 的是 `SnapshotManager` 内部的 `ManifestStore`，它有自己的一份开关（默认关）。于是 `restore --yes` 在 OSS 上照样报"需要条件写能力" | `yb-cli restore` / `pitr` | 开关看起来有了、命令仍然不可用；而当时的测试**没走到真正切指针那一步**（没传 `--yes`，只断言了告警文本），所以是绿的 | `YbCliTest > restore 在没有 CAS 的后端上带上开关才真的能恢复`（真备份 → 真恢复，断言"已恢复 1 个分片"） |

> S13/S14 是**这一轮修复自己带出来的**（评审指出），共同点是同一条：
> **"开关传到了"不等于"它在该生效的地方生效了"**——S14 是开关没传到真正动指针的组件，
> S13 是开关传到了**不该生效的部署形态**上。两条的守护测试都从"真的做那件事"出发
> （真领第二段、真备份真恢复），而不是断言"开关被读到了"。

**为什么 2026-09-25 的"OSS 实测全过"没抓到**——这是这一条最值得记的地方：

> 测试夹具每次启动都**清桶**，于是水位对象不存在 → 走 `If-None-Match`
> （OSS 用 `x-oss-forbid-overwrite` 支持它）→ 一路 ✓。
> **只有水位对象已经存在时**才走 `If-Match`。夹具的"干净起点"把最关键的那条路径
> 藏起来了。这与「内存后端上全绿 ≠ 没问题」是同一条教训的另一个面孔：
> **测试环境的初始化动作会替被测代码挡掉一整类状态**。

修法与 S10 同构，也是三层：
1. 三处协调写入（manifest 指针 · 时间戳水位 · 自增段水位）共用**同一个**显式开关；
2. 没有 CAS 时退化为「无前置条件 PUT + **写后回读校验**」，回读发现被别人覆盖就**拒绝**
   （宁可这条语句失败，也不发出可能重复的时间戳 / 自增 id）；
3. 判断必须在**发请求之前**做。我第一版把判据交给"`put` 会不会抛异常"兜底——
   走弱化路径时压根不发 `If-Match`，于是没有任何一层会报错，
   **开关没开也照样降级**（一个静默的、无声的失去互斥）。是"去掉修复看用例会不会红"
   这一步把它抓出来的：用例当时**照样全绿**。

**S10 的修法值得记一笔**：放宽判据（回到静默成功）与收紧判据（只看本地目录）
**都是错的**——真正的修法是 `USE` / 连接串库名 / `COM_INIT_DB` 在本地目录未命中时
**先刷新一次目录再判**（`Catalog.findDatabaseRefreshing`），并且只在**目录版本确实变了**
时才真的重载：命中 0 次请求、敲错库名仍然报错、多 CN 的窗口不再被当成"库不存在"。
新增的回归用例**刻意不加等待、不做重试**，只用连接串里的库名——去掉刷新它立刻红
（实测报 `No database selected`）。两类窗口（时间戳窗口、目录传播窗口）写进了
用户文档：`compatibility.md` §4.4.3。

---

## 三、文档一致性

> 两轮都做了。第一轮的清单在下面；第二轮补的是**本轮改掉的行为**——
> 行为变了而文档没变，本身就是这一类审查要找的东西：
>
> - **`VARCHAR(n)` 超长 / 整数溢出**：用户手册原本写"不报错、不校验"，
>   而实现早在第一轮就改成了报 1406 / 1264 → 已改成与实现一致（并注明严格模式语义）。
> - **`/*! … */` 版本注释**：手册写"整段忽略"，实现已改为**执行其中的语句**
>   （mysqldump 的条件 DDL 靠它）→ 已改正。
> - **`INSERT IGNORE` 的值错误**：手册只说"撞键跳过" → 补上"值非法也跳过，并如实回报行数"。
> - **`@@yangbase_isolation_degraded`**：新增的对外可观测项，此前没有任何文档提到它 →
>   写进 compatibility.md §4.1（含"该把它接进启动自检"的用法）。
> - **`UNION` 的类型归一**：compatibility.md 的 `UNION` 行补上"去重前按左侧分支类型归一"。
> - **空闲分片淘汰**（`--shard-idle-evict-min`，默认 15 分钟）与
>   **`gc --window-hours` 的 1 小时下限**：新增的运维行为 → 写进用户手册第十二/十四章。
> - **`ls` / `cost` 的请求成本**：README 与用户手册都写明"只花 LIST 的请求"。
> - **`compose` 不再是 SPI 的一部分**：README 的 ✅ 行删除；设计文档 §3.2 加决策记录；
>   PROJECT_GUIDE 的偏离清单加一行（含"什么条件下该把它加回来"）。

### 第一轮修掉的（清单）

审计了 `README.md` / `PROJECT_GUIDE.md` / `docs/README.md` / `docs/compatibility.md` / `docs/yangbase-design.md` / `scripts/`，修掉的主要项：

- **快速开始的命令跑不起来**：`--data-dir` 示例缺 `--i-know-this-is-not-durable`（服务端硬拒绝）。
- **README 自相矛盾**：MS 既"✅ 已实现"又"❌ 刻意排在最后"。
- **假 ✅**：`yb-cli` 说支持 9 个命令、实际只分发 5 个（已接线）。
- **模块数四种说法**：统一为 11 个（`settings.gradle.kts` 实点数）。
- **性能数字互相打架**：统一到 `compatibility.md` §4.9 的口径，并注明"请求数才是确定性的那一列"。
- **验证命令用错方式**：文档写 `YB_S3_ENDPOINT=... ./gradlew …`（环境变量），而 Gradle 守护进程不继承它 → 会**静默跑在内嵌 S3 上**得出"已在 MinIO 验证"的假结论。改成 `-P` 属性。
- **`./gradlew build` 被说成"含集成测试"**（实际不含）。
- **设计文档 §15.2 的依赖表与实现几乎整表不符**（ANTLR4 / gRPC / RocksDB JNI / jieba / JMeter… 都没引入）→ 在 PROJECT_GUIDE 里补成一张完整的"偏离清单"。
- **字符串比较语义没写进用户文档**（按二进制序，等价 `utf8mb4_bin`；而服务端上报 `utf8mb4_0900_ai_ci`）→ 新增 §4.3.1。
- 新增 [用户手册](user-guide.md)（本轮新写）。

---

## 四、结论

**59 条发现项，59 条有结论。**

- 第一轮（审查当轮）：修了 35 条，其中 4 条属于"用了就会出事"——
  列级主键丢失、`DROP INDEX` 删表、DECIMAL 行错位、TSO 重复时间戳。
- 第二轮（O1–O24）：修了 23 条，1 条判定不该接线（O23）。
  含"会静默丢数据"的 5 条（2PC 读可见性、WAL 段解码、manifest fencing、
  WAL 序号回退、`UPDATE` 改主键）与"会一直花钱"的 3 条（GC 覆盖面、空闲分片、容量盘点的 N+1）。

**修完之后，这套系统的几个关键性质现在是"有断言盯着"的**，而不只是"设计上应该如此"：

| 性质 | 守护它的断言 |
|---|---|
| 稳态点查 ≤ 1 次 GET | `LsmStoreTest > 点查稳态只应产生 1 次 GET` |
| 只读视角的重复读 ≤ 1 次 GET | `LsmStoreTest > 只读分片重复读不重新下载已消费的 WAL` |
| `COUNT(*)` 0 次 GET 且与扫描逐次相等 | `LsmStoreTest > COUNT 无谓词 - 0 次 GET` + 集成测试的属性断言 |
| 只读事务开始 0 次请求 | `IoBudgetIntegrationTest > 只读事务开始不产生任何对象存储请求` |
| 读不到半笔跨分片事务 | `ReviewFixesTest > 读不到半笔跨分片事务` |
| WAL 的每种对象形态都在 GC 视野内 | `GcWalCoverageTest`（4 条） |
| 容量盘点不逐对象 `head` | `YbCliTest > ls 与 cost 不逐对象发 head` |

**仍然没做的**（都在 [compatibility.md](compatibility.md) 与 PROJECT_GUIDE 里明确记录了）：
认证 / 权限 / TLS（用户要求先不做）· 跨 Region 只读副本（需第二个 Region 才能验证）·
存储分层（已决定短期不做）· 完整的 MySQL 方言（`GEOMETRY`、`SAVEPOINT`、多表 `UPDATE`…）·
以及设计文档 §16.3 里那份"未实现清单"的其余部分。

**一句话**：把"会静默给错答案 / 静默丢数据 / 一直悄悄花钱"这三类清完之后，
剩下的都是**明确写在文档里的能力边界**，而不是藏在代码里的陷阱。
