# YangBase 共享开发指引

本文件是 Codex、Claude Code 等 AI 编程助手在本仓库工作的统一指引。
项目指引只在本文件维护；`AGENTS.md` 与 `CLAUDE.md` 仅作为引用入口。

## 项目是什么

**YangBase** —— 一个完全构建在对象存储之上的分布式 TP 数据库，Kotlin 实现，兼容 MySQL 协议。

**为什么存在**：云厂商 ECS 在节假日有大额折扣、续费时回归原价，「ECS + 本地盘数据库」的架构因此被计费周期绑架——续费贵就得重新购机、全量搬迁、停机。对象存储单价极低、与计算完全解耦、自带多副本与强一致读；把持久状态放进去，计算层就能随时销毁重建，跟随折扣周期迁移。

**定位**：面向**低频、读多写少、需要事务与 SQL** 的应用。不试图在性能上击败本地盘数据库，而是消解「没有客户的时候仍要为数据库付一笔固定费用」这个问题。

> 📖 **动手前先读 [docs/zh/yangbase-design.md](docs/zh/yangbase-design.md)。** 它是本项目的权威设计，3007 行 / §0–§17 + 4 附录。
> 评审指引与「实现过程中仍需持续守护的 9 条」见 [docs/README.md](docs/README.md)。
>
> ✅ **所有架构决策已于 2026-09-23 确认**，见设计文档 §16.1（含九项决策记录与被否决的方案）。
> **不要重新讨论已决策的事项**；如果发现决策有问题，先读 §16.1 里当时的理由，再提出改变。

---

## 技术架构（简要）

```
 应用 / JDBC ──MySQL 协议──▶ Compute Node (CN) × N      无状态，可随时增删
                              ├ SQL 解析 / 优化 / 执行
                              ├ 事务层 (MVCC + 2PC)
                              ├ LSM 存储引擎 + 索引层
                              └ 内存 BlockCache + 本地 NVMe 缓存（可丢弃）
                                        │
                                        │ RPC（手写 JSON，见「与设计文档的偏离清单」）
                                        ▼
                             Meta Service (MS) × 3      Raft，可整体重启
                              TSO · 租约 · 锁 · Catalog
                                        │
                    ┌───────────────────┴───────────────────┐
                    ▼                                       ▼
        ┌───────────────────────────────────────────────────────┐
        │  对象存储（S3 / OSS / COS / R2 / MinIO）★唯一真相源★   │
        │  catalog/  shard/.../wal|sst|manifest  index/  stats/ │
        └───────────────────────────────────────────────────────┘
```

**四个核心机制：**

| 机制 | 做法 | 设计文档 |
|---|---|---|
| **写路径** | 每个分片任一时刻**只有一个写者**（MS 授予租约 + epoch fencing）。写入先进 MemTable，由**组提交**攒批后一次 PUT 成 WAL 对象 | §6.3 |
| **不可变 + 指针** | 对象不可原地改。一切变更 = 生成新对象 + 切换 `manifest/CURRENT` 指针 | §5、§6.5 |
| **点查 = 1 次 GET** | SST 拆成 `.idx`（小、常驻内存）与 `.dat`（大、按字节范围读）两个对象 | §6.4 |
| **事务** | MVCC 快照隔离；单分片内由单写者天然串行（**无需锁**）；跨分片用 2PC + primary record | §7 |

**索引体系**（统一抽象：一切索引 = 不可变文件 + manifest 指针）：主索引(L) · 二级有序索引(B+树语义) · 哈希索引 · 跳数索引(minmax/set/bloom/ngram) · **计数索引** · 自研段式倒排索引(FST + postings 分块 + WAND + BM25) —— 见 §8。

**⭐ `SELECT COUNT(*)` 必须做到 0 次 GET**（§8.9），这是本项目最重要的性能优化之一。三个机制叠加：manifest 上由单写者维护的精确行数 · `.idx` 的块级 `commit_ts`/`row_count`（单写者 ⇒ 文件内 `commit_ts` 沿块序单调 ⇒ 一个 `read_ts` 至多切到一个块）· 跳数索引扩展出的块级 `值→计数`。**任何把 `COUNT(*)` 计划成全表扫描的实现都是缺陷。**

---

## 不可违反的设计原则

违反以下任一条，就破坏了整个架构的前提。**改动任何代码前先自查。**

| # | 原则 | 具体含义 |
|---|---|---|
| **P1** | **对象存储是唯一真相源** | 本地磁盘与内存的一切内容必须可丢弃。任何「为了方便把权威状态放本地」的设计一律否决 |
| **P2** | **不可变优先** | 凡可能被并发读的对象一律不可变；变更通过新对象 + 指针切换表达 |
| **P3** | **元数据小而热，数据大而冷** | 元数据对象（manifest、`.idx`、统计）要小到能整体常驻内存；数据只按需范围读 |
| **P4** | **请求数是一等成本** | 任何功能都要能回答「它引入了多少额外对象存储请求」。预算表见 §14.2 |
| **P5** | **无状态计算** | CN 不持有任何不可重建的状态。写者身份是租约，不是节点属性 |
| **P6** | **显式降级** | 后端能力差异（是否支持条件写、强一致）靠运行时探测 + 显式降级，不假设最低公分母 |

**常见的会破坏设计的错误**（写代码时警惕）：

- ❌ **在热路径上调用 `LIST`** —— 它是分页的、有请求成本的慢操作。恢复路径用 `start-after` 增量列举 + 缓存游标
- ❌ **把 `.idx` 与 `.dat` 合并成一个对象** —— 会让每次点查下载整个 8–64 MB 对象，摧毁性能前提
- ❌ **假设对象存储支持条件写** —— 必须查 `ObjectStoreCapabilities`。GCS 的 S3 互操作层会**静默忽略**条件头
- ❌ **在对象名里省掉零填充** —— LIST 按字节字典序，`10.log` 会排在 `9.log` 前，导致恢复时日志乱序
- ❌ **用虚拟线程包装 RocksDB/Speedb 的 JNI 调用** —— JNI 会钉住载体线程，虚拟线程无法卸载。用固定平台线程池（当前实现没有 JNI，但同一条理由适用于阻塞式的 S3 客户端——所以它跑在有界平台线程池上，见「编码约定」）
- ❌ **让读者读到「比它 `read_ts` 更新」的数据** —— 详见 §6.5 的 manifest 一致性推理链
- ❌ **把 L1 及以下改成 tiered/universal 布局** —— leveled 不是性能偏好，是 §8.9.3 计数去重的**语义前提**。改成 tiered 会让 `SELECT COUNT(*)` **静默返回错误数字**（不报错、不降级），而用户拿它做分页和对账
- ❌ **让 `COUNT(*)` 走扫描路径** —— 元数据计数代价是 0 GET，永远优于扫描。`EXPLAIN` 应显示 `type: METADATA_COUNT`
- ❌ **拿未做类型转换的字面量去和分区边界比较** —— 边界是按**列类型**求值的（`'2026-04-01'` → Ts），而 WHERE 里的字面量词法上只是字符串。不转换就走跨类型比较 → 永不相等 → **整片分区被错误裁掉、行静默消失**。裁剪裁错比不裁剪严重得多
- ❌ **为同一种字面量写两个解析器** —— 插入用 `systemDefault`、分区边界用 UTC，同一个字符串会解出两个不同时刻，`WHERE dt = '...'` 因此**静默返回空集**。全系统只能有一个时间字面量解析器
- ❌ **在事务缓冲期内维护全文索引** —— 未提交的行会立刻可被检索到。那是脏读，而且在模糊匹配场景下多出几行极难被发现。索引必须等 COMMIT、拿到真正的 commitTs
- ❌ **让未提交的数据进 MemTable** —— `flush()` 会把整个 MemTable 写成 SST，于是未提交的数据变成了持久数据。凡是「先应用、后决定」的写入路径都必须先想清楚崩溃时它会不会被刷下去
- ❌ **在 `stop()` 之后还占着 `autoCommit=false` 的连接** —— 关闭连接时驱动会去 rollback 一个已经停掉的服务。测试里重启引擎前先把连接关掉或恢复 `autoCommit`
- ❌ **按对象键缓存内容会变的对象** —— `manifest/CURRENT` 这类键的内容会变，按 key 缓存它会让下次读到陈旧内容。二级缓存只在调用方显式声明 `immutableKey = true` 时才写入
- ❌ **让缓存的失败变成查询的失败** —— 缓存目录只读、磁盘满、块损坏，都必须降级为「未命中」，回对象存储重取。**一个缓存不该有能力让系统不可用**。代价是这类故障会静默失效，所以读路径要有命中率/请求数断言盯着
- ❌ **把索引建好了却不接进读写路径** —— 构件齐、单元测试绿、却没有一行生产代码调用它，是比「没实现」更糟的状态：它有测试、有文档、看起来完成了，却对系统行为零影响。**新索引的第一条集成断言应当是「它真的被写了、真的被用了」**
- ❌ **在错误的粒度上测剪枝** —— 数据按列递增时，一个 64KB 块装得下近千行；查一个落在**首个块内部**的窄区间，没有任何完整块可被排除，剪枝为 0 是**正确行为**。剪枝测试必须让区间跨越块边界（通常取数据末尾的窄段）
- ❌ **只扫顶层目录就宣布「缓存是空的」** —— 磁盘缓存按哈希分两级子目录存放，`Files.list` 只看一层会得到「一个文件都没有」，淘汰因此永不触发，缓存一路涨到磁盘满。这是一类**不报错只失效**的错误

---

## 模块结构

> **实际是 11 个模块**，而不是设计文档 §15.3 列的 23 个——后者是目标态，当前为降低构建
> 复杂度做了合并（`yb-storage` 吸收 catalog/meta/coordination，`yb-index` 吸收四类索引，
> `yb-sql` 吸收 parser/planner/exec）。模块边界仍按职责划分，只是不再一一对应独立 artifact。
> 权威清单是 [settings.gradle.kts](settings.gradle.kts) 的 `include(...)`。

```
yang-base/
├── yb-common/         # 类型、保序字节编码、行/值编解码、schema
├── yb-objectstore/    # ObjectStore SPI + 3 种后端（内存/本地FS/S3）
│                      #   + EmbeddedS3Server（内嵌 S3 测试服务器）
│                      #   + 3 个装饰器：Counting / Instrumented / Retrying
├── yb-storage/        # ★ LSM：MemTable、WAL（组提交）、SST、Manifest、缓存、计数、compaction
├── yb-txn/            # TSO、Catalog、DatabaseEngine、分区路由与裁剪、
│                      #   TransactionBuffer（事务缓冲）、Partitioning、GC 与时间旅行、
│                      #   ★ CoordinationService（协调能力的唯一接缝）+ 三种实现
│                      #   （SingleProcessCoordination / ObjectStoreCoordination /
│                      #    MetaServiceClient，对应 --coord-mode single/objectstore/ms）
├── yb-ms/             # ★ 元数据服务：Raft（日志/快照落对象存储）+ 状态机（水位/
│                      #   租约/catalog 版本）+ 行锁表 + CN 侧客户端 + RPC 传输
├── yb-index/          # 跳数索引（minmax/set/bloom/ngram）+ 段式倒排索引（FST/postings/WAND/BM25）
├── yb-sql/            # 词法/语法分析、执行器、information_schema 虚拟表
├── yb-mysql/          # MySQL 协议服务端
├── yb-server/         # CN 主程序 + 端到端测试
├── yb-cli/            # 运维命令行（ls / shards / gc / recover-check / cost /
│                      #   backup / backups / restore / pitr / cdc / analyze-sql）
└── yb-bench/          # 基准测试
```

★ = 技术风险与工作量最集中处，接口设计需先评审。

---

## 构建与测试

```bash
./gradlew build                        # 编译 + 全部检查
./gradlew test                         # 单元测试
./gradlew integrationTest              # 集成测试（内嵌 S3 服务器，无需 Docker）
./gradlew :yb-server:koverVerify       # 覆盖率门禁（全项目聚合，≥50% 分支）
./gradlew :yb-cli:test                 # 运维 CLI 测试
./gradlew :yb-cli:cli -Pargs="ls --fs /path"   # 跑一条 CLI 命令
./gradlew :yb-bench:bench              # 基准测试
./gradlew :yb-bench:benchIndexes       # 全部索引的 SQL + IO 性能门禁（18 场景）
./gradlew check                        # 静态检查 + 单元测试（**不含** integrationTest）
./gradlew build integrationTest        # 提交前跑这个
```

> **覆盖率为什么在 `yb-server` 上判定**：端到端测试挂在 yb-server，但它真正跑的是
> yb-sql / yb-mysql / yb-txn / yb-storage 的代码。逐模块统计会把它们记成 0%
> ——数字很低，代码却是被覆盖的。yb-server 配置了 `kover(project(...))` 聚合这些模块。

### 测试要求（项目硬性规定）

| 类型 | 位置 | 要求 |
|---|---|---|
| **单元测试** | `src/test/kotlin` | 各模块自带，`./gradlew test` |
| **集成测试** | `src/integrationTest/kotlin`（**独立源集**） | `./gradlew integrationTest`；用**内嵌 S3 服务器**，无需 Docker |
| **覆盖率** | 全项目聚合 | **分支覆盖率 ≥ 50%**，`:yb-server:koverVerify` 强制门禁 |

> 集成测试是独立源集，不是靠 tag 从 `src/test` 里筛。理由：它要真的连对象存储，
> 共用一套类路径会让「误把集成用例当单元测试跑」变成一个随时可能发生的意外，
> 代价却是 CI 忽然需要一个外部服务。**物理隔离比 tag 过滤可靠。**
>
> ⚠️ **代价是 `check` / `build` 都不包含 `integrationTest`**（Gradle 只会把任务接进
> 生命周期，不会自动接一个自己注册的源集）。提交前请跑
> `./gradlew build integrationTest`，或用 `./scripts/minio.sh test` 把它指向真实 MinIO。

**集成测试必须覆盖的路径**（对应设计文档的验收标准）：

| 用例 | 断言要点 | 文档 |
|---|---|---|
| 点查（稳态 / 冷缓存） | **稳态 ≤ 1 次 GET**；冷 ≤ 4 次 | §14.2 |
| 范围扫描 | 并行预取生效；吞吐符合预期 | §10.2 |
| 小事务提交 | 组提交后 ≤ 1 PUT（摊薄）；提交延迟符合档位 | §6.3.4 |
| 只读事务开始 | **0 次对象存储请求**（TSO 预取） | §4.2 |
| **崩溃恢复** | 随机时点 kill CN，恢复后无丢数据、无幻读 | §6.8 |
| **并发：长读事务 vs GC vs compaction** | 读事务不丢行、不报错 | §6.5 |
| Fencing | 旧写者的写入被忽略 | §4.3 |
| 二级索引 / 分区裁剪 | 结果正确 + **读的字节数减少** | §8、§9.2 |
| 全文检索 | 建索引 / 写入维护 / 更新淘汰旧词 / 中文分词 | §8.6 |
| 跳数索引 | flush 时写出 `.skp`；块级剪枝**读的字节数显著减少**；**结果与全表扫描逐条一致**（属性测试） | §8.5 |
| **跨分片 2PC** | 提交标记写失败时**整笔事务原子消失**（不能留下半份） | §7.3 |
| 时间旅行 / GC 水位 | 能读历史；越水位明确报错 | §4.2、§6.9 |
| **`COUNT(*)` 与谓词计数** | **0 GET**；且**元数据计数与全表扫描的结果逐次相等**（属性测试，覆盖 R11 的静默错误风险） | §8.9 |
| MySQL 协议（JDBC 端到端） | 建表-插入-查询-事务 | §11 |
| **多 CN + MS（2 个 CN + 3 个走 TCP 的 MS）** | 跨 CN 互相可见（窗口模型下 ≤ 一个窗口）· **行锁挡住另一个进程里的事务** · 分片接管后 epoch 被超越且前任数据不丢 · MS 全挂时读继续、写明确失败 | §4.3、§4.4、§4.7 |

### 测试基建

- **单元测试**：JUnit 5 + `kotlin.test` 断言
- **集成测试**：`EmbeddedS3Server`（走真实 HTTP、实现真实条件写语义）——**不需要 Docker**
- **`CountingObjectStore`**（`yb-objectstore`）—— **本项目最重要的测试夹具**。它装饰真实 `ObjectStore` 并计数请求数与字节数，把 §14.2 的 IO 预算变成可执行的断言：

```kotlin
@Test fun `点查稳态只应产生 1 次 GET`() = runTest {
    val counting = CountingObjectStore(minioStore)
    val engine = openEngine(counting)
    engine.pointGet(pk = 42)      // 预热，填充缓存
    counting.reset()
    engine.pointGet(pk = 43)      // 测量
    assertEquals(1, counting.count("get"))   // ★ 这条断言守护整个存储层设计
}
```

> **比测延迟更可靠的是测请求数**——延迟受环境影响大，请求数是确定性的。新增任何读路径功能时，先写它的请求数断言。

> ⚠️ **另一条同样重要的纪律：内存后端上全绿 ≠ 没问题。** 共识与协调的失效全部长在
> **时序**上（落盘要花几毫秒、RPC 会重叠、心跳与请求驱动的复制会并发），而 `MemoryObjectStore`
> 的 PUT 是纳秒级的——那些交错在那里几乎不会发生。所以：
>
> - 涉及 Raft / 租约 / 时间戳的改动，**必须**跑 `:yb-ms:integrationTest`
>   （`S3BackedMsTest`：MS 的日志落在内嵌 S3 上，走真实 HTTP）与
>   `:yb-bench:benchCoordination`（它同时打印**每次操作的对象存储请求数**）。
> - "请求数比预期多好几倍"是活性问题最可靠的信号（例如租约 acquire+release 花了
>   20 次请求而该有的只有 6 次 —— 那是集群在反复重新选举）。
> - 需要让交错变得常见时，用一个**只给 `put` 加延迟**的装饰器
>   （见 `RaftConcurrencyStressTest.SlowPutStore`），而不是模拟故障：
>   把"落盘要几毫秒"如实搬到内存里就够了。

### 版本状态（2026-09）

**已实现并测试通过**：ObjectStore SPI 与 **3 种后端**（内存 / 本地 FS / S3）
+ 内嵌 S3 **测试服务器**（`EmbeddedS3Server`）+ **3 个装饰器**（计数 / 重试 / 埋点）
（Counting / Instrumented / Retrying / AuditLog）·
LSM 引擎（WAL 组提交 · SST 的 `.idx`/`.dat` 分离 · Manifest · 崩溃恢复 · compaction ·
SST 压缩）· MVCC 快照隔离 · COUNT 的三个元数据机制 · 四种分区（NONE/RANGE/LIST/HASH）·
MySQL 协议 · SQL 子集 · TSO 与 Catalog · 计算节点无状态 · **分片写者租约与 fencing**
（§4.3，含 epoch 单调、续期、`LeaseLost`）· **跨分片 2PC**（§7.3，未决 WAL + 提交标记）·
**二级索引**（有序 / 哈希 / 唯一 / 复合，含写路径维护、NULL 不参与唯一性、
原子判重）· **跳数索引**（minmax/set/bloom/ngram，块级剪枝）· **自研倒排索引与
全文检索**（§8.6：FST + postings 分块 + WAND + BM25，`WITH PARSER ngram`）·
**GC 与时间旅行**（GC 水位 · `AS OF TIMESTAMP` · PITR · 备份恢复 ·
`DROP TABLE` 墓碑）· **ALTER TABLE**（加列 / 删末尾列 / 加删索引 / 改名 / TRUNCATE，
无重写）· **`SHOW CREATE TABLE` / `SHOW INDEX` / `information_schema`**
（系统库能像真 schema 一样进去：`SHOW DATABASES` 列出它 · `USE information_schema`
之后 `SHOW TABLES` 与裸表名查询都可用 · 写它报"只读的系统库"）·
**CDC 变更流**（读现有 WAL，无第二份日志）· **自增分段分配**（§11.4）·
**在线索引构建**（§8.7：BUILDING → 快照回填 → 增量追平 → ACTIVE）·
**可观测性**（§12.5：`/metrics` + `/healthz`、慢查询日志、对象存储审计日志；
MS 进程（`--role ms`）同样有这两个端点）·
**对象存储后端**（§3.3：内存 / 本地 FS / S3 · 内嵌 S3 测试服务器；
**阿里云 OSS** 走 `--s3-oss`（`x-oss-forbid-overwrite` 原子创建 + 三级域名寻址 +
无 CAS 时显式降级的指针切换），2026-09-25 在真实 OSS 上实测通过）·
**运维与成本治理**（2026-09-25 审查后补齐：空闲分片淘汰 `--shard-idle-evict-min` ·
GC 覆盖 MS 日志与已删表遗留 · `ls`/`cost` 只花 LIST 的请求、不逐对象 `head` ·
重试令牌桶 · `yb_lease_renewal_failed_total` 区分"没问出来"与"被抢走"）·
**SQL 能力面**（聚合 `SUM/AVG/MIN/MAX/GROUP_CONCAT` + `GROUP BY`/`HAVING` + 强制
`ONLY_FULL_GROUP_BY` · `JOIN`（内/左/右/交叉，等值走哈希）· `UNION`（含并集级
`ORDER BY`/`LIMIT`）· `SELECT DISTINCT` · **窗口函数**（排名 / 取值 / 聚合开窗，
`PARTITION BY` + `ROWS`/`RANGE` 帧）· 子查询（非相关 + **谓词位置**改写成半/反连接 +
**标量位置**按关联键建表查值）· `JSON` 函数族（`JSON_EXTRACT` / `->` / `->>` /
`JSON_KEYS` / `JSON_VALID` / `JSON_TYPE` / `JSON_LENGTH` / `JSON_UNQUOTE`）·
**只读视图**（`CREATE [OR REPLACE] VIEW` / `DROP VIEW` / `SHOW CREATE VIEW`）·
`ORDER BY` 序号 · 三值逻辑与 `<=>`）·
**L1 适配层**（§2.2：重试/退避/限流，分类放在后端；本地 FS 的生产守卫）·
**元数据服务 MS**（§4：Raft + 状态机 + 行锁表 + CN 客户端，`--role ms` 独立部署，
也可 `--run-meta-service` 与 CN 同进程）· **`SELECT ... FOR UPDATE` / `LOCK IN SHARE MODE`**
（行锁走协调服务，autocommit 下随语句结束释放）· **双模式协调**
（§4.7：`single` / `objectstore` / `ms` 三种实现共用**同一套一致性测试**）· `yb-cli`。

> 📊 **协调模式的量化对比：`./gradlew :yb-bench:benchCoordination`**（三种模式的延迟 +
> **每次操作的对象存储请求数**）。结论：MS 不让常规查询变快——点查与提交三种模式完全
> 一样（协调不在热路径上，时间戳整窗口预取）；它让**协调操作**不再走对象存储：
> `FOR UPDATE` 在 `objectstore` 模式下是 **6 次请求 / 98ms**，在 `ms` 下是 **0 次请求 / 37µs**。
> 反过来 MS 的**租约**（11ms / 6 请求）比 `objectstore`（49ms / 4 请求）快，但比
> `single`（5µs / 0 请求）慢三个数量级——因为租约要进共识日志 = 真实 PUT。
> 所以**选型看负载形状**：行锁密集选 `ms`，几乎不用 `FOR UPDATE` 又想要零常驻成本选 `objectstore`。
> 数字与解读同步维护在 [docs/zh/compatibility.md](docs/zh/compatibility.md) §4.4.2（**以那一份为准**）。
>
> ⚠️ **MS 的选举超时默认 500ms–1s，比内存共识的经验值大一个量级**：它的日志落在对象
> 存储上，一次追加就是一次 PUT。超时只有 150ms 时，一次慢复制就足以让跟随者发起选举，
> 集群会在负载下反复换领导者——不报错，只是每次换都丢掉锁表（§4.4）。

**未实现**（见设计文档 §16.3 的完整清单）：`UNION` 分支内联的 `ORDER BY`、`LIMIT`（§11.3）·
`GEOMETRY` 与空间函数（空间索引不在 §8 的设计范围内）· `RANGE n PRECEDING` 帧与
`GROUPS` 帧 · 藏在 `OR` 分支里的相关子查询 · 视图参与 `JOIN` · `GROUP_CONCAT` 开窗 ·
认证 / 权限 / TLS（用户明确要求先不做）· 跨 Region 只读副本 ·
存储分层（§13.3，**已决定短期不做**；设计文档明确警告归档层是成本陷阱）·
~~阿里云 OSS 上的写入~~ —— ✅ **2026-09-25 已支持并实测**（`--s3-oss
--s3-path-style false --i-accept-nonatomic-pointer-switch`：原子创建走
`x-oss-forbid-overwrite`，指针切换在无 CAS 时显式降级 + 接管时审计孤儿快照）。
真实桶上通过：单 CN（`S3IntegrationTest` 8/8、`IoBudgetIntegrationTest` 15/15）·
**多 CN + 3 节点 MS**（`MultiCnCoordinationTest` 4/4，MS 的 Raft 日志也在 OSS 上）·
`S3BackedMsTest` 1/1。**限制**：OSS 上不能用 `objectstore` 协调模式
（租约/行锁依赖 ETag CAS），请用 `single` 或 `ms`。详见设计文档 §3.3。

> 🔍 **一轮完整的代码审查（59 条发现项）与它的回归清单在
> [docs/zh/code-review-2026-09.md](docs/zh/code-review-2026-09.md)**：每条都写了
> 「当初为什么会发生」与「哪条测试盯着它」。改到相关代码时，请连同那条测试一起看。

> 📄 **面向使用者的兼容性与限制清单在 [docs/zh/compatibility.md](docs/zh/compatibility.md)**
> （§11.3 要求「必须写进用户文档」的那一份）。改协议、改类型、改语义时同步更新它：
> 「协议兼容」不等于「语义兼容」，而使用者只会读到那份文档。

> ⚠️ **没有 MS 时有一个绕不开的假设：各 CN 时钟大致同步。**
> `commit_ts` 的高位取自本机物理毫秒（§4.2 要求 `commit_ts` 实时分配，
> 不可预取，否则时间戳集合在可见性判断上不再稠密）。有了 MS 就由它统一发号，
> 这条假设随之消失。

### 默认测试不使用真实云对象存储

集成测试与默认基准使用内嵌 S3。需要验证后端差异（条件写、强一致）时，
优先在 `EmbeddedS3Server` 上构造对应场景。

用户明确要求真实后端验收时，可运行独立的 `:yb-bench:benchIndexes`，
使用随机前缀隔离和按前缀清理，禁止清空整个桶；密钥仅通过运行环境传入。
完整覆盖矩阵、IO 门禁、MinIO/OSS 入口见 [docs/zh/index-bench.md](docs/zh/index-bench.md)。

---

## 编码约定

- **手工验证材料仅保留本地，不提交 Git**：包括手工 SQL、对应执行/验证代码、结果与报告。正式代码和文档不要依赖这些未入库文件。

- **当前阶段无需兼容存量数据格式**（2026-09-27 用户确认：全新项目）。调整持久化结构时直接使用新格式，不添加旧格式探测、迁移或兼容分支。

- **Kotlin 2.x / JDK 21+**，用协程处理对象存储 IO
- **并发模型分层**：协程（对象存储 IO、SQL 执行）· Netty EventLoop（协议）· **固定平台线程池 + 信号量**（阻塞式调用层：`S3ObjectStore` 的固定 IO 调度器、`RetryingObjectStore` 的并发上限；**不要用虚拟线程包装它们**）
- **序列化分层**：元数据用 kotlinx-serialization（可读性优先）；SST/WAL 热路径**手写二进制**（要精确控制格式）
- **热路径零分配**：读路径避免装箱、避免 `ByteArray` 拷贝；用 `value class` 表示 ID
- **所有对象键通过 `ObjectKeys` 生成**，禁止手写字符串拼接（零填充是正确性要求，不是风格问题）
- **协程中禁止阻塞调用**（除 JNI 层，且必须在该层用 `Dispatchers.IO` 隔离并限流）
- 公开 API 写 KDoc；内部实现按需注释，不写「这段代码做了什么」式的废话

### 与设计文档的偏离清单（§15 技术栈）

设计文档 §15 的技术栈表是**目标态**，实现时有若干处偏离。**逐条列在这里**，
读 §15.2 时请对照本表——下面每一行的「实际用的」都能在源码里指到具体文件。
（与 §15 无关的接口级偏离也一并列在表末，例如 §3.2 的 `ObjectStore` 方法数。）

| 项 | 设计文档写的（§15） | 实际用的 | 理由 |
|---|---|---|---|
| CN ↔ MS 的 RPC | gRPC + Protobuf（`yb-proto` 模块） | **手写的长度前缀 JSON**（`yb-ms/MetaRpc.kt`，传输层只有 `MetaTransport` 一个接口） | 协议面很窄（`MetaRequest` 9 个请求类型 + Raft 传输一条）；本仓库一贯手写协议（MySQL 线协议、SST 格式、SQL 解析器），引入 protoc 会给构建加一份与源码平行的生成物。**代价是不与 gRPC 客户端互通**——MS 是纯内部服务，这个代价目前为零；要开放协议时替换边界就是那一个接口 |
| SQL 解析 | ANTLR4 文法 + AST（`yb-sql-parser`） | **手写的词法 + 递归下降**（`yb-sql/Lexer.kt`、`Parser.kt`） | 支持的语法是一个**明确的子集**；手写没有构建期代码生成、错误信息更可控。`Parser.kt` 的注释里写着：日后要覆盖完整 MySQL 方言，再换 ANTLR |
| 对象存储客户端 | AWS SDK v2 的**异步**客户端 + Transfer Manager | AWS SDK v2 的**同步** `S3Client` + `UrlConnectionHttpClient`（`S3ObjectStore.kt`），multipart 手写 | 异步化已经由协程层承担（IO 跑在一个有界平台线程池上），同步客户端的错误语义更直白、依赖面更小 |
| 本地磁盘缓存 | RocksDB JNI / Speedb，只用它的 KV 能力（§6.7） | **纯 Kotlin 的 `DiskCache`**（两级哈希目录 + 整文件读写） | 不引 JNI 就没有「JNI 钉住载体线程」那一整类约束，构建也不需要 native 产物；缓存本身可丢弃（P1），用不到 KV 引擎的能力 |
| 内存缓存 | Caffeine（按权重淘汰，支持异步加载） | **自研的 `BlockCache`**（按字节数的 LRU + **在途请求去重**） | 缓存对象全部不可变 ⇒ 永不过期、只按容量淘汰，用不上 Caffeine 的权重/过期体系；在途去重（§6.7.1）本身是业务语义，库不提供 |
| 序列化 | 元数据 kotlinx-serialization + 热路径**手写二进制** | **JSON（元数据）+ CBOR（`.idx`、跳数索引、段元数据）+ 手写二进制（SST 数据块 / WAL）** | CBOR 用在「要被解析但不在最内层热路径」的那几类元数据上：比 JSON 小、比手写省事 |
| 位图 | RoaringBitmap | 手写的位图/集合运算（倒排索引的 postings 与删除位图） | 只需要集合运算的一个子集（§8.6） |
| 哈希 | xxHash64（库） | **手写的 `XxHash64`**（`yb-storage/Bloom.kt` 里的 `object XxHash64`） | 实现短、速度快、雪崩性好；bloom 要的只是一个稳定的 64 位哈希，不必为它引一个库 |
| 压缩 | LZ4-java（默认）+ zstd-jni（高压缩档） | **只有 LZ4**（`org.lz4:lz4-java:1.8.0`） | 高压缩档目前没有真实需求，SST 压缩已由 LZ4 覆盖 |
| 中文分词 | jieba-analysis 或 HanLP | **只有 `standard` 与 `ngram` 两种分析器**（`yb-index/.../fulltext/Analyzer.kt`） | 不为分词引入外部依赖与词典体积；`Analyzer` 是可插拔接口，中文检索走 §8.6.4 的 `ngram` 兜底路径，替换时不需要动索引结构 |
| 指标 | Micrometer + OpenTelemetry（§12.5） | **自研的指标原语 + Prometheus 文本导出**（`yb-common/.../metrics/Metrics.kt` + `yb-server/MetricsHttpServer.kt`） | 要的只是「几个线程安全的计数 + 按 Prometheus 文本格式导出」，不到 200 行；引框架会顺带拖进注册表抽象、标签层与导出器插件体系 |
| 测试 | JUnit 5 + Kotest（断言/属性测试）+ Testcontainers | **JUnit 5（`kotlin-test`）+ `kotlinx-coroutines-test`**；对象存储用 `EmbeddedS3Server` | 属性测试的需求直接用「与全表扫描逐条对比」的普通断言表达更清楚；内嵌 S3 走真实 HTTP 与真实条件写，比 Testcontainers 起 MinIO 更快，且不需要 Docker |
| 基准 | JMH | **`main` 函数手写计时**（`yb-bench`，`:yb-bench:bench` / `:yb-bench:benchCoordination`） | 避免 JMH 的注解处理器与构建开销；这里要回答的是「请求数」与「数量级」，不是纳秒级微基准 |
| 模块划分 | §15.3 的 23 个模块 | **11 个**（见上文「模块结构」） | 降低构建复杂度，模块边界仍按职责划分，只是不再一一对应独立 artifact |
| `ObjectStore` 的方法数 | §3.2 的 6 个（含 `compose` 服务端拼接） | **5 个**（`get`/`put`/`delete`/`list`/`head`） | 原型实现过 `compose`（S3 multipart copy + 自愈降级 + 能力探测），审查后移除：两层 LSM 下不存在"可原样保留的区间"，S3 的 5 MiB 最小片长让 KB 级对象根本走不到服务端路径，兜底反而多花请求。**完整理由与重新引入的条件见设计文档 §3.2 的决策记录** |

> **一句话总结**：偏离集中在「**少一份生成物 / 少一个依赖**」这一个方向上——
> 手写协议、手写解析器、手写哈希与指标、同步客户端。设计文档 §15.1 的两条硬约束
> （对象存储 IO 用协程、阻塞调用用固定平台线程池而不是虚拟线程）**一条都没有让步**。

---

## 里程碑

**M0 已完成**（验收：JDBC 建表-插入-点查 + 稳态点查 ≤ 1 GET），并已推进到
M1–M3 的核心部分（见上方「版本状态」）。

**下一步的优先级**（2026-09-24 用户指定的顺序）：

1. ~~**SQL 的剩余部分**~~ —— ✅ 已完成：窗口函数、标量位置的相关子查询、
   `JSON` 函数族、`SELECT DISTINCT`（含 3065 检查）、`FOR UPDATE` 行锁
   （随 MS 一起交付，§7.3）。剩余：`GEOMETRY`（不在设计范围内）
2. ~~**元数据服务 MS（§4）**~~ —— ✅ 已完成（`yb-ms`：Raft + TSO/租约/行锁/catalog
   串行化 + CN 客户端；`--coord-mode ms` 启用，`--role ms` 独立部署）。
   下一步的候选：内置函数面（`CASE WHEN` / `IF` / `SUBSTRING` / `ROUND` / 日期函数——
   目前都明确报错，但实际写查询会立刻撞上）· 只读副本之外的多 CN 互操作测试
   （两个 CN 共享一个对象存储时的租约与 fencing 端到端）

> **跨 Region 只读副本：暂缓，暂不规划**（2026-09-24 用户的决定）。
> 设计文档 §12.2 把它列为 Region 级容灾手段（异步、RPO > 0），
> 但它需要第二个 Region 才能真正验证；在单 Region 部署下它不是瓶颈。

> **存储分层（§13.3）已决定短期不做**（2026-09-24）：它省的是存储单价，而本系统的
> 成本主线是「计算与存储解耦」，那条线已经实现了；真正该做的冷却是 `DROP PARTITION`
> （已实现，成本 ≈ 0）。理由与"如果以后要做该守住什么边界"见
> [docs/zh/compatibility.md](docs/zh/compatibility.md)。

完整里程碑见设计文档 §17。
