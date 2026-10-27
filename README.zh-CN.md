# YangBase

[English](README.md) | [简体中文](README.zh-CN.md)

[![CI](https://github.com/clouddea/yang-base/actions/workflows/ci.yml/badge.svg)](https://github.com/clouddea/yang-base/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)
[![JDK](https://img.shields.io/badge/JDK-21-orange.svg)](https://adoptium.net/)
[![Kotlin](https://img.shields.io/badge/Kotlin-2.1-7F52FF.svg)](https://kotlinlang.org/)

**一个完全构建在对象存储之上的分布式 TP 数据库** —— Kotlin 实现，兼容 MySQL 协议。

> 📖 完整设计见 [docs/zh/yangbase-design.md](docs/zh/yangbase-design.md)（3007 行 / §0–§17 + 4 附录）。
> 实现指引与项目约定见 [CLAUDE.md](CLAUDE.md)。

---

## 它解决什么问题

云厂商 ECS 在节假日有大额折扣、续费时回归原价。「ECS + 本地盘数据库」的架构因此被**计费周期绑架**：续费贵就得重新购机、全量搬迁、停机。

对象存储单价极低、与计算完全解耦、自带多副本与强一致读。把持久状态放进去，计算层就能随时销毁重建。

**代价是对象存储的三条物理约束**，整个设计都围绕它们展开：

| 约束 | 应对 |
|---|---|
| 对象级接口，不能原地更新 | 不可变对象 + manifest 指针切换 |
| RTT 高（同 Region 约 10–60ms，尾部数百 ms） | 组提交摊薄、多级缓存、元数据常驻内存 |
| 按请求计费 | **请求数是一等性能指标**，不只是延迟 |

---

## 当前状态

**已实现并测试通过的部分**（M0 + M1/M2/M3 的核心）：

| 能力 | 状态 | 验证方式 |
|---|---|---|
| `ObjectStore` SPI + 能力探测 | ✅ | 5 个操作（`get`/`put`/`delete`/`list`/`head`）+ 3 种后端（内存 / 本地 FS / S3）+ 内嵌 S3 测试服务器 + 3 个装饰器（计数 / 重试 / 埋点） |
| S3 后端（条件写、Range、分页 LIST） | ✅ | 内嵌 S3 服务器 + **真实 MinIO** 的集成测试 |
| **阿里云 OSS 后端**（`--s3-oss`） | ✅ | 走 OSS 自己的原语：原子创建用 `x-oss-forbid-overwrite`（409）、三级域名寻址；**没有 ETag CAS**，指针切换需 `--i-accept-nonatomic-pointer-switch` 显式降级。**2026-09 在真实 OSS 上实测**：单 CN（`S3IntegrationTest` 8/8、`IoBudgetIntegrationTest` 15/15）与**多 CN + 3 节点 MS**（`MultiCnCoordinationTest` 4/4，MS 的 Raft 日志同样落在 OSS）。⚠️ OSS 上不能用 `objectstore` 协调模式 |
| LSM 引擎（MemTable / WAL / SST / Manifest / 恢复） | ✅ | 单元测试 + 崩溃恢复测试 |
| **SST 的 `.idx`/`.dat` 分离** | ✅ | 点查请求数断言（稳态 0.004 次/查询） |
| **SST 数据块压缩** | ✅ | 2 万行实测 `.dat` 减少 56.6%；`.idx` 刻意不压缩（要常驻内存） |
| 组提交 WAL | ✅ | 提交延迟与 PUT 摊薄（16 并发写者 14.5 事务/PUT） |
| MVCC 快照隔离 | ✅ | 跨连接可见性测试、长读事务 vs GC |
| **COUNT 优化（0 次数据 GET）** | ✅ | 集成测试断言 `get_range == 0`；比全表扫描快 300× |
| 分区（`HASH` / `KEY` / **`RANGE` / `LIST`**） | ✅ | 按值路由、越界拒绝、DDL 校验 |
| **分区裁剪 + 复合主键键范围下推** | ✅ | 区间外查询断言 **0 次 GET** |
| **分片写者租约与 fencing**（§4.3） | ✅ | epoch 单调不重置、续期、`LeaseLost`、旧写者被忽略 |
| **跨分片事务的两阶段提交** | ✅ | 未决 WAL + 提交标记；崩溃后整笔原子消失 |
| **二级索引**（有序 / 哈希 / 唯一 / 复合） | ✅ | 写路径维护、覆盖索引免回表、唯一性原子判重、NULL 不参与 |
| **在线索引构建**（§8.7） | ✅ | BUILDING → 快照回填 → 增量追平 → ACTIVE；构建窗口内的写入不丢 |
| **跳数索引**（minmax / set / bloom / ngram） | ✅ | 写 SST 时顺带建 `.skp`；扫描时**跳过整个块、0 次 GET** |
| **全文检索**（`MATCH ... AGAINST`） | ✅ | 自研倒排索引（FST + postings + WAND + BM25）；`WITH PARSER ngram` |
| **垃圾回收与时间旅行** | ✅ | GC 水位 · `AS OF TIMESTAMP` · **PITR** · 备份恢复 · `DROP TABLE` 墓碑可恢复 |
| **`ALTER TABLE`** | ✅ | 加列 / 删末尾列 / 加删索引 / 改名 / `TRUNCATE`，全部无重写（O(1) 元数据） |
| **CDC 变更流** | ✅ | 直接读已有 WAL（没有第二份变更日志）；游标可持久化 |
| **自增主键的分段分配**（§11.4） | ✅ | 重启/多节点不重号；1000 次取号只写 1 次水位对象 |
| **优化器与统计**（§10.3） | ✅ | `ANALYZE TABLE`（只读 `.idx`，0 次 `get_range`）+ 直方图 + HLL 基数 + 代价模型 + `EXPLAIN` |
| **SQL 能力面** | ✅ | 聚合（`SUM`/`AVG`/`MIN`/`MAX`/`GROUP_CONCAT`）· `GROUP BY`/`HAVING`（强制 `ONLY_FULL_GROUP_BY`）· `JOIN`（内/左/右/交叉，等值走哈希）· `UNION` · `SELECT DISTINCT` · **窗口函数**（排名/取值/聚合开窗 + `PARTITION BY`/帧）· 子查询（非相关 · 谓词位置与**标量位置**的相关子查询）· `JSON` 函数族 |
| **元数据服务 MS**（§4） | ✅ | Raft（日志/快照落对象存储，冷启动 <3s）+ 状态机（时间戳水位 / 租约 / catalog 版本）+ 行锁表 + CN 客户端；`--role ms` 独立部署，或 `--run-meta-service` 与 CN 同进程 |
| **双模式协调**（§4.7） | ✅ | `single` / `objectstore` / `ms` 三种实现共用**同一套一致性测试**（时间戳不重叠、epoch 不重置、排他锁真的互斥） |
| **`SELECT ... FOR UPDATE`** | ✅ | 行锁走协调服务；提交前自检（MS 换领导者会丢锁 → 报 1213 重试，而不是静默丢更新） |
| **L1 适配层**（§2.2） | ✅ | 重试/退避/限流：瞬时故障重发、`412`/`404` 绝不重发；本地 FS 后端必须显式声明 `--i-know-this-is-not-durable` |
| **可观测性**（§12.5） | ✅ | `/metrics` + `/healthz`、慢查询日志、对象存储审计日志；抓取**不产生对象存储请求** |
| MySQL 协议（握手 / COM_QUERY / 预处理 / 游标） | ✅ | **真实 JDBC 驱动**端到端；`useCursorFetch` 服务端游标、多语句、字符集 |
| SQL 类型 | ✅ | 整数 / 浮点 / `DECIMAL` / 字符串 / `BLOB` / 时间 / `YEAR` / `ENUM` / `SET` / `JSON` |
| `information_schema` + `SHOW` 系列 | ✅ | DataGrip 等客户端的 introspection 可用 |
| 计算节点无状态（重启后从对象存储恢复） | ✅ | 销毁 CN 后重建的恢复测试 |
| **磁盘（NVMe）二级缓存** | ✅ | CN 重启后读同一批数据 **0 次对象存储请求**；CRC 校验、LRU 淘汰 |
| **`yb-cli` 运维工具** | ✅ | `ls` / `shards` / `gc` / `recover-check` / `cost` / `backup` / `backups` / `restore` / `pitr` / `cdc` / `analyze-sql`；直接操作对象存储，**不需要 CN 在线**；容量盘点只花 LIST 的请求（不逐对象 `head`） |
| 认证 / 权限 / TLS | ❌ | 接受任意用户名密码，仅供可信网络内使用 |
| 存储分层与生命周期规则（§13.3） | ❌ | 设计文档对此有明确警告（归档层几乎总是亏的），见 [docs/zh/compatibility.md](docs/zh/compatibility.md) |

> **兼容性与限制**（哪些 MySQL 特性不支持、哪些语义有差异）集中在
> [docs/zh/compatibility.md](docs/zh/compatibility.md)——**迁移前请先读它**，
> 尤其是「默认隔离级别不防写偏斜」与「自增不保证连续」两条。

---

## 快速开始

### 构建与测试

```bash
./gradlew build              # 编译 + 全部检查
./gradlew test               # 单元测试
./gradlew integrationTest    # 集成测试（内嵌 S3 服务器，无需 Docker）
./gradlew koverVerify        # 分支覆盖率门禁（≥ 50%）
./gradlew :yb-bench:bench    # 基准测试
```

> 集成测试用项目内置的 **S3 兼容服务器**（`EmbeddedS3Server`），它走真实 HTTP、
> 实现真实的条件写语义，因此不需要 Docker 或外部二进制。
> 生产验证仍建议用 MinIO 或真实对象存储 —— 见下方「用 MinIO 验证」。
>
> ⚠️ **`build` 与 `check` 都不包含 `integrationTest`** —— 它在独立源集里（见
> [PROJECT_GUIDE.md](PROJECT_GUIDE.md) 的「构建与测试」）。提交前请两个都跑：
> `./gradlew build integrationTest`。

### 启动服务

```bash
# 本地文件系统后端（仅开发）
# ⚠️ --i-know-this-is-not-durable 是**必需**的：本地 FS 没有多副本、也没有跨进程的
#    条件写保证，而写者选举、manifest 指针切换、fencing 与崩溃恢复都依赖对象存储语义。
#    不给这个标志，服务端会硬拒绝启动（YangBaseServer.requireDurableStorageUnlessAllowed）。
./gradlew :yb-server:run --args="--data-dir /tmp/yangbase --i-know-this-is-not-durable --port 3306"

# S3 兼容对象存储（生产）
./gradlew :yb-server:run --args="\
  --s3-bucket my-yangbase-bucket \
  --s3-endpoint https://oss-cn-hangzhou.aliyuncs.com \
  --s3-access-key $AK --s3-secret-key $SK \
  --port 3306"
```

### 用 JDBC 连接

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

### 用 MinIO 验证

集成测试**默认用项目内置的 S3 兼容服务器**（走真实 HTTP + 真实条件写），不需要任何外部依赖。
但同一套断言也能原样跑在真实 MinIO 上（`minio.sh test` 会把 `:yb-server:integrationTest`
与 `:yb-ms:integrationTest` 都指向它——后者才是「MS 的日志真的落在对象存储上」的那一个）：

```bash
# 一键（二进制模式，零依赖）
./scripts/minio.sh test

# 一键（容器模式）
./scripts/minio.sh test docker

# 只启动 / 停止 / 清理数据
./scripts/minio.sh start          # 二进制
./scripts/minio.sh start docker   # 容器
./scripts/minio.sh stop
./scripts/minio.sh clean          # 数据损坏或权限错乱时用
```

> 若当前 shell 没激活 `docker` 组（`usermod` 只对新登录会话生效），
> 脚本内部会自动用 `sg docker` 包一层。

两种模式跑出来的基准数字几乎一样（同机，容器网络开销可忽略）：

| | 提交 p50 | 16 并发写 TPS | 组提交摊薄 |
|---|---|---|---|
| 裸二进制 | 4006µs | 3376 | 14.48 |
| 容器 | 4005µs | 3333 | 14.61 |

启动服务连接 MinIO：

```bash
./gradlew :yb-server:run --args="--s3-bucket yangbase --s3-endpoint http://127.0.0.1:9000 \
  --s3-access-key yangbase --s3-secret-key yangbase123"
```

> 用 Docker 起 MinIO 也可以（`docker run -d -p 9000:9000 ... minio/minio server /data`），
> 但需要当前用户在 `docker` 组里（`sudo usermod -aG docker $USER` 后重新登录）。
> `scripts/minio.sh` 用单个静态二进制，不需要任何权限。

> ⚠️ **本机 Docker 的两个已知问题**
>
> **(1) daemon 被配成走本地代理**
> （`/lib/systemd/system/docker.service` 里的 `HTTP_PROXY=http://127.0.0.1:7890`），
> 而该代理当前不可用，导致**任何 `docker pull` 都报 EOF**——即使 registry 本身是直连可达的。
> `scripts/minio.Dockerfile` 因此用 `FROM scratch` **离线构建**（MinIO 是静态二进制，
> 不需要基础镜像），完全绕开这个问题。
> 想彻底修好代理，把那段 `Environment="*_PROXY=..."` 从 service 文件里挪到
> `/etc/systemd/system/docker.service.d/proxy.conf` 并改成可用的地址，然后
> `sudo systemctl daemon-reload && sudo systemctl restart docker`。
>
> **(2) scratch 镜像默认以 root 运行**，写出的数据文件归 root 所有，
> 会让宿主机的二进制模式连读都读不了。脚本已加 `--user "$(id -u):$(id -g)"`，
> 两种模式也用**各自独立的数据目录**避免互相污染。
>
> ⚠️ **下 MinIO 时的一个坑**：`dl.min.io/aistor/...` 现在给的是 **MinIO AIStor（商业版）**，
> 没有 license 时它会拒绝**所有 S3 操作**（日志里写 `All S3 operations are denied`）。
> 开源版在 **GitHub Releases**：
> ```
> https://github.com/minio/minio/releases/download/RELEASE.2025-09-07T16-13-09Z/minio.linux-amd64.RELEASE.2025-09-07T16-13-09Z
> ```
> 判断方法：`./minio --version` 后启动，日志里若是 `License: GNU AGPLv3` 就是开源版。

---

## 跳数索引

跳数索引不返回行，只回答一个问题：**这个数据块可能含有我要找的值吗**。
答「不可能」的块**一次 GET 都不产生**——这是它省下 IO 的全部机制。

```sql
CREATE TABLE events (
    id BIGINT NOT NULL,
    created_at BIGINT NOT NULL,
    payload VARCHAR(64),
    PRIMARY KEY (id),
    KEY idx_created (created_at) USING MINMAX     -- 范围裁剪
);
-- 或 set/bloom（等值裁剪）、ngram（LIKE 前缀）
CREATE INDEX idx_payload ON events(payload) USING BLOOM;
```

它的写路径是**顺带的**：`SstWriter` 在写数据块的同时把每行的列值喂给
`SkipIndexBuilder`，落盘时一并写出 `.skp`。没有跳数索引的表连行解码都不会多发生一次。

读路径是**在读块之前**问一句 `BlockPruner`——顺序很重要，先读再判断等于没剪。
判定优先用 `.idx` 里已有的块级 min/max（**0 次 GET、0 字节传输**，那份数据本就常驻内存），
`.skp` 只补充 `.idx` 没有的 set / bloom / ngram。

> **一个使用前提**：min/max 剪枝的效力取决于数据是否按该列**聚集**。
> 写入完全乱序时每个块的 min/max 都会覆盖全域，索引就退化为「永远剪不掉」。
> 本系统尚未提供聚簇提示机制（设计文档 §9.2 提到过），因此按时间递增写入是最有效的场景。

---

## 全文检索

倒排索引是自研的：FST 词典 + 分块 postings + WAND 提前终止 + BM25 打分，
本身也是「不可变文件 + manifest 指针」，存在对象存储上（§8.6、原则 P2）。

```sql
CREATE TABLE articles (id BIGINT NOT NULL, body TEXT, PRIMARY KEY (id));

-- 英文：standard 分词器（小写化 + 去停用词）
CREATE FULLTEXT INDEX ft_body ON articles(body) WITH PARSER standard;

-- 中文：必须显式指定 ngram —— standard 会把 CJK 全部当分隔符
CREATE FULLTEXT INDEX ft_body ON articles(body) WITH PARSER ngram;

-- 建索引时会把**已有数据**一并索引进去，不需要重建表

SELECT id FROM articles WHERE MATCH(body) AGAINST('storage engine');
SELECT id FROM articles WHERE MATCH(body) AGAINST('+quick -rabbit' IN BOOLEAN MODE);
SELECT id FROM articles WHERE MATCH(body) AGAINST('data*' IN BOOLEAN MODE);
SELECT id FROM articles WHERE MATCH(body) AGAINST('"quick brown"' IN BOOLEAN MODE);
```

两个值得注意的设计点：

- **`MATCH` 是访问路径，不是逐行谓词。** BM25 打分依赖文档频率、平均文档长度这些
  **全语料统计量**，它们不在任何一行里。所以执行器先用倒排索引取候选主键，再回表，
  最后用剩余的 `WHERE` 过滤。把它塞进逐行求值在语义上就是错的（代码里会明确报错）。
- **索引随数据一起维护，且在事务提交时才更新。** 立即索引会让未提交的行可以被检索到
  ——那是脏读，而且在模糊匹配场景下多出几行很难被人发现。

### 二级缓存（本地 NVMe）

`BlockCache` 是**内存**缓存，CN 一重启就全空——重建之后的一段时间里所有读都打回
对象存储。本地 NVMe 把这段「重新变热」的过程从几百次网络往返变成几百次本地读：

```bash
./gradlew :yb-server:run --args="--data-dir /var/lib/yangbase --i-know-this-is-not-durable \
    --disk-cache-dir /mnt/nvme/yangbase-cache --disk-cache-mb 51200"
```

> `--data-dir` 必须配 `--i-know-this-is-not-durable`（见上方「启动服务」）；
> 生产环境请改用 `--s3-bucket`，那时这个标志不需要、也不应该加。

它**省的是请求数，不是延迟**（延迟只是顺带），所以断言打在请求数上：集成测试里
「重启后读同一批数据」的 `get_range` 必须为 0。

它是安全的，因为它**什么都不是**——不是状态，只是缓存。删掉整个目录，系统照常工作。
这是它敢存在的前提（原则 P1：本地磁盘的一切内容必须可丢弃）。

两条设计上的注意：只缓存**不可变**对象（`manifest/CURRENT` 这类内容会变的键
按 key 缓存会读到陈旧内容，那是静默的错读）；每个块前面放 4 字节 CRC32C，
读时校验，不匹配就当作未命中——**缓存可以慢，但不能撒谎**。

---

## 运维

对象存储上的数据库有一个特殊困难：**看不见的东西才是问题**。数据在哪个分片、
manifest 有多大、哪些对象已经没人引用了——这些在传统数据库里是 `du` 和 `ls` 就够的事，
在对象存储上需要专门的工具。`yb-cli` 就是干这个的，而且它**直接操作对象存储，
不需要 CN 在线**（计算层随时可以销毁，运维动作不该依赖它）。

```bash
# 存储里有什么（按前缀归组）
./gradlew :yb-cli:cli -Pargs="ls --endpoint http://127.0.0.1:9000 --bucket yangbase"

# 每个分片多大、多少行
./gradlew :yb-cli:cli -Pargs="shards --endpoint ..."

# 垃圾回收。默认**只标记不删除**，加 --delete 才真删
./gradlew :yb-cli:cli -Pargs="gc --endpoint ..."
./gradlew :yb-cli:cli -Pargs="gc --delete --endpoint ..."

# 检查恢复路径健康度：有没有被 manifest 引用但已丢失的对象
./gradlew :yb-cli:cli -Pargs="recover-check --endpoint ..."

# 按设计文档 §13 的成本模型估算月度费用
./gradlew :yb-cli:cli -Pargs="cost --endpoint ..."
```

其余几个命令（同样直接操作对象存储）：

```bash
# 备份（只写清单，成本与数据量无关）/ 列出备份 / 恢复（--yes 才真做）
./gradlew :yb-cli:cli -Pargs="backup --note pre-upgrade --endpoint ..."
./gradlew :yb-cli:cli -Pargs="backups --endpoint ..."
./gradlew :yb-cli:cli -Pargs="restore --yes --endpoint ..."

# 时间点恢复：把某张表切回某个时刻
# ⚠️ 时刻里用 T 而不是空格：-Pargs 按空格切分参数，写 "2026-09-25 10:00:00"
#    会被切成两个参数，只有前半段被 --to 吃到（静默变成当天 00:00:00）
./gradlew :yb-cli:cli -Pargs="pitr --table users --to 2026-09-25T10:00:00 --yes --endpoint ..."

# 变更流：直接读已有 WAL，没有第二份变更日志
./gradlew :yb-cli:cli -Pargs="cdc --since 2026-09-25T10:00:00 --limit 100 --endpoint ..."

# 迁移风险扫描：目标 SQL 里哪些部分本系统不支持、或语义与 MySQL 不同
./gradlew :yb-cli:cli -Pargs="analyze-sql --path ./migrations"
```

GC 是**两阶段**的（设计文档 §6.9）：先标记、写清单，**安全窗口（默认 24 小时）之后**
才真正删除。窗口的作用不是「等读者读完」，而是把误判的代价从「静默读到错数据」
降级为「多占一天存储」。`gc` 报告会把**被 fence 的旧写者 WAL** 与
**compaction 残留**分开计数——前者是 fencing 的正常代价（§4.3），
混在一起看会以为泄漏很严重。

---

## 架构

```
 应用 / JDBC ──MySQL 协议──▶ Compute Node (CN) × N      无状态，可随时增删
                              ├ SQL 解析 / 优化 / 执行
                              ├ 事务层 (MVCC)
                              ├ LSM 存储引擎 + 索引层
                              └ 内存 BlockCache（可丢弃）
                                        │
                                        ▼
        ┌───────────────────────────────────────────────────────┐
        │  对象存储（S3 / OSS / COS / MinIO）★唯一真相源★        │
        │  catalog/  shard/.../wal|sst|manifest  stats/          │
        └───────────────────────────────────────────────────────┘
```

### 四个核心机制

**1. 点查 = 1 次 GET**（`yb-storage`）

SST 拆成两个对象：`.idx`（小、常驻内存）与 `.dat`（大、按字节范围读）。
因为索引在内存里，点查只有最后一跳需要真的去对象存储取数据块。

```
manifest（常驻）→ bloom 判定（内存）→ .idx 二分定位块（内存）→ 1 次 Range GET
```

**2. COUNT = 0 次 GET**（§8.9）

`SELECT COUNT(*)` 在对象存储上是最贵又最常见的查询。本系统让它**不读任何数据**：

- **manifest 上的精确行数** —— 由单写者维护（它串行化了所有提交，知道每个事务的净行数变化）
- **块级 `commit_ts` 累积计数表** —— 支持历史快照的精确计数
- **块级「值→计数」** —— 让 `COUNT(*) WHERE status='x'` 也是 0 GET

**3. 组提交**（§6.3.4）

对象存储上 PUT 比 GET 贵 12.5 倍（AWS），且延迟高。写者攒批后一次 PUT 持久化一批事务。
基准里 16 个并发写者把 3200 个事务摊薄到 221 次 PUT（**14.5 事务/PUT**）；
单线程串行写入没有攒批空间（1.0 事务/PUT）——组提交的收益只在并发下体现。

**4. 分区即写入分片**（§9.3）

MySQL 的「分区」与本系统的「分片」天然对齐：`PARTITION BY HASH(user_id) PARTITIONS 8`
直接得到 8 个独立写者与 8 倍写入并行度。分区函数是确定性的，因此**不存在数据搬迁**。

---

## 模块

| 模块 | 职责 |
|---|---|
| `yb-common` | 类型、保序字节编码、行/值编解码、schema |
| `yb-objectstore` | `ObjectStore` SPI + 内存/本地FS/S3 实现 + **内嵌 S3 测试服务器** + 请求计数器 |
| `yb-storage` | LSM 引擎：MemTable、WAL、SST、Manifest、缓存、计数 |
| `yb-txn` | TSO、Catalog、DatabaseEngine、分区路由与裁剪、事务缓冲、GC 与时间旅行、`CoordinationService`（single / objectstore / ms 三种实现共用一套一致性测试） |
| `yb-ms` | 元数据服务（Raft 日志/快照落对象存储 + 状态机 + 行锁表 + CN 客户端） |
| `yb-index` | 跳数索引（minmax / set / bloom / ngram）与自研段式倒排索引（FST / postings / WAND / BM25） |
| `yb-sql` | 词法/语法分析、执行器、`information_schema` 虚拟表 |
| `yb-mysql` | MySQL 协议服务端 |
| `yb-server` | 计算节点主程序 |
| `yb-bench` | 基准测试 |
| `yb-cli` | 运维命令行（对象清单、分片、GC、恢复检查、成本估算） |

---

## 性能（实测）

`./gradlew :yb-bench:bench`，后端是内嵌 S3 服务器（真实 HTTP）。**实测数据**
（数字来自某次基准运行，口径见 [docs/zh/compatibility.md](docs/zh/compatibility.md) §4.9；
延迟受环境影响大，**请求数才是确定性的那一列**）：

| 场景 | 量级 |
|---|---|
| 点查（稳态，缓存命中） | p50 **9µs**，平均 **0.004 次**对象存储请求/查询 |
| 点查（冷缓存） | p50 **124µs**，**1 次** Range GET |
| `COUNT(*)` | **0 次**请求，约 0.37ms（比全表扫描快 300×） |
| `COUNT(*) WHERE status='x'` | **0 次**请求 |
| 组提交摊薄 | 3200 事务 → **220–221 次 PUT**（**14.5 事务/PUT**） |

同一套基准**换后端**再跑一遍（`./gradlew :yb-bench:bench`，加
`-PYB_S3_ENDPOINT=http://127.0.0.1:9000 ...` 就打真实 MinIO）——差异全在延迟与吞吐上，
**请求数不随后端变化**：

| 场景 | 内嵌 S3 服务器 | **真实 MinIO** |
|---|---|---|
| 范围扫描 1000 行 | 1 次 GET，35 万行/秒 | 1 次 GET，**17 万行/秒** |
| 写入（单线程） | 1451 TPS，提交 p50 689µs | **249 TPS，提交 p50 4.0ms** |
| **写入（16 并发）** | 18605 TPS | **3376 TPS** |
| `.idx` 元数据开销 | 数据的 6.9% | 数据的 6.9% |

**两组数字的差异本身就是结论**：内嵌服务器把对象存在 HashMap 里，PUT 是进程内调用；
真实 MinIO 要走 HTTP + 磁盘 fsync，单次 PUT 约 **4ms**。云上对象存储的 RTT 还要再高一个
数量级（设计文档 §14.1 给的实测参照是 S3 PUT p99 400–600ms）。

但**设计的三条核心主张在真实对象存储上全部成立且与后端无关**：COUNT 是 0 次请求、
稳态点查是 0.004 次请求、组提交把 3200 个事务摊薄到 221 次 PUT。这些是架构性质，
不是特定后端的产物。

> 两个关键结论：
> 1. **对象存储请求数比延迟更值得断言** —— 它是确定性的，且直接决定账单。所有读路径的
>    优化目标都是把请求数降到 0 或 1。
> 2. **组提交的收益只在并发下体现**。单线程串行写入没有攒批空间（1.0 事务/PUT）；
>    16 个并发写者能把 PUT 数摊薄 14.5 倍。这也是为什么基准里专门有一节并发写入
>    —— 它是实现期间发现「组提交实际没生效」的原因。

---

## 已知限制

设计文档 §11.3 与 §16.3 列出了完整清单。最需要注意的：

| 限制 | 说明 |
|---|---|
| **默认快照隔离不防写偏斜** | InnoDB 的 RR 靠 next-key lock 避免，本系统不提供。`SHOW VARIABLES LIKE 'yangbase_isolation_detail'` 会如实报告 |
| **自增主键不保证连续** | 与 `innodb_autoinc_lock_mode=2` 行为一致 |
| **字符串比较是二进制序** | 等价 `utf8mb4_bin`：`WHERE name = 'ABC'` **匹配不到** `'abc'`，`ORDER BY` / `DISTINCT` / `GROUP BY` 的分组口径同样按字节序。⚠️ 握手包与 `SHOW VARIABLES` 上报的排序规则名仍是 `utf8mb4_0900_ai_ci`（MySQL 默认，大小写**不敏感**），与实际语义不符——**以实际语义为准**，见 [docs/zh/compatibility.md](docs/zh/compatibility.md) §4.3.1 |
| **提交延迟 p50 约 30–50ms** | 下界是一次对象存储 PUT 的 RTT，是物理约束 |
| **跨分片事务在提交失败时可能短暂可见部分行** | 崩溃不影响正确性（恢复会整笔丢弃），但提交路径失败时，本进程内存中可能残留部分行，重启即清除。**按分区键切分事务可以完全避开这一点**（§9.3） |
| **全文检索需要显式选分词器** | `standard` 只切 ASCII，中文会切出 0 个词条（此时会明确报错而不是返回空集）。中文请用 `WITH PARSER ngram` |
| **全文索引是单列的** | 一个索引覆盖一个字段，多字段需要多个索引 |
| **冷热查询延迟差约 60 倍** | 对象存储作为数据层的物理下界，无法通过优化消除 |
| 不支持 | 存储过程、触发器、视图更新、外键级联、`LOAD DATA INFILE`、binlog |

---

## 开发约定

见 [CLAUDE.md](CLAUDE.md)。几条最容易踩的：

- **对象存储是唯一真相源** —— 本地的一切必须可丢弃
- **禁止在热路径上调用 `LIST`** —— 它是慢操作
- **不要把 `.idx` 与 `.dat` 合并** —— 会让点查下载整个数据文件
- **L1 及以下必须 leveled** —— 这是 COUNT 精确性的语义前提
- **新增读路径时，先写它的请求数断言**

---

## 参与贡献

欢迎贡献。先看 [CONTRIBUTING.md](docs/zh/CONTRIBUTING.md)——里面写了怎么构建、测试的期望，以及几条不可谈判的项目规则（请求数预算、对象不可变、`.idx`/`.dat` 分离）。所有参与者都要遵守[行为准则](docs/zh/CODE_OF_CONDUCT.md)。

分支模型：`feature/*`、`bugfix/*` 从 `dev/<version>` 切出、squash 合回；`main` 只在发版时更新。**已打 tag 的版本分支冻结、不再接受任何提交，新改动一律开新版本**；每周五发一个版本。版本号语义、冻结规则与发布门禁（有已知重大缺陷就不发）见 [版本号与发布策略](docs/zh/versioning.md)，分支操作细节见 [开发流程](docs/zh/development-process.md)。

---

## 许可证

Apache-2.0
