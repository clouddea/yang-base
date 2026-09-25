# YangBase 用户手册

> **面向谁**：要把应用接到 YangBase 上的开发者与运维。
> 想先判断"能不能迁"→ 读 [compatibility.md](compatibility.md)；
> 想知道"为什么这么设计"→ 读 [yangbase-design.md](yangbase-design.md)；
> 本手册只回答**怎么用**。

---

## 目录

1. [它是什么，什么时候该用](#一它是什么什么时候该用)
2. [快速开始](#二快速开始)
3. [连接与驱动](#三连接与驱动)
4. [数据类型](#四数据类型)
5. [SQL 能力面](#五sql-能力面)
6. [内置函数一览](#六内置函数一览)
7. [事务、隔离级别与行锁](#七事务隔离级别与行锁)
8. [索引](#八索引)
9. [分区](#九分区)
10. [时间旅行、备份与恢复](#十时间旅行备份与恢复)
11. [CDC 变更流](#十一cdc-变更流)
12. [多节点部署](#十二多节点部署)
13. [可观测性](#十三可观测性)
14. [运维命令行](#十四运维命令行)
15. [性能与成本](#十五性能与成本)
16. [从 MySQL 迁移的检查清单](#十六从-mysql-迁移的检查清单)

---

## 一、它是什么，什么时候该用

YangBase 是一个**把全部持久状态放在对象存储上的关系型数据库**，兼容 MySQL 协议。
计算节点（CN）是**无状态**的：进程随时可以销毁、换一台机器重建，数据一行不丢。

**该用它**：

- 数据量在 GB 级以上、单机放不下或不想为一个"平时没什么人用"的库付固定费用；
- 读多写少（报表、配置、订单归档、后台管理、SaaS 多租户的冷数据）；
- 需要事务与 SQL，不想退化成"键值 + 自己写事务"；
- 需要**能随时把整个实例搬走**（换云、换区域、跟随折扣周期重建）。

**不该用它**：

- 高频点写（每毫秒几十笔）或要求 p99 在个位数毫秒——受对象存储 RTT 限制；
- 需要 `SELECT ... FOR UPDATE` 之外的重型锁定语义、需要存储过程/触发器；
- 需要跨行强一致的复杂约束（如可延迟外键）。

**一句话**：它是"**低频、读多写少、要事务与 SQL**"的库，
用**每次写入多花几十毫秒**换"没有客户的时候不花钱、计算层随时可丢"。

---

## 二、快速开始

### 构建

```bash
./gradlew build           # 编译 + 全部检查（单元测试、静态检查）
./gradlew integrationTest # 集成测试（用内嵌 S3，不需要 Docker）
```

> ⚠️ **`build` 与 `check` 都不包含 `integrationTest`**（它在独立源集里），
> 所以要跑全就得写 `./gradlew build integrationTest`。

### 启动一个最小实例（本地文件系统，仅开发用）

```bash
./gradlew :yb-server:run --args="\
  --data-dir /tmp/yangbase \
  --i-know-this-is-not-durable \
  --port 3306"
```

> ⚠️ `--data-dir` 走本地文件系统，**没有多副本、也不保证持久**。正因为如此，
> 服务端对它的要求是**显式声明**：不加 `--i-know-this-is-not-durable`
> 会**拒绝启动**（宁可启动失败，也不要让人以为数据在安全的地方）。生产必须换成对象存储。
>
> ⚠️ **3306 是本机 MySQL/MariaDB 的默认端口**，开发机上常常已经被它占着
> （容器里的 MySQL 也会占）。那种情况下启动会明确报「MySQL 端口 3306 已被占用」
> 并给出查占用的命令——换一个端口即可：`--port 3307`。

### 启动一个生产实例（S3 兼容对象存储）

以自建 MinIO 为例（AWS S3 / COS / R2 同理，换 endpoint 与 region）：

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

> ⚠️ **自建网关（MinIO 等）走路径式寻址，这是本项目的默认**；而**阿里云 OSS 恰好相反**
> ——它的 endpoint 长得像 S3，但原语不同，照抄上面这段起不来。见下一节。

启动日志会打印**能力实测**结果（条件写、强一致读、Range 读、LIST 一致性）。
任何一项关键能力不通过都会拒绝启动——那不是"警告"，因为缺了条件写的后端
会让租约与 manifest 指针静默失效。

### 启动一个阿里云 OSS 实例

```bash
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

四个参数各有原因，**少一个都开不了机**：

| 参数 | 为什么必须 |
|---|---|
| `--s3-oss` | 让客户端用 **OSS 自己的原语**：原子创建发 `x-oss-forbid-overwrite`（冲突是 **409 FileAlreadyExists**，不是 S3 的 412）。少了它，条件创建会被 OSS 直接拒绝（400） |
| `--s3-path-style false` | OSS **拒绝路径式寻址**（`SecondLevelDomainForbidden`：要求三级域名）。本项目默认对自建网关用路径式——MinIO 需要那样 |
| `--s3-region cn-hangzhou` | 进 SigV4 的签名域，要填 OSS 的地域 |
| `--i-accept-nonatomic-pointer-switch` | OSS **没有 ETag CAS**（实测 `If-Match` 返回 400 `NotImplemented`），而每次 flush/compaction 都要切 manifest 指针。不加它，服务端会在**能力探测阶段拒绝启动**（原则 P6：宁可拒绝，也不要悄悄失去互斥）；加了它会打一行醒目告警 |

> **降级的确切含义**：指针切换退化为「无前置条件 PUT + 写后回读校验」，接管分片时
> 再做一次**孤儿快照审计**（`yb_manifest_orphan_snapshots_total`）。为什么可以接受
> （被覆盖的是 **SST 引用**而不是数据：水位一起回退，那批 WAL 不会被回收，恢复时会重放）
> 见设计文档 §3.3 的不变式论证。**它不等于 CAS**，多 CN + OSS 属已知边界。

**两条硬限制**：

1. **协调模式只能用 `single`（单 CN）或 `ms`**：`objectstore` 模式的租约与行锁
   建立在 ETag CAS 上，OSS 做不到（会明确报错）。
2. **MS 的选举超时用生产默认量级**（500ms–1s）：OSS 上 MS 每次日志追加都是一次真实
   PUT，照搬内存后端的经验值会让集群在负载下反复换领导者——不报错，只是每次换都丢锁表。

`yb-cli` 同样支持 OSS：加 `--s3-oss --path-style false`（或环境变量 `YB_S3_OSS=true`）。

实测（2026-09-25，杭州）：单 CN 与**多 CN + 3 节点 MS**（MS 的 Raft 日志同样落在 OSS）
全部通过，含**点查稳态 ≤ 1 次 GET**、COUNT 0 GET、索引 vs 全扫、分区裁剪、提交预算、
重启恢复、Range/LIST、跨 CN 可见性与分片接管。

### 连上它

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

## 三、连接与驱动

标准 MySQL JDBC 驱动即可（Connector/J 8.x 是主要测试目标）：

```java
String url = "jdbc:mysql://127.0.0.1:3306/app"
           + "?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC";
Connection c = DriverManager.getConnection(url, "root", "");
```

| 参数 | 建议 | 原因 |
|---|---|---|
| `serverTimezone` | **`UTC`** | 见下方「时间」 |
| `useSSL` | `false`（内网） | 本版本没有 TLS（见「未实现」） |
| `useCursorFetch` | 大结果集时开 | 服务端支持只读游标（`COM_STMT_FETCH`） |
| `rewriteBatchedStatements` | 批量插入时开 | 多条 VALUES 合并成一条语句 |
| 连接池 | HikariCP / Druid 均可 | `COM_PING`、`COM_RESET_CONNECTION` 已实现 |

### 时间：一切按 UTC

**时间字面量按 UTC 解释，也按 UTC 渲染**，与服务器所在时区无关：

```sql
INSERT INTO t (dt) VALUES ('2026-02-01 10:00:00');
SELECT dt FROM t;          -- 永远是 2026-02-01 10:00:00
SELECT NOW();              -- 当前时刻（UTC 表示）
```

所以 JDBC 连接串里写 `serverTimezone=UTC`，两边才对得上。
`SET time_zone` 目前**是空实现**（收下但不改变行为），不要依赖它做时区转换——
在应用层转换，或存 UTC 时间戳（`BIGINT`）。

`DATE` 列只显示日期部分（`2026-02-01`），`DATETIME` 显示到秒/毫秒。

### 认证与权限

当前版本**没有认证**：接受任意用户名与密码，也没有权限模型。
**只能部署在可信网络内**（安全组 / 内网 / VPN），不要把端口暴露到公网。

---

## 四、数据类型

| 类型 | 说明 | 注意 |
|---|---|---|
| `TINYINT` / `SMALLINT` / `MEDIUMINT` / `INT` / `BIGINT` | 整数 | 内部统一 64 位；写入超范围的值**报 1264**（`TINYINT` -128~127、`SMALLINT` ±32767、`INT` ±2147483647）；`MEDIUMINT` 与 `BIGINT` 按 64 位处理 |
| `BOOL` / `BOOLEAN` | `TINYINT(1)` 的别名 | 存 0/1 |
| `FLOAT` / `DOUBLE` / `REAL` | 浮点 | 8 字节 IEEE754；`FLOAT(p,s)` 的精度声明被忽略 |
| `DECIMAL(p,s)` / `NUMERIC` | **定点数** | 按声明标度存储与舍入（`DECIMAL(12,2)` 存 `1.005` → `1.01`）。金额请用它，不要用 `DOUBLE` |
| `VARCHAR(n)` / `CHAR(n)` / `TEXT` / `TINYTEXT` / `MEDIUMTEXT` / `LONGTEXT` | 字符串（UTF-8） | `CHAR` 被当 `VARCHAR`；超长**报 1406**（按**字符数**计，不是字节数）；`TEXT` 无长度声明则不校验 |
| `BINARY` / `VARBINARY` / `BLOB` / `TINYBLOB` / `MEDIUMBLOB` / `LONGBLOB` | 二进制 | `BINARY`/`VARBINARY` 映射到 `BLOB`（按字节存，不做定长填充） |
| `DATE` / `DATETIME` / `TIMESTAMP` | 时间（毫秒精度） | 三者内部同构、都按 UTC；`TIMESTAMP` 没有自动时区转换 |
| `YEAR` | 年份 | 存储上就是个整数；非数字的写入**报 22007**，但**不做 1901–2155 范围检查** |
| `ENUM(...)` / `SET(...)` | 封闭集合 | 写入非成员**报错**（不是静默截断）；比较与排序按**字符串**（MySQL 按声明序号，见 compatibility.md） |
| `JSON` | 按文本存 + 写入校验 | 提供 `JSON_EXTRACT` / `->` / `->>` 等函数；没有二进制 JSON 与 JSON 索引 |

**不存在**的类型：`TIME`、`BIT`、`GEOMETRY`、空间类型——写进 DDL 会报 1235。

> **长度与范围的校验现状**：`VARCHAR(n)` 超长报 1406、整数宽度溢出报 1264
> （与 MySQL 严格模式一致）；`DECIMAL(p,s)` 的精度溢出**不报错**，按声明标度舍入。
> `INSERT IGNORE` 下这些错误会被降级为"跳过该行"，而不是像 MySQL 那样
> 写入一个"最接近的值"——**本系统不会替你改数据**。
> 受影响的行数会如实回报，所以应用能发现"有几行没进去"，而不是拿到一个被悄悄改过的值。

---

## 五、SQL 能力面

### 支持的语句

```sql
-- DDL
CREATE DATABASE [IF NOT EXISTS] db;
DROP DATABASE [IF EXISTS] db;
CREATE TABLE [IF NOT EXISTS] t (...);      -- 含 PRIMARY KEY / UNIQUE / KEY / 分区 / USING <索引类型>
DROP TABLE [IF EXISTS] a[, b, ...];
ALTER TABLE t ADD|DROP COLUMN, ADD|DROP INDEX, RENAME TO, TRUNCATE PARTITION, ADD|DROP|REORGANIZE PARTITION;
TRUNCATE [TABLE] t;
CREATE [UNIQUE] [FULLTEXT] INDEX i ON t (cols) [USING <类型>] [WITH PARSER <分析器>];
DROP INDEX i ON t;                          -- 只删索引，不动数据
CREATE [OR REPLACE] VIEW v AS SELECT ...;   -- 只读视图
DROP VIEW [IF EXISTS] v;

-- DML
INSERT INTO t [(cols)] VALUES (...), (...);
INSERT INTO t [(cols)] SELECT ...;                       -- 从查询插
INSERT ... ON DUPLICATE KEY UPDATE col = VALUES(col), ...; -- upsert
REPLACE INTO t ...;                                      -- 撞键则先删后插
INSERT IGNORE INTO t ...;                                -- 撞键**或值非法**都跳过该行（如实回报插了几行）
UPDATE t SET ... [WHERE ...];
DELETE FROM t [WHERE ...];

-- 元数据查询（客户端与导出工具依赖）
SHOW DATABASES;  SHOW TABLES [FROM db] [LIKE 'x'];
SHOW FULL TABLES [FROM db];         -- 多一列 Table_type（BASE TABLE / VIEW）
SHOW TABLE STATUS [FROM db] [LIKE 'x'];   -- mysqldump 逐表取元数据用
SHOW TRIGGERS [FROM db] [LIKE 'x']; -- 本系统不支持触发器，返回空集
SHOW CREATE TABLE t;  SHOW INDEX FROM t;  SHOW VARIABLES [LIKE 'x'];

-- DQL
SELECT [DISTINCT] ... FROM ... [JOIN ...] [WHERE] [GROUP BY] [HAVING] [ORDER BY] [LIMIT]
       [FOR UPDATE | LOCK IN SHARE MODE] [AS OF TIMESTAMP '...'];
SELECT ... UNION [ALL] SELECT ... [ORDER BY ...] [LIMIT ...];
EXPLAIN SELECT ...;
ANALYZE TABLE t;

-- 事务
BEGIN | START TRANSACTION;  COMMIT;  ROLLBACK;
SET autocommit = 0|1;

-- 管理
SHOW DATABASES | TABLES | COLUMNS | INDEX | CREATE TABLE | VARIABLES | STATUS | WARNINGS | ENGINES;
DESC t;  USE db;  SET NAMES utf8mb4;
```

### 表达式

- 比较：`=` `<=>` `<>` `!=` `<` `<=` `>` `>=`，`BETWEEN`、`IN`、`LIKE`、`IS [NOT] NULL`
- 逻辑：`AND` `OR` `NOT`（**三值逻辑**：`NULL` 是"未知"，不是"假"）
- 算术：`+ - * / %`（整数溢出**报 1690**，不回绕；除零部分见下）
- `CASE WHEN ... THEN ... ELSE ... END`（搜索式与简单式都支持）
- `CAST(x AS 类型)`、`CONVERT(x, 类型)`
- JSON：`->`、`->>`
- 子查询：`IN` / `EXISTS` / 标量位置的相关子查询（谓词位置的相关子查询会被改写成半/反连接）
- 窗口函数：`ROW_NUMBER` `RANK` `DENSE_RANK` `PERCENT_RANK` `CUME_DIST` `NTILE` `LAG` `LEAD`
  `FIRST_VALUE` `LAST_VALUE` `NTH_VALUE` + 聚合开窗，支持 `PARTITION BY` / `ORDER BY` /
  `ROWS` 帧与 `RANGE UNBOUNDED PRECEDING` 帧
- 聚合：`COUNT` `SUM` `AVG` `MIN` `MAX` `GROUP_CONCAT`（`DISTINCT` 可用），
  `GROUP BY` / `HAVING`，**强制 `ONLY_FULL_GROUP_BY`**

### 明确报错、不会静默忽略的写法

`INTO OUTFILE`、`USING`/`NATURAL JOIN`、`FOR UPDATE NOWAIT`/`SKIP LOCKED`、
`RANGE n PRECEDING` 帧、`GROUPS` 帧、`UNION` 分支内部的 `ORDER BY`/`LIMIT`、
派生表 `FROM (SELECT ...) x`、多表 `UPDATE`/`DELETE`、`SAVEPOINT`。

> 这些写法的共同点是：**照直实现会给出错误结果**（例如把 `USING` 当普通 JOIN
> 会让外连接退化成笛卡尔积），所以本系统选择报错。
> 完整清单与替代写法见 [compatibility.md](compatibility.md) 第三节。

---

## 六、内置函数一览

> 约定：**绝大多数函数遇到 `NULL` 就返回 `NULL`**（`UPPER(NULL)`、`ROUND(NULL)`、
> `CONCAT('a', NULL)` 都是 `NULL`）。下表标了 NULL 特例的是例外。
> 参数个数写错报 **1210**；函数不存在报 **1235**。

### 控制流与 NULL

| 函数 | 说明 |
|---|---|
| `CASE WHEN c THEN v ... [ELSE e] END` | 搜索式；简单式 `CASE x WHEN 1 THEN ...` 等价于 `x = 1`。`NULL` 条件不算真 |
| `IF(c, a, b)` | `c` 为真取 `a`；`c` 为 NULL 取 `b` |
| `IFNULL(a, b)` / `NVL` | `a` 为 NULL 取 `b` |
| `NULLIF(a, b)` | `a = b` 时返回 NULL |
| `COALESCE(a, b, ...)` | 第一个非 NULL |
| `GREATEST(...)` / `LEAST(...)` | 最大/最小；**任一参数为 NULL 则结果为 NULL** |
| `ISNULL(x)` | 是否 NULL |

### 字符串

| 函数 | 说明 |
|---|---|
| `UPPER`/`UCASE`、`LOWER`/`LCASE` | 大小写 |
| `LENGTH` / `OCTET_LENGTH` / `BIT_LENGTH` | **字节数**（`LENGTH('中')` = 3） |
| `CHAR_LENGTH` / `CHARACTER_LENGTH` | **字符数**（`CHAR_LENGTH('中')` = 1） |
| `CONCAT(...)` | 拼接；**任一参数为 NULL 则整体 NULL** |
| `CONCAT_WS(sep, ...)` | 用分隔符拼接、**跳过 NULL**；分隔符为 NULL 则整体 NULL |
| `SUBSTRING(s, pos[, len])` / `SUBSTR` / `MID` | `pos` 从 1 起；负数从末尾数；`pos = 0` 得空串 |
| `LEFT(s, n)` / `RIGHT(s, n)` | 取左右 n 个字符 |
| `TRIM(s)` / `LTRIM` / `RTRIM` | 去空白 |
| `TRIM(BOTH \| LEADING \| TRAILING 'x' FROM s)` | 去指定字符（语法形式） |
| `REPLACE(s, from, to)` | 替换（`from` 为空则原样返回） |
| `INSTR(s, sub)` / `LOCATE(sub, s[, pos])` / `POSITION(sub, s)` | 位置，找不到返回 0 |
| `LPAD(s, n, pad)` / `RPAD(s, n, pad)` | 填充/截断到 n |
| `SUBSTRING_INDEX(s, delim, count)` | 按分隔符取第 count 段（负数从右边数） |
| `REVERSE`、`REPEAT`、`SPACE` | 反转 / 重复 / 空格 |
| `ASCII(s)`、`CHAR(n, ...)`、`HEX(v)`、`UNHEX(s)` | 编码转换 |
| `MD5(s)`、`SHA1(s)`、`SHA2(s[, bits])` | 摘要（十六进制小写） |
| `STRCMP(a, b)` | 比较：返回 -1 / 0 / 1（按二进制序） |

### 数值

| 函数 | 说明 |
|---|---|
| `ABS` | 绝对值（`ABS(-9223372036854775808)` **报 1690**，不回绕） |
| `CEIL`/`CEILING`、`FLOOR`、`SIGN` | 取整与符号 |
| `ROUND(x[, d])` | 四舍五入**远离零**（`ROUND(-2.5)` = -3，不是 -2） |
| `TRUNCATE(x, d)` | 向零截断 |
| `MOD(a, b)` | 取模；**除数为 0 返回 NULL** |
| `POW`/`POWER`、`SQRT`、`EXP`、`LN`、`LOG([base,] x)`、`LOG2`、`LOG10` | 幂与对数（负数开方/取对数返回 NULL） |
| `SIN` `COS` `TAN` `ASIN` `ACOS` `ATAN` `ATAN2` `DEGREES` `RADIANS` `PI` | 三角 |

### 日期时间（全部按 UTC）

| 函数 | 说明 |
|---|---|
| `NOW()` / `CURRENT_TIMESTAMP` / `LOCALTIME` / `SYSDATE` / `UTC_TIMESTAMP` | 当前时刻。**同一条语句里是常量** |
| `CURDATE()` / `CURRENT_DATE` / `UTC_DATE` | 今天零点 |
| `CURTIME()` / `CURRENT_TIME` | 当前时间（字符串） |
| `DATE(x)`、`TIME(x)` | 取日期部分 / 时间部分 |
| `YEAR` `MONTH` `DAY`/`DAYOFMONTH` `HOUR` `MINUTE` `SECOND` `MICROSECOND` | 取字段 |
| `DAYOFWEEK`（1=周日）`WEEKDAY`（0=周一）`DAYOFYEAR` `WEEKOFYEAR` `QUARTER` | 星期/年内序 |
| `MONTHNAME` `DAYNAME` | 英文名 |
| `LAST_DAY(x)` | 当月最后一天 |
| `DATE_FORMAT(x, fmt)` | 按格式串格式化（`%Y %m %d %H %i %s %b %e %W %j %p %T %r …`）；不认识的格式符原样输出 |
| `STR_TO_DATE(s, fmt)` | 按格式串解析（与 `DATE_FORMAT` 同一张格式符表） |
| `DATEDIFF(a, b)` | 相差天数（只算日期部分） |
| `TIMESTAMPDIFF(unit, a, b)` | 单位见下；`TIMESTAMPADD(unit, n, d)` 加减 |
| `DATE_ADD(d, INTERVAL n unit)` / `DATE_SUB(...)`；`ADDDATE(d, n[, unit])` / `SUBDATE(...)` | 加减；单位：`MICROSECOND SECOND MINUTE HOUR DAY WEEK MONTH QUARTER YEAR` |
| `TIME_FORMAT(x, fmt)` | 只格式化时间部分（格式符与 `DATE_FORMAT` 同一张表） |
| `UNIX_TIMESTAMP([d])` / `FROM_UNIXTIME(ts[, fmt])` | 与 Unix 秒互转 |
| `TO_DAYS(x)` | 天数序号 |

### 类型转换

```sql
CAST(x AS SIGNED | UNSIGNED | CHAR | BINARY | DATE | DATETIME | TIME | DECIMAL(p,s) | DOUBLE | JSON)
CONVERT(x, 类型)
```

转换失败**报 1292**（MySQL 宽松模式会给 0 + warning；本系统没有 warning 通道，
静默给 0 属于"看起来正常的错值"）。

---

## 七、事务、隔离级别与行锁

```sql
SET autocommit = 0;          -- 或 BEGIN / START TRANSACTION
INSERT INTO t VALUES (...);
UPDATE t SET v = v + 1 WHERE id = 1;
COMMIT;                      -- 或 ROLLBACK
```

| 项 | 行为 |
|---|---|
| 隔离级别 | **快照隔离**（对外报告 `REPEATABLE-READ`） |
| 幻读 | 不会发生（快照） |
| **写偏斜** | ⚠️ **不防**——两个事务各读对方要改的行时可能双写。需要时用 `FOR UPDATE` 加锁 |
| 跨分片事务 | 2PC + 提交标记（`COMMIT` 失败时整笔原子消失，不会留半份） |
| 提交档 | `sync`（默认，RPO=0）/ `batch`（高吞吐导入） |
| 提交延迟 | `sync` 约 0.4–2 ms（本地）／真实对象存储下一次 PUT 的 RTT |

### 行锁

```sql
BEGIN;
SELECT balance FROM acct WHERE id = 1 FOR UPDATE;  -- 排他锁
UPDATE acct SET balance = balance - 100 WHERE id = 1;
COMMIT;                                            -- 锁随提交释放
```

- `LOCK IN SHARE MODE` 是共享锁。
- 锁在**事务提交/回滚时释放**；`autocommit = 1` 时随语句结束释放。
- 等待超时按 `innodb_lock_wait_timeout`（默认 50 秒）报 **1205**；
  死锁检测报 **1213**（`objectstore` 模式没有死锁检测，只能等超时）。
- 锁有 TTL（默认取 `2 × 等待上限`，下限 30 秒）：**连接异常断开**时锁不会立刻释放。
  长事务请同时调大 `innodb_lock_wait_timeout`（TTL 跟着变长）。
- `FOR UPDATE` 用在 JOIN / 视图上会**明确报错**（本系统按基表主键加锁，多表语义未定义）。
- `NOWAIT` / `SKIP LOCKED` 会报错（它们改变加锁语义，静默忽略等于给了个"看起来加了锁"的事务）。

---

## 八、索引

```sql
CREATE INDEX idx_city ON t (city);                   -- 有序索引（B+ 树语义）
CREATE UNIQUE INDEX uq_email ON t (email);           -- 唯一（NULL 不参与唯一性）
CREATE INDEX idx_h ON t (x) USING HASH;              -- 哈希：只服务等值点查
CREATE INDEX idx_ts ON t (created_at) USING MINMAX;  -- 跳数索引：块级剪枝
CREATE INDEX idx_s  ON t (status) USING SET;         -- 低基数列的等值剪枝
CREATE INDEX idx_p  ON t (payload) USING BLOOM;
CREATE INDEX idx_n  ON t (name) USING NGRAM;         -- LIKE '前缀%'
CREATE FULLTEXT INDEX ft ON articles (body) WITH PARSER ngram;  -- 全文（中文必须 ngram）
```

**跳数索引怎么用才对**：它不返回行，只回答"这个块可能含我要的值吗"，
所以**数据要按该列聚集**才有效（按时间递增写入是最典型的场景）。
写入完全乱序时每个块的 min/max 覆盖全域，索引退化为"永远剪不掉"。

**全文检索**：

```sql
SELECT id FROM articles WHERE MATCH(body) AGAINST('storage engine');
SELECT id FROM articles WHERE MATCH(body) AGAINST('+quick -rabbit' IN BOOLEAN MODE);
SELECT id FROM articles WHERE MATCH(body) AGAINST('"quick brown"' IN BOOLEAN MODE);
```

- `MATCH` 是**访问路径**而不是逐行谓词（BM25 需要全语料统计量），执行器先用倒排索引取候选。
- 索引在**事务提交时**更新（未提交的行检索不到，这是刻意的）。
- 没有 `LIMIT` 时最多返回 1000 条候选。要全部命中请显式写 `LIMIT` 或分批取。

---

## 九、分区

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

支持 `RANGE` / `LIST` / `HASH` / `KEY`。**分区裁剪**让区间外的查询读 0 个对象——
这是对象存储上最划算的优化之一：按时间分区，冷分区永远不会被读。

`DROP PARTITION` 是**成本 ≈ 0 的冷却手段**（删元数据，不搬数据），
比 DELETE 或归档到低频存储都便宜。

---

## 十、时间旅行、备份与恢复

```sql
SELECT * FROM t AS OF TIMESTAMP '2026-02-01 10:00:00';   -- 读历史版本
SELECT * FROM t AS OF TSO 471234567890123456;
```

- 能读多久取决于 **GC 水位**（默认保留窗口见 `yb-cli gc` 报告）；越水位**明确报错**，不会返回残缺结果。
- 备份 / 恢复 / PITR 见 [运维命令行](#十四运维命令行)。

---

## 十一、CDC 变更流

变更流**直接读已有的 WAL**（不维护第二份日志），游标可以持久化：

```bash
./gradlew :yb-cli:cli -Pargs="cdc --endpoint http://127.0.0.1:9000 --bucket yangbase --from-ts 471234567890123456"
```

---

## 十二、多节点部署

### 单 CN（最简单，也是默认）

```bash
./gradlew :yb-server:run --args="--s3-bucket b --coord-mode single ..."
```

协调由部署方式保证：时间戳走对象存储水位，租约与锁退化为进程内（只有一个写者）。
⚠️ 前提是**各 CN 时钟大致同步**（`commit_ts` 高位取本机毫秒）。

### 多 CN

| 模式 | 机制 | 常驻成本 | 适用 |
|---|---|---|---|
| `objectstore` | 全程用对象存储条件写做 CAS 授时/选举/行锁 | 0 | 多 CN、能接受每次协调 2–6 次请求 |
| `ms` | 3 节点元数据服务（Raft） | 3 台小规格 | 多 CN、行锁密集、追求低延迟 |

```bash
# 三个 MS 节点（各自一台机器）
./gradlew :yb-server:run --args="--role ms --ms-node-id ms-1 \
  --ms-peers 10.0.0.1:9000,10.0.0.2:9000,10.0.0.3:9000 \
  --ms-advertise 10.0.0.1:9000 --ms-bind-port 9000 --s3-bucket b ..."

# CN 连过去
./gradlew :yb-server:run --args="--coord-mode ms \
  --ms-peers 10.0.0.1:9000,10.0.0.2:9000,10.0.0.3:9000 --s3-bucket b ..."
```

- 三种模式的**契约相同**，差别只在延迟、成本与故障表现（实测数字见 compatibility.md §4.4.2）。
- `ms` 换领导者会**丢掉行锁**（锁表在 MS 内存里）：持锁事务提交前自检，发现锁没了报 1213 让事务重试——**最坏是重试，不会静默丢更新**。
- `ms` 全部不可用时：CN 用已取到的时间戳区间继续服务读写，区间用尽后写事务明确失败，只读仍可继续。
- **不要调小** `--ms-election-timeout-ms`：MS 的日志在对象存储上，超时太小会让集群在负载下反复重新选举。
- **空闲分片会被淘汰**（默认 15 分钟）：打开过的分区不会永远常驻——每个打开的分片
  都要定期续租（`objectstore` 模式约 480 次 PUT/小时/分片），一个 128 分区的表扫一遍
  就是一笔持续的固定开销。淘汰之后下次访问重新打开（多几次请求，不影响正确性）。
  用 `--shard-idle-evict-min` 调整，`0` 表示不淘汰（只适合单机调试）。

### 在阿里云 OSS 上部署

启动命令与四个必需参数的逐条解释在
[第二章的「启动一个阿里云 OSS 实例」](#启动一个阿里云-oss-实例)，这里只补部署要点：

- **协调模式只能用 `single` 或 `ms`**——`objectstore` 模式的租约与行锁建立在
  ETag CAS 上，而 OSS 没有（实测 `If-Match` → 400 `NotImplemented`）。
- **MS 的选举超时别调小**：OSS 上 MS 的每次日志追加都是一次真实 PUT（几十毫秒），
  超时太小会在负载下反复换领导者，每次换都丢掉锁表（§4.4）。
- **`--i-accept-nonatomic-pointer-switch` 不是 CAS**：它把"指针切换失去原子性"
  降级成"可发现 + 可恢复"（写后回读校验 + 接管时审计孤儿快照，指标
  `yb_manifest_orphan_snapshots_total`）。单 CN 下这条风险由部署方式兜住；
  多 CN 场景请当作已知边界，论证见设计文档 §3.3。

---

## 十三、可观测性

```bash
./gradlew :yb-server:run --args="--metrics-port 9104 ..."
curl -s localhost:9104/healthz
curl -s localhost:9104/metrics | grep yangbase_
```

- `/metrics`（Prometheus 文本格式）与 `/healthz` **不产生对象存储请求**——
  可以放心高频抓取。
- 关键指标：对象存储请求数（按 `op` 与 `cached` 分标签）、查询耗时、慢查询计数、
  连接数、锁等待、缓存命中率。
- **慢查询日志**：超过 `--slow-query-ms`（默认 500ms）的语句会打一行，包含
  SQL、耗时、**对象存储读取明细**、行数——回答"为什么这个查询读了 500 个对象"。
- **对象存储审计日志**：DDL 与 GC 会写 `ops/audit/{date}.jsonl`（批量异步，失败不影响主流程）。
  用 `--no-audit` 关闭。

---

## 十四、运维命令行

`yb-cli` **直接操作对象存储，不需要 CN 在线**（计算层随时可以销毁，运维动作不该依赖它）。

```bash
# 存储里有什么（按前缀归组）
./gradlew :yb-cli:cli -Pargs="ls --endpoint http://127.0.0.1:9000 --bucket yangbase"

# 每个分片多大、多少行
./gradlew :yb-cli:cli -Pargs="shards --endpoint ... --bucket ..."

# 垃圾回收：默认**只标记不删除**，加 --delete 才真删
./gradlew :yb-cli:cli -Pargs="gc --endpoint ... --bucket ..."
./gradlew :yb-cli:cli -Pargs="gc --delete --endpoint ... --bucket ..."
# ⚠️ --window-hours 有 1 小时下限：窗口是保护"刚写入但还没进 manifest"的对象的
#    唯一机制，调得太小会把在途对象当垃圾删掉。要绕过必须显式写
#    --i-know-what-im-doing（它不会问你第二次）

# 恢复路径健康检查：有没有被 manifest 引用但已丢失的对象
./gradlew :yb-cli:cli -Pargs="recover-check --endpoint ... --bucket ..."

# 按设计文档 §13 的成本模型估算月度费用
./gradlew :yb-cli:cli -Pargs="cost --endpoint ... --bucket ..."

# 备份（只写清单，成本与数据量无关）/ 列出备份 / 恢复（--yes 才真做）
./gradlew :yb-cli:cli -Pargs="backup --note pre-upgrade --endpoint ... --bucket ..."
./gradlew :yb-cli:cli -Pargs="backups --endpoint ... --bucket ..."
./gradlew :yb-cli:cli -Pargs="restore --yes --endpoint ... --bucket ..."

# 时间点恢复：把某张表切回某个时刻（--yes 才真做）
# ⚠️ 时刻里用 T 而不是空格：-Pargs 按空格切分参数，写 "2026-02-01 10:00:00"
#    会被切成两个参数，只有前半段被 --to 吃到（静默变成当天 00:00:00）
./gradlew :yb-cli:cli -Pargs="pitr --table users --to 2026-02-01T10:00:00 --yes --endpoint ..."

# 变更流：直接读已有 WAL，没有第二份变更日志
./gradlew :yb-cli:cli -Pargs="cdc --since 2026-02-01T10:00:00 --limit 100 --endpoint ..."

# 迁移风险扫描：目标 SQL 里哪些部分本系统不支持、或语义与 MySQL 不同
./gradlew :yb-cli:cli -Pargs="analyze-sql --path ./migrations"
```

> 运行 `./gradlew :yb-cli:cli -Pargs="--help"` 看完整参数。

**GC 是两阶段的**：先标记、写清单，**安全窗口（默认 24 小时）之后**才真正删除。
窗口的作用不是"等读者读完"，而是把误判的代价从"静默读到错数据"降级为"多占一天存储"。

**`ls` 与 `cost` 不会逐对象发 `head`**：对象大小取自 LIST 响应本身（0 次额外请求）。
一个十万对象的桶，逐对象 `head` 会让"做一次容量评估"比一整天的业务请求还贵——
所以这条命令自己也要守 P4 的预算。后端在 LIST 里不返回大小时，它会退化成
**有上限的采样+外推**，并把"估算值"三个字印在输出里。

---

## 十五、性能与成本

**这个数据库的成本主线是"请求数"，不是"延迟"。** 记住三个数：

| 场景 | 量级 | 对象存储请求 |
|---|---|---|
| 点查（缓存命中） | 几十微秒 | **0** |
| 点查（冷缓存） | 百微秒级 | **1 次 GET** |
| `COUNT(*)` / `WHERE col = 常量` 计数 | 亚毫秒 | **0**（元数据路径） |
| 单行提交（`sync`） | 几毫秒 | 1 次 PUT（组提交后摊薄） |
| 范围扫描 | 每块一次 GET | 并行预取 |

**省请求的三条实操建议**：

1. **`COUNT(*)` 与等值计数不要自己写全表扫描**——本系统走元数据，0 次 GET，
   `EXPLAIN` 会显示 `type: METADATA_COUNT`。
2. **按时间分区**：冷分区 `DROP PARTITION` 的成本≈0，而 DELETE 要遍历。
3. **开本地 NVMe 二级缓存**（`--disk-cache-dir`）：CN 重启后"重新变热"只读本地盘。

**冷热差是几十倍的量级**，这是对象存储作为数据层的物理下界。
所以 SLO 请分"缓存命中"与"未命中"两栏陈述。

---

## 十六、从 MySQL 迁移的检查清单

- [ ] **应用是否依赖 `AUTO_INCREMENT` 连续**？→ 本系统**不保证连续**（分段分配，重启/多节点会跳号）。
- [ ] **是否依赖写偏斜被防止**？→ 快照隔离**不防**，请加 `FOR UPDATE`。
- [ ] **是否依赖字符串大小写不敏感比较**？→ 本系统按**二进制序**（`WHERE name = 'ABC'` 匹配不到 `'abc'`）。
- [ ] **是否用 `DECIMAL` 存金额**？→ 可以，按声明标度舍入。
- [ ] 是否依赖 **`VARCHAR(n)` 超长静默截断**或**整数溢出回绕**？→ 本系统**报错**（1406 / 1264），比 MySQL 非严格模式严格。
- [ ] 是否用 **存储过程 / 触发器 / 事件 / 外键级联**？→ 不支持（外键约束可声明并校验，级联不会发生）。
- [ ] 是否用 **`REPLACE INTO` / `INSERT IGNORE` / `ON DUPLICATE KEY UPDATE`**？→ 支持。注意 `INSERT IGNORE` 遇到**值错误**（超长、类型转不过去）时是**跳过该行**，MySQL 是写入"最接近的值"。
- [ ] 是否用 **`INSERT ... SELECT`**？→ 支持。
- [ ] 是否用 **`SAVEPOINT` / 派生表 / 多表 UPDATE**？→ **不支持**，需要改写。
- [ ] 连接串是否设了 **`serverTimezone=UTC`**？
- [ ] 是否依赖 **`NOW()` 在语句内变化**？→ 它是语句级常量。
- [ ] 是否有 **`/*! ... */` 版本注释包裹的 SQL**（mysqldump 产物）？→ **其中的语句会被执行**（与 MySQL 一致）；普通块注释仍然只是注释。
- [ ] 是否用 **`mysqldump` / `mysql` 命令行**做备份？→ ✅ 已实测可用（`mysqldump` 的导出能直接灌回本系统）。
- [ ] 备份/恢复流程是否改成 **`yb-cli backup` / `pitr`**？

> 这份清单与 [compatibility.md](compatibility.md) 是同源的，但那边更全（含每条差异的验证方式与出错时的表现）。
> **迁移前请完整读一遍 compatibility.md。**
