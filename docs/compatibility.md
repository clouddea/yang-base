# YangBase 兼容性与限制清单

> **迁移之前请先读这一页。** 它的存在本身就是设计的一部分：设计文档 §11.3 要求
> 「明确的限制清单**必须写进用户文档**」，而 `docs/README.md` 里那条贯穿性的教训是
> ——**「协议兼容」不等于「语义兼容」**。Bufstream 通过了 Jepsen 的存储正确性验证，
> 却仍然继承了 Kafka 的事务隔离缺陷。把差异写清楚，比多支持几个语法重要得多。
>
> 本文件对应设计文档 §11.2（兼容性矩阵）与 §11.3（限制清单），
> 每一条都标注了**它是怎么被验证的**，以及**出错时你会看到什么**。

---

> **2026-09-27 SQL 回归修复**：DROP PARTITION 保留物理分片编号及二级索引一致性；
> INSERT 对遗漏列应用 DEFAULT；JOIN 的 GROUP BY/HAVING/ORDER BY 识别输出别名；
> LAST_INSERT_ID() 保留本语句首个成功插入的自增值；VARBINARY 与 HEX 保留原始字节。

## 一、一句话版本

**能让 `mysqldump` 与 Connector/J 正常跑通；单表查询、聚合、连接、并集、子查询都能用；
会让依赖 InnoDB 加锁语义、`AUTO_INCREMENT` 连续性、或外键级联的应用出现行为差异。**

---

## 二、支持的客户端与工具

| 客户端/工具 | 状态 | 依赖了什么 |
|---|---|---|
| **MySQL Connector/J 8.x** | ✅ | 主要测试目标；JDBC 端到端用例覆盖 |
| MariaDB Connector/J | ⚠️ 未实测 | 认证协商路径相同 |
| HikariCP / Druid 连接池 | ✅ | `COM_PING`、`COM_RESET_CONNECTION` |
| MyBatis / MyBatis-Plus | ✅ | `information_schema`、`LAST_INSERT_ID()` |
| `mysql` 命令行客户端 | ✅ **已实测**（2026-09） | 握手、`show databases`、建表插入查询、`\G` 输出；**应答包序号必须与客户端连续**——libmysqlclient 严格校验，Connector/J 则宽容（曾经因此只有 JDBC 能用） |
| `mysqldump` | ✅ **已实测**（2026-09） | **导出 → 灌进一个全新实例 → 逐字段对照**全部通过（含中文、`DECIMAL`、`NULL`、`DATE`、`BLOB`、`ENUM`、视图、主键与二级索引）。为此补齐：`SHOW TABLE STATUS`（18 列）· `SHOW FULL TABLES` 的 `Table_type` 列 · `SHOW TRIGGERS`（空集）· 用户变量 · `@@collation_database` 等变量 · `ALTER TABLE … ENABLE/DISABLE KEYS`（空操作）· `SET x = 裸标识符` |
| `mysql` CLI | ✅ | |
| Navicat / DataGrip / DBeaver | ⚠️ **未实测** | 需要 `information_schema` 完整；Navicat 在**没有任何 schema** 时连不上（见下） |
| Spring Data JPA / Hibernate | ⚠️ 未实测 | 需要更精确的 `information_schema` 与 `SHOW` |
| `mysqlbinlog` / 主从复制 | ❌ | 无 binlog；变更订阅请用 `yb-cli cdc` |
| X Protocol / MySQL Shell | ❌ | 未实现 |

### 图形化客户端连不上时怎么办

Navicat 这类工具在**连接期**就会发元数据查询，而一个全新的实例里一个数据库都没有，
于是它拿到空结果后直接判定连接失败（这是 §11.1.1 记录的陷阱 T4）。
绕过办法：先用命令行建一个库。

```sql
CREATE DATABASE app;   -- 之后图形化客户端即可正常连接
```

DataGrip 建议开启 *introspect using JDBC metadata*。

---

## 三、不支持，且**会明确报错**（不会静默失败）

以下功能对应的语句会返回明确的错误码（多为 `1235 ER_NOT_SUPPORTED_YET`），
**不会**出现「看起来成功了、其实什么也没做」。这条区分很重要：静默的无操作
比报错危险得多——`SELECT ... INTO OUTFILE` 如果静默成功，用户会以为文件已经写出去了。

- 存储过程、函数、触发器、事件调度器、游标
- 视图的可更新性（视图本身可以只读查询）
- 外键的 `ON DELETE CASCADE`（约束可声明并校验，**级联不会自动发生**）
- `LOAD DATA INFILE`（本地文件路径）
- `SELECT ... INTO OUTFILE / DUMPFILE`
- 空间索引与 `GEOMETRY` 函数
- `EXPLAIN ANALYZE`（会真的执行语句，因此明确拒绝而不是假装支持）
- 复制相关：`CHANGE MASTER TO`、`START SLAVE`、`binlog`
- `ALTER TABLE` 中**需要重写数据**的操作：`MODIFY COLUMN`、中间插入列、删中间列
  （无重写的场景——末尾加列、删末尾列、加删索引、改名——全部支持且是 O(1)）

### 少数「成功的无操作」

`LOCK TABLES` / `UNLOCK TABLES` / `FLUSH TABLES` 返回**成功但不做任何事**。
这是刻意的，不是遗漏：`mysqldump` 的固定开场就是 `LOCK TABLES ... WRITE`，
拒绝它会让 mysqldump 完全不可用；而本系统的写者身份是**分片租约**
（任一时刻只有一个写者）、读是**快照读**，没有「锁表」能提供的额外保证。
反过来，假装加锁才是危险的——那会让调用方以为自己被保护了。

---

## 四、语义有差异（**这一节是重点**）

### 4.0 SQL 能力面（**先看这节判断能不能迁**）

| 能力 | 状态 | 说明 |
|---|---|---|
| 单表 `SELECT` / `WHERE` / `ORDER BY` / `LIMIT` | ✅ | SELECT 列别名支持单引号（如 `id AS 'name'`，也可省略 AS）；其它位置的单引号仍表示字符串字面量 |
| 聚合 `COUNT` / `SUM` / `AVG` / `MIN` / `MAX` / `GROUP_CONCAT` | ✅ | `DISTINCT` 可用；**忽略 NULL**（`COUNT(列)` 只数非空） |
| `GROUP BY` / `HAVING` | ✅ | **强制 `ONLY_FULL_GROUP_BY`**（见下） |
| `JOIN`（`INNER` / `LEFT` / `RIGHT` / `CROSS` / 逗号） | ✅ | 等值连接走哈希，非等值回退双层循环；`USING` / `NATURAL` 明确报错 |
| `UNION` / `UNION ALL` | ✅ | 末尾可带 `ORDER BY` / `LIMIT`（作用于整个并集）；写在分支里报错（见下）；**去重前按左侧分支的类型归一**（`SELECT 10 UNION SELECT '10'` 是 1 行，与 MySQL 一致），`UNION ALL` 不做归一 |
| `ORDER BY <序号>` | ✅ | `ORDER BY 1` 指第 1 个输出列（不是常量） |
| `SELECT DISTINCT` | ✅ | 去重在 `LIMIT` **之前**；`DISTINCT` 下按结果里没有的列排序报 3065（与 MySQL 一致） |
| 非相关子查询（标量 / `IN` / `EXISTS`） | ✅ | |
| **相关子查询**（谓词位置：`EXISTS` / `NOT EXISTS` / `IN` / `NOT IN`） | ✅ | 改写成半/反连接，**内表只扫一次**；`NOT IN` 的 NULL 语义与 MySQL 一致 |
| **相关子查询**（标量位置：`SELECT (SELECT … WHERE i.k = o.k) FROM o`） | ✅ | 内层按关联键**扫一次**建表，外层每行查表；`ORDER BY` / `LIMIT 1` / `DISTINCT` 都可用 |
| **窗口函数** | ✅ | `ROW_NUMBER` / `RANK` / `DENSE_RANK` / `PERCENT_RANK` / `CUME_DIST` / `NTILE` / `LAG` / `LEAD` / `FIRST_VALUE` / `LAST_VALUE` / `NTH_VALUE` + `SUM` / `AVG` / `COUNT` / `MIN` / `MAX`；`PARTITION BY` / `ORDER BY` / `ROWS` / `RANGE` 帧见下 |
| **视图** | ✅ **只读** | `CREATE [OR REPLACE] VIEW` / `DROP VIEW` / `SHOW CREATE VIEW` / `information_schema.VIEWS`；**对它写入会报错** |
| `COUNT(*)` / `col = 常量` 的计数 | ✅ | **0 次数据读取**（§8.9），这条最值得利用 |
| 索引（二级 / 哈希 / 唯一 / 复合 / 全文 / 跳数） | ✅ | 唯一约束、`MATCH ... AGAINST` 可用 |
| **全文检索的候选上限** | ⚠️ | 没有 `LIMIT` 时最多返回 **1000** 条候选（回表前截断）。要全部命中请显式写 `LIMIT`，或分批取 |
| `ALTER TABLE`（末尾加列 / 删末尾列 / 加删索引 / 改名 / `TRUNCATE`） | ✅ | O(1)，无重写 |
| 时间旅行 `AS OF TIMESTAMP/TSO`、`ANALYZE`、`EXPLAIN` | ✅ | |
| **`GROUP BY` 下选非分组列** | ❌ **报 1055** | 见下，这是**有意**的 |
| 藏在 `OR` 分支里的相关子查询 | ❌ 明确报错 | `OR` 两侧不能改写成连接（可拆成 `UNION`） |
| 非等值关联的相关子查询 | ❌ 明确报错 | 只能双层循环，代价是行数之积 |
| 视图参与 `JOIN` | ❌ 明确报错 | 需要派生表，暂不支持 |
| `GROUP_CONCAT` 作为**窗口**函数 | ❌ 明确报错 | 其它聚合都可开窗 |
| `EXPLAIN` 显示窗口函数 | ❌ | 窗口函数照常执行，只是计划里不额外标注 |
| `SELECT ... FOR UPDATE`、`LOCK IN SHARE MODE` | ✅ | 行锁走协调服务（§4.4）；锁在**事务提交/回滚**时释放，autocommit 下随语句结束释放；等待超时按 `innodb_lock_wait_timeout`（默认 50 秒）报 1205。锁有 TTL（默认取 2 × 等待上限，下限 30 秒）：**连接异常断开**时锁不会立刻释放，最多 TTL 后自动失效——所以长事务请同时调大 `innodb_lock_wait_timeout`（TTL 跟着变长） |
| `FOR UPDATE` 的修饰符（`NOWAIT` / `SKIP LOCKED` / `OF t`） | ❌ 明确报错 | 它们都会改变加锁语义，静默忽略等于给了一个"看起来加锁了"的事务 |
| `FOR UPDATE` 用在 JOIN / 视图上 | ❌ 明确报错 | 本系统的行锁按**基表主键**加锁，多表连接的语义没有定义——宁可报错也不给"只锁一半"的实现 |

#### 强制 `ONLY_FULL_GROUP_BY`（与 MySQL 5.7+ 默认一致）

`SELECT city, name FROM t GROUP BY city` 会**报错**（1055），而不是"每组随便挑一行"。

MySQL 关掉这个规则时的行为是**不确定**的：同一查询在不同时刻可能返回不同的 `name`，
而 SQL 层看起来一切正常。本系统的 `sql_mode` 如实回报 `ONLY_FULL_GROUP_BY`，
所以这里真的强制它——**报了却不做，等于用"看起来更兼容"换一个静默的不确定结果**。

#### `UNION` 的分支不能带 `ORDER BY` / `LIMIT`

`A UNION B ORDER BY x` 在 MySQL 里 `ORDER BY` 作用于**整个并集**；
本系统的子句解析挂在各个 `SELECT` 上，照直实现会**只排右边那一半然后拼接**
（排序错了但结果看起来正常）。所以这种情况**明确报错**，请拆成两条查询在应用层合并。

#### 窗口函数

求值位置与 MySQL 一致：`WHERE` → `GROUP BY` → 聚合 → `HAVING` → **窗口函数** → `SELECT`
→ `DISTINCT` → `ORDER BY` → `LIMIT`。因此：

- 它**不改变行数**（`SELECT ROW_NUMBER() OVER () FROM t` 仍是每行一行）；
- 有 `GROUP BY` 时它作用在**分组之后**的行上，于是 `SUM(SUM(v)) OVER (ORDER BY x)` 合法；
- 可以出现在 `ORDER BY` 里（别名也行）；写在 `WHERE` / `HAVING` / `GROUP BY` 里**报错**。

| 帧写法 | 支持 |
|---|---|
| 不写帧，窗口里有 `ORDER BY` | ✅ 默认帧 = `RANGE UNBOUNDED PRECEDING → CURRENT ROW`（含同键行） |
| 不写帧，窗口里没有 `ORDER BY` | ✅ 整个分区 |
| `ROWS BETWEEN … AND …`（`UNBOUNDED` / `CURRENT ROW` / `n PRECEDING` / `n FOLLOWING`） | ✅ |
| `RANGE` + `UNBOUNDED` / `CURRENT ROW` | ✅ |
| `RANGE n PRECEDING` / `n FOLLOWING` | ❌ 报错（要按**排序键的值**做区间运算，不是数行数；按 ROWS 算会给出错行） |
| `GROUPS` | ❌ 报错 |

`RANK` / `DENSE_RANK` / `LAG` / `LEAD` / `NTILE` 没有 `ORDER BY` 时报错——
"第几行"没有顺序就没有定义，按扫描顺序给一个答案会随执行计划变化。
`LAST_VALUE` 的默认帧到**当前行**为止（与 MySQL 一致），要整个分区的最后一个值
必须显式写 `ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING`。

#### 子查询

| 形态 | 状态 |
|---|---|
| `WHERE id IN (SELECT …)` / `EXISTS (SELECT …)` | ✅ |
| `WHERE EXISTS (SELECT … WHERE inner.k = outer.k)` | ✅ **改写成半连接**（内表只扫一次） |
| `NOT EXISTS` / 相关 `NOT IN` | ✅ 改写成反连接，`NOT IN` 的 NULL 语义与 MySQL 一致 |
| `SELECT (SELECT … WHERE inner.k = outer.k) FROM …`（标量位置） | ✅ **内层按关联键扫一次建表**，外层每行查表；`MAX`/`COUNT` 等聚合、`ORDER BY … LIMIT 1`、`DISTINCT` 都可用 |
| `WHERE x = 1 OR EXISTS (…)` | ❌ 报错（`OR` 两侧不能改写成连接） |
| 非等值关联（`ON inner.x > outer.x`） | ❌ 报错（只能双层循环 → 行数之积） |

**为什么不逐行驱动**：相关子查询也可以"外层每行执行一次子查询"来实现，
它覆盖的形状更全，但代价是 `外层行数 × 一次内表扫描`——在对象存储上，
一千行的外层表就是一千次内表扫描。本系统宁可对不能改写的形状**明确报错**。

#### 视图是**只读**的

```sql
CREATE VIEW v AS SELECT id, name FROM users WHERE city = 'bj';
SELECT * FROM v WHERE id = 10;      -- ✅ 外层的 WHERE 叠加在视图结果之上
INSERT INTO v VALUES (1, 'x');      -- ✗ 1347 'v' is not BASE TABLE
```

**外层的谓词不会下推进视图**：本系统没有派生表这一层，所以视图是"先算完、再套外层子句"。
`SELECT * FROM v WHERE id = 1` 会先把视图整个算出来再过滤——视图内层若是一次全表扫描，
那笔代价就付了。之所以仍然这样做：它不会给出错误结果，而"展开成派生表"需要动查询规划器。
（视图也**不能**参与 `JOIN`，原因相同。）

#### `UNION` 的 `ORDER BY` / `LIMIT` 写在最后

```sql
SELECT a FROM t1 UNION SELECT b FROM t2 ORDER BY 1;   -- ✅ 作用于整个并集
SELECT a FROM t1 ORDER BY a UNION SELECT b FROM t2;   -- ✗ 报错
```

写在**最后一个分支之后**是合法的（作用于整个并集）；写在某个分支里报错——
MySQL 里那也是语法错误，而本系统照直实现会"只排右边那一半然后拼接"。

### 4.1 隔离级别：默认**不防写偏斜**（迁移风险最高）

| | MySQL / InnoDB 默认 | YangBase |
|---|---|---|
| 隔离级别 | `REPEATABLE READ` | 快照隔离（对外同样报告 `REPEATABLE-READ`） |
| 幻读 | next-key lock 挡住 | 快照隔离本就无幻读 |
| **写偏斜** | next-key lock 在很大程度上避免 | ⚠️ **不防** |

**写偏斜是什么**：两个事务各自读了**对方将要修改**的数据，然后各自写入，
结果违反了一个本应由应用层维持的不变量。经典例子是「双人值班」——
两个事务同时检查「至少还有两人在岗」，然后各自把自己调走，最后没人值班。

**怎么办**：任何依赖读写交集的不变量，都必须在应用层用显式锁
（`SELECT ... FOR UPDATE`）或唯一约束来保证，不要依赖隔离级别。
`SHOW VARIABLES LIKE 'yangbase_isolation_detail'` 会如实回报 `snapshot-isolation`。

**`SET TRANSACTION ISOLATION LEVEL …` 会被收下，但降级会被如实回报**：
连接池与 Connector/J 的 `transactionIsolation` 配置都会在连接建立时发这条语句，
拒绝它会让客户端连不上，所以本系统**接受**它——但不会假装换了隔离级别。
请求 `READ COMMITTED` / `READ UNCOMMITTED` 时（这两档本系统没有实现），
`SELECT @@yangbase_isolation_degraded` 会返回被请求的那一档（例如 `READ-COMMITTED`），
同时服务端日志打一行警告；请求的正是本系统提供的那一档（`REPEATABLE-READ` /
`SERIALIZABLE`）时它返回空串。**把这个变量接进你的启动自检**，
就能在连接池配错时立刻发现，而不是等到某次对账数字对不上。

### 4.2 `AUTO_INCREMENT` 不保证连续（§11.4）

自增主键采用**分段分配**：每个 CN 一次领一段（默认 1000 个），用完再领。

| 场景 | MySQL | YangBase |
|---|---|---|
| 单连接连续插入 | 1, 2, 3, … | 1, 2, 3, … |
| 事务回滚 | 留下空洞 | 留下空洞 |
| **CN 重启 / 多 CN 交替插入** | 仍连续 | **会跳跃**（段与段之间可能差很多） |
| `TRUNCATE TABLE` | 重置为 1 | 重置为 1 |

这与 MySQL 8.0 的 `innodb_autoinc_lock_mode=2`（交错模式）一致，
MySQL 文档明确说明该模式下自增值不保证连续。

⚠️ **依赖 `AUTO_INCREMENT` 连续性的业务会受影响**——例如用它做「第 N 个用户」的编号。
需要连续编号请自己在应用层维护，或改用显式的业务编号列。

### 4.3 `ENUM` / `SET` 按**字符串**比较，不按声明序号

```sql
CREATE TABLE t (s ENUM('z','a'));   -- 声明顺序与字典序不一致
```

| | MySQL | YangBase |
|---|---|---|
| `'z' < 'a'`? | **真**（比序号 1 < 2） | **假**（比字符串） |
| `ORDER BY s` | 按声明顺序 | 按字典序 |
| 存储 | 1–2 字节序号 | 字符串（有压缩兜底） |
| `SET` 的显示顺序 | 恒为声明顺序 | 恒为声明顺序（✅ 一致） |

**为什么不做成序号**：序号语义要求比较时能拿到列定义，而比较发生在求值器里——
那里只有 `Datum`。两处不一致（索引路径按序号、扫描路径按字符串）会给出
**不同的顺序且不报错**，那比一处差异更危险。

**服务端会主动警告**：建表时若发现声明顺序不是字典序，会向 stderr 打一条警告，
指出该列的比较语义与 MySQL 不同。把声明写成字典序即可完全避开这个差异。

### 4.3.1 字符串比较按**二进制序**（等价 `utf8mb4_bin`）

这是最容易在迁移后**静默改变结果集**的一条，而且它影响的不止 `WHERE`：

| | MySQL 8 默认（`utf8mb4_0900_ai_ci`） | YangBase |
|---|---|---|
| `'ABC' = 'abc'` | **真**（大小写不敏感） | **假**（按字节比较） |
| `WHERE name = 'ABC'` | 匹配 `'abc'`、`'Abc'`… | **只匹配 `'ABC'`** |
| `ORDER BY name` | 大小写混排（`a` 与 `A` 相邻） | 大写字母的字节值更小，`'Z' < 'a'` |
| `DISTINCT` / `GROUP BY` / 唯一索引 | `'ABC'` 与 `'abc'` 是**同一个**值 | 是**两个**值 |
| 尾部空格 | `'a' = 'a '`（PAD SPACE 语义下相等） | 不相等（逐字节比较，不补空格） |

**怎么办**：需要大小写不敏感的等值/分组语义时，在应用层归一化（例如统一存小写，
或另存一列归一化后的键），不要依赖服务端的排序规则。想确认某段 SQL 有没有踩到，
用 `yb-cli analyze-sql --path <目录>`：它会把带 `COLLATE`（非 `_bin`）的写法报成
「语义不同」，并把 `SET NAMES` 非 utf8mb4 一并列出来。

> ⚠️ **当前上报值与实际语义不一致，按实际语义理解。**
> 握手包、列定义与 `SHOW VARIABLES` 里回报的排序规则名是 `utf8mb4_0900_ai_ci`
> （`SessionCharset.UTF8MB4`、`ColumnSchema.collation` 的默认值，也是 MySQL 8 的默认名），
> 而 **实际比较一律按二进制序**（`Datum.compareTo` 直接比字符串、`KeyEncoder` 是保序字节编码）。
> 换句话说：**客户端从协议里读到的是「大小写不敏感」，得到的却是「大小写敏感」的行为**。
> 上报值是为了让客户端与 introspection 工具不因为一个陌生的 collation 名而出错，
> 但它确实与语义不符——请以本节的表格为准，不要以 `SHOW VARIABLES` 的回报为准。
> （`utf8mb4_0900_ai_ci` 本身**未实现**；列上的 `COLLATE` 声明会被解析但不改变比较行为。）

### 4.4 `JSON` 按文本存取

- 以**文本**形式存储，写入时校验合法性（非法 JSON 报错，与 MySQL 严格模式一致）。
- **没有**二进制 JSON、**没有** JSON 路径索引；比较与排序按文本
  （MySQL 按规范化后的 JSON，键顺序不同可能不等）。
- 函数：`JSON_EXTRACT` / `->` / `->>` / `JSON_UNQUOTE` / `JSON_VALID` / `JSON_TYPE` /
  `JSON_LENGTH` / `JSON_KEYS`（`JSON_KEYS` 的键序同 MySQL：先按键长、同长按键本身）。
  写函数（`JSON_SET` / `JSON_INSERT` / …）与 `JSON_TABLE`、`JSON_ARRAYAGG` 未实现。
- 路径不存在给 `NULL`，路径**语法错报 3143**——拼错的路径若静默给 NULL，
  看起来就像"这一行没有那个字段"。`->` 与 `->>` 的区别是**类型**：
  `->` 给 JSON（字符串带引号），`->>` 给去引号的文本，所以按值过滤要用 `->>`。

### 4.4.1 整数：溢出**报错**，不回绕

`SELECT 9223372036854775807 + 1` 报 **1690**（`BIGINT value is out of range`），
而不是回绕成 `-9223372036854775808`。回绕给出的数字**看起来完全正常**，
只是符号反了——使用者拿它做对账时不会想到去怀疑这一行。这与 `SUM` 的立场一致
（无 `DECIMAL` 累加类型，溢出即报错）。

- `+` / `-` / `*` 在整数上检查溢出；`/` 一律按浮点算（`5 / 2` = 2.5，与 MySQL 一致），
  所以整数除法没有溢出一说。
- **超出 `BIGINT` 的整数字面量直接报错**：`9223372036854775809` 不会退化成一个
  丢了精度的浮点（MySQL 把它当 `DECIMAL`；本系统没有 `DECIMAL` 字面量）。
- `-9223372036854775808` 是合法字面量，且**精确**（负号在解析期就折进字面量）。

### 4.4.2 协调模式：三种，部署时选择（§4.7）

时间戳、分片写者租约、行锁、catalog 失效判据这四件事**必须有一个全系统唯一的判断**，
它们由同一个接口（`CoordinationService`）提供，三种实现由 `--coord-mode` 选择：

| 模式 | 机制 | 常驻成本 | 适用 |
|---|---|---|---|
| `single`（默认） | 进程内：时间戳走对象存储水位，租约与锁由**部署方式**保证（单 CN） | 0 | 单 CN 部署 |
| `objectstore` | 全程用对象存储的条件写做 CAS 授时、选举、行锁 | 0 | 多 CN，能接受每次提交 +1–2 个 RTT |
| `ms` | 3 节点元数据服务（Raft），TSO / 租约 / 锁 / DDL 串行化都走它 | 3 台小规格 | 多 CN，追求低延迟 |

两种多 CN 模式的**契约完全相同**（同一套一致性测试对它们各跑一遍），差别只在延迟、成本
与故障时的表现：

- **`objectstore` 没有死锁检测**：等待图需要一个中心节点看全貌，而这里没有。互相等待的两个
  事务只能等到 `innodb_lock_wait_timeout` 超时（1205）。`ms` 模式会立刻报 1213。
- **`ms` 换领导者会丢掉行锁**（锁表在 MS 内存里，§4.4 决定不持久化）。持锁事务在提交前自检，
  发现锁没了就回滚报 1213——**最坏是事务重试，不会静默丢更新**。
- **`ms` 不可用时**：CN 用已经取到的时间戳区间继续服务读写；区间用尽后写事务报错，
  而**只读事务**仍可继续（§4.2 的降级语义）。
- **`ms` 的时间戳窗口默认 1 秒**（`single` 模式是 64 秒）。窗口越大，跨 CN 的可见性
  延迟越大：本 CN 发出去的 `commit_ts` 最多可以超前另一个 CN 的 `read_ts` 一个窗口
  （见 4.4.3）。多 CN 部署下不要把这个值调大。
- ⚠️ **不走 MS 的多 CN 部署（`objectstore`）有一个绕不开的前提：各 CN 的时钟大致同步。**
  `commit_ts` 的高位取自本机物理毫秒（§4.2 要求 `commit_ts` 实时分配、不可预取，否则
  时间戳集合在可见性判断上不再稠密），对象存储只提供水位与 CAS，不会替你对时。
  有了 MS 就由它统一发号，这条假设随之消失——这也是多 CN 场景推荐 `ms` 的理由之一。

`--role ms` 让一个进程只跑元数据服务（不建引擎、不开 MySQL 端口），
`--run-meta-service` 让 CN 进程内顺带跑一个 MS 节点（单机部署与测试用）。

> ⚠️ **后端的条件写能力会限制可选的协调模式**（实测，2026-09-25）：
> **阿里云 OSS 没有 ETag CAS**（`If-Match` 返回 400 `NotImplemented`），
> 而 `objectstore` 模式的租约与行锁**正是建立在 CAS 之上**——在 OSS 上只能用
> `single`（单 CN）或 `ms`。另外 OSS 上写入还需要
> `--s3-oss --s3-path-style false --i-accept-nonatomic-pointer-switch`
> （最后一个是显式接受「协调写入没有原子性」——它覆盖**三处**：manifest 指针切换 ·
> 时间戳水位 · 自增段水位；三处都退化为「无前置条件 PUT + 写后回读校验」，
> 见用户手册第二章「启动一个阿里云 OSS 实例」与设计文档 §3.3）。
> ⚠️ **自增段那一处只在 `single`（单 CN）模式下可降级**：`ms` 模式下它的互斥完全
> 依赖这次 CAS（自增领段没有走 MS），降级 ⇒ 两个 CN 领到同一段 ⇒ 重复主键。
> 所以 OSS + `ms` 下 `AUTO_INCREMENT` 是**明确报错**的（不可用，但不是数据损坏）。

**协调操作的代价（微基准数字，见 `:yb-bench:benchCoordination`）**：

| 操作 | `single` | `objectstore` | `ms` |
|---|---|---|---|
| `SELECT ... FOR UPDATE` 加锁+解锁 | 22 µs（0 请求） | **98 ms（6 请求）** | 37 µs（0 请求） |
| 租约 acquire+release | 5 µs（0 请求） | 49 ms（4 请求） | 11 ms（6 请求） |
| 点查（稳态） | 370 µs（**0 请求**） | 244 µs（0 请求） | 206 µs（0 请求） |
| 单行提交 | 2.1 ms（1 次 PUT） | 1.3 ms（1 次 PUT） | 1.2 ms（1 次 PUT） |
| `SELECT ... FOR UPDATE`（端到端 SQL） | 535 µs（0 请求） | **133 ms（7 请求）** | 431 µs（0 请求） |

> 口径：单机内嵌 S3、p50。微秒级的数字里含每次采样的固定开销（约 10–20 µs），
> 读作「≤ 这个数」；毫秒级的那几格才是真实代价。**请求数是确定性的那一列。**

**结论：MS 不会让常规查询变快——它让「协调」不再走对象存储。** 点查与提交三种模式
完全一样（协调**不在**热路径上：时间戳整窗口预取）。差异全部在行锁与租约这类
"每次都要问一个权威方"的操作上：`ms` 用一次内存 RPC 换掉了 `objectstore` 的 2–6 次
对象存储请求，行锁因此快三个数量级。

反过来，`ms` 的**租约**（11 ms）比 `objectstore`（49 ms）快，但比 `single` 慢三个数量级
——因为租约与时间戳窗口都要进共识日志，每次都是一次真实 PUT（6 次请求 = 2 条日志 × 3 节点）。所以选择依据是**负载形状**：行锁密集（大量 `FOR UPDATE`）
选 `ms`；几乎不用 `FOR UPDATE`、又想要零常驻成本，选 `objectstore`。

> ⚠️ **MS 的选举超时默认是 500 ms–1 s，比"内存共识"的经验值大一个量级**——因为它的
> 日志落在对象存储上，一次追加就是一次 PUT。选举超时只有 150 ms 时，一次慢复制就足以
> 让跟随者发起选举：集群会在负载下反复换领导者，不报错，只是每次换都丢掉锁表（§4.4）。
> 复制的等待上限由选举超时推导，所以只调 `--ms-election-timeout-ms` 这一个参数就够了。

### 4.4.3 多 CN 下的两个「窗口」：刚提交的行、刚做的 DDL

多 CN 部署里有两处**固有**的传播延迟，都不是抖动，重试即好——但要分清是哪一个：

| 你看到的 | 真实原因 | 最长滞后 | 怎么避免 |
|---|---|---|---|
| 刚在 CN1 提交的行，在 CN2 上查不到 | **时间戳窗口**：CN1 发出去的 `commit_ts` 最多超前 CN2 的 `read_ts` 一个窗口 | 一个窗口（`ms` 默认 1 s，`single` 64 s） | 多 CN 下不要把 `--tso-batch-ms` 调大 |
| 刚在 CN1 建的表，在 CN2 上报「表不存在」 | **目录（catalog）异步传播**：CN2 的内存目录要等下一次轮询（5 s）才会知道 | 一个轮询周期（5 s） | 等 5 s 或直接重试 |

**「库」这一层不会滞后**：`USE db`、连接串里的库名、协议层的 `COM_INIT_DB` 三条路径
在本地目录里找不到该库时，会**先刷新一次目录再判**（只在确有版本变化时才真的重载）。
所以「在 CN1 上 `CREATE DATABASE d`，紧接着用 CN2 的连接连 `d`」不会得到
`Unknown database 'd'`——那是**最坏的一类错误**：库确实存在，使用者看到的却是
"我刚建的库没了"，而重试一下又好了，于是被当成偶发抖动。

**「表」这一层仍然是异步的**：另一个 CN 刚建的表，在本节点的内存目录里
最多要等一个轮询周期（5 s）才出现，这段时间内查询它会报「表不存在」——
**重试即可**，不是 DDL 没生效。

> 「库能立刻用、表要等 5 秒」看着不对称，但它对应的是两种代价：判一个**库名**
> 值得先刷新一次目录（否则就是"我刚建的库没了"这种指错方向的错误），
> 而每张表的解析都在查询热路径上，不能在那里挂一次目录刷新。

### 4.5 字符集：支持 4 种，其余**显式拒绝**

支持 `utf8mb4`（默认）、`utf8`、`latin1`、`binary`。

```sql
SET NAMES gbk;   -- ✗ 1115 Unknown character set: 'gbk'
```

**不做「收下但按 utf8mb4 处理」**——那正是设计文档 §11.1.1 陷阱 T1 描述的静默错误：
客户端以为在用 gbk、服务端在用 utf8mb4，中文写进去看着正常、读出来是乱码，
而链路上没有任何一处报错。§11.1.1 对此的应对栏写的是
「要么完整支持按列字符集，**要么在其余位置显式拒绝**」——本系统选后者。

`latin1` / `binary` 连接下返回中文会**报错**，而不是替换成 `?`：
静默替换会让客户端拿到一个「看起来正常、但内容已经变了」的字符串。

`SET character_set_results = NULL`（Connector/J 连接期会发）等价于 utf8mb4：
它的语义是「不做转换、按列自身字符集给原始字节」，而本系统所有字符串列都是 utf8mb4，
两者恰好等价。

### 4.5.1 对象存储的重试与限流（L1 适配层）

对象存储的失败**不是异常事件，而是常态**（S3 官方 SLA 允许 5xx）。本系统在
对象存储之上加了一层重试/退避/限流：

| 失败 | 处置 |
|---|---|
| 5xx / 429 / 超时 / 连接中断 | **重试**（指数退避 + 抖动，默认最多 4 次、总时限 5s） |
| `409 ConditionalRequestConflict` | **重试**（条件结论未知，重发会得到明确结论） |
| `412 PreconditionFailed` | **绝不重试**（条件确定不成立） |
| `404` | **绝不重试**（明确的业务语义） |

重试**不会破坏 CAS**：只有服务端接受了条件写，调用方才会看到成功；条件确实不成立时，
重发得到的是 412，调用方拿到的仍是"CAS 失败"。

并发在途请求数默认上限 64（§10.2 的并行预取天然会并发发出大量 GET；
不设上限会换来一片 503，重试层再把它们变成更长的尾延迟）。

**SDK 自带的重试被显式关闭**（`RetryPolicy.none()`）：重试只能有一个主人，
否则层数与预算相乘、指标失真、总时限也管不住它。

观测：`yb_objectstore_retries_total{op,reason}` 与 `yb_objectstore_retries_exhausted_total{op}`。

`yb_objectstore_requests_total` 带 `cached` 标签，两个取值含义**相反**，别看错：

| 标签 | 含义 | 是不是成本 |
|---|---|---|
| `cached="false"` | **真正打到对象存储的请求**（含重试次数，一次重试算一次） | ✅ **这才是账单上的请求数** |
| `cached="true"` | 由内存块缓存 / 本地 NVMe 二级缓存**满足、因而没有发出**的请求（由 `yb-server` 的采集器从缓存命中数折算，只记在 `op="get"` 下） | ❌ 成本为 0 |

所以做成本口径的告警与回归断言时，**要过滤 `cached="false"`**；
而看「缓存救回了多少」看 `cached="true"`。两者的和不是请求总数，把它们加在一起没有意义。
（重试的口径见上：`cached="false"` 里已经包含重试发出的那几次——那是真实成本。）

### 4.5.2 本地文件系统后端需要显式开关

`--data-dir`（本地 FS）**仅用于开发与测试**，且必须显式声明：

```bash
# 不给这个标志会被拒绝启动
./gradlew :yb-server:run --args="--data-dir /tmp/yb --i-know-this-is-not-durable"
```

理由：本地 FS 不提供条件写的跨进程原子性，也没有多副本持久性。"本地 FS 通过测试、
生产环境依赖对象存储语义"是最危险的一类缺陷来源（设计文档 §3.3）。
带了标志会打一条醒目警告后继续。

### 4.5.3 提交档（`commit-mode`）：出厂 `sync`，`batch` 可选，`async` 明确拒绝

| 档位 | 状态 | 语义 |
|---|---|---|
| `sync` | ✅ **出厂默认** | 组提交写者等 PUT 落盘后才 ACK。**RPO = 0**（攒批窗口 5ms / 256KB） |
| `batch` | ✅ 可用 | 高吞吐导入用；**同样等落盘，RPO = 0**，只是窗口更宽（50ms / 4MB）⇒ 摊薄更多 PUT、单事务延迟更高 |
| `async` | ❌ **明确拒绝** | 服务端启动时直接报「尚未实现」并**退出**，不会带病启动 |

```bash
# 部署级设置，只能在这里选：sync（默认）/ batch
./gradlew :yb-server:run --args="--s3-bucket my-bucket --commit-mode batch"
```

**为什么 `async` 是硬拒绝而不是「先收下」**：它要求「不等 PUT 完成就 ACK」，
而当前的组提交写者会等待落盘。收下它会得到**行为是 RPO=0、对外却声称 RPO>0**
的实例——一个假的持久性承诺比没有这个档位危险得多（设计文档 §6.3.4）。
真要做它，得先有独立的攒批/刷盘路径。

**会话级 `SET commit_mode` 只能等于部署值**：

```sql
SET commit_mode = 'batch';   -- 部署值是 sync 时报 1235（ER_NOT_SUPPORTED_YET）
```

不是草率拒绝：组提交的 WalWriter **每个分片一个、被所有会话共享**，
同一批里混着不同持久性承诺的事务，「攒批」就没有定义。所以 `SET` 在值相同时是**无操作**，
不同时报 1235，并指路 `--commit-mode`。**别把它写进连接池的初始化 SQL 去"统一档位"。**

想确认当前实例是哪一档：

```sql
SHOW VARIABLES LIKE 'commit_mode';   -- 如实回报部署值，出厂为 sync
```

### 4.6 协议模式

协议层按客户端协商的能力工作，两种模式都完整支持：

| 能力 | 说明 |
|---|---|
| `CLIENT_DEPRECATE_EOF` | ✅ 支持。协商后结果集收尾包从 EOF 换成 OK 包（每段少一个包的往返）；列定义段之后**不再**发收尾包——这是该能力的要求 |
| `CLIENT_MULTI_STATEMENTS` | 见 4.7，需显式开启 |
| `CLIENT_MULTI_RESULTS` | ✅ |
| 服务端游标（`CURSOR_TYPE_READ_ONLY` + `COM_STMT_FETCH`） | ✅ 支持。Connector/J 的 `useCursorFetch=true` 走这条路 |
| `CLIENT_SESSION_TRACK` | ❌ 未实现（不声明，客户端会回退到传统字段布局） |

排查"这条连接处在哪种协议模式"：

```sql
SELECT @@yangbase_protocol_flags;   -- 例如 deprecate_eof,multi_statements,multi_results
```

### 4.7 多语句：需要**显式协商**

一条 `COM_QUERY` 里的多条语句只在客户端协商了 `CLIENT_MULTI_STATEMENTS` 时执行
（Connector/J 是 `allowMultiQueries=true`）。未协商时 `a; b` 会被判为语法错误。

MySQL 也是这样——`CLIENT_MULTI_STATEMENTS` 是一个让客户端声明
「我知道这条 SQL 里可能有多个语句」的开关，SQL 注入防护依赖客户端不开启它。
服务端把它当默认行为等于替客户端关掉了那道闸。

### 4.8 其它差异一览

| 项 | MySQL / InnoDB | YangBase |
|---|---|---|
| `SELECT ... FOR UPDATE` | 行锁（间隙锁 + next-key lock） | **只有行锁，没有间隙锁**：不加 `FOR UPDATE` 的普通读不会被任何锁挡住，所以 `SELECT ... WHERE` 之后的 `INSERT` 冲突仍需唯一约束兜底。MS 模式有死锁检测（报 1213），对象存储模式只有等待超时（1205） |
| `FOR UPDATE` 的锁在 MS 换领导者时 | 不存在这个问题（无领导者概念） | 锁表在 MS 内存里、随领导者更替消失。持锁事务在**提交前会自检**，发现锁没了就报 1213 回滚——最坏是事务重试，**不会**静默丢更新 |
| DDL | 部分场景锁表 | 全部在线（元数据操作），建索引也是（§8.7） |
| 唯一约束的实现 | next-key lock | 索引分片上的原子判重（跨分片多一次 RTT） |
| **字符串比较 / 排序规则** | 按列的 collation，默认 `utf8mb4_0900_ai_ci`（大小写不敏感） | **一律二进制序**（等价 `utf8mb4_bin`）；上报的 collation 名与实际语义不符，**见 4.3.1** |
| **提交档** | — | 部署级 `--commit-mode`：`sync`（默认）/ `batch`；`async` 明确拒绝。会话级 `SET commit_mode` 只能等于部署值，**见 4.5.3** |
| `ORDER BY` 的 NULL 位置 | NULL 最小 | 与 MySQL 一致 |
| 自增列显式插入大值后的下一个值 | 值 + 1 | 值 + 1（✅ 一致） |
| `information_schema` 覆盖度 | 完整 | 常用视图齐全，细节字段可能为空 |
| 错误码 | MySQL 全套 | 常用错误码与 MySQL 数值一致；少数场景用 `1105` |
| 整数运算溢出 | 报 1690 | 报 1690（**不回绕**，见 4.4.1） |
| 表被起别名后用**表名**引用列（`FROM t AS x` 里写 `t.v`） | 报 1054 | 接受（更宽松；连接查询里的歧义仍按 1052 报错） |
| `CASE WHEN` / `IF()` / `SUBSTRING` / `ROUND` / `CAST` | 支持 | **报错**（`1235` 或语法错），不会静默返回 NULL |
| `INSERT ... SELECT` / 派生表（`FROM (SELECT ...) x`） | 支持 | **报错**（语法错），不会静默插入 0 行 |
| 全文检索的返回行数 | 全部命中 | 无 `LIMIT` 时**最多 1000 行**（top-k 候选，避免全表回表）；写 `LIMIT` 即可指定 |

### 4.9 性能特征（不是缺陷，是物理约束）

| 场景 | 量级 |
|---|---|
| 点查（缓存命中） | p50 **9 µs**（0.004 次对象存储请求/查询） |
| 点查（冷缓存） | p50 **124 µs**，1 次 GET |
| 写提交（`sync` 档） | p50 ~0.5 ms（本地 FS）/ 真实对象存储下由一次 PUT 的 RTT 决定 |
| 组提交摊薄（16 并发写者） | 3200 个事务 → **220–221 次 PUT**（14.5 事务/PUT）；**单线程串行写入没有攒批空间**（1.0 事务/PUT） |
| 范围扫描 1000 行 | 1 次 Range GET（内嵌 S3 约 35 万行/秒；真实 MinIO 约 17 万行/秒） |
| `.idx` 元数据开销 | 数据量的 **6.9%**（刻意不压缩——它要常驻内存） |
| `COUNT(*)` | **0 次 GET**，约 0.37 ms（比全表扫描快 300×） |

> **口径**：`./gradlew :yb-bench:bench`，后端是内嵌 S3 服务器（真实 HTTP）、p50。
> 上表里**只有最后一列「请求数」是确定性的**——延迟与吞吐受环境影响大，换真实
> 对象存储只改延迟（单次 PUT 约 4 ms，云上还要再高一个数量级），**不改请求数**：
> 点查 0.004 次、`COUNT(*)` 0 次、组提交摊薄 14.5 倍都是架构性质。完整的后端对比见
> [README](../README.md) 的「性能（实测）」一节。

**冷热差是几十倍的量级**，这与对象存储作为数据层的物理下界有关，
任何优化都消不掉。因此 SLO 必须分「缓存命中」与「未命中」两栏陈述
（设计文档 §14.1）。与 MySQL 用户的心理预期差 2–3 个数量级的事情是：
**冷缓存下的点查延迟**。把它当成"数据库变慢了"之前，先看缓存是否命中。

---

## 五、已知的未实现（按优先级）

| 项 | 影响 | 备注 |
|---|---|---|
| 认证 / 权限 / TLS | 接受任意用户名密码 | 仅供可信网络内使用，**不要直接暴露到公网** |
| 存储分层与生命周期规则 | 冷数据无法自动下沉 | **已决定短期（未来数月）不做**，见下方说明；设计文档亦警告归档层几乎永远是亏的 |
| 跨 Region 只读副本 | 单 Region 故障不可用 | 依赖对象存储自身的跨区复制（CRR） |
| 跨库 JOIN / 跨库事务 | 行为未定义 | 设计空白（§16.3） |
| `GEOMETRY` 及空间函数 | 未实现 | 空间索引不在设计范围内（§8 无此类索引） |

### 关于存储分层：短期不做（决定记录）

**2026-09-24 决定：存储分层在未来几个月内不实现。** 这不是遗漏，是取舍：

- 它省的是**存储单价**，而本系统的成本主线是「计算与存储解耦、按折扣周期迁移」
  ——那条线已经由「CN 无状态 + 对象存储唯一真相源」实现了；
- 真正该用的冷却是 **`DROP PARTITION`（已实现，成本 ≈ 0）**，不是把对象下沉；
- 而做错的方向代价很高（见下），不值得在功能面还没铺满时先做它。

如果以后要做，边界应当是：**只下沉到低频访问层、只对大到值得的对象（如 `.dat` > 8MB）、
绝不碰 `.idx` / `manifest` / WAL，并在执行前先算出「省下的存储费 vs 一次全量取回的成本」**。

### 关于存储分层的完整警告（§13.3）

**不要**把对象下沉到归档层（Glacier 之类）。以 AWS 为例：Glacier IR 存储价
$0.004/GB-月，但**取回**价 $0.03/GB——**取回一次相当于存放 7.5 个月**。
数据库中「永远不会被再读」的数据几乎不存在（备份校验、审计查询、偶发的历史回溯
都会触碰它）。**存储分层的下界是「低频访问层」，不是「归档层」。**

也**不要**开启 Intelligent-Tiering：它按**对象数**收监控费（$0.0025 / 1000 对象-月），
而 LSM 的特征恰恰是**对象多而小**。1 亿个小对象每月监控费 $250，远超这些对象本身的存储费。

真正该用来归档冷数据的机制是 `DROP PARTITION`（§9.4）：成本 ≈ 0，
且顺带减少 manifest 体积与读放大。

---

## 六、怎么验证这些说法

```bash
# 单元测试 + 集成测试。⚠️ integrationTest **不在** check/build 里
# （它是独立源集、独立任务），必须显式写出来才会跑
./gradlew build integrationTest

# 同一套集成断言跑在真实 MinIO 上。⚠️ 必须用 -P 属性而不是环境变量：
# Gradle 守护进程长期存活，它在启动时就固定了环境，之后 shell 里 export
# 的变量传导不到它启动的测试 JVM（build.gradle.kts 里写了这条）
./gradlew integrationTest \
  -PYB_S3_ENDPOINT=http://127.0.0.1:9000 -PYB_S3_BUCKET=yangbase \
  -PYB_S3_ACCESS_KEY=yangbase -PYB_S3_SECRET_KEY=yangbase123

# 或者用脚本把上面两件事一次做完（启动 MinIO + 跑 :yb-server 与 :yb-ms 的集成测试）
./scripts/minio.sh test

# 分支覆盖率门禁（≥ 50%）
./gradlew :yb-server:koverVerify

# 基准（点查请求数、组提交摊薄、COUNT 的 0 GET）
./gradlew :yb-bench:bench
```

与本文件各条对应的测试（都能单独跑）：

| 条目 | 测试 |
|---|---|
| 4.1 隔离级别 | `TransactionTest`、`IoBudgetIntegrationTest > 长读事务不会被并发写入干扰` |
| 4.2 自增不连续 | `AutoIncrementTest`（重启不重号、TRUNCATE 重置） |
| 4.0 SQL 能力面 | `SqlSurfaceTest`（端到端）、`AggregateTest` / `JoinTest`（纯逻辑） |
| 4.3 ENUM/SET 顺序 | `ColumnTypesTest` |
| 4.3.1 字符串比较语义 | 无专门用例（它是 `Datum.compareTo` 与 `KeyEncoder` 的直接后果）；`yb-cli analyze-sql` 会把带 `COLLATE`（非 `_bin`）的写法报成「语义不同」 |
| 4.4 JSON | `ColumnTypesTest` |
| 4.5 字符集 | `ProtocolCompatTest`（中文往返、`SET NAMES gbk` 被拒） |
| 4.5.3 提交档 | `EndToEndTest > SHOW 与元数据查询`（断言出厂默认是 `sync`）；`async` 的拒绝在 `YangBaseServer.parseCommitMode` 里，启动即失败 |
| 4.4.2 协调模式 | `MultiCnCoordinationTest`（同一套一致性测试对各模式各跑一遍）、`:yb-bench:benchCoordination` |
| 4.6 协议模式 | `RawProtocolTest`（字节级断言）、`ProtocolCompatTest` |
| 4.7 多语句 | `ProtocolCompatTest` |
| 三、无操作语句 | `ProtocolCompatTest > LOCK TABLES 与 SET 序列不打断 dump` |
| 4.5.1 重试/限流 | `RetryingObjectStoreTest`、`S3FailureClassificationTest`（走内嵌 S3 的真实 HTTP） |
| 4.5.2 本地 FS 守卫 | 手动验证（两条路径：无标志拒绝启动 / 带标志警告后启动） |
| 4.9 性能特征 | `:yb-bench:bench`（请求数与延迟）、`IoBudgetIntegrationTest`（请求数断言） |
