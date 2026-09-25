package com.cloudea.yangbase.sql

import com.cloudea.yangbase.common.ColumnDef
import com.cloudea.yangbase.common.ColumnType
import com.cloudea.yangbase.common.Datum
import com.cloudea.yangbase.common.IndexKind

// ── 表达式 ────────────────────────────────────────────────────────

sealed interface Expr

object Star : Expr { override fun toString() = "*" }

data class Literal(val value: Datum) : Expr {
    override fun toString() = value.toString()
}

data class ColumnRef(val name: String, val table: String? = null) : Expr {
    override fun toString() = name
}

/**
 * `COUNT(...)`。
 *
 * 单独一类（而不是并进 [AggCall]）是刻意的：它是**唯一**有「0 次 GET 的元数据路径」
 * 的聚合（设计文档 §8.9）——`COUNT(*)` 与 `列 = 常量` 的计数可以完全不读数据块。
 * 把它降级成普通聚合会让那条路径失去落点，而那正是本项目最重要的性能优化之一。
 */
data class CountCall(val arg: Expr?, val distinct: Boolean) : Expr {
    override fun toString() = if (arg == null) "COUNT(*)" else "COUNT($arg)"
}

/**
 * 除 `COUNT` 之外的聚合：`SUM` / `AVG` / `MIN` / `MAX` / `GROUP_CONCAT`。
 *
 * 它们**必须走扫描**——没有一个能从元数据直接得出。所以这里不做任何"优化路径"，
 * 老老实实按组归约。
 *
 * `arg` 为 null 只可能来自 `COUNT(*)`，而那由 [CountCall] 承担，故此处非空。
 */
data class AggCall(
    val name: String,
    val arg: Expr,
    val distinct: Boolean = false,
) : Expr {
    override fun toString() = "$name(${if (distinct) "DISTINCT " else ""}$arg)"
}

data class FuncCall(val name: String, val args: List<Expr>, val distinct: Boolean = false) : Expr {
    override fun toString() = "$name(${args.joinToString()})"
}

/**
 * 窗口函数调用：`ROW_NUMBER() OVER (PARTITION BY x ORDER BY y)`。
 *
 * ## 它为什么**不是**聚合
 *
 * [containsAggregate] 认的是 [CountCall] / [AggCall]，窗口调用不在其中——
 * 这是有意的：`SELECT ROW_NUMBER() OVER () FROM t` **不是**分组查询，
 * 它每一行都出一行。若把窗口函数当成聚合，这个查询会掉进分组路径，
 * 于是整张表被归约成**一行**，而查询照常返回、不报任何错。
 *
 * 反过来，窗口函数的参数**可以**是聚合：`SUM(SUM(v)) OVER (ORDER BY x)`
 * 先按 `GROUP BY` 把每组归约成一个值，再在这些值上开窗。
 * 所以 [childrenOf] 必须走遍本节点的所有子表达式。
 */
data class WindowCall(
    val name: String,
    val args: List<Expr>,
    val distinct: Boolean = false,
    val partitionBy: List<Expr> = emptyList(),
    val orderBy: List<OrderItem> = emptyList(),
    /** null = 默认帧（有 `ORDER BY` 时是 `RANGE UNBOUNDED PRECEDING → CURRENT ROW`）。 */
    val frame: WindowFrame? = null,
) : Expr {
    /**
     * 规范化文本，**同时用作"同一个窗口函数"的键**。
     *
     * 因此它必须带上全部决定结果的东西（函数、参数、分区、排序、帧）：
     * 少写一项，两个本该各算各的窗口调用就会命中同一个键，
     * 于是其中一个悄悄拿到另一个的结果。
     */
    override fun toString() = buildString {
        append(name.uppercase()).append('(')
        if (distinct) append("DISTINCT ")
        append(args.joinToString(","))
        append(") OVER (")
        val parts = ArrayList<String>(3)
        if (partitionBy.isNotEmpty()) parts.add("PARTITION BY ${partitionBy.joinToString(",")}")
        if (orderBy.isNotEmpty()) {
            parts.add("ORDER BY " + orderBy.joinToString(",") { "${it.expr}${if (it.ascending) " ASC" else " DESC"}" })
        }
        frame?.let { parts.add(it.toString()) }
        append(parts.joinToString(" "))
        append(')')
    }
}

/** 窗口帧（`ROWS BETWEEN 1 PRECEDING AND CURRENT ROW`）。 */
data class WindowFrame(val unit: FrameUnit, val start: WindowBound, val end: WindowBound) {
    override fun toString() = "$unit BETWEEN $start AND $end"
}

/** 帧的计量单位。SQL 有三种；本系统只实现前两种（`GROUPS` 明确拒绝）。 */
enum class FrameUnit { ROWS, RANGE }

/** 帧的一个边界。 */
sealed interface WindowBound {
    object UnboundedPreceding : WindowBound {
        override fun toString() = "UNBOUNDED PRECEDING"
    }
    object UnboundedFollowing : WindowBound {
        override fun toString() = "UNBOUNDED FOLLOWING"
    }
    object CurrentRow : WindowBound {
        override fun toString() = "CURRENT ROW"
    }
    data class Preceding(val n: Long) : WindowBound {
        override fun toString() = "$n PRECEDING"
    }
    data class Following(val n: Long) : WindowBound {
        override fun toString() = "$n FOLLOWING"
    }
}

/**
 * `CASE [operand] WHEN … THEN … [ELSE …] END`。
 *
 * 两种形式共用这一个节点：
 * - **搜索式**（`operand == null`）：`CASE WHEN 条件 THEN 值 …`，条件为真就取那个值；
 * - **简单式**：`CASE x WHEN 1 THEN …`，等价于 `x = 1` 的搜索式。
 *
 * ## 三值逻辑在这里是**关键**，不是学究
 *
 * `WHEN NULL THEN …` 永远不匹配（`x = NULL` 是 unknown 而不是 true），
 * 所以 `CASE x WHEN NULL THEN 'a' ELSE 'b' END` 对任何 x 都返回 `'b'`，
 * 包括 x 为 NULL 时（MySQL 也是如此）。把 unknown 当成"真"会让一批行取到错误的分支，
 * 而结果是**每行都有值、看起来正常**。
 *
 * 没有匹配且没有 `ELSE` 时结果是 NULL（不是空串、不是 0）。
 */
data class CaseExpr(
    /** 简单式的比较对象；搜索式为 null。 */
    val operand: Expr?,
    val whens: List<WhenClause>,
    val elseExpr: Expr?,
) : Expr {
    override fun toString(): String {
        val head = if (operand == null) "CASE" else "CASE $operand"
        val body = whens.joinToString(" ") { "WHEN ${it.condition} THEN ${it.result}" }
        val tail = if (elseExpr == null) "" else " ELSE $elseExpr"
        return "$head $body$tail END"
    }
}

/**
 * `CAST(expr AS type)` / `CONVERT(expr, type)`。
 *
 * [typeName] 是**原样的类型名文本**（大写，可能带 `(10,2)`）。之所以不在这里
 * 就把它解析成 `ColumnType`：SQL 的 CAST 目标类型集合比列类型更宽
 * （`SIGNED` / `UNSIGNED` / `CHAR` 都不是列类型），映射放在求值器里一处做。
 */
data class CastTo(val expr: Expr, val typeName: String) : Expr {
    override fun toString() = "CAST($expr AS $typeName)"
}

/** `CASE` 的一个分支。 */
data class WhenClause(val condition: Expr, val result: Expr)

data class BinaryOp(val op: String, val left: Expr, val right: Expr) : Expr {
    override fun toString() = "($left $op $right)"
}

data class UnaryOp(val op: String, val expr: Expr) : Expr {
    override fun toString() = "$op$expr"
}

data class InList(val expr: Expr, val values: List<Expr>, val negated: Boolean) : Expr

/**
 * 标量子查询：`(SELECT ...)` 出现在需要**一个值**的位置。
 *
 * 与 [InSubquery] / [ExistsSubquery] 一样，它**不是逐行求值的**：
 * 子查询的结果是一个集合，与当前行无关（非相关子查询）。
 * 执行器会在取数之前把它**算成字面量**（见 [Subqueries]），
 * 于是逐行求值器完全不需要知道子查询的存在。
 */
data class ScalarSubquery(
    val query: Statement,
    /**
     * 子查询的 SQL 原文（词法片段重新拼回）。
     *
     * 它是这个子查询在"预计算值表"里的**键**：同一段 SQL 在不同位置出现时
     * 必须命中同一份算好的值。用 `toString()` 当键不行——那是给人看的
     * （`(子查询)`），所有标量子查询都会撞成同一个键，于是其中一个悄悄
     * 拿到另一个的值。
     */
    val text: String = "",
) : Expr {
    override fun toString() = if (text.isEmpty()) "(子查询)" else "(子查询 $text)"
}

/** `expr [NOT] IN (SELECT ...)`。 */
data class InSubquery(val expr: Expr, val query: Statement, val negated: Boolean) : Expr {
    override fun toString() = "$expr ${if (negated) "NOT " else ""}IN (子查询)"
}

/** `[NOT] EXISTS (SELECT ...)`。 */
data class ExistsSubquery(val query: Statement, val negated: Boolean) : Expr {
    override fun toString() = "${if (negated) "NOT " else ""}EXISTS (子查询)"
}

data class Between(val expr: Expr, val low: Expr, val high: Expr, val negated: Boolean) : Expr

data class IsNull(val expr: Expr, val negated: Boolean) : Expr

data class Like(val expr: Expr, val pattern: Expr, val negated: Boolean) : Expr

/**
 * `MATCH (col) AGAINST ('query' [IN BOOLEAN MODE])` —— 全文检索谓词（设计文档 §8.6）。
 *
 * 它不是普通的标量表达式：求值它需要**倒排索引**，而不是当前行。
 * 因此执行器会把它当作一个「取候选集」的访问路径，而不是逐行判定——
 * 逐行判定在语义上根本做不到（相关性打分是全语料统计量，不是行内信息）。
 */
data class MatchAgainst(
    val columns: List<String>,
    val query: String,
    val booleanMode: Boolean = false,
) : Expr {
    override fun toString() = "MATCH(${columns.joinToString()}) AGAINST('$query')"
}

// ── 语句 ──────────────────────────────────────────────────────────

sealed interface Statement

data class CreateDatabaseStmt(val name: String, val ifNotExists: Boolean) : Statement
data class DropDatabaseStmt(val name: String, val ifExists: Boolean) : Statement
data class UseStmt(val name: String) : Statement

/**
 * `CREATE TABLE` 里的一个列定义。
 *
 * [primaryKey] 是**列级** `PRIMARY KEY` 的标记。它单独放而不是塞进 [ColumnDef]：
 * `ColumnDef` 会被序列化进 catalog，属于持久格式；而"主键"是**表**的属性
 * （`TableSchema.primaryKey`），不是列的属性——写进列定义会让同一件事有两个来源。
 */
data class ColumnAst(val column: ColumnDef, val primaryKey: Boolean = false)

data class IndexAst(
    val name: String,
    val columns: List<String>,
    val unique: Boolean,
    val kind: IndexKind,
    val includes: List<String> = emptyList(),
    /** 全文索引的分词器（`WITH PARSER ngram`）。 */
    val parser: String? = null,
)

data class CreateTableStmt(
    val name: String,
    val columns: List<ColumnAst>,
    val primaryKey: List<String>,
    val indexes: List<IndexAst>,
    val ifNotExists: Boolean,
    /** `PARTITION BY HASH(...) PARTITIONS n` 的分区数；1 表示不分区。 */
    val partitionCount: Int = 1,
    val partitionKindName: String = "NONE",
    /**
     * RANGE/LIST 的路由列（`PARTITION BY RANGE (created_at)` 里的 `created_at`）。
     * HASH/KEY 为 null——它们按主键哈希，不需要声明列。
     */
    val partitionColumn: String? = null,
    /** RANGE/LIST 的分区定义列表，按声明顺序。 */
    val partitions: List<PartitionAst> = emptyList(),
    /**
     * 分区表达式原文，如 `TO_DAYS(created_at)`；纯列引用时为 null。
     *
     * 必须与 [partitionColumn] 分开带着：边界是表达式作用**之后**的值域
     * （`TO_DAYS` 得到天数），路由时列值要先过同一个表达式才能比较。
     */
    val partitionExpr: String? = null,
) : Statement

/**
 * 一个分区的定义。
 *
 * 边界保存为**字面量文本**而不是求值后的值：求值需要知道列类型，
 * 而列类型要到 [CreateTableStmt] 全部解析完才知道。求值发生在建表时
 * （[com.cloudea.yangbase.txn.Partitioning.evalBoundExpr]）。
 */
data class PartitionAst(
    val name: String,
    /** RANGE：`VALUES LESS THAN (x)` 的 `x`；`MAXVALUE` 原样保留。 */
    val upperBound: String? = null,
    /** LIST：`VALUES IN (a, b, c)`。 */
    val values: List<String> = emptyList(),
)

/**
 * `DROP TABLE [IF EXISTS] a[, b, ...]`。
 *
 * ⚠️ [names] 是列表而不是单个名字：曾经这里只有一个名字，而解析器把
 * `DROP TABLE a, b` 的第二个名字**吃掉丢掉**——于是「删两张表」只删了一张，
 * 返回成功。多表删除在 MySQL 里是合法语法，静默少删一张属于最难发现的一类问题
 * （脚本照常往下跑，另一半表还在，而后续操作可能因此"莫名"看到旧数据）。
 */
data class DropTableStmt(val names: List<String>, val ifExists: Boolean) : Statement {
    constructor(name: String, ifExists: Boolean) : this(listOf(name), ifExists)
    /** 单表删除时的名字（兼容旧调用点）。 */
    val name: String get() = names.first()
}

/**
 * `CREATE [OR REPLACE] VIEW name AS SELECT ...`。
 *
 * [sql] 存的是内层 SELECT 的**原文**（由 token 的原样片段拼回），
 * 查询时再解析一次。为什么存文本而不是 AST：AST 不是可序列化的，
 * 而视图定义要写进 catalog 并且**跨版本**读得出来——存文本是唯一不需要
 * 为整棵 AST 写一套编解码的做法（而且审查视图定义时能直接读懂）。
 */
data class CreateViewStmt(val name: String, val orReplace: Boolean, val sql: String) : Statement

data class DropViewStmt(val names: List<String>, val ifExists: Boolean) : Statement {
    constructor(name: String, ifExists: Boolean) : this(listOf(name), ifExists)
    val name: String get() = names.first()
}

/**
 * `ALTER TABLE`。
 *
 * **只覆盖设计文档 §4.5 明确划定的那类「无重写」变更**：
 * 加列只能加在末尾、删列只能删末尾。理由见 [com.cloudea.yangbase.common.RowCodec]：
 * 行的列数写在行首，所以「末尾增减」是纯粹的元数据操作；
 * 而在中间插删会让列的位置整体错位，必须重写全部数据——那种情况这里**明确报错**，
 * 不提供半吊子的实现。
 */
data class AlterTableStmt(val table: String, val actions: List<AlterAction>) : Statement

sealed interface AlterAction

/**
 * **明确的无操作**：语法上合法、但本系统里没有可执行的语义。
 *
 * 目前的唯一成员是 `ALTER TABLE t ENABLE|DISABLE KEYS`（MyISAM 的批量导入提示，
 * InnoDB 上本身就是空操作，而 `mysqldump` 每张表都会发）。它与"暂不支持"是
 * 两回事：后者必须报错，因为忽略它会让结果不同；前者的语义在我们这里就是"什么都不做"。
 */
data object NoOpAlterAction : AlterAction

/** `ADD COLUMN c INT [AFTER col]`。只支持追加到末尾。 */
data class AddColumn(val column: ColumnDef, val after: String? = null, val first: Boolean = false) : AlterAction

/** `DROP COLUMN c`。只支持删末尾的列。 */
data class DropColumn(val name: String) : AlterAction

data class AddIndexAction(val index: IndexAst) : AlterAction

data class DropIndexAction(val name: String) : AlterAction

/** `ADD PARTITION (PARTITION p VALUES LESS THAN (...))` */
data class AddPartitionAction(val partitions: List<PartitionAst>) : AlterAction

/** `DROP PARTITION p0, p1` —— 对象存储上这是一次**近乎免费**的操作（§9.4）。 */
data class DropPartitionAction(val names: List<String>) : AlterAction

/** `TRUNCATE PARTITION p0` —— 只清数据，保留分区定义。 */
data class TruncatePartitionAction(val names: List<String>) : AlterAction

/** `REORGANIZE PARTITION a, b INTO (...)` —— 扩容/重划分（§9.5）。 */
data class ReorganizePartitionAction(
    val from: List<String>,
    val into: List<PartitionAst>,
) : AlterAction

data class RenameTable(val to: String) : AlterAction

/**
 * `TRUNCATE TABLE` —— 清空整表但保留定义。
 *
 * 复用 `ALTER TABLE` 的通道而不是单独一个语句类型：它们都要走
 * 「拿表、改存储、改元数据」这条路径，分开写只会得到两份会漂移的实现。
 */
data object TruncateWholeTable : AlterAction

data class CreateIndexStmt(
    val name: String,
    val table: String,
    val columns: List<String>,
    val unique: Boolean,
    val kind: IndexKind,
    val parser: String? = null,
) : Statement

/**
 * `INSERT` / `INSERT IGNORE` / `REPLACE`，可带 `ON DUPLICATE KEY UPDATE`。
 *
 * 三种动词的差别**全在"撞到已存在的行时怎么办"**上，所以它们是同一个语句的三种模式，
 * 而不是三个语句：
 *
 * | 模式 | 撞到主键/唯一键时 |
 * |---|---|
 * | [InsertMode.INSERT] | 报 1062 |
 * | [InsertMode.IGNORE] | 跳过这一行（不报错） |
 * | [InsertMode.REPLACE] | **先删掉旧行再插入**（等价于 DELETE + INSERT） |
 * | 任意模式 + [onDuplicate] | 按给定的赋值更新旧行 |
 */
data class InsertStmt(
    val table: String,
    val columns: List<String>?,
    val rows: List<List<Expr>>,
    val mode: InsertMode = InsertMode.INSERT,
    /**
     * `ON DUPLICATE KEY UPDATE` 的赋值列表（列名 → 表达式）。
     *
     * 表达式里可以用 `VALUES(col)` 引用"本来要插入的那个值"（MySQL 的写法，
     * 在 8.0.20 之后被别名写法取代但仍在广泛使用）。
     */
    val onDuplicate: List<Pair<String, Expr>> = emptyList(),
) : Statement

/** `INSERT` 的三种动词。 */
enum class InsertMode { INSERT, IGNORE, REPLACE }

/**
 * `INSERT INTO t [(cols)] SELECT …`。
 *
 * 与 [InsertStmt] 分开是因为来源完全不同：一个是 VALUES 列表，一个是查询结果。
 * 合成一个节点会让"这个 INSERT 到底有没有查询"变成到处判断的 if。
 */
data class InsertSelectStmt(
    val table: String,
    val columns: List<String>?,
    val query: Statement,
    val mode: InsertMode = InsertMode.INSERT,
) : Statement

sealed interface SelectItem {
    data object All : SelectItem
    /** 不叫 `Expr`——那样会和顶层的 `Expr` 接口重名，导致类型解析歧义。 */
    data class Field(val expr: Expr, val alias: String?) : SelectItem
}

data class OrderItem(val expr: Expr, val ascending: Boolean)

/**
 * `SELECT` 的处理方式由执行器根据是否有聚合、是否有 WHERE 决定。
 *
 * [table] 可以为 null：MySQL 允许 `SELECT 1`、`SELECT @@version_comment`、
 * `SELECT DATABASE()` 这类**无 FROM 的查询**，而 Connector/J 在建立连接与
 * 健康检查时就会发它们。缺了这条，任何客户端都连不上。
 */
data class SelectStmt(
    val projections: List<SelectItem>,
    val table: String?,
    val where: Expr?,
    val orderBy: List<OrderItem>,
    val limit: Int?,
    val offset: Int?,
    /**
     * 时间旅行：`AS OF TIMESTAMP '...'` / `AS OF TSO <n>`。
     *
     * 本系统的时间戳高位就是物理毫秒，因此「回到过去」是一次位运算，
     * 不需要额外维护时间索引。
     */
    val asOf: AsOf? = null,
    /** `GROUP BY` 的分组表达式。为空表示不分组（但可能仍有聚合，此时整表一组）。 */
    val groupBy: List<Expr> = emptyList(),
    /** `HAVING`：**分组之后**的过滤，可以引用聚合与分组键。 */
    val having: Expr? = null,
    /**
     * `JOIN` 子句，按书写顺序。空表示单表查询。
     *
     * 首个表仍然放在 [table] 里（不合并成一张统一的 FROM 列表）：单表查询是
     * 绝大多数情况，把它降级成"一个元素的列表"会让所有既有路径都多一层拆包。
     */
    val joins: List<JoinClause> = emptyList(),
    /** 首个表的别名（`FROM t AS x`）。 */
    val tableAlias: String? = null,
    /**
     * `SELECT DISTINCT`：投影之后、`LIMIT` 之前去重。
     *
     * 它是**语义**而不是格式：漏掉它不会报错，只会把重复行照常返回，
     * 行数偏多。使用者拿它对账或分页时会得到错误的数字。
     */
    val distinct: Boolean = false,
    /**
     * 行锁：`FOR UPDATE`（EXCLUSIVE）/ `LOCK IN SHARE MODE`（SHARED），否则 null。
     *
     * 它锁的是**匹配到的行**（设计文档 §4.4 的行锁表），锁在事务提交/回滚时释放。
     * 没有它，`SELECT` 之后再 `UPDATE` 的"读-改-写"在并发下会丢更新——
     * 而快照隔离**不**阻止这种丢失（它是写偏斜的一种，见 §7.4）。
     */
    val lockMode: com.cloudea.yangbase.txn.LockMode? = null,
) : Statement

/**
 * 一个 `JOIN` 子句。
 *
 * [on] 对 `CROSS JOIN` 与逗号连接为 null（它们没有连接条件）；其余类型必须给出
 * `ON`——`USING` 与 `NATURAL` 尚未支持（它们改变的是"连接列怎么找"，
 * 而不影响连接本身的语义，所以延后实现不会引入错误语义，只是不支持）。
 */
data class JoinClause(
    val table: String,
    val alias: String?,
    val type: JoinType,
    val on: Expr?,
)

enum class JoinType {
    /** 内连接：只保留两边都匹配的行。 */
    INNER,

    /** 左外连接：左表全保留，右表没匹配上的列补 NULL。 */
    LEFT,

    /** 右外连接：右表全保留。 */
    RIGHT,

    /** 笛卡尔积。 */
    CROSS,

    /**
     * **半连接**：左行只要有一行右行满足 `ON` 就输出**一次左行**。
     *
     * 用户语法里没有这个关键字（MySQL 也没有）——它由**相关子查询的重写**产生：
     * `WHERE EXISTS (SELECT 1 FROM b WHERE b.k = a.k)` 恰好就是「a 与 b 按 k 做半连接」。
     *
     * 语义上必须"输出一次"：内连接在有多个匹配行时会输出多行，
     * 而 `EXISTS` 只关心"有没有"。用内连接来实现 EXISTS 会让结果**多出重复行**——
     * 一个不报错、只是行数不对的缺陷。
     */
    SEMI,

    /**
     * **反连接**：左行没有任何右行满足 `ON` 时输出该左行。
     *
     * `NOT EXISTS` 用它。`NOT IN` 的 NULL 语义由 [AntiSemantics] 另外处理
     * ——两者在 NULL 上**不等价**，这是 SQL 里最出名的坑之一。
     */
    ANTI,
    ;

    val isOuter: Boolean get() = this == LEFT || this == RIGHT

    /** 半/反连接只输出左行（右表只用来判断"有没有"）。 */
    val isSemiOrAnti: Boolean get() = this == SEMI || this == ANTI
}

/**
 * 反连接的两种语义。
 *
 * `NOT EXISTS` 与 `NOT IN` **在 NULL 上不等价**，这不是学究式的区分：
 *
 * ```
 *   表 b 的值：{1, NULL}
 *   a.x = 2  →  `2 NOT IN (1, NULL)`  =  未知   → 该行**不返回**
 *               而 `NOT EXISTS (… y = 2)` = 真      → 该行**返回**
 * ```
 *
 * 把 `NOT IN` 当成 `NOT EXISTS` 实现会让不符合条件的行**多返回一批**，
 * 而且只有在内表恰好含 NULL 时才出现——测试数据里通常没有 NULL。
 */
enum class AntiSemantics {
    /** `NOT EXISTS`：只看有没有匹配的行。 */
    EXISTS,

    /** `NOT IN`：匹配行的比较列含 NULL 时，整个子查询是"未知"，一行都不能返回。 */
    NOT_IN,
}

/**
 * `UNION` / `UNION ALL`。
 *
 * ## 为什么 ORDER BY / LIMIT 在这里**明确不支持**
 *
 * MySQL 里 `A UNION B ORDER BY x` 的 `ORDER BY` 作用于**整个并集**，
 * 而不是 `B`。而本项目的解析器是把子句直接挂在各自的 `SELECT` 上的——
 * 照直支持的话，`ORDER BY` 会被挂到右侧那个 `SELECT` 上，
 * 于是它只排右边的结果集，然后拼接。**那是一个排序错了但看起来很正常的结果**。
 *
 * 所以这里宁可不支持：并集的任一分支带 `ORDER BY` / `LIMIT` 时直接报错，
 * 使用者改用两条查询或应用层合并。要真支持，得把两侧的子句解析"截断"到
 * `UNION` 关键字为止——那是解析器结构上的改动，不该混在功能补齐里做。
 */
data class UnionStmt(
    val left: Statement,
    val right: Statement,
    /** `UNION ALL` 保留重复行；`UNION` 去重。 */
    val all: Boolean,
    /**
     * 并集级的 `ORDER BY`——它作用于**整个并集**，不是某一个分支。
     *
     * 这几个子句挂在并集节点上而不是某个分支上，是解析阶段就要定下来的事：
     * MySQL 的语法里它们写在**最后一个分支之后**，而它们的作用域是整棵树。
     * 挂错了地方不会报错，只会给出一个"排序看起来对、其实只排了一半"的结果。
     */
    val orderBy: List<OrderItem> = emptyList(),
    val limit: Int? = null,
    val offset: Int? = null,
) : Statement

/** 一个历史读时间点。[kind] 是 `TIMESTAMP` 或 `TSO`。 */
data class AsOf(val kind: String, val text: String)

/** 系统变量 / 用户变量引用，如 `@@version_comment`、`@@session.autocommit`、`@x`。 */
data class VarRef(val name: String) : Expr {
    override fun toString() = name
}

data class UpdateStmt(
    val table: String,
    val assignments: List<Pair<String, Expr>>,
    val where: Expr?,
) : Statement

data class DeleteStmt(val table: String, val where: Expr?) : Statement

data class ShowStmt(
    val what: String,
    val like: String? = null,
    /**
     * `SHOW CREATE TABLE t` / `SHOW INDEX FROM t` 里的对象名。
     *
     * 此前解析器把 `CREATE TABLE t` 整段丢掉、只留 `what = "CREATE"`，
     * 于是这些语句落到兜底分支返回一个「暂未实现」的**成功结果集**——
     * 客户端拿到的是「查到了，但内容是占位符」，而不是一个错误。
     * mysqldump 正是卡在这里。
     */
    val name: String? = null,
    /**
     * `SHOW FULL …`。
     *
     * 对 `SHOW TABLES` 来说它**有语义**：结果会多一列 `Table_type`
     * （`BASE TABLE` / `VIEW`）。缺了这一列，客户端读 `row[1]` 会拿到 NULL——
     * 实测这会让 **mysqldump 段错误**（`strncpy(dest, NULL, 30)`）。
     */
    val full: Boolean = false,
) : Statement

data class DescribeStmt(val table: String) : Statement

data class SetStmt(val name: String, val value: Expr) : Statement

data class BeginStmt(val readOnly: Boolean) : Statement
object CommitStmt : Statement
object RollbackStmt : Statement

/** 无法识别或暂不支持的语句。 */
data class UnsupportedStmt(val raw: String) : Statement

/**
 * `ANALYZE [LOCAL] TABLE t [, t2 ...]` —— 采集优化器统计信息（设计文档 §10.3）。
 *
 * 这是一个**显式**的重操作：它只读 `.idx`（不读任何数据块），但仍然要把
 * 各分片的索引走一遍。因此它不做任何自动触发——由使用者决定什么时候付这笔成本
 * （§13.3：「避免全表 ANALYZE」，指的是别在后台自动跑）。
 */
data class AnalyzeStmt(val tables: List<String>) : Statement

/**
 * `EXPLAIN <statement>` —— 只规划、不执行（设计文档 §8.9.6）。
 *
 * 它存在的首要理由是让 `COUNT(*)` 的 0 次 GET 路径**可见**：
 * `type` 列必须能显示 `METADATA_COUNT`，否则这条优化是否正确生效无从验证。
 */
data class ExplainStmt(val statement: Statement) : Statement

// ── 执行结果 ──────────────────────────────────────────────────────

/** 结果集。列名与行数据。 */
/**
 * 「成功的无操作」语句（`LOCK TABLES` / `UNLOCK TABLES` / `FLUSH TABLES`）。
 *
 * 与 [UnsupportedStmt] 的区别是**刻意的**：那一个必须报错（静默忽略会让
 * `SELECT ... INTO OUTFILE` 之类的语句看起来成功了、实际什么也没做），
 * 而这几个本来就只是给工具链开场用的，**无操作就是它们的正确语义**。
 */
data object NoOpStmt : Statement

class ResultSet(
    val columns: List<String>,
    val rows: List<List<Datum>>,
    /**
     * 每列的**声明类型**（能推断出来的时候）。
     *
     * ## 为什么结果集要带类型，而不是让协议层看值
     *
     * 协议层此前按**值**推断列类型（见 `MysqlServer.inferType`），于是
     * `DATE` 列与 `DATETIME` 列长得一模一样——都是 `Datum.Ts`，
     * 于是 DATE 也被声明成 DATETIME、渲染成 `2026-02-01 00:00:00`。
     * 客户端拿 `getString` 就会看到多余的时刻部分。
     *
     * 现在执行器把"这一列是从哪个列/函数来的"如实带下来，协议层优先用它。
     * 推断不出来（表达式、聚合、窗口）时留 null，协议层退回按值推断——
     * 那条路一直能用，只是精度差一点。
     */
    val declaredTypes: List<ColumnType?> = emptyList(),
) {
    val rowCount: Int get() = rows.size
}

/** 非查询语句的执行结果（影响行数 + 可选的最后插入 id）。 */
class UpdateResult(
    val affectedRows: Long,
    val lastInsertId: Long = 0,
    val info: String = "",
)
