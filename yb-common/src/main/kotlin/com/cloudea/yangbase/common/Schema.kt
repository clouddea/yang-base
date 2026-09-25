package com.cloudea.yangbase.common

import kotlinx.serialization.Serializable

/** 列类型。对齐 MySQL 的子集（设计文档 §11.3 明确列出了不支持的部分）。 */
@Serializable
enum class ColumnType(val mysqlName: String) {
    TINYINT("tinyint"),
    SMALLINT("smallint"),
    INT("int"),
    BIGINT("bigint"),
    FLOAT("float"),
    DOUBLE("double"),
    DECIMAL("decimal"),
    BOOLEAN("tinyint"),
    VARCHAR("varchar"),
    TEXT("text"),
    BLOB("blob"),
    DATE("date"),
    DATETIME("datetime"),
    TIMESTAMP("timestamp"),

    /**
     * `YEAR`：取值 1901–2155，外加 0（MySQL 的"零年"）。
     *
     * 在行里就是一个整数，与别的整数列走同一套编码——MySQL 内部也是这么存的
     * （1 字节的"年份 - 1900"），所以排序与比较语义天然一致。
     */
    YEAR("year"),

    /**
     * `ENUM('a','b',...)`：值的集合是**封闭**的。
     *
     * ## 与 MySQL 的两处差异（如实写在 §11.3 的限制清单里）
     *
     * 1. **比较与排序按字符串，不按声明序号。** MySQL 里 `ENUM('z','a')` 的
     *    `'z' < 'a'`（比的是序号 1 < 2）；本系统比的是字符串，`'a' < 'z'`。
     *    声明顺序与字典序不一致时，`ORDER BY` / 范围比较的结果会与 MySQL 不同。
     *    建表时若发现这种列，服务端会**主动打警告**（§7.4 的「主动警告」要求）。
     *    选字符串而不是序号是为了**处处一致**：序号语义要求比较时能拿到列定义，
     *    而比较发生在求值器里（那里只有 `Datum`）。两处不一致比一处差异更危险——
     *    索引路径与扫描路径会给出不同的顺序，且不报错。
     * 2. **存储是字符串，不是 1–2 字节的序号。** 多占一点空间（有压缩兜底），
     *    换来的是「值就是它自己」——不存在序号与成员对不上的可能。
     */
    ENUM("enum"),

    /**
     * `SET('a','b',...)`：一列可存多个成员，MySQL 显示成 `'a,b'`。
     *
     * 与 ENUM 同样的两处差异（按字符串比较、按字符串存储）。
     * 成员在行里**按声明顺序规范化**（与 MySQL 的 `SET` 显示顺序一致），
     * 所以 `SET('a','b')` 存 `'b,a'` 会读回 `'a,b'`。
     */
    SET("set"),

    /**
     * `JSON`：按**文本**存储，写入时校验合法性。
     *
     * 不实现二进制 JSON 与 JSON 路径索引；`JSON_EXTRACT` 之类的函数见 §11.3
     * 的限制清单。比较与排序按文本，MySQL 按规范化的 JSON —— 这是差异，如实记录。
     */
    JSON("json"),
    ;

    val isInteger: Boolean get() = this == TINYINT || this == SMALLINT || this == INT || this == BIGINT
    val isNumeric: Boolean get() = isInteger || this == FLOAT || this == DOUBLE || this == DECIMAL
    val isString: Boolean get() = this == VARCHAR || this == TEXT || this == ENUM || this == SET || this == JSON
    val isTemporal: Boolean get() = this == DATETIME || this == TIMESTAMP || this == DATE
}

@Serializable
data class ColumnDef(
    val name: String,
    val type: ColumnType,
    val nullable: Boolean = true,
    val length: Int? = null,          // VARCHAR(n)
    /**
     * `ENUM` / `SET` 的成员表，**按声明顺序**（顺序即显示顺序）。
     *
     * 写进 schema 而不是每次解析时现取：它是列的**类型定义**的一部分，
     * 校验、规范化、`SHOW CREATE TABLE` 都要用它，而且必须与建表时一致——
     * 「校验用一份、显示用另一份」迟早会飘。
     */
    val enumValues: List<String> = emptyList(),
    val precision: Int? = null,       // DECIMAL(p,s)
    val scale: Int? = null,
    val defaultValue: String? = null, // 以字面量形式保存，DDL 时求值
    val autoIncrement: Boolean = false,
    val comment: String? = null,
)

@Serializable
enum class IndexKind {
    /** 主索引（聚簇），key = 主键。LSM 本身。 */
    PRIMARY,

    /** 二级有序索引，对外提供 B+ 树语义（设计文档 §8.3）。 */
    ORDERED,

    /** 哈希索引，仅等值点查（设计文档 §8.4）。 */
    HASH,

    /** 跳数索引（设计文档 §8.5）。不返回行，只做块级剪枝。 */
    SKIP_MINMAX,
    SKIP_SET,
    SKIP_BLOOM,
    SKIP_NGRAM_BLOOM,

    /** 全文/倒排索引（设计文档 §8.6）。 */
    FULLTEXT,
}

@Serializable
data class IndexDef(
    val id: Long,
    val name: String,
    val kind: IndexKind,
    val columns: List<String>,
    val unique: Boolean = false,
    /** 索引包含的额外列（covering index），用于消除回表。 */
    val includes: List<String> = emptyList(),
    /** 跳数索引的粒度：每 N 个数据块合并成一个索引项。 */
    val granularity: Int = 1,
    /**
     * 全文索引的分词器（`WITH PARSER <name>`）。
     *
     * **索引期与查询期必须用同一个分析器**，否则「索引时切出『数据库』、
     * 查询时切出『数据』『据库』」会让查询静默返回空集。
     * 因此它被写进 schema 并随索引一起持久化，而不是每次查询时现取。
     */
    val parser: String? = null,
    /**
     * 索引的状态（设计文档 §8.7 的在线构建）。
     *
     * `BUILDING` 期间索引**已经发布在 schema 里**（所以新的写入会维护它），
     * 但**查询一律不用它**——回填还没追平历史数据，用它就会静默少返回行。
     * 构建完成、增量追平后才翻成 `ACTIVE`。
     */
    val state: IndexState = IndexState.ACTIVE,
)

/**
 * 索引的可见性状态。
 *
 * 只有两个状态，是刻意的：多一个「失败」状态并不能改变任何行为
 * ——构建失败时索引停在 `BUILDING`，重跑一次就是幂等的（§8.7 的失败处理）。
 */
@Serializable
enum class IndexState {
    /** 正在构建：写入会维护它，**查询不用它**。 */
    BUILDING,

    /** 可用。 */
    ACTIVE,
}

@Serializable
enum class PartitionKind {
    NONE,
    RANGE,
    LIST,

    /** 按 `hash(主键编码)` 取模。分区函数只依赖主键，因此支持任意主键点查路由。 */
    HASH,

    /** 与 HASH 同义（差别只在 MySQL 的哈希函数选择上，本系统统一用 xxhash64）。 */
    KEY,
    ;

    /** 分区数是否由「定义列表」而非「PARTITIONS n」决定。 */
    val isEnumerated: Boolean get() = this == RANGE || this == LIST
}

/** RANGE 分区的无上界哨兵。与 MySQL 的 `VALUES LESS THAN MAXVALUE` 对应。 */
const val MAXVALUE = "MAXVALUE"

@Serializable
data class PartitionDef(
    val name: String,
    val kind: PartitionKind,
    /** RANGE: 上界字面量（**不含**）；[MAXVALUE] 表示无上界。LIST: 未使用。 */
    val rangeUpperBound: String? = null,
    /** LIST: 值集合的字面量文本。 */
    val listValues: List<String> = emptyList(),
    val partitionId: Long = 0,
)

@Serializable
data class TableSchema(
    val tableId: Long,
    val databaseId: Long,
    val name: String,
    val version: Long,
    val columns: List<ColumnDef>,
    val primaryKey: List<String>,
    val indexes: List<IndexDef> = emptyList(),
    val partitionKind: PartitionKind = PartitionKind.NONE,
    val partitionCount: Int = 1,
    val partitions: List<PartitionDef> = emptyList(),
    /**
     * RANGE/LIST 的路由列。
     *
     * **必须是主键的一部分**（与 MySQL 的约束一致）。理由是这里的每个分区
     * 是一棵**按主键排序**的独立 LSM：点查只知道主键，只有能从主键推出分区，
     * 才可能做到 `SELECT ... WHERE pk = ?` 只读一个分区。
     */
    val partitionColumn: String? = null,
    /**
     * 分区**表达式**的原文，例如 `TO_DAYS(created_at)`；纯列引用时为 null。
     *
     * 必须与 [partitionColumn] 分开保存：`PARTITION BY RANGE (TO_DAYS(d))` 的
     * 边界是**天数**（整数），而列的原始值是**时间戳**。只记列名而不记表达式，
     * 就会拿时间戳去和天数比较——两个不同的量纲，比较结果毫无意义，
     * 而且是静默的（不会报错，只会路由到错的分区）。
     */
    val partitionExpr: String? = null,
    /** 是否维护精确行数（设计文档 §8.9.1 的 `exact_count` 开关）。 */
    val exactCount: Boolean = true,
    /**
     * 额外的**列名 → 列下标**映射（键必须已是小写）。
     *
     * 存在的唯一理由是**连接查询的合成 schema**：一张连接表的行是多张表的列拼起来的，
     * 列名带限定前缀（`a.id` / `b.id`），而未限定的 `id` 在唯一时必须仍然可用。
     *
     * 为什么不是"再加一份列定义"：在 [columns] 里追加同义列会让它拿到一个**不同的下标**，
     * 于是同一列有两处位置、其中一处永远取不到值（静默 NULL）。
     * 别名映射指向**同一个下标**，从根上避免这件事。
     *
     * 真实表永远是空的——它只描述"怎么找列"，不描述表结构。
     */
    val extraNames: Map<String, Int> = emptyMap(),
    /**
     * 视图：非 null 时这是一张**视图**，值是它的 `SELECT` 语句原文。
     *
     * 视图复用表的那套元数据（schema 版本、`CURRENT` 指针、墓碑），
     * 因此 `CREATE/DROP/OR REPLACE VIEW` 与建表的持久化路径完全一致——
     * 不需要为它另写一份"元数据怎么存"的逻辑。
     *
     * [columns] 描述视图的**输出列**（建视图时从内层 SELECT 推导出来），
     * 让 `information_schema.COLUMNS` 与客户端 introspection 能正常工作。
     */
    val viewSql: String? = null,
) {
    /** 是不是视图。 */
    val isView: Boolean get() = viewSql != null

    /** 实际的分区数。RANGE/LIST 由定义列表决定，不读 partitionCount。 */
    val effectivePartitionCount: Int
        get() = when {
            partitionKind == PartitionKind.NONE -> 1
            partitionKind.isEnumerated -> partitions.size.coerceAtLeast(1)
            else -> partitionCount.coerceAtLeast(1)
        }

    /** 分区是否按**列值**路由（而非按主键哈希）。 */
    val isEnumeratedPartition: Boolean get() = partitionKind.isEnumerated && partitions.isNotEmpty()

    private val byName: Map<String, Int> by lazy {
        columns.withIndex().associate { (i, c) -> c.name.lowercase() to i }
    }

    fun indexOf(column: String): Int = indexOfOrNull(column)
        ?: throw SqlException(MySqlErrors.ER_BAD_FIELD_ERROR, "42S22", "Unknown column '$column' in 'field list'")

    fun indexOfOrNull(column: String): Int? {
        val k = column.lowercase()
        return byName[k] ?: extraNames[k]
    }

    fun column(name: String): ColumnDef = columns[indexOf(name)]

    /** 主键在 columns 中的下标序列。 */
    val primaryKeyIndexes: List<Int> get() = primaryKey.map { indexOf(it) }

    /**
     * 查询侧可见的索引（`state == ACTIVE`）。
     *
     * **读路径一律用这个，不要直接用 `indexes`**：`BUILDING` 的索引还没回填完，
     * 拿它取数会**静默少返回行**（不报错、不降级）——正是本项目最不能接受的那类缺陷。
     * 写路径相反，必须用全部索引（含 `BUILDING`），否则构建期间新写入的数据
     * 永远进不了索引，构建完也补不回来。
     */
    val activeIndexes: List<IndexDef> get() = indexes.filter { it.state == IndexState.ACTIVE }

    /** 该列是否是低基数列（可能适合建跳数索引 / 计数索引）。 */
    fun isLowCardinality(column: String, threshold: Int = 1000): Boolean {
        val i = indexOfOrNull(column) ?: return false
        return columns[i].type.isString || columns[i].type == ColumnType.BOOLEAN
    }
}

@Serializable
data class DatabaseSchema(
    val databaseId: Long,
    val name: String,
    val charset: String = "utf8mb4",
    val collation: String = "utf8mb4_0900_ai_ci",
)
