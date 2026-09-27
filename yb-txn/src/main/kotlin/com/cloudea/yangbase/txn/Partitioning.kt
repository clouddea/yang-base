package com.cloudea.yangbase.txn

import com.cloudea.yangbase.common.ColumnType
import com.cloudea.yangbase.common.Datum
import com.cloudea.yangbase.common.KeyEncoder
import com.cloudea.yangbase.common.MAXVALUE
import com.cloudea.yangbase.common.MySqlErrors
import com.cloudea.yangbase.common.PartitionDef
import com.cloudea.yangbase.common.PartitionKind
import com.cloudea.yangbase.common.SqlException
import com.cloudea.yangbase.common.TableSchema

/**
 * 分区路由与裁剪（设计文档 §9.1 / §9.2）。
 *
 * 两块逻辑放在一起，是因为它们共享同一个前提：**分区边界是一个可比较的值**。
 * 路由问「这一行去哪」，裁剪问「这个谓词可能命中哪些分区」——同一个边界的两个方向。
 *
 * 注意这里**完全不碰存储**：裁剪是纯元数据运算（原则 P3），
 * 被裁掉的分区不产生任何对象存储请求。
 */
object Partitioning {

    // ── 字面量 → Datum ────────────────────────────────────────────

    /**
     * 把 DDL 里的分区边界字面量转成与分区列同类型的 [Datum]。
     *
     * 必须转成**同类型**，否则比较会退化：`Datum.compareTo` 在类型不同时
     * 走的是跨类型兜底路径，`I64(100) <= Str("100")` 的结果不是使用者预期的那个。
     */
    fun literal(text: String, type: ColumnType): Datum {
        val t = text.trim()
        val raw = when {
            t.length >= 2 && (t.first() == '\'' || t.first() == '"') && t.last() == t.first() -> t.substring(1, t.length - 1)
            else -> t
        }
        return when (type) {
            ColumnType.TINYINT, ColumnType.SMALLINT, ColumnType.INT, ColumnType.BIGINT, ColumnType.BOOLEAN ->
                Datum.I64(raw.toLongOrNull() ?: throw badBound(text, type))
            ColumnType.FLOAT, ColumnType.DOUBLE ->
                Datum.F64(raw.toDoubleOrNull() ?: throw badBound(text, type))
            ColumnType.DECIMAL -> {
                val dot = raw.indexOf('.')
                val scale = if (dot < 0) 0 else raw.length - dot - 1
                val unscaled = raw.replace(".", "").toLongOrNull() ?: throw badBound(text, type)
                Datum.Dec(unscaled, scale)
            }
            ColumnType.DATE, ColumnType.DATETIME, ColumnType.TIMESTAMP -> Datum.Ts(parseDateMillis(raw) ?: throw badBound(text, type))
            else -> Datum.Str(raw)
        }
    }

    private fun badBound(text: String, type: ColumnType) =
        SqlException(MySqlErrors.ER_PARSE_ERROR, "42000", "分区边界 '$text' 无法转换为 ${type.mysqlName}")

    /**
     * `yyyy-MM-dd` 或 `yyyy-MM-dd HH:mm:ss`，按 **UTC** 解释。
     *
     * 直接复用 [Datum.parseDateTime]：分区边界与查询谓词必须是**同一个**解析器，
     * 否则同一个字符串会解出两个不同的时刻，裁剪与路由都会算错。
     */
    fun parseDateMillis(s: String): Long? = Datum.parseDateTime(s)

    /**
     * 求值 `TO_DAYS('...')` / `YEAR('...')` 这类分区边界表达式。
     *
     * 设计文档 §9.1 明确列了这两种写法，它们是按时间分区的实际写法，
     * 因此不支持的话「按时间分区」这件事就落不了地。
     */
    fun evalBoundExpr(text: String, type: ColumnType): Datum {
        val t = text.trim()
        val upper = t.uppercase()
        if (upper.startsWith("TO_DAYS(") || upper.startsWith("TO_DAYS (")) {
            val millis = parseDateMillis(literalArg(t)) ?: throw badBound(text, type)
            return Datum.I64(toDays(millis))
        }
        if (upper.startsWith("YEAR(")) {
            val millis = parseDateMillis(literalArg(t)) ?: throw badBound(text, type)
            return Datum.I64(yearOf(millis))
        }
        return literal(t, type)
    }

    /** MySQL 的 `TO_DAYS('0000-01-01')` = 1。求值与求值域必须用同一个函数。 */
    private fun toDays(millis: Long): Long {
        val d = java.time.LocalDate.ofInstant(java.time.Instant.ofEpochMilli(millis), java.time.ZoneOffset.UTC)
        return d.toEpochDay() - java.time.LocalDate.of(1, 1, 1).toEpochDay() + 366
    }

    private fun yearOf(millis: Long): Long =
        java.time.LocalDate.ofInstant(java.time.Instant.ofEpochMilli(millis), java.time.ZoneOffset.UTC).year.toLong()

    /**
     * 把分区表达式作用到**列的值**上。
     *
     * 这是 `PARTITION BY RANGE (TO_DAYS(d))` 能被正确路由的关键：边界求值得到的是
     * 天数，所以列值也必须先变成天数才能比较。两边用**同一组函数**
     * （[toDays] / [yearOf]）是刻意的——分别实现两份，迟早会有一份先改。
     */
    fun applyExpr(table: TableSchema, value: Datum): Datum {
        val expr = table.partitionExpr ?: return value
        val upper = expr.trim().uppercase()
        val millis = when (value) {
            is Datum.Ts -> value.millis
            is Datum.Str -> parseDateMillis(value.v) ?: return value
            is Datum.I64 -> value.v
            else -> return value
        }
        return when {
            upper.startsWith("TO_DAYS") -> Datum.I64(toDays(millis))
            upper.startsWith("YEAR") -> Datum.I64(yearOf(millis))
            else -> value
        }
    }

    private fun literalArg(call: String): String {
        val open = call.indexOf('(')
        val close = call.lastIndexOf(')')
        if (open < 0 || close <= open) return call
        return call.substring(open + 1, close).trim().trim('\'', '"')
    }

    /** HASH 声明列与等值裁剪共用的路由；没有显式列的内部 schema 仍按完整主键分片。 */
    fun hasHashColumn(table: TableSchema): Boolean =
        table.partitionKind == PartitionKind.HASH && table.partitionColumn != null

    /** 单列 HASH 的值路由。写入、主键点查、分区裁剪必须调用同一个函数。 */
    fun hashPartitionOf(table: TableSchema, value: Datum): Int {
        val type = table.column(requireNotNull(table.partitionColumn)).type
        val v = applyExpr(table, value.coerceTo(type))
        val encoded = KeyEncoder.encodeDatum(v, type)
        val hash = com.cloudea.yangbase.storage.XxHash64.hash(encoded, 0x5DEECE66DL)
        val n = table.effectivePartitionCount
        return (((hash % n) + n) % n).toInt()
    }

    // ── 路由 ──────────────────────────────────────────────────────

    /**
     * 一行落在哪个分区。
     *
     * RANGE/LIST 与显式单列 HASH 走值路由；KEY 的完整主键哈希在 [DatabaseEngine.shardFor]。
     */
    fun partitionIndexFor(table: TableSchema, row: List<Datum>): Int {
        if (!table.isEnumeratedPartition && !hasHashColumn(table)) return -1
        val col = table.partitionColumn ?: return 0
        val idx = table.indexOf(col)
        // 分区表达式（如 TO_DAYS）必须作用在列值上，否则就是拿时间戳比天数
        val raw = row.getOrNull(idx) ?: Datum.Null
        if (hasHashColumn(table)) return hashPartitionOf(table, raw)
        val v = applyExpr(table, raw)
        return when (table.partitionKind) {
            PartitionKind.RANGE -> rangePartitionOf(table, v)
            PartitionKind.LIST -> listPartitionOf(table, v)
            else -> 0
        }
    }

    /** RANGE：第一个「上界 > 值」的分区。找不到说明表定义有缺口（MySQL 里是 pmax）。 */
    private fun rangePartitionOf(table: TableSchema, v: Datum): Int {
        val col = table.columns[table.indexOf(table.partitionColumn!!)]
        val parts = table.partitions
        for ((i, p) in parts.withIndex()) {
            val bound = p.rangeUpperBound ?: continue
            if (bound.equals(MAXVALUE, ignoreCase = true)) return i
            val b = evalBoundExpr(bound, col.type)
            if (v.compareTo(b) < 0) return i
        }
        throw SqlException(
            MySqlErrors.ER_NO_PARTITION_FOR_GIVEN_VALUE, "HY000",
            "Table '${table.name}' has no partition for value $v——RANGE 分区的最后一个分区应为 VALUES LESS THAN MAXVALUE",
        )
    }

    private fun listPartitionOf(table: TableSchema, v: Datum): Int {
        val col = table.columns[table.indexOf(table.partitionColumn!!)]
        for ((i, p) in table.partitions.withIndex()) {
            for (lit in p.listValues) {
                if (v.compareTo(evalBoundExpr(lit, col.type)) == 0) return i
            }
        }
        throw SqlException(
            MySqlErrors.ER_NO_PARTITION_FOR_GIVEN_VALUE, "HY000",
            "Table '${table.name}' has no partition for value $v",
        )
    }

    // ── 裁剪 ──────────────────────────────────────────────────────

    /**
     * 分区列上的约束，表示为若干**闭区间段**的并集。
     *
     * `null` 表示无上界/无下界，而不是「空」——`col > 100` 的上界就是 null。
     */
    data class Interval(val low: Datum?, val lowInclusive: Boolean, val high: Datum?, val highInclusive: Boolean) {
        fun contains(v: Datum): Boolean {
            low?.let { val c = v.compareTo(it); if (c < 0 || (c == 0 && !lowInclusive)) return false }
            high?.let { val c = v.compareTo(it); if (c > 0 || (c == 0 && !highInclusive)) return false }
            return true
        }

        /** 与半开区间 `[lo, hi)` 是否有交集。[lo, hi) 用来描述 RANGE 分区的覆盖范围。 */
        fun intersectsHalfOpen(lo: Datum?, hi: Datum?): Boolean {
            // 本区间整体在分区之前
            if (high != null && lo != null && high.compareTo(lo) < 0) return false
            if (high != null && lo != null && high.compareTo(lo) == 0 && !highInclusive) return false
            // 分区整体在本区间之前
            if (low != null && hi != null && low.compareTo(hi) >= 0) return false
            return true
        }
    }

    /** 一个 WHERE 子句在分区列上能推出的全部约束。`null` 表示推不出任何可用约束（=不能裁剪）。 */
    data class Constraint(val intervals: List<Interval>) {
        fun satisfies(v: Datum): Boolean = intervals.any { it.contains(v) }
    }

    /**
     * 哪些分区**可能**含有满足 [constraint] 的行。
     *
     * 返回 null 表示无法裁剪（调用方应扫描全部分区）。返回空集表示该查询
     * 一定没有结果——这是一个**零 IO 的空结果**，也是裁剪最有价值的形态。
     */
    fun prune(table: TableSchema, constraint: Constraint?): Set<Int>? {
        if (constraint == null) return null
        if (hasHashColumn(table)) {
            // 哈希没有单调性：只有闭区间退化为单个值时才能路由，范围必须保守全扫。
            if (constraint.intervals.any { it.low == null || it.high == null ||
                    !it.lowInclusive || !it.highInclusive || it.low.compareTo(it.high) != 0 }) return null
            return constraint.intervals.map { hashPartitionOf(table, it.low!!) }.toSet()
        }
        if (!table.isEnumeratedPartition) return null
        val col = table.columns[table.indexOf(table.partitionColumn!!)]
        val n = table.effectivePartitionCount
        val keep = LinkedHashSet<Int>()

        when (table.partitionKind) {
            PartitionKind.RANGE -> {
                var lower: Datum? = null
                for (i in 0 until n) {
                    val boundText = table.partitions[i].rangeUpperBound
                    val isMax = boundText == null || boundText.equals(MAXVALUE, ignoreCase = true)
                    val upper: Datum? = if (isMax) null else evalBoundExpr(boundText, col.type)
                    if (constraint.intervals.any { it.intersectsHalfOpen(lower, upper) }) keep.add(i)
                    lower = upper
                    if (isMax) break
                }
            }
            PartitionKind.LIST -> {
                for (i in 0 until n) {
                    val def = table.partitions[i]
                    if (def.listValues.any { lit -> constraint.satisfies(evalBoundExpr(lit, col.type)) }) keep.add(i)
                }
            }
            else -> return null
        }
        return keep
    }

    /**
     * 校验分区定义。**在 DDL 时失败，而不是在插入时才失败。**
     *
     * 最关键的一条是「分区列必须是主键的一部分」：本系统的每个分区是一棵
     * 独立的主键有序 LSM，点查只拿到主键。若分区列不在主键里，
     * `WHERE pk = ?` 就无法确定该读哪个分区，只能退化成扫描所有分区——
     * 那分区就从「写入并行度」变成了纯粹的负担。MySQL 有同样的约束。
     */
    fun validate(table: TableSchema) {
        if (table.partitionKind == PartitionKind.NONE) return
        if (!table.partitionKind.isEnumerated) {
            if (table.partitionCount <= 0) throw SqlException(MySqlErrors.ER_PARSE_ERROR, "42000", "PARTITIONS 必须为正数")
            if (!hasHashColumn(table)) return
        }
        val col = table.partitionColumn
            ?: throw SqlException(MySqlErrors.ER_PARSE_ERROR, "42000", "${table.partitionKind} 分区必须指定分区列")
        table.indexOf(col) // 列不存在时抛出
        if (col.lowercase() !in table.primaryKey.map { it.lowercase() }) {
            throw SqlException(
                MySqlErrors.ER_PARSE_ERROR, "42000",
                "分区列 '$col' 必须是主键的一部分（否则主键点查无法定位分区）",
            )
        }
        if (hasHashColumn(table)) {
            if (table.partitionExpr != null) throw SqlException(MySqlErrors.ER_PARSE_ERROR, "42000",
                "HASH 当前支持直接引用分区列，不支持分区表达式")
            if (!table.column(col).type.isInteger) throw SqlException(MySqlErrors.ER_PARSE_ERROR, "42000",
                "HASH 分区列必须为整数类型")
            return
        }
        if (table.partitions.isEmpty()) {
            throw SqlException(MySqlErrors.ER_PARSE_ERROR, "42000", "${table.partitionKind} 分区必须给出分区定义")
        }
        // 分区表达式只支持 TO_DAYS / YEAR。**不支持的表达式必须在此刻报错**——
        // 悄悄忽略它会让边界按原始列值比较，而使用者以为按天数分区：
        // 数据会落进「看起来对」的分区里，直到某天范围查询少了几行。
        table.partitionExpr?.let { e ->
            val u = e.trim().uppercase()
            if (!(u.startsWith("TO_DAYS") || u.startsWith("YEAR"))) {
                throw SqlException(
                    MySqlErrors.ER_PARSE_ERROR, "42000",
                    "不支持的 RANGE 分区表达式 '$e'（当前支持 TO_DAYS / YEAR，或直接引用列）",
                )
            }
        }

        if (table.partitionKind == PartitionKind.RANGE) {
            var prev: Datum? = null
            var sawMax = false
            for (p in table.partitions) {
                if (sawMax) throw SqlException(MySqlErrors.ER_PARSE_ERROR, "42000", "MAXVALUE 只能是最后一个分区")
                val text = p.rangeUpperBound
                    ?: throw SqlException(MySqlErrors.ER_PARSE_ERROR, "42000", "分区 '${p.name}' 缺少 VALUES LESS THAN")
                if (text.equals(MAXVALUE, ignoreCase = true)) { sawMax = true; continue }
                val b = evalBoundExpr(text, table.column(col).type)
                if (prev != null && prev!!.compareTo(b) >= 0) {
                    throw SqlException(MySqlErrors.ER_PARSE_ERROR, "42000", "RANGE 分区边界必须严格递增（分区 '${p.name}'）")
                }
                prev = b
            }
        } else {
            val seen = HashMap<String, String>()
            for (p in table.partitions) {
                if (p.listValues.isEmpty()) {
                    throw SqlException(MySqlErrors.ER_PARSE_ERROR, "42000", "LIST 分区 '${p.name}' 缺少 VALUES IN")
                }
                for (lit in p.listValues) {
                    val prev = seen.put(lit, p.name)
                    if (prev != null) {
                        throw SqlException(MySqlErrors.ER_PARSE_ERROR, "42000", "值 '$lit' 同时出现在分区 '$prev' 与 '${p.name}' 中")
                    }
                }
            }
        }
    }

    /** 由主键编码反查分区。分区列在主键里，所以可以从编码中解出它的值。 */
    fun partitionIndexFromPk(table: TableSchema, encodedPk: ByteArray): Int {
        if (!table.isEnumeratedPartition && !hasHashColumn(table)) return -1
        val colName = table.partitionColumn ?: return 0
        val colIdx = table.indexOf(colName)
        val pkPos = table.primaryKeyIndexes.indexOf(colIdx)
        if (pkPos < 0) return 0
        val values = KeyEncoder.decodeComponents(encodedPk, table, table.primaryKeyIndexes)
        val raw = values.getOrNull(pkPos) ?: Datum.Null
        if (hasHashColumn(table)) return hashPartitionOf(table, raw)
        val v = applyExpr(table, raw)
        return when (table.partitionKind) {
            PartitionKind.RANGE -> rangePartitionOf(table, v)
            PartitionKind.LIST -> listPartitionOf(table, v)
            else -> 0
        }
    }
}
