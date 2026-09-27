package com.cloudea.yangbase.txn

import com.cloudea.yangbase.common.ColumnDef
import com.cloudea.yangbase.common.ColumnType
import com.cloudea.yangbase.common.Datum
import com.cloudea.yangbase.common.MAXVALUE
import com.cloudea.yangbase.common.PartitionDef
import com.cloudea.yangbase.common.PartitionKind
import com.cloudea.yangbase.common.SqlException
import com.cloudea.yangbase.common.TableSchema
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * 分区路由与裁剪的**纯逻辑**测试。
 *
 * JDBC 端到端测试能证明「查询结果对」，但证明不了边界算术对——
 * 那需要把边界本身拿出来逐个验证。裁剪裁错的后果是**静默丢行**，
 * 所以每一个边界都要有一条断言盯着。
 */
class PartitioningTest {

    private fun col(n: String, t: ColumnType) = ColumnDef(n, t, nullable = false)

    private fun rangeTable(
        bounds: List<String>,
        colType: ColumnType = ColumnType.BIGINT,
    ): TableSchema = TableSchema(
        tableId = 1, databaseId = 1, name = "t", version = 1,
        columns = listOf(col("id", colType)),
        primaryKey = listOf("id"),
        partitionKind = PartitionKind.RANGE,
        partitionColumn = "id",
        partitions = bounds.mapIndexed { i, b -> PartitionDef("p$i", PartitionKind.RANGE, rangeUpperBound = b) },
    )

    private fun listTable(values: List<List<String>>): TableSchema = TableSchema(
        tableId = 1, databaseId = 1, name = "t", version = 1,
        columns = listOf(col("region", ColumnType.VARCHAR)),
        primaryKey = listOf("region"),
        partitionKind = PartitionKind.LIST,
        partitionColumn = "region",
        partitions = values.mapIndexed { i, v -> PartitionDef("p$i", PartitionKind.LIST, listValues = v) },
    )

    private fun idx(table: TableSchema, v: Datum): Int =
        Partitioning.partitionIndexFor(table, listOf(v))

    // ── RANGE 路由 ────────────────────────────────────────────────

    @Test
    fun `RANGE 上界是开区间`() {
        val t = rangeTable(listOf("100", "200", MAXVALUE))
        assertEquals(0, idx(t, Datum.I64(99)), "99 落在 p0")
        assertEquals(0, idx(t, Datum.I64(0)))
        // 上界本身属于**下一个**分区——这是 `LESS THAN` 的定义
        assertEquals(1, idx(t, Datum.I64(100)), "100 不属于 p0，而是属于 p1")
        assertEquals(1, idx(t, Datum.I64(199)))
        assertEquals(2, idx(t, Datum.I64(200)))
        assertEquals(2, idx(t, Datum.I64(Long.MAX_VALUE)))
    }

    @Test
    fun `RANGE 负数与 0 都落到第一个分区`() {
        val t = rangeTable(listOf("0", MAXVALUE))
        assertEquals(0, idx(t, Datum.I64(-1)))
        assertEquals(0, idx(t, Datum.I64(-99999)))
        assertEquals(1, idx(t, Datum.I64(0)))
    }

    @Test
    fun `RANGE 缺 MAXVALUE 时越界值报错而不是随便塞`() {
        val t = rangeTable(listOf("100", "200"))
        // 悄悄塞进最后一个分区是最糟的处理方式：使用者会以为数据分区是正确的
        val e = assertFailsWith<SqlException> { idx(t, Datum.I64(500)) }
        assertEquals(1526, e.errorCode, "应为 ER_NO_PARTITION_FOR_GIVEN_VALUE")
    }

    @Test
    fun `RANGE 日期边界`() {
        val t = rangeTable(
            listOf("'2026-04-01 00:00:00'", "'2026-07-01 00:00:00'", MAXVALUE),
            ColumnType.DATETIME,
        )
        val feb = Datum.Ts(Datum.parseDateTime("2026-02-01 10:00:00")!!)
        val may = Datum.Ts(Datum.parseDateTime("2026-05-01 10:00:00")!!)
        val aug = Datum.Ts(Datum.parseDateTime("2026-08-01 10:00:00")!!)
        assertEquals(0, idx(t, feb))
        assertEquals(1, idx(t, may))
        assertEquals(2, idx(t, aug))
    }

    @Test
    fun `TO_DAYS 与 YEAR 边界表达式可求值`() {
        val t = TableSchema(
            tableId = 1, databaseId = 1, name = "t", version = 1,
            columns = listOf(col("d", ColumnType.DATETIME), col("y", ColumnType.INT)),
            primaryKey = listOf("d", "y"),
            partitionKind = PartitionKind.RANGE,
            partitionColumn = "d",
            // 表达式必须一起给：边界是**天数**，列值是**时间戳**。
            // 只给列名会让两者在不同量纲上比较——这正是不记表达式的危害。
            partitionExpr = "TO_DAYS(d)",
            partitions = listOf(
                PartitionDef("p0", PartitionKind.RANGE, rangeUpperBound = "TO_DAYS('2026-04-01')"),
                PartitionDef("pmax", PartitionKind.RANGE, rangeUpperBound = MAXVALUE),
            ),
        )
        val before = Datum.Ts(Datum.parseDateTime("2026-03-01 00:00:00")!!)
        val after = Datum.Ts(Datum.parseDateTime("2026-05-01 00:00:00")!!)
        // TO_DAYS 的分区边界只能通过「求值后再比较」来路由：
        // 边界是整数天数，列是时间戳，两边都要先过 TO_DAYS 才能比。
        assertEquals(0, Partitioning.partitionIndexFor(t, listOf(before, Datum.I64(1))))
        assertEquals(1, Partitioning.partitionIndexFor(t, listOf(after, Datum.I64(1))))
    }

    // ── LIST 路由 ─────────────────────────────────────────────────

    @Test
    fun `LIST 按值集合路由`() {
        val t = listTable(listOf(listOf("'cn'", "'hk'"), listOf("'us'", "'ca'")))
        assertEquals(0, idx(t, Datum.Str("cn")))
        assertEquals(0, idx(t, Datum.Str("hk")))
        assertEquals(1, idx(t, Datum.Str("us")))
        assertFailsWith<SqlException> { idx(t, Datum.Str("jp")) }
    }

    @Test
    fun `LIST 数值集合`() {
        val t = TableSchema(
            tableId = 1, databaseId = 1, name = "t", version = 1,
            columns = listOf(col("id", ColumnType.BIGINT)),
            primaryKey = listOf("id"),
            partitionKind = PartitionKind.LIST,
            partitionColumn = "id",
            partitions = listOf(
                PartitionDef("p0", PartitionKind.LIST, listValues = listOf("1", "2")),
                PartitionDef("p1", PartitionKind.LIST, listValues = listOf("3")),
            ),
        )
        assertEquals(0, idx(t, Datum.I64(1)))
        assertEquals(0, idx(t, Datum.I64(2)))
        assertEquals(1, idx(t, Datum.I64(3)))
    }

    // ── DDL 校验 ──────────────────────────────────────────────────

    @Test
    fun `分区列必须在主键里`() {
        val t = TableSchema(
            tableId = 1, databaseId = 1, name = "t", version = 1,
            columns = listOf(col("id", ColumnType.BIGINT), col("region", ColumnType.VARCHAR)),
            primaryKey = listOf("id"),
            partitionKind = PartitionKind.LIST,
            partitionColumn = "region",
            partitions = listOf(PartitionDef("p0", PartitionKind.LIST, listValues = listOf("'cn'"))),
        )
        // 理由不是「MySQL 也这么要求」，而是：每个分区是一棵按主键排序的独立 LSM，
        // 点查只拿得到主键。分区列不在主键里，就无从确定该读哪个分区。
        assertFailsWith<SqlException> { Partitioning.validate(t) }
    }

    @Test
    fun `RANGE 边界必须严格递增`() {
        assertFailsWith<SqlException> { Partitioning.validate(rangeTable(listOf("200", "100", MAXVALUE))) }
        assertFailsWith<SqlException> { Partitioning.validate(rangeTable(listOf("100", "100", MAXVALUE))) }
        Partitioning.validate(rangeTable(listOf("100", "200", MAXVALUE)))   // 合法
    }

    @Test
    fun `MAXVALUE 只能是最后一个`() {
        assertFailsWith<SqlException> { Partitioning.validate(rangeTable(listOf(MAXVALUE, "100"))) }
    }

    @Test
    fun `LIST 的值不能跨分区重复`() {
        assertFailsWith<SqlException> {
            Partitioning.validate(listTable(listOf(listOf("'cn'"), listOf("'cn'", "'us'"))))
        }
        Partitioning.validate(listTable(listOf(listOf("'cn'"), listOf("'us'"))))
    }

    @Test
    fun `HASH 分区不做枚举校验`() {
        val t = TableSchema(
            tableId = 1, databaseId = 1, name = "t", version = 1,
            columns = listOf(col("id", ColumnType.BIGINT)),
            primaryKey = listOf("id"),
            partitionKind = PartitionKind.HASH,
            partitionCount = 8,
        )
        Partitioning.validate(t)
        assertEquals(8, t.effectivePartitionCount)
    }

    @Test
    fun `未分区表的有效分区数是 1`() {
        val t = TableSchema(
            tableId = 1, databaseId = 1, name = "t", version = 1,
            columns = listOf(col("id", ColumnType.BIGINT)),
            primaryKey = listOf("id"),
        )
        assertEquals(1, t.effectivePartitionCount)
        assertTrue(!t.isEnumeratedPartition)
    }

    // ── 裁剪 ──────────────────────────────────────────────────────

    /** `col ∈ [lo, hi]` 形式的约束。 */
    private fun constraint(lo: Datum?, hi: Datum?): Partitioning.Constraint =
        Partitioning.Constraint(listOf(Partitioning.Interval(lo, true, hi, true)))

    private fun eq(v: Datum) = Partitioning.Constraint(listOf(Partitioning.Interval(v, true, v, true)))

    @Test
    fun `裁剪 - 等值命中唯一分区`() {
        val t = rangeTable(listOf("100", "200", "300", MAXVALUE))
        assertEquals(setOf(1), Partitioning.prune(t, eq(Datum.I64(150))))
        assertEquals(setOf(0), Partitioning.prune(t, eq(Datum.I64(99))))
        assertEquals(setOf(3), Partitioning.prune(t, eq(Datum.I64(99999))))
    }

    @Test
    fun `裁剪 - 区间跨越多个分区`() {
        val t = rangeTable(listOf("100", "200", "300", MAXVALUE))
        assertEquals(setOf(0, 1), Partitioning.prune(t, constraint(Datum.I64(50), Datum.I64(150))))
        assertEquals(setOf(0, 1, 2), Partitioning.prune(t, constraint(Datum.I64(0), Datum.I64(250))))
    }

    @Test
    fun `裁剪 - 边界恰好落在分区上界时不能漏掉右侧分区`() {
        val t = rangeTable(listOf("100", "200", "300", MAXVALUE))
        // `col = 100` 属于 p1（LESS THAN 100 是开区间）。裁成 p0 就会**静默丢行**。
        assertEquals(setOf(1), Partitioning.prune(t, eq(Datum.I64(100))))
        // `col >= 100 AND col <= 100` 同上
        assertEquals(setOf(1), Partitioning.prune(t, constraint(Datum.I64(100), Datum.I64(100))))
    }

    @Test
    fun `裁剪 - 完全在范围外时裁成空集`() {
        val t = rangeTable(listOf("100", "200", "300"))
        // 空集是裁剪最有价值的形态：**零 IO 的空结果**
        assertEquals(emptySet(), Partitioning.prune(t, constraint(Datum.I64(1000), Datum.I64(2000))))
    }

    @Test
    fun `裁剪 - 无上界的约束不会误裁`() {
        val t = rangeTable(listOf("100", "200", MAXVALUE))
        assertEquals(setOf(2), Partitioning.prune(t, constraint(Datum.I64(500), null)))
        assertEquals(setOf(0, 1, 2), Partitioning.prune(t, constraint(null, null)))
    }

    @Test
    fun `裁剪 - LIST 按值集合排除`() {
        val t = listTable(listOf(listOf("'cn'", "'hk'"), listOf("'us'", "'ca'")))
        assertEquals(setOf(0), Partitioning.prune(t, eq(Datum.Str("cn"))))
        assertEquals(setOf(1), Partitioning.prune(t, eq(Datum.Str("us"))))
        // 'jp' 不在任何分区里 → 空集（零 IO）
        assertEquals(emptySet(), Partitioning.prune(t, eq(Datum.Str("jp"))))
        // 区间约束与 LIST 的交集
        assertEquals(setOf(0, 1), Partitioning.prune(t, constraint(Datum.Str("cn"), Datum.Str("us"))))
    }

    @Test
    fun `裁剪 - 非枚举分区不做裁剪`() {
        val t = TableSchema(
            tableId = 1, databaseId = 1, name = "t", version = 1,
            columns = listOf(col("id", ColumnType.BIGINT)),
            primaryKey = listOf("id"),
            partitionKind = PartitionKind.HASH,
            partitionCount = 4,
        )
        // 哈希分区只能做等值裁剪，而等值需要先算出哈希——
        // 返回 null（=不裁剪）而不是猜一个，是正确的保守行为
        assertNull(Partitioning.prune(t, eq(Datum.I64(1))))
    }

    @Test
    fun `裁剪 - 约束为 null 时返回 null 表示无法裁剪`() {
        val t = rangeTable(listOf("100", MAXVALUE))
        assertNull(Partitioning.prune(t, null))
    }

    // ── 类型转换 ──────────────────────────────────────────────────

    @Test
    fun `字面量按列类型转换`() {
        assertEquals(Datum.I64(100), Partitioning.literal("100", ColumnType.BIGINT))
        assertEquals(Datum.Str("cn"), Partitioning.literal("'cn'", ColumnType.VARCHAR))
        assertEquals(Datum.F64(1.5), Partitioning.literal("1.5", ColumnType.DOUBLE))
        assertEquals(Datum.Dec(1234, 2), Partitioning.literal("12.34", ColumnType.DECIMAL))
        val ts = Partitioning.literal("'2026-01-01 00:00:00'", ColumnType.DATETIME) as Datum.Ts
        assertEquals(1_767_225_600_000L, ts.millis)
    }

    @Test
    fun `越界的字面量在 DDL 时就报错`() {
        // 分区边界写错了必须在建表时报错，而不是等第一条插入落不进去才发现
        assertFailsWith<SqlException> { Partitioning.literal("abc", ColumnType.BIGINT) }
        assertFailsWith<SqlException> { Partitioning.literal("'not-a-date'", ColumnType.DATETIME) }
    }

    @Test
    fun `分区表达式只支持 TO_DAYS 与 YEAR - 其余在 DDL 时拒绝`() {
        val t = TableSchema(
            tableId = 1, databaseId = 1, name = "t", version = 1,
            columns = listOf(col("d", ColumnType.DATETIME)),
            primaryKey = listOf("d"),
            partitionKind = PartitionKind.RANGE,
            partitionColumn = "d",
            // 悄悄忽略不认识的表达式，会让使用者以为按它分区了——
            // 数据落进「看起来对」的分区，直到某天范围查询少几行才发现
            partitionExpr = "MONTH(d)",
            partitions = listOf(PartitionDef("p0", PartitionKind.RANGE, rangeUpperBound = MAXVALUE)),
        )
        assertFailsWith<SqlException> { Partitioning.validate(t) }
    }

    @Test
    fun `分区表达式的求值与边界求值在同一量纲上`() {
        val t = TableSchema(
            tableId = 1, databaseId = 1, name = "t", version = 1,
            columns = listOf(col("d", ColumnType.DATETIME)),
            primaryKey = listOf("d"),
            partitionKind = PartitionKind.RANGE,
            partitionColumn = "d",
            partitionExpr = "TO_DAYS(d)",
            partitions = listOf(
                PartitionDef("p0", PartitionKind.RANGE, rangeUpperBound = "TO_DAYS('2026-04-01')"),
                PartitionDef("pmax", PartitionKind.RANGE, rangeUpperBound = MAXVALUE),
            ),
        )
        // 边界求值得到天数，列值也必须变成天数——两边是同一个函数
        val bound = Partitioning.evalBoundExpr("TO_DAYS('2026-04-01')", ColumnType.DATETIME) as Datum.I64
        assertTrue(bound.v > 700_000 && bound.v < 800_000, "TO_DAYS 应得到天数级数量级，实际 ${bound.v}")
        val colVal = Partitioning.applyExpr(t, Datum.Ts(Datum.parseDateTime("2026-04-01 00:00:00")!!)) as Datum.I64
        assertEquals(bound.v, colVal.v, "边界值与列值在同一时刻必须相等——否则路由会在边界上错位")
    }

    @Test
    fun `时间字面量与谓词转换使用同一个解析器`() {
        // 曾经插入用 systemDefault、分区边界用 UTC，同一个字符串解出两个时刻，
        // 于是 WHERE dt = '...' 静默返回空集。这里把「只有一个解析器」钉死。
        val a = Partitioning.parseDateMillis("2026-01-01 10:00:00")
        val b = Datum.parseDateTime("2026-01-01 10:00:00")
        assertEquals(b, a)
        assertEquals(a, Datum.Str("2026-01-01 10:00:00").coerceTo(ColumnType.DATETIME).let { (it as Datum.Ts).millis })
    }

    // ── 由主键编码反查 ────────────────────────────────────────────

    @Test
    fun `从主键编码能反查分区`() {
        val t = rangeTable(listOf("100", "200", MAXVALUE))
        val pk = com.cloudea.yangbase.common.KeyEncoder.encodeKey(
            listOf(Datum.I64(150)), t, t.primaryKeyIndexes,
        )
        assertEquals(1, Partitioning.partitionIndexFromPk(t, pk))
    }

    @Test
    fun `复合主键里分区列不是首列也能反查`() {
        val t = TableSchema(
            tableId = 1, databaseId = 1, name = "t", version = 1,
            columns = listOf(col("id", ColumnType.BIGINT), col("d", ColumnType.DATETIME)),
            primaryKey = listOf("id", "d"),
            partitionKind = PartitionKind.RANGE,
            partitionColumn = "d",
            partitions = listOf(
                PartitionDef("p0", PartitionKind.RANGE, rangeUpperBound = "'2026-04-01 00:00:00'"),
                PartitionDef("pmax", PartitionKind.RANGE, rangeUpperBound = MAXVALUE),
            ),
        )
        val row = listOf(Datum.I64(7), Datum.Ts(Datum.parseDateTime("2026-05-01 00:00:00")!!))
        val pk = com.cloudea.yangbase.common.KeyEncoder.encodeKey(row, t, t.primaryKeyIndexes)
        // 分区列在第二个位置——这正是「分区列必须在主键里」这条约束的用处：
        // 只要在主键里，位置无关，总能从编码里解出来
        assertEquals(1, Partitioning.partitionIndexFromPk(t, pk))
    }
    @Test
    fun `显式 HASH 列在复合主键末尾 - 写入点查裁剪必须同路由`() {
        val t = TableSchema(tableId = 1, databaseId = 1, name = "h", version = 1,
            columns = listOf(col("id", ColumnType.BIGINT), col("tenant", ColumnType.BIGINT)),
            primaryKey = listOf("id", "tenant"), partitionKind = PartitionKind.HASH,
            partitionCount = 4, partitionColumn = "tenant")
        Partitioning.validate(t)
        for (tenant in -20L..20L) {
            val expected = Partitioning.hashPartitionOf(t, Datum.I64(tenant))
            for (id in 1L..8L) {
                val row = listOf(Datum.I64(id), Datum.I64(tenant))
                val pk = com.cloudea.yangbase.common.KeyEncoder.encodeKey(row, t, t.primaryKeyIndexes)
                assertEquals(expected, Partitioning.partitionIndexFor(t, row))
                assertEquals(expected, Partitioning.partitionIndexFromPk(t, pk))
            }
            assertEquals(setOf(expected), Partitioning.prune(t, eq(Datum.Str(tenant.toString()))))
        }
        assertNull(Partitioning.prune(t, Partitioning.Constraint(listOf(
            Partitioning.Interval(Datum.I64(1), true, Datum.I64(9), true)))))
        assertFailsWith<SqlException> { Partitioning.validate(t.copy(primaryKey = listOf("id"))) }
        assertFailsWith<SqlException> { Partitioning.validate(t.copy(partitionExpr = "ABS(tenant)")) }
    }

}
