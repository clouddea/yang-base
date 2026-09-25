package com.cloudea.yangbase.sql

import com.cloudea.yangbase.common.ColumnDef
import com.cloudea.yangbase.common.ColumnType
import com.cloudea.yangbase.common.Datum
import com.cloudea.yangbase.common.IndexDef
import com.cloudea.yangbase.common.IndexKind
import com.cloudea.yangbase.common.TableSchema
import com.cloudea.yangbase.common.stats.ColumnStats
import com.cloudea.yangbase.common.stats.Histogram
import com.cloudea.yangbase.common.stats.NdvSource
import com.cloudea.yangbase.common.stats.PartitionStats
import com.cloudea.yangbase.common.stats.TableStats
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.test.assertTrue

/**
 * 代价模型（§10.2 的 R1–R6）与 `EXPLAIN`/`ANALYZE` 语法的**纯逻辑**测试。
 *
 * 为什么这些必须在单元测试里而不是端到端：选路是**决策**，端到端只能看到它
 * 的后果（读了多少字节）。选错路不会报错、不会算错——只会慢。而「慢」在一张
 * 几万行的测试表上根本测不出来。所以路径选择必须逐条直接断言。
 */
class OptimizerTest {

    // ── 夹具 ──────────────────────────────────────────────────────

    private val table = TableSchema(
        tableId = 1, databaseId = 1, name = "t", version = 1,
        columns = listOf(
            ColumnDef("id", ColumnType.BIGINT, nullable = false),
            ColumnDef("n", ColumnType.INT, nullable = true),
            ColumnDef("city", ColumnType.VARCHAR, nullable = true, length = 16),
        ),
        primaryKey = listOf("id"),
        indexes = listOf(
            IndexDef(id = 0, name = "idx_n", kind = IndexKind.ORDERED, columns = listOf("n"), unique = false),
        ),
    )

    /** 复合主键表：键是逐列拼接的，前缀约束的规则与单列不同（最左前缀）。 */
    private val composite = TableSchema(
        tableId = 2, databaseId = 1, name = "t3", version = 1,
        columns = listOf(
            ColumnDef("a", ColumnType.INT, nullable = false),
            ColumnDef("b", ColumnType.INT, nullable = false),
            ColumnDef("v", ColumnType.INT),
        ),
        primaryKey = listOf("a", "b"),
    )

    private fun col(
        name: String,
        ordinal: Int,
        type: String = "BIGINT",
        ndv: Long = ColumnStats.NDV_UNKNOWN,
        min: Double? = null,
        max: Double? = null,
        observed: Long = 1_000_000,
        histogram: Histogram? = null,
    ) = ColumnStats(
        name = name, ordinal = ordinal, type = type, observedRows = observed, nullCount = 0,
        minNum = min, maxNum = max, ndv = ndv,
        ndvExact = ndv >= 0, ndvSource = if (ndv >= 0) NdvSource.VALUES else NdvSource.UNKNOWN,
        histogram = histogram,
    )

    /** 一张 100 万行、10 万块（每块 10 行 → 估算成 100 次请求量级）的表。 */
    private fun bigStats(
        columns: List<ColumnStats> = listOf(col("id", 0, min = 0.0, max = 999_999.0), col("n", 1, "INT", ndv = 1000, min = 0.0, max = 999.0)),
        rows: Long = 1_000_000,
        blockCount: Int = 10_000,
    ) = TableStats(
        tableId = 1, tableName = "t", statsVersion = 2, rowCount = rows, rowCountExact = true,
        partitions = listOf(PartitionStats(0, "p0", rows, rows, fileCount = 10, blockCount = blockCount)),
        columns = columns,
    )

    private fun input(
        where: Expr? = null,
        stats: TableStats? = null,
        constPk: Boolean = false,
        rangeSelectivity: Double? = null,
        indexCandidates: List<IndexCandidate> = emptyList(),
        rows: Long = 1_000_000,
        shards: Int = 1,
        totalShards: Int = shards,
        metadataRows: Long? = null,
        metadataBlocked: String? = null,
        fullText: Boolean = false,
        orderBy: Boolean = false,
        blockPruning: Boolean = false,
    ) = PlannerInput(
        table = table, stats = stats, rowCount = rows, shards = shards, totalShards = totalShards,
        where = where, constPk = constPk, rangeSelectivity = rangeSelectivity,
        indexCandidates = indexCandidates, metadataCountRows = metadataRows,
        metadataCountBlocked = metadataBlocked, fullText = fullText, orderBy = orderBy,
        blockPruning = blockPruning,
    )

    private fun idxCandidate(bound: Int = 1, selectivity: Double = 0.001) = IndexCandidate(
        index = table.indexes[0], value = Datum.I64(7), boundColumns = bound, selectivity = selectivity,
    )

    // ── R6：计数走元数据（硬规则）─────────────────────────────────

    @Test
    fun `R6 - 元数据计数可用时直接胜出 不比代价`() {
        // 即便同时存在一个「更便宜」的点查条件，计数也必须走元数据路径
        val plan = CostModel.choose(
            input(stats = bigStats(), constPk = true, metadataRows = 42, where = null)
        )
        assertEquals(AccessType.METADATA_COUNT, plan.type)
        assertEquals("METADATA_COUNT", plan.type.explainName, "EXPLAIN 的 type 必须能显示它（§8.9.6）")
        assertEquals(42L, plan.rows, "rows 必须是元数据给出的精确行数，不是估算")
        assertEquals(0.0, plan.gets, "元数据计数的代价是 0 次对象存储请求")
        assertTrue(
            plan.extra.any { it.contains("0 object reads") },
            "Extra 必须写明它不读对象存储：${plan.extra}",
        )
    }

    @Test
    fun `元数据计数不可用时 - 如实写出原因`() {
        // ⚠️ 这条是最容易漏掉的诚实性缺口：`COUNT(*)` 本该 0 次 GET、却退化成扫描时，
        // 原因**必须**出现在 EXPLAIN 里。它不能藏在计数路径自己的 Extra 里——
        // 那段文案只在计数路径被选中时才出现，而这里恰恰是它没被选中。
        val blocked = "metadata count unavailable: transaction has uncommitted writes (metadata reflects committed state only)"
        val inp = input(stats = bigStats(), where = parseWhere("n = 7"), metadataBlocked = blocked)
        val plan = CostModel.choose(inp)
        assertTrue(plan.type != AccessType.METADATA_COUNT)
        val extra = plan.extra + CostModel.trailingExtras(inp)
        assertTrue(
            extra.any { it.contains("uncommitted writes") },
            "被挡住的原因必须出现在 Extra 里，否则使用者分不清「本来就该扫描」与「退化了」：$extra",
        )
    }

    // ── 兜底链：没有统计时不退化 ──────────────────────────────────

    @Test
    fun `没有统计时退回硬编码优先级链 - 且顺序不变`() {
        // 精确主键 → 键范围 → 二级索引 → 全表
        assertEquals(
            AccessType.CONST,
            CostModel.choose(input(stats = null, constPk = true, rangeSelectivity = 0.9, indexCandidates = listOf(idxCandidate()))).type,
        )
        assertEquals(
            AccessType.RANGE,
            CostModel.choose(input(stats = null, rangeSelectivity = 0.9, indexCandidates = listOf(idxCandidate()))).type,
        )
        assertEquals(
            AccessType.REF,
            CostModel.choose(input(stats = null, indexCandidates = listOf(idxCandidate()))).type,
        )
        assertEquals(AccessType.ALL, CostModel.choose(input(stats = null)).type)
    }

    @Test
    fun `没有统计时不做代价比较 - 索引可用就必须用`() {
        // 小表上按默认选择率算，扫描（1.0 次请求）会显得比索引（2 次请求）便宜，
        // 于是代价比较会放弃索引。没有统计时**必须**不比较——否则未经 ANALYZE
        // 的表会比优化器引入之前更慢，这是不可接受的退化。
        val plan = CostModel.choose(
            input(stats = null, rows = 100, indexCandidates = listOf(idxCandidate(selectivity = 0.1)))
        )
        assertEquals(AccessType.REF, plan.type)
    }

    @Test
    fun `没有统计时 EXPLAIN 必须说明这一点`() {
        val extras = CostModel.trailingExtras(input(stats = null))
        assertTrue(extras.any { it.contains("no statistics") }, extras.toString())
        assertTrue(
            CostModel.trailingExtras(input(stats = bigStats())).none { it.contains("no statistics") },
            "有统计时不该出现这条提示",
        )
    }

    // ── R1/R4：按请求数比价 ───────────────────────────────────────

    @Test
    fun `大表上高选择性的索引赢过全表扫描`() {
        val plan = CostModel.choose(
            input(stats = bigStats(), indexCandidates = listOf(idxCandidate(selectivity = 0.0001)))
        )
        assertEquals(AccessType.REF, plan.type)
        assertTrue(plan.gets < 5.0, "命中 100 行的索引查找应当是几次请求，实际 ${plan.gets}")
        assertTrue(plan.extra.any { it == "Using index" }, "索引项存整行 ⇒ 不回表，Extra 如实写：${plan.extra}")
    }

    @Test
    fun `索引命中的块数与扫描一样多时 - 索引自身那次请求让它输掉`() {
        // 模型里唯一能让索引输的情形：索引几乎不减小读块范围（`ndv=1`，
        // 整列同一个值）时，它要和扫描读一样多的块，**还多读一次索引分片自身**。
        // 1 次请求的差距就够了——这正是 R1 把请求数当主成本项的意义：
        // 在一个 1 万块的扫描里，1 次请求是 0.008%，但它决定了走哪条路。
        val sameValue = bigStats(
            columns = listOf(
                col("id", 0, min = 0.0, max = 999_999.0),
                col("n", 1, "INT", ndv = 1, min = 7.0, max = 7.0),
            )
        )
        val plan = CostModel.choose(
            input(stats = sameValue, where = parseWhere("n = 7"), indexCandidates = listOf(idxCandidate(selectivity = 1.0)))
        )
        assertEquals(AccessType.ALL, plan.type, "索引没有缩小任何读取范围时不该被选中")
    }

    @Test
    fun `二级索引在小表上输给扫描 - 交叉点被钉在 5 块`() {
        // 索引路 = 1 次索引分片 + 命中块的扫描（命中集装得进一块时恰好 2 次请求，与表大小无关）；
        // 扫描路 = 1 + (块数 − 1)/8（R4 的并行折算）。
        //
        // 严格比较的交叉点在 9 块（1+(B−1)/8 < 2 ⟺ B > 9），但**实际选择**的交叉点
        // 在 5 块：差在半次请求以内时按「越具体越好」取索引（TIE_EPSILON）。
        // 两个数字都要钉住——它决定了「一张几千行的小表上，二级索引等值查询走不走
        // 索引」，是启用代价比较之后最容易被人察觉的行为变化。
        fun plan(blocks: Int): PathPlan {
            val rows = blocks * 10L
            return CostModel.choose(
                input(
                    stats = bigStats(
                        columns = listOf(col("n", 1, "INT", ndv = rows, min = 0.0, max = 999.0)),
                        rows = rows, blockCount = blocks,
                    ),
                    rows = rows,
                    where = parseWhere("n = 7"),
                    // 命中集恰好一块：sel = 1/块数
                    indexCandidates = listOf(idxCandidate(selectivity = 1.0 / blocks)),
                )
            )
        }

        assertEquals(2.0, plan(16).gets, 0.001, "索引路的代价与表大小无关（命中集仍是一块）")
        assertEquals(2.875, CostModel.scanGets(160, 10.0, 1), 0.001, "16 块全扫 = 1 + 15/8")

        assertEquals(AccessType.ALL, plan(4).type, "4 块：扫描 1+3/8=1.375，索引 2.0 超出容差")
        assertEquals(AccessType.REF, plan(5).type, "5 块：扫描 1+4/8=1.5，索引 2.0 落在半次容差内 ⇒ 取更具体的")
        assertEquals(AccessType.REF, plan(9).type, "9 块：扫描 2.0 与索引 2.0 严格打平")
        assertEquals(AccessType.REF, plan(16).type, "16 块：扫描 2.875 > 索引 2.0")
    }

    @Test
    fun `扫描代价按 R4 的并行度折算 - 块数除以并行度`() {
        // 80 块的扫描：1（首块串行）+ 79/8 ≈ 10.875
        assertEquals(10.875, CostModel.scanGets(800, 10.0, 1), 0.001)
        // 分片多时「首块串行」的那部分按分片数增加
        assertTrue(CostModel.scanGets(800, 10.0, 4) > CostModel.scanGets(800, 10.0, 1))
        assertEquals(0.0, CostModel.scanGets(0, 10.0, 1), 0.001)
    }

    @Test
    fun `范围扫描按全部分片估代价 - 因为键区间不做分区裁剪`() {
        // shards 是「分区裁剪后」的分片数，totalShards 是全部。
        // 主键范围扫描目前只看键区间、不看分区，所以它的代价必须按全部分片算——
        // 按裁剪后的算会让优化器在一个 100 分区的表上高估范围扫描的收益
        val pruned = input(stats = bigStats(), rangeSelectivity = 0.5, shards = 1, totalShards = 100)
        val plan = CostModel.choose(pruned)
        assertEquals(
            CostModel.scanGets(500_000, pruned.rowsPerBlock, 100),
            plan.gets, 0.001,
            "范围扫描的代价被按 totalShards 算了",
        )
    }

    @Test
    fun `打平容差内选更具体的那条路`() {
        // 1 行的小表上，点查（1.0）与全表扫描（1.0）代价相同 → 必须是 const
        val plan = CostModel.choose(
            input(stats = bigStats(rows = 1), rows = 1, constPk = true, rangeSelectivity = 1.0)
        )
        assertEquals(AccessType.CONST, plan.type, "代价相同时应选更具体的路径，而不是扫描")
    }

    @Test
    fun `顺序与排序列如实报告`() {
        assertEquals(listOf("Using filesort"), CostModel.trailingExtras(input(stats = bigStats(), orderBy = true)))
    }

    @Test
    fun `块级剪枝下推时 Extra 如实标注`() {
        val plan = CostModel.choose(input(stats = bigStats(rows = 100), rows = 100, blockPruning = true))
        assertEquals(AccessType.ALL, plan.type)
        assertTrue(plan.extra.any { it.contains("skip index") }, plan.extra.toString())
    }

    @Test
    fun `全文检索是独立的访问路径`() {
        val plan = CostModel.choose(input(stats = bigStats(), fullText = true))
        assertEquals(AccessType.FULLTEXT, plan.type)
        assertEquals("FULLTEXT", plan.type.explainName)
    }

    // ── 选择率 ────────────────────────────────────────────────────

    @Test
    fun `AND 相乘 OR 取并集`() {
        val stats = bigStats()
        val eq = parseWhere("n = 7 AND city = 'x'")
        val s = CostModel.selectivity(eq, table, stats)
        // n 的 ndv=1000 → 0.001；city 无统计 → 默认 0.1
        assertEquals(0.001 * 0.1, s, 1e-9)

        val or = CostModel.selectivity(parseWhere("n = 7 OR city = 'x'"), table, stats)
        assertEquals(1.0 - (1.0 - 0.001) * (1.0 - 0.1), or, 1e-9)

        assertEquals(1.0, CostModel.selectivity(null, table, stats), 1e-9)
    }

    @Test
    fun `范围选择率用直方图而不是均匀假设`() {
        // 桶宽 100：桶 0 = [0,100) 装 990 行，桶 1 = [100,200) 装 10 行。
        // 落盘形态是稀疏的 `[下标, 计数]` 扁平序列（桶边界由 min/max/桶数推导）。
        val hist = Histogram(
            min = 0.0, max = 1000.0, bucketCount = 10,
            counts = listOf(0L, 990L, 1L, 10L), totalCount = 1000L,
        )
        val stats = bigStats(
            columns = listOf(col("n", 1, "INT", ndv = 500, min = 0.0, max = 1000.0, histogram = hist)),
            rows = 1000,
        )
        val sel = CostModel.selectivity(parseWhere("n > 100"), table, stats)
        assertTrue(sel < 0.05, "直方图显示 100 以上只有 10 行（1%），实际估出 ${"%.4f".format(sel)}")

        // 两个方向必须互补——这条断言是这条测试真正拦下的那个缺陷的形态：
        // `x > v` 曾经被算成 `1 − P(x ≥ v)`，也就是 P(x < v)。
        // 均匀分布上两者的数字碰巧接近，偏斜分布上会整个反掉。
        val le = CostModel.selectivity(parseWhere("n <= 100"), table, stats)
        assertEquals(1.0, sel + le, 1e-9, "`n > 100` 与 `n <= 100` 必须互补")
        assertTrue(le > 0.95, "99% 的行都 ≤ 100，实际估出 ${"%.4f".format(le)}")
    }

    @Test
    fun `常量在左侧时比较方向要翻转`() {
        val stats = bigStats(columns = listOf(col("n", 1, "INT", ndv = 100, min = 0.0, max = 99.0)), rows = 1000)
        val a = CostModel.selectivity(parseWhere("n < 50"), table, stats)
        val b = CostModel.selectivity(parseWhere("50 > n"), table, stats)
        assertEquals(a, b, 1e-9, "`50 > n` 与 `n < 50` 必须估成同一个选择率")
        assertTrue(a > 0.4 && a < 0.6, "均匀分布下应约 50%，实际 $a")
    }

    @Test
    fun `IS NULL 用 null 比例`() {
        val withNulls = bigStats(
            columns = listOf(
                ColumnStats("n", 1, "INT", observedRows = 1000, nullCount = 100, minNum = 0.0, maxNum = 99.0)
            ),
            rows = 1000,
        )
        assertEquals(0.1, CostModel.selectivity(parseWhere("n IS NULL"), table, withNulls), 1e-9)
        assertEquals(0.9, CostModel.selectivity(parseWhere("n IS NOT NULL"), table, withNulls), 1e-9)
    }

    // ── 约束提取 ──────────────────────────────────────────────────

    @Test
    fun `索引候选从 AND 合取项里挑 - 其余谓词不影响可用性`() {
        val cands = CostModel.indexCandidates(table, parseWhere("n = 7 AND city = 'x'"), emptyMap(), null)
        assertEquals(1, cands.size)
        assertEquals("idx_n", cands[0].index.name)
        assertEquals(1, cands[0].boundColumns)
        assertEquals(Datum.I64(7), cands[0].value)
    }

    @Test
    fun `OR 之下不提取索引候选`() {
        // `n = 7 OR n = 8` 用索引要两次查找再合并；把它当成一次等值查找是错的
        assertTrue(CostModel.indexCandidates(table, parseWhere("n = 7 OR city = 'x'"), emptyMap(), null).isEmpty())
    }

    @Test
    fun `非等值谓词不产生索引候选`() {
        assertTrue(CostModel.indexCandidates(table, parseWhere("n > 7"), emptyMap(), null).isEmpty())
        assertTrue(CostModel.indexCandidates(table, parseWhere("n = n"), emptyMap(), null).isEmpty())
    }

    @Test
    fun `多列索引按绑定的前缀列数排序`() {
        val multi = TableSchema(
            tableId = 1, databaseId = 1, name = "t2", version = 1,
            columns = listOf(
                ColumnDef("id", ColumnType.BIGINT, nullable = false),
                ColumnDef("a", ColumnType.INT), ColumnDef("b", ColumnType.INT),
            ),
            primaryKey = listOf("id"),
            indexes = listOf(
                IndexDef(0, "idx_a", IndexKind.ORDERED, listOf("a")),
                IndexDef(0, "idx_ab", IndexKind.ORDERED, listOf("a", "b")),
            ),
        )
        val cands = CostModel.indexCandidates(multi, parseWhere("a = 1 AND b = 2"), emptyMap(), null)
        assertEquals("idx_ab", cands[0].index.name, "绑定两列的索引比只绑一列的更有选择性，应排在前面")
        assertEquals(2, cands[0].boundColumns)
    }

    @Test
    fun `键区间选择率 - 前缀相乘 停止列用范围`() {
        val stats = bigStats(
            columns = listOf(col("id", 0, min = 0.0, max = 999_999.0), col("n", 1, "INT", ndv = 10, min = 0.0, max = 9.0)),
            rows = 1_000_000,
        )
        // 单列主键上的 `id > 100`：整个主键域的一段
        val r = KeyRange(prefix = emptyList(), low = Datum.I64(100_000))
        val sel = CostModel.keyRangeSelectivity(r, table, stats, 1_000_000)
        assertTrue(sel in 0.85..0.95, "id > 100000 应排除约 10% 的行，实际 ${"%.3f".format(sel)}")

        // 精确主键 → 1/行数
        val exact = KeyRange(prefix = listOf(Datum.I64(5)), exactPk = listOf(Datum.I64(5)))
        assertEquals(1e-6, CostModel.keyRangeSelectivity(exact, table, stats, 1_000_000), 1e-9)
    }

    @Test
    fun `没有统计时键区间选择率退化为保守默认值`() {
        // 单列主键、只有下界：拿不到任何依据 ⇒ 范围的保守默认值
        val r = KeyRange(prefix = emptyList(), low = Datum.I64(2))
        assertEquals(
            ColumnStats.DEFAULT_RANGE_SELECTIVITY,
            CostModel.keyRangeSelectivity(r, table, null, 1000), 1e-9,
        )

        // 复合主键上，前缀等值用等值默认值、停止列用范围默认值——两者相乘
        val r2 = KeyRange(prefix = listOf(Datum.I64(1)), low = Datum.I64(2))
        assertEquals(
            ColumnStats.DEFAULT_EQ_SELECTIVITY * ColumnStats.DEFAULT_RANGE_SELECTIVITY,
            CostModel.keyRangeSelectivity(r2, composite, null, 1000), 1e-9,
        )
    }

    // ── 路径估算的一致性 ──────────────────────────────────────────

    @Test
    fun `索引路径的 rows 与 filtered 与执行语义一致`() {
        // 索引项里存的是整行，而且 indexLookup 用**完整** WHERE 过滤（见 Executor），
        // 所以「取出 100 行、100% 满足」是事实。写成一个好看的 10% 才是在说谎——
        // 那会让人以为索引路径之后还有一道过滤，从而误判它的成本。
        val plan = CostModel.choose(
            input(
                stats = bigStats(),
                indexCandidates = listOf(idxCandidate(selectivity = 0.0001)),
                where = parseWhere("n = 7 AND city = 'x'"),
            )
        )
        assertEquals(AccessType.REF, plan.type)
        assertEquals("ref", plan.type.explainName)
        assertEquals("idx_n", plan.key)
        assertEquals(listOf("idx_n"), plan.possibleKeys)
        assertEquals(listOf("n"), plan.keyColumns)
        assertEquals(100L, plan.rows)
        assertEquals(100.0, plan.filteredPercent, 0.001)
        assertEquals(100L, plan.outputRows)
        assertTrue(plan.extra.contains("Using index"), plan.extra.toString())
        assertTrue(plan.extra.contains("Using where"), plan.extra.toString())
    }

    @Test
    fun `扫描路径的 rows 是检查的行数 filtered 是通过的比例`() {
        val plan = CostModel.choose(input(stats = bigStats(rows = 1000), rows = 1000, where = parseWhere("n = 7")))
        assertEquals(AccessType.ALL, plan.type)
        assertEquals(1000L, plan.rows)
        assertEquals(0.1, plan.filteredPercent, 0.001, "n 的 ndv=1000 ⇒ 等值通过 0.1%")
        assertEquals(1L, plan.outputRows)
    }

    @Test
    fun `EXPLAIN 的 type 取值覆盖要求的全部形态`() {
        assertEquals("const", AccessType.CONST.explainName)
        assertEquals("range", AccessType.RANGE.explainName)
        assertEquals("ref", AccessType.REF.explainName)
        assertEquals("ALL", AccessType.ALL.explainName)
        assertEquals("METADATA_COUNT", AccessType.METADATA_COUNT.explainName)
        assertEquals("FULLTEXT", AccessType.FULLTEXT.explainName)
        assertEquals("NULL", AccessType.NO_TABLE.explainName)
    }

    @Test
    fun `打平时按枚举顺序选更具体的类型`() {
        // 声明顺序就是具体程度的顺序：点查 > 索引 > 键范围 > 全表
        assertTrue(AccessType.CONST.ordinal < AccessType.REF.ordinal)
        assertTrue(AccessType.REF.ordinal < AccessType.RANGE.ordinal)
        assertTrue(AccessType.RANGE.ordinal < AccessType.ALL.ordinal)
    }

    // ── 语法：EXPLAIN / ANALYZE ───────────────────────────────────

    private fun parseWhere(sql: String): Expr {
        val s = assertIs<SelectStmt>(Parser.parse("SELECT * FROM t WHERE $sql"))
        return s.where!!
    }

    @Test
    fun `EXPLAIN 解析出被包裹的语句`() {
        val e = assertIs<ExplainStmt>(Parser.parse("EXPLAIN SELECT * FROM t WHERE id = 1"))
        val inner = assertIs<SelectStmt>(e.statement)
        assertEquals("t", inner.table)
    }

    @Test
    fun `EXPLAIN EXTENDED 被接受并忽略`() {
        assertIs<ExplainStmt>(Parser.parse("EXPLAIN EXTENDED SELECT COUNT(*) FROM t"))
    }

    @Test
    fun `EXPLAIN ANALYZE 被明确拒绝`() {
        // 它会真的执行语句——把一个执行语义伪装成规划语义是最难预料的一类意外
        val e = kotlin.runCatching { Parser.parse("EXPLAIN ANALYZE SELECT * FROM t") }.exceptionOrNull()
        assertTrue(e is com.cloudea.yangbase.common.SqlException, "应报语法错误，而不是被当成 EXPLAIN $e")
        assertTrue(e.message!!.contains("EXPLAIN ANALYZE"), e.message!!)
    }

    @Test
    fun `ANALYZE TABLE 支持 LOCAL 与多表`() {
        val a = assertIs<AnalyzeStmt>(Parser.parse("ANALYZE TABLE t"))
        assertEquals(listOf("t"), a.tables)

        val b = assertIs<AnalyzeStmt>(Parser.parse("ANALYZE LOCAL TABLE t, db2.t2"))
        assertEquals(listOf("t", "db2.t2"), b.tables)

        assertIs<AnalyzeStmt>(Parser.parse("ANALYZE NO_WRITE_TO_BINLOG TABLE t"))
    }

    @Test
    fun `ANALYZE 不带 TABLE 是语法错误`() {
        kotlin.runCatching { Parser.parse("ANALYZE t") }.let {
            assertTrue(it.isFailure, "ANALYZE 必须要求 TABLE 关键字")
        }
    }

    @Test
    fun `DESCRIBE 不会被 EXPLAIN 分支抢走`() {
        // 解析器里 EXPLAIN 与 DESCRIBE 是两个不同的入口，不能互相污染
        val d = assertIs<DescribeStmt>(Parser.parse("DESCRIBE t"))
        assertEquals("t", d.table)
        assertIs<DescribeStmt>(Parser.parse("DESC t"))
    }
}
