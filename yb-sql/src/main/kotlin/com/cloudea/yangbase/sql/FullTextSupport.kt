package com.cloudea.yangbase.sql

import com.cloudea.yangbase.common.Datum
import com.cloudea.yangbase.common.IndexDef
import com.cloudea.yangbase.common.IndexId
import com.cloudea.yangbase.common.IndexKind
import com.cloudea.yangbase.common.MySqlErrors
import com.cloudea.yangbase.common.ReadTs
import com.cloudea.yangbase.common.SqlException
import com.cloudea.yangbase.common.TableSchema
import com.cloudea.yangbase.index.fulltext.Analyzer
import com.cloudea.yangbase.index.fulltext.ChineseNgramAnalyzer
import com.cloudea.yangbase.index.fulltext.FullTextIndex
import com.cloudea.yangbase.index.fulltext.IndexDoc
import com.cloudea.yangbase.index.fulltext.MatchAgainstParser
import com.cloudea.yangbase.index.fulltext.MatchMode
import com.cloudea.yangbase.index.fulltext.StandardAnalyzer
import com.cloudea.yangbase.index.fulltext.TextQuery
import com.cloudea.yangbase.txn.DatabaseEngine
import java.util.concurrent.ConcurrentHashMap

/**
 * 把倒排索引接到 SQL 层（设计文档 §8.6）。
 *
 * 这一层要解决的是**语义落差**：`MATCH ... AGAINST` 看起来像一个谓词，
 * 但它不是逐行判定的——相关性打分（BM25）依赖全语料统计量（文档频率、平均文档长度），
 * 那不是一行数据里有的信息。所以它必须被当作**访问路径**：
 * 先用倒排索引取出候选主键，再回表取行，最后用剩余的 WHERE 过滤。
 *
 * 把它硬塞进逐行求值（`Eval`）在语义上就是错的，而不是「性能差一点」。
 */
class FullTextSupport(private val engine: DatabaseEngine) {

    companion object {
        /**
         * 每个引擎一份实例。
         *
         * 必须共享而不是每个 Session 一份：[FullTextIndex] 内部缓存着段读取器与
         * manifest，每个连接各建一份会让同一个索引被反复打开、反复校验，
         * 而且**提交时的条件写会互相打架**（各自用不同的 etag）。
         */
        private val instances = ConcurrentHashMap<DatabaseEngine, FullTextSupport>()

        fun of(engine: DatabaseEngine): FullTextSupport = instances.computeIfAbsent(engine) { FullTextSupport(it) }
    }

    /** 一个 (表, 索引) 对应一个 [FullTextIndex]。懒创建，之后常驻。 */
    private val indexes = ConcurrentHashMap<Long, FullTextIndex>()

    /** 分析器缓存：名字 → 实例。索引期与查询期**必须**用同一个。 */
    private val analyzers = ConcurrentHashMap<String, Analyzer>()

    // ── 分析器 ────────────────────────────────────────────────────

    /**
     * 解析分词器名。
     *
     * 默认 `standard`（ASCII 分词）。中文文本必须显式指定 `ngram`——
     * 用 standard 切中文会得到**空结果集**：它把 CJK 全当分隔符，
     * 索引里一个词都没有，而查询不报错。这是最容易踩的坑，所以 `ngram`
     * 的文档里要把这点写清楚。
     */
    fun analyzerFor(name: String?): Analyzer = analyzers.computeIfAbsent(name ?: "standard") { n ->
        when (n.lowercase()) {
            "standard", "default" -> StandardAnalyzer()
            "ngram", "cjk", "cjk-ngram" -> ChineseNgramAnalyzer()
            else -> throw SqlException(
                MySqlErrors.ER_PARSE_ERROR, "42000",
                "未知的分词器 '$n'（支持 standard / ngram）。" +
                    "中文文本请用 WITH PARSER ngram，standard 会把中文全部当分隔符。",
            )
        }
    }

    // ── 索引实例 ──────────────────────────────────────────────────

    fun indexFor(table: TableSchema, idx: IndexDef): FullTextIndex {
        require(idx.kind == IndexKind.FULLTEXT) { "索引 ${idx.name} 不是全文索引" }
        require(idx.columns.size == 1) {
            "全文索引 ${idx.name} 只支持单列（当前 ${idx.columns.size} 列）。" +
                "多列需要多个索引——一个段只索引一个字段。"
        }
        return indexes.computeIfAbsent(idx.id) {
            FullTextIndex(
                store = engine.store,
                cache = engine.cache_,
                indexId = IndexId(idx.id),
                field = idx.columns[0],
                analyzer = analyzerFor(idx.parser),
            )
        }
    }

    /**
     * 表上的全文索引，**含正在构建的**。
     *
     * 写路径（[onInsert] / [onDelete] / [applyPending]）必须用这个：
     * 构建中的索引也要被维护，否则构建期间新写入的文档永远补不回来
     * （§8.7 的追平阶段只补「构建窗口」那一段，之后的写入靠写路径）。
     */
    fun fullTextIndexes(table: TableSchema): List<IndexDef> =
        table.indexes.filter { it.kind == IndexKind.FULLTEXT }

    /**
     * **查询可以用的**全文索引——只含 ACTIVE。
     *
     * 与 [fullTextIndexes] 是一对相反的判据，写反任何一边都会丢数据：
     * 查询用了 BUILDING 的索引 → 倒排还没追平历史，文档**静默消失**；
     * 写入漏了 BUILDING 的索引 → 构建期间写入的文档永远进不了索引。
     */
    fun searchableFullTextIndexes(table: TableSchema): List<IndexDef> =
        table.activeIndexes.filter { it.kind == IndexKind.FULLTEXT }

    private fun textOf(table: TableSchema, idx: IndexDef, row: List<Datum>): String? {
        val colIdx = table.indexOf(idx.columns[0])
        val v = row.getOrNull(colIdx) ?: return null
        if (v is Datum.Null) return null
        return v.asString()
    }

    // ── 索引维护（写路径）────────────────────────────────────────

    /**
     * 一批行写入后同步索引。
     *
     * **索引必须随数据一起维护**，否则全文检索会返回「曾经匹配但现在已改」的行。
     * 那类错误不会报错，只会给出过期结果——比查不到更糟。
     */
    suspend fun onInsert(table: TableSchema, rows: List<List<Datum>>, commitTs: Long): Int {
        val idxs = fullTextIndexes(table)
        if (idxs.isEmpty() || rows.isEmpty()) return 0
        var n = 0
        for (idx in idxs) {
            val docs = ArrayList<IndexDoc>(rows.size)
            for (row in rows) {
                val text = textOf(table, idx, row) ?: continue   // NULL 不索引
                val pk = com.cloudea.yangbase.common.KeyEncoder.encodeKey(row, table, table.primaryKeyIndexes)
                docs.add(IndexDoc(pk, commitTs, text))
            }
            if (docs.isNotEmpty()) {
                indexFor(table, idx).indexDocuments(docs)
                n += docs.size
            }
        }
        return n
    }

    /** 一批行删除后清理索引项。 */
    suspend fun onDelete(table: TableSchema, rows: List<List<Datum>>, commitTs: Long): Int {
        val idxs = fullTextIndexes(table)
        if (idxs.isEmpty() || rows.isEmpty()) return 0
        val pks = rows.map {
            com.cloudea.yangbase.common.KeyEncoder.encodeKey(it, table, table.primaryKeyIndexes)
        }
        var n = 0
        for (idx in idxs) {
            n += indexFor(table, idx).deleteDocuments(pks, commitTs)
        }
        return n
    }

    /**
     * 一次待做的索引维护。
     *
     * 为什么要排队而不是立即执行：**事务缓冲期内不能索引**。
     * 立刻索引会让未提交的行可以被检索到——那是脏读，而且是
     * 「文本检索」这个场景下最不容易察觉的一种脏读（结果是模糊匹配，
     * 多出几行很难被人发现）。所以索引必须等到 COMMIT、拿到真正的
     * commitTs 之后再做。
     */
    sealed interface Pending {
        val table: TableSchema

        data class Insert(override val table: TableSchema, val rows: List<List<Datum>>) : Pending
        data class Delete(override val table: TableSchema, val rows: List<List<Datum>>) : Pending
        data class Update(
            override val table: TableSchema,
            val oldRows: List<List<Datum>>,
            val newRows: List<List<Datum>>,
        ) : Pending
    }

    /** 提交时执行排队的索引维护。 */
    suspend fun applyPending(ops: List<Pending>, commitTs: Long) {
        for (op in ops) {
            when (op) {
                is Pending.Insert -> onInsert(op.table, op.rows, commitTs)
                is Pending.Delete -> onDelete(op.table, op.rows, commitTs)
                is Pending.Update -> onUpdate(op.table, op.oldRows, op.newRows, commitTs)
            }
        }
    }

    /**
     * 更新 = 删旧 + 插新。
     *
     * 顺序不能反：反过来会让「新文本恰好等于旧文本」的行先被插入再被删除，
     * 索引里最终一条不剩。
     */
    suspend fun onUpdate(
        table: TableSchema,
        oldRows: List<List<Datum>>,
        newRows: List<List<Datum>>,
        commitTs: Long,
    ): Int {
        if (fullTextIndexes(table).isEmpty()) return 0
        val del = onDelete(table, oldRows, commitTs)
        val ins = onInsert(table, newRows, commitTs)
        return del + ins
    }

    // ── 查询 ──────────────────────────────────────────────────────

    /**
     * 执行 `MATCH(cols) AGAINST('...')`，返回**候选行**（已回表）。
     *
     * 返回 null 表示表上没有可用的全文索引——调用方应回退到扫描并给出提示，
     * 而不是静默返回空集。
     */
    suspend fun search(
        table: TableSchema,
        match: MatchAgainst,
        topK: Int,
        readTs: ReadTs,
    ): List<List<Datum>>? {
        val col = match.columns.firstOrNull() ?: return null
        // 查询侧只看 ACTIVE：构建中的倒排还没追平历史数据，查它 = 静默漏文档。
        // 此时返回 null（回退扫描 + 提示），而不是返回一个残缺的结果集
        val idx = searchableFullTextIndexes(table).firstOrNull { it.columns[0].equals(col, ignoreCase = true) }
            ?: return null

        val analyzer = analyzerFor(idx.parser)
        // 分词器切不出任何词条时**必须报错**，不能返回空集。
        // 最典型的场景：用 standard 分词器去查中文——它把 CJK 全部当分隔符，
        // 索引里一个词都没有，任何中文查询都切出 0 个词。返回空集会让使用者
        // 以为「就是没有匹配的行」，而真正的问题是分词器选错了。
        if (analyzer.analyze(match.query).isEmpty()) {
            throw SqlException(
                MySqlErrors.ER_PARSE_ERROR, "42000",
                "分词器 '${idx.parser ?: "standard"}' 无法从查询 '${match.query}' 中切出任何词条。" +
                    "中文文本请建索引时指定 WITH PARSER ngram；" +
                    "standard 会把 CJK 字符全部当作分隔符，从而匹配不到任何东西。",
            )
        }

        val ft = indexFor(table, idx)
        val query = try {
            MatchAgainstParser.parse(
                field = idx.columns[0],
                text = match.query,
                mode = if (match.booleanMode) MatchMode.BOOLEAN else MatchMode.NATURAL_LANGUAGE,
            )
        } catch (e: Exception) {
            throw SqlException(
                MySqlErrors.ER_PARSE_ERROR, "42000",
                "无法解析全文查询 '${match.query}': ${e.message}",
            )
        }

        val result = ft.search(query, topK = topK.coerceAtLeast(1), readTs = readTs)

        // 回表：索引里存的是主键，行数据仍在主分片里。
        // 索引里可能有已被删除的行（删除是打位图，段要等 compaction 才物理清理），
        // 因此 `get` 返回 null 是**正常**的，跳过即可。
        val out = ArrayList<List<Datum>>(result.hits.size)
        for (hit in result.hits) {
            engine.get(table, hit.pk, readTs)?.let { out.add(it) }
        }
        return out
    }

    /** 从 WHERE 里挑出一个可用的 `MATCH ... AGAINST`，没有则返回 null。 */
    fun findMatch(where: Expr?): MatchAgainst? = when (where) {
        is MatchAgainst -> where
        is BinaryOp -> if (where.op.equals("AND", true)) findMatch(where.left) ?: findMatch(where.right) else null
        else -> null
    }

    /**
     * 从 WHERE 里**摘掉**全文谓词，剩下的是需要逐行判定的一般谓词。
     *
     * 必须摘掉而不是留着：`MatchAgainst` 无法逐行求值（它依赖全语料统计量），
     * 留着它会在过滤阶段抛「不支持的表达式」。
     */
    fun removeMatch(where: Expr?, match: MatchAgainst): Expr? {
        if (where == null) return null
        if (where === match) return null
        if (where is BinaryOp && where.op.equals("AND", true)) {
            val l = removeMatch(where.left, match)
            val r = removeMatch(where.right, match)
            return when {
                l == null -> r
                r == null -> l
                else -> BinaryOp("AND", l, r)
            }
        }
        return where
    }
}
