package com.cloudea.yangbase.server

import com.cloudea.yangbase.common.audit.Audit
import com.cloudea.yangbase.common.audit.AuditActions
import com.cloudea.yangbase.common.audit.AuditEvent
import com.cloudea.yangbase.common.metrics.MetricType
import com.cloudea.yangbase.common.metrics.Metrics
import com.cloudea.yangbase.common.metrics.QueryType
import com.cloudea.yangbase.common.metrics.Sample
import com.cloudea.yangbase.common.metrics.StoreOp
import com.cloudea.yangbase.mysql.QueryObserver
import com.cloudea.yangbase.sql.AlterTableStmt
import com.cloudea.yangbase.sql.Between
import com.cloudea.yangbase.sql.BinaryOp
import com.cloudea.yangbase.sql.ColumnRef
import com.cloudea.yangbase.sql.CreateDatabaseStmt
import com.cloudea.yangbase.sql.CreateIndexStmt
import com.cloudea.yangbase.sql.CreateTableStmt
import com.cloudea.yangbase.sql.DropDatabaseStmt
import com.cloudea.yangbase.sql.DropTableStmt
import com.cloudea.yangbase.sql.Expr
import com.cloudea.yangbase.sql.InList
import com.cloudea.yangbase.sql.Literal
import com.cloudea.yangbase.sql.MatchAgainst
import com.cloudea.yangbase.sql.RollbackStmt
import com.cloudea.yangbase.sql.SelectStmt
import com.cloudea.yangbase.sql.Session
import com.cloudea.yangbase.sql.Statement
import com.cloudea.yangbase.sql.VarRef
import com.cloudea.yangbase.common.TableSchema
import com.cloudea.yangbase.txn.DatabaseEngine
import java.util.Locale
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong

/**
 * CN 的指标采集与查询观测（设计文档 §12.5）。
 *
 * ## 两类职责，一个类
 *
 * 1. **[QueryObserver]**——协议层在每条语句前后各通知一次。这里据此算出
 *    `yb_query_duration_seconds`、★`yb_query_objects_fetched`，并在超过阈值时
 *    打一行慢查询日志。选择挂在协议层而不是执行器里的理由见 [QueryObserver]。
 * 2. **采集器**——注册到 [Metrics.registry] 的**唯一**一个 collector，
 *    在抓取 `/metrics` 时把「值在别处」的量（缓存命中、分片统计、
 *    组提交摊薄倍数）折算成样本。
 *
 * ## 抓取绝不产生对象存储请求（原则 P4）
 *
 * 采集器只读**内存**状态：缓存计数器是 `AtomicLong`，分片统计来自内存里的
 * 版本链与常驻 `.idx`。没有一次 `LIST`、没有一次 `GET`——这条由
 * `MetricsNoIoIntegrationTest` 用 `CountingObjectStore` 断言。
 *
 * 这里刻意用 `BlockCache.counters()` / `DiskCache.counters()` 而不是它们的
 * `stats()`：后者是挂起函数（要走互斥锁），而抓取发生在协议线程上，不能等锁。
 *
 * ## 为什么 collector 只装一次、且经 [ACTIVE] 间接取值
 *
 * 注册表是进程级的（见 `Metrics` 的说明），同一个 JVM 里第二次注册**不会**覆盖
 * 第一次的采集器——两个采集器会各自产出一份同名同标签的样本，那是**非法的
 * Prometheus 输出**（重复序列）。因此采集器只在首次构造时装入一次，
 * 之后通过 [ACTIVE] 指向当前实例；旧实例关闭后 `ACTIVE` 清空，采集器退化为
 * 只报 `yb_up 0`。这也与事实相符：**一个 CN 进程就是一个实例**。
 *
 * ## 开销
 *
 * `queryStarted`/`queryFinished` 各做一次 `ConcurrentHashMap` 读/写与一次
 * 四个计数器的快照（每查询一个小 `LongArray`）。这条路径已经要解析 SQL、
 * 建 AST、做 IO，因此这点开销是噪声级的；真正的热路径（存储、WAL、缓存）
 * 上没有任何来自本类的东西。
 */
class ServerMetrics(
    private val engine: DatabaseEngine,
    /**
     * 慢查询阈值（毫秒）。0 表示每条语句都算慢——**不要这么配**，
     * 它会把日志变成一条语句一份的快照。
     */
    private val slowQueryMillis: Long = 500,
    /** 慢查询日志的输出端口。默认 stderr；测试注入以捕获日志。 */
    private val log: (String) -> Unit = { System.err.println(it) },
) : QueryObserver {

    private val startedAtMillis = System.currentTimeMillis()

    /** 活跃连接数的来源。由 [YangBaseServer] 在 MySQL 服务端起来之后接上。 */
    @Volatile
    var connectionCount: () -> Int = { 0 }

    /** 累计观测到的语句数。用于 `/healthz` 自证「这个进程还在干活」。 */
    private val statementsObserved = AtomicLong()

    /**
     * catalog 重载失败次数。
     *
     * 这类故障的症状是"表莫名不见了/删掉的表还在"，与真正的原因（重载失败）
     * 隔得很远——所以它必须在 `/metrics` 上可见，而不是只打一行日志
     */
    private val catalogReloadFailed = AtomicLong()

    fun onCatalogReloadFailed() = catalogReloadFailed.incrementAndGet()

    init {
        installCollector()
        ACTIVE = this
    }

    /** 关闭：把全局采集点从本实例摘下来（见类注释的「只装一次」）。 */
    fun close() {
        if (ACTIVE === this) ACTIVE = null
    }

    // ── 查询观测 ────────────────────────────────────────────────────

    /**
     * 一次查询的观测状态。
     *
     * 用 [Session] 作键而不是线程局部变量：语句执行器是挂起的，恢复后可能落在
     * 另一个线程上，线程局部变量会**静默错配**（把 A 查询的开始时间算到 B 上）。
     * 每条连接一个 Session，且协议是每连接串行的，因此 Session 就是天然的查询键。
     */
    private class InFlight(val type: QueryType, val readBefore: LongArray) {
        /** 结束时的快照复用同一个数组，避免每查询分配两次。 */
        val readAfter: LongArray = LongArray(readBefore.size)

        var objectsFetched: Long = 0
    }

    private val inFlight = ConcurrentHashMap<Session, InFlight>()

    override fun queryStarted(sql: String, stmt: Statement, session: Session): Long {
        statementsObserved.incrementAndGet()
        val state = InFlight(classify(stmt, session), LongArray(READ_OPS.size))
        snapshotReads(state.readBefore)
        inFlight[session] = state
        return System.nanoTime()
    }

    override fun queryFinished(
        token: Long,
        sql: String,
        stmt: Statement,
        session: Session,
        rows: Long,
        error: Throwable?,
    ) {
        val seconds = (System.nanoTime() - token) / 1e9
        val state = inFlight.remove(session)
        var objects = 0L
        if (state != null) {
            snapshotReads(state.readAfter)
            for (i in READ_OPS.indices) {
                objects += (state.readAfter[i] - state.readBefore[i]).coerceAtLeast(0L)
            }
            state.objectsFetched = objects
        }
        val type = state?.type ?: QueryType.OTHER

        val millis = seconds * 1000.0
        val slow = millis >= slowQueryMillis
        Metrics.onQuery(type, seconds, objects, slow)

        // 只有成功的语句才会改变结构，因此审计只在成功路径上写。
        if (error == null) {
            auditDdl(stmt, sql, session)
            // 回滚计数：显式 ROLLBACK 是协议层唯一能无歧义识别的回滚。
            // 隐式回滚（连接断开、DDL 隐式提交）发生在执行器内部，协议层看不到，
            // 这里不猜——`yb_transactions_total{result="rollback"}` 因此是个下界。
            if (stmt === RollbackStmt) Metrics.onTxnRollback()
        }

        if (slow) {
            log(
                "[yb-slow-query] ${fmt(millis)}ms objects=$objects rows=$rows type=${type.wire}" +
                    " detail=${detailOf(state)}" +
                    (if (error != null) " error=${error::class.simpleName}" else "") +
                    " sql=${oneLine(sql)}"
            )
        }
    }

    /** 每查询对象读取明细——慢查询日志要能回答「为什么这个查询读了 500 个对象」。 */
    private fun detailOf(state: InFlight?): String {
        if (state == null) return "-"
        val sb = StringBuilder(64)
        for (i in READ_OPS.indices) {
            if (i > 0) sb.append(',')
            sb.append(READ_OPS[i].wire).append('=')
                .append((state.readAfter[i] - state.readBefore[i]).coerceAtLeast(0L))
        }
        return sb.toString()
    }

    private fun snapshotReads(into: LongArray) {
        for (i in READ_OPS.indices) into[i] = Metrics.storeRequestCount(READ_OPS[i])
    }

    private fun oneLine(sql: String): String {
        val t = sql.trim()
        return if (t.none { it == '\n' || it == '\r' }) t else t.replace('\n', ' ').replace('\r', ' ')
    }

    private fun fmt(v: Double) = "%.1f".format(Locale.ROOT, v)

    // ── DDL 审计 ────────────────────────────────────────────────────

    /**
     * DDL 的审计埋点。
     *
     * 落在协议层是**唯一**能拿到原始 SQL 的位置（执行器里只剩 AST）。没有安装
     * 审计落点时 [Audit.sink] 是 `NOOP`，这里只是一次 volatile 读加一次虚调用。
     */
    private fun auditDdl(stmt: Statement, sql: String, session: Session) {
        val action: String
        val target: String
        when (stmt) {
            is CreateDatabaseStmt -> { action = AuditActions.DDL_CREATE_DATABASE; target = stmt.name }
            is DropDatabaseStmt -> { action = AuditActions.DDL_DROP_DATABASE; target = stmt.name }
            is CreateTableStmt -> { action = AuditActions.DDL_CREATE_TABLE; target = stmt.name }
            is DropTableStmt -> { action = AuditActions.DDL_DROP_TABLE; target = stmt.name }
            is CreateIndexStmt -> { action = AuditActions.DDL_CREATE_INDEX; target = stmt.name }
            is AlterTableStmt -> { action = AuditActions.DDL_ALTER_TABLE; target = stmt.table }
            else -> return
        }
        Audit.record(
            AuditEvent(
                ts = System.currentTimeMillis(),
                action = action,
                database = session.currentDbName ?: "",
                target = target,
                sql = oneLine(sql),
            )
        )
    }

    // ── 访问路径分类 ────────────────────────────────────────────────

    /**
     * 把语句归类成 [QueryType]。
     *
     * 这是一条**启发式**，不是优化器的计划：执行器不暴露计划 API（本次改动不碰
     * `yb-sql`），因此这里按 AST + schema 推断访问路径。判据只有一条原则——
     * **只有能证明「一次点查」时才叫 point**：
     *
     * - `point`：WHERE 里主键的**每一列**都有等值条件（`WHERE pk = 1`）
     * - `fulltext`：WHERE 里有 `MATCH ... AGAINST`
     * - `range`：主键或任一索引列上出现了等值/范围条件（是一次有界扫描）
     * - `scan`：其余（含无 WHERE 的 `COUNT(*)`——它在 `yb_query_objects_fetched`
     *   的 0 格子里，那正是它该待的地方）
     * - `OTHER`：不是 SELECT
     *
     * `OR` 子树里的条件**不参与判定**：`WHERE pk = 1 OR x = 2` 不能保证一次 GET，
     * 把它算成 point 会让这个指标在最需要它准确的地方（成本归因）骗人。
     */
    private fun classify(stmt: Statement, session: Session): QueryType {
        val sel = stmt as? SelectStmt ?: return QueryType.OTHER
        val where = sel.where
        if (where != null && containsMatch(where)) return QueryType.FULLTEXT
        val tableName = sel.table ?: return QueryType.OTHER
        val table = lookup(session, tableName) ?: return QueryType.OTHER

        val eq = HashSet<String>(2)
        val rng = HashSet<String>(2)
        if (where != null) collectConditions(where, eq, rng, underOr = false)

        val pk = table.primaryKey.mapTo(HashSet(table.primaryKey.size)) { it.lowercase() }
        if (pk.isNotEmpty() && eq.containsAll(pk)) return QueryType.POINT

        val indexed = HashSet<String>(4)
        for (idx in table.indexes) for (c in idx.columns) indexed.add(c.lowercase())
        if (eq.any { it in pk || it in indexed }) return QueryType.RANGE
        if (rng.any { it in pk || it in indexed }) return QueryType.RANGE
        return QueryType.SCAN
    }

    private fun containsMatch(e: Expr): Boolean = when (e) {
        is MatchAgainst -> true
        is BinaryOp -> containsMatch(e.left) || containsMatch(e.right)
        is com.cloudea.yangbase.sql.UnaryOp -> containsMatch(e.expr)
        is InList -> containsMatch(e.expr)
        is Between -> containsMatch(e.expr)
        is com.cloudea.yangbase.sql.IsNull -> containsMatch(e.expr)
        is com.cloudea.yangbase.sql.Like -> containsMatch(e.expr) || containsMatch(e.pattern)
        else -> false
    }

    /**
     * 收集「可用于定位的行条件」。
     *
     * [underOr] 为真时**只遍历不收集**：OR 的任一分支单独成立都不足以跳过扫描。
     */
    private fun collectConditions(e: Expr?, eq: MutableSet<String>, rng: MutableSet<String>, underOr: Boolean) {
        when (e) {
            null -> return
            is BinaryOp -> when (e.op.uppercase(Locale.ROOT)) {
                "AND" -> {
                    collectConditions(e.left, eq, rng, underOr)
                    collectConditions(e.right, eq, rng, underOr)
                }
                "OR" -> {
                    collectConditions(e.left, eq, rng, true)
                    collectConditions(e.right, eq, rng, true)
                }
                "=", "<=>" -> if (!underOr) columnOf(e.left, e.right)?.let { eq.add(it) }
                "<", "<=", ">", ">=", "<>", "!=" ->
                    if (!underOr) columnOf(e.left, e.right)?.let { rng.add(it) }
                else -> Unit
            }
            is InList -> if (!underOr) (e.expr as? ColumnRef)?.let { eq.add(it.name.lowercase()) }
            is Between -> if (!underOr) (e.expr as? ColumnRef)?.let { rng.add(it.name.lowercase()) }
            else -> Unit
        }
    }

    /** 「列 op 常量」或「常量 op 列」里的那个列名。两侧都是列或都是表达式时返回 null。 */
    private fun columnOf(a: Expr, b: Expr): String? = when {
        a is ColumnRef && isConst(b) -> a.name.lowercase()
        b is ColumnRef && isConst(a) -> b.name.lowercase()
        else -> null
    }

    private fun isConst(e: Expr): Boolean = e is Literal || e is VarRef

    /** `db.table` / `table`（用当前库）→ schema。取不到就返回 null，让分类退化为 OTHER。 */
    private fun lookup(session: Session, name: String): TableSchema? {
        val dot = name.indexOf('.')
        val dbName: String
        val tableName: String
        if (dot >= 0) {
            dbName = name.substring(0, dot)
            tableName = name.substring(dot + 1)
        } else {
            dbName = session.currentDbName ?: return null
            tableName = name
        }
        val db = engine.catalog.findDatabase(dbName) ?: return null
        return engine.catalog.findTable(db.databaseId, tableName)
    }

    // ── 健康检查（供 /healthz） ──────────────────────────────────────

    /**
     * 健康检查。**只读内存，不做任何 IO**——一个会打对象存储的健康检查
     * 会在后端抖动时同时打挂自己（探活风暴），而且直接违反原则 P4。
     *
     * `warn=` 行提示需要排查但不影响服务的状态；只有引擎不可用时才是 `status=fail`
     * 与 503——`/healthz` 回答的是「这个进程还能不能服务」，不是「一切是否完美」。
     */
    fun health(): HealthReport {
        val sb = StringBuilder(256)
        val stats = try {
            engine.stats()
        } catch (t: Throwable) {
            return HealthReport(
                healthy = false,
                body = "status=fail\nreason=engine_unavailable:${t::class.simpleName}\n",
            )
        }
        sb.append("status=ok\n")
        sb.append("uptime_seconds=").append((System.currentTimeMillis() - startedAtMillis) / 1000).append('\n')
        sb.append("open_shards=").append(stats.openShards).append('\n')
        sb.append("databases=").append(stats.databases).append('\n')
        sb.append("tables=").append(stats.tables).append('\n')
        sb.append("connections=").append(connectionCount()).append('\n')
        sb.append("statements_observed=").append(statementsObserved.get()).append('\n')
        sb.append("cache_hit_rate=").append(fmt(stats.cacheHitRate)).append('\n')
        sb.append("group_commit_dilution=").append(fmt(Metrics.groupCommitDilution())).append('\n')

        val orphans = Metrics.orphanObjects.value()
        if (orphans > 0) sb.append("warn=orphan_objects=").append(orphans).append('\n')
        val dropped = Metrics.auditDropped.value()
        if (dropped > 0) sb.append("warn=audit_dropped=").append(dropped).append('\n')
        val auditFail = Metrics.auditWriteFailed.value()
        if (auditFail > 0) sb.append("warn=audit_write_failed=").append(auditFail).append('\n')
        val pendingDrop = com.cloudea.yangbase.storage.pendingDropped.get()
        if (pendingDrop > 0) sb.append("warn=pending_wal_dropped=").append(pendingDrop).append('\n')
        return HealthReport(healthy = true, body = sb.toString())
    }

    // ── 采集器 ──────────────────────────────────────────────────────

    private fun collect(out: MutableList<Sample>) {
        out.add(Sample("yb_uptime_seconds", emptyList(), (System.currentTimeMillis() - startedAtMillis) / 1000.0, MetricType.GAUGE))
        out.add(Sample("yb_statements_observed_total", emptyList(), statementsObserved.get().toDouble(), MetricType.COUNTER))
        out.add(Sample("yb_mysql_connections", emptyList(), connectionCount().toDouble(), MetricType.GAUGE))
        // 未决 WAL 被丢弃的次数（Wal.kt 的全局计数器）：非零意味着恢复时丢掉了
        // 无法判定的事务——这是必须被看见的事件，所以它必须出现在监控里。
        out.add(
            Sample(
                "yb_pending_wal_dropped_total",
                emptyList(),
                com.cloudea.yangbase.storage.pendingDropped.get().toDouble(),
                MetricType.GAUGE,
            )
        )
        cacheSamples(out)
        shardSamples(out)
    }

    /**
     * 缓存。★ 这里把命中数折算成 `yb_objectstore_requests_total{cached="true"}`——
     * **缓存命中不是成本**，过滤 `cached="false"` 得到的才是真实请求数。
     *
     * 口径：折算值**统一记在 `op="get"` 下**。块缓存不区分它服务的是 `.idx` 的
     * 整对象读还是 `.dat` 的范围读（同一个缓存、同一套键），硬拆只能靠猜，
     * 而猜出来的成本数字还不如一个诚实的聚合值。
     */
    private fun cacheSamples(out: MutableList<Sample>) {
        val cache = engine.cache_
        val mem = cache.counters()
        var avoided = mem.hits
        addCacheTier(out, "mem", mem.hits, mem.misses, mem.evictions, mem.coalescedRequests, mem.usedBytes)

        val disk = cache.secondLevel?.counters()
        if (disk != null) {
            avoided += disk.hits
            addCacheTier(out, "disk", disk.hits, disk.misses, disk.evictions, 0, disk.usedBytes)
        }

        out.add(
            Sample(
                "yb_objectstore_requests_total",
                listOf("cached" to "true", "op" to StoreOp.GET.wire),
                avoided.toDouble(),
                MetricType.COUNTER,
            )
        )
    }

    private fun addCacheTier(
        out: MutableList<Sample>,
        tier: String,
        hits: Long,
        misses: Long,
        evictions: Long,
        coalesced: Long,
        usedBytes: Long,
    ) {
        val labels = listOf("tier" to tier)
        out.add(Sample("yb_block_cache_hits_total", labels, hits.toDouble(), MetricType.COUNTER))
        out.add(Sample("yb_block_cache_misses_total", labels, misses.toDouble(), MetricType.COUNTER))
        out.add(Sample("yb_block_cache_evictions_total", labels, evictions.toDouble(), MetricType.COUNTER))
        out.add(Sample("yb_block_cache_bytes_used", labels, usedBytes.toDouble(), MetricType.GAUGE))
        if (tier == "mem") {
            // 在途去重省下的请求数：它直接等于省下的钱（设计文档 §6.7.1）
            out.add(Sample("yb_block_cache_coalesced_requests_total", labels, coalesced.toDouble(), MetricType.COUNTER))
        }
        val total = hits + misses
        val rate = if (total == 0L) 0.0 else hits.toDouble() / total
        out.add(Sample("yb_block_cache_hit_rate", labels, rate, MetricType.GAUGE))
    }

    /** 分片级统计。全部来自内存（版本链、常驻 `.idx` 句柄）——见类注释的 P4 说明。 */
    private fun shardSamples(out: MutableList<Sample>) {
        val stats = engine.shardStats()
        var resident = 0L
        var memBytes = 0L
        var memEntries = 0L
        val filesByLevel = HashMap<Int, Long>(8)
        val rowsByLevel = HashMap<Int, Long>(8)
        for (s in stats) {
            resident += s.residentIndexBytes
            memBytes += s.memtableBytes
            memEntries += s.memtableEntries
            for (level in s.levelFiles.indices) {
                filesByLevel.merge(level, s.levelFiles[level].toLong(), Long::plus)
                rowsByLevel.merge(level, s.levelRows.getOrElse(level) { 0L }, Long::plus)
            }
        }
        out.add(Sample("yb_open_shards", emptyList(), stats.size.toDouble(), MetricType.GAUGE))
        out.add(Sample("yb_index_resident_bytes", emptyList(), resident.toDouble(), MetricType.GAUGE))
        out.add(Sample("yb_memtable_bytes", emptyList(), memBytes.toDouble(), MetricType.GAUGE))
        out.add(Sample("yb_memtable_entries", emptyList(), memEntries.toDouble(), MetricType.GAUGE))
        for ((level, files) in filesByLevel) {
            out.add(Sample("yb_sst_files", listOf("level" to level.toString()), files.toDouble(), MetricType.GAUGE))
        }
        for ((level, rows) in rowsByLevel) {
            out.add(Sample("yb_sst_rows", listOf("level" to level.toString()), rows.toDouble(), MetricType.GAUGE))
        }
    }

    companion object {
        /** 读操作类别。`yb_query_objects_fetched` 统计的就是这四类之和。 */
        private val READ_OPS = arrayOf(StoreOp.GET, StoreOp.GET_RANGE, StoreOp.LIST, StoreOp.HEAD)

        private val REGISTERED = AtomicBoolean(false)

        @Volatile
        private var ACTIVE: ServerMetrics? = null

        /** 当前活跃实例；没有实例（进程里没有 CN）时为 null。测试与 /healthz 用。 */
        val active: ServerMetrics? get() = ACTIVE

        private fun installCollector() {
            if (!REGISTERED.compareAndSet(false, true)) return
            val reg = Metrics.registry
            reg.help("yb_up", "CN 是否在运行（1/0）。没有 CN 的进程里不会出现这条；CN 关闭后变为 0，用于聚合侧的缺席检测")
            reg.help("yb_uptime_seconds", "进程启动至今的秒数")
            reg.help("yb_statements_observed_total", "协议层观测到的语句数（含非 SELECT）")
            reg.help("yb_mysql_connections", "当前活跃的 MySQL 连接数")
            reg.help("yb_pending_wal_dropped_total", "★ 未决 WAL 无法判定而被丢弃的次数。非零需排查（它们代表丢了事务）")
            reg.help("yb_block_cache_hits_total", "块缓存命中数。tier=mem 为进程内缓存，disk 为本地 NVMe 二级缓存")
            reg.help("yb_block_cache_misses_total", "块缓存未命中数（会穿透到对象存储，除非被在途去重合并）")
            reg.help("yb_block_cache_evictions_total", "被容量挤出的条目数。与命中率一起看：低命中+高淘汰 = 工作集大于缓存")
            reg.help("yb_block_cache_coalesced_requests_total", "在途去重省下的请求数（§6.7.1）")
            reg.help("yb_block_cache_bytes_used", "缓存占用字节。tier=disk 为计数器维护的近似值")
            reg.help("yb_block_cache_hit_rate", "缓存命中率（0–1）")
            reg.help("yb_index_resident_bytes", "常驻内存的 .idx 索引字节数（§6.7 的 L3）")
            reg.help("yb_open_shards", "当前打开的分片数")
            reg.help("yb_memtable_bytes", "全部分片的 MemTable 占用字节")
            reg.help("yb_memtable_entries", "全部分片的 MemTable 条目数")
            reg.help("yb_sst_files", "各层级的 SST 文件数")
            reg.help("yb_sst_rows", "各层级的行数（近似，来自 manifest）")
            reg.collector {
                val m = ACTIVE
                if (m == null) {
                    listOf(Sample("yb_up", emptyList(), 0.0, MetricType.GAUGE))
                } else {
                    val out = ArrayList<Sample>(48)
                    out.add(Sample("yb_up", emptyList(), 1.0, MetricType.GAUGE))
                    m.collect(out)
                    out
                }
            }
        }
    }
}
