package com.cloudea.yangbase.sql

import com.cloudea.yangbase.common.ColumnType
import com.cloudea.yangbase.common.ColumnDef
import com.cloudea.yangbase.common.Datum
import com.cloudea.yangbase.common.KeyEncoder
import com.cloudea.yangbase.common.MySqlErrors
import com.cloudea.yangbase.common.ReadTs
import com.cloudea.yangbase.common.RowCodec
import com.cloudea.yangbase.common.SqlException
import com.cloudea.yangbase.common.TableSchema
import com.cloudea.yangbase.common.stats.TableStats
import com.cloudea.yangbase.txn.DatabaseEngine
import com.cloudea.yangbase.txn.TransactionBuffer
import com.cloudea.yangbase.txn.LookupResult as TxLookup

/**
 * 会话状态。MySQL 协议的每个连接持有一个。
 *
 * **事务语义**（对齐 MySQL）：
 *  - `autocommit=1`（默认）且没有显式 `BEGIN`：每条语句自成一个事务，写完即生效
 *  - 显式 `BEGIN` 或 `autocommit=0`：写入先进入 [TransactionBuffer]，
 *    `COMMIT` 时一次性落地，`ROLLBACK` 直接丢弃
 *
 * 在此之前写入是**直接落盘**的，`ROLLBACK` 什么也没撤销——那是个会误导使用者的语义错误。
 */
/** 单条语句的字节上限（`max_allowed_packet`）。读侧按 `Packets.MAX_PAYLOAD` 重组包，
 *  所以这里报的值就是客户端可以依赖的上限。 */
const val MAX_ALLOWED_PACKET: Long = 64L * 1024 * 1024

class Session(val engine: DatabaseEngine) {
    /** 全文索引接入层。**按引擎共享**——每个连接各建一份会让条件写互相打架。 */
    private val fullText = FullTextSupport.of(engine)

    /**
     * 会话字符集（设计文档 §11.1.1 陷阱 T1 / T2）。
     *
     * 协议层在**每次**解码 `COM_QUERY` / `COM_STMT_EXECUTE` 参数、以及编码
     * 字符串结果时都读它——不是在连接建立时读一次。原因见 T2：
     * `PREPARE` 与 `EXECUTE` 之间 `SET NAMES` 可能已经把它换掉了。
     */
    var charset: SessionCharset = SessionCharset.DEFAULT

    /**
     * 客户端在握手时协商出的能力位（`CLIENT_DEPRECATE_EOF` 等）。
     *
     * 暴露给 `@@yangbase_protocol_flags` 有两个用途，都是"如实"这条线上的：
     * 排查问题时能一眼看出这条连接处在哪种协议模式；测试能**证明**某条路径真的
     * 被走到了——「协商了 DEPRECATE_EOF 的连接也能正常取数」这种用例，
     * 如果客户端根本没协商，它是**空跑也全绿**的。
     */
    var protocolFlags: Int = 0

    var currentDbName: String? = null
    var explicitReadTs: ReadTs? = null
    var inTransaction: Boolean = false

    /**
     * **本语句**开始的时刻（毫秒）。
     *
     * `NOW()` / `CURDATE()` 在 MySQL 里是语句级常量：同一条语句的每一行看到的时间相同。
     * 逐行读时钟会让 `WHERE ts > NOW()` 变成"每一行用不同的基准去比"，
     * 结果不可复现（一个扫百万行的查询里，首尾两行的 NOW() 能差出整条语句的执行时间）。
     * 由 [execute] 在每条语句开始时刷新。
     */
    var statementNowMillis: Long = System.currentTimeMillis()

    /**
     * 客户端**请求过**的隔离级别（`SET TRANSACTION ISOLATION LEVEL …`）。
     *
     * 本系统只有快照隔离，所以这个值只用于**如实回报降级**
     * （`@@yangbase_isolation_degraded`），不改变行为——
     * 收下不回报就是"配了 READ COMMITTED 却按快照隔离跑"的静默不一致
     */
    var requestedIsolation: String? = null

    /**
     * **用户变量**（`SET @a = 1` / `SELECT @a`）。
     *
     * 此前它们被"接受但丢弃"：`SET @a = 42` 不报错也不存，`SELECT @a` 永远返回 NULL。
     * 那是本项目最反对的一类缺陷——不报错、只是答案是空的。
     * 受影响的不是我方功能，而是**客户端协议**：`mysqldump` 用它保存/恢复会话设置
     * （`SET @saved_cs_client = @@character_set_client` … `SET character_set_client = @saved_cs_client`），
     * 拿到 NULL 之后再去 `SET character_set_client = @saved_cs_client` 就会报
     * "Unknown character set: ''"——错误信息与真正的原因（用户变量没存）隔得很远。
     */
    val userVars: MutableMap<String, Datum> = java.util.HashMap()

    /**
     * 本会话**最后一次提交**的 commit_ts（`-1` 表示还没提交过）。
     *
     * 用途只有一个：读快照被并发 2PC 压低时，保证"读自己的写"仍然成立。
     */
    var lastCommitTs: Long = -1L
    var autocommit: Boolean = true

    /**
     * 提交模式**不在这里**。它是部署级设置（组提交写者是每分片一个、被所有会话共享的），
     * 会话里存一个副本只会让 `SHOW` 说谎：曾经这里有个恒为 "sync" 的字段，
     * 于是服务端 `--commit-mode batch` 启动后 `SHOW VARIABLES LIKE 'commit_mode'`
     * 依然报 sync——使用者据此判断该用多大并发，而判据是假的。
     * 现在一律读 `engine.commitMode`。
     */

    /** 最近一次 INSERT 产生的自增值，供 LAST_INSERT_ID() 使用。 */
    var lastInsertId: Long = 0

    /** 本事务的写缓冲。非 null 表示当前处于事务中。 */
    var txBuffer: TransactionBuffer? = null
        private set

    /**
     * 本事务待做的全文索引维护。
     *
     * 必须等到 COMMIT 才执行——立即执行会让未提交的行可以被检索到（脏读）。
     * 见 [FullTextSupport.Pending] 的说明。
     */
    val pendingFullText = ArrayList<FullTextSupport.Pending>()

    /** DDL 会隐式提交事务（MySQL 语义）。 */
    var pendingDdl = false

    /**
     * 本事务通过 `SELECT ... FOR UPDATE` / `LOCK IN SHARE MODE` 拿到的行锁。
     *
     * 生命周期**跟事务走**（不是跟语句走）：这正是 `FOR UPDATE` 的意义所在——
     * "我读过的这些行，在我提交之前别人不许改"。所以提交/回滚时才释放，
     * 而 autocommit 模式下事务就是这一条语句本身（见 [releaseRowLocks]）。
     */
    private val rowLocks = ArrayList<com.cloudea.yangbase.txn.LockHandle>()

    /**
     * 行锁的等待上限（毫秒），语义对齐 `innodb_lock_wait_timeout`（默认 50 秒）。
     *
     * 等到超时**报 1205**而不是无限等待：无限等待会让应用看起来"卡住"，
     * 而 50 秒足够让正常的短事务通过。
     */
    var lockWaitMillis: Long = 50_000

    /**
     * 行锁的事务标识：谁在等、谁持有，诊断与死锁检测都靠它。
     *
     * ⚠️ 必须**全局唯一**。第一版只用了事务序号，而那个序号是**每个 Session 各自
     * 从 0 开始**的——于是两个连接的 owner 都是 `node#1`，锁表把"等自己"当成了环，
     * 立刻报 1213 死锁（等待图还是空的，看起来莫名其妙）。
     */
    val lockOwner: String get() = "${engine.nodeId}#s$sessionSerial#t$txSerial"

    private var txSerial: Long = 0

    private val sessionSerial = nextSessionSerial.incrementAndGet()

    private companion object {
        val nextSessionSerial = java.util.concurrent.atomic.AtomicLong(0)
    }

    fun rememberRowLock(handle: com.cloudea.yangbase.txn.LockHandle) {
        rowLocks.add(handle)
    }

    fun rowLocks(): List<com.cloudea.yangbase.txn.LockHandle> = rowLocks

    /**
     * 释放本事务持有的全部行锁。
     *
     * `leaveLocksToTtl = true` 用于**连接异常断开**：那时不再发 RPC（MS 可能已经
     * 联系不上），让锁自己过期；而超时是有限的，所以不会永久占着。
     */
    suspend fun releaseRowLocks(leaveLocksToTtl: Boolean = false) {
        if (rowLocks.isEmpty()) return
        val handles = rowLocks.toList()
        rowLocks.clear()
        if (leaveLocksToTtl) return
        for (h in handles) {
            runCatching { engine.coord.locks.unlock(h) }
                .onFailure { println("[YangBase] 释放行锁失败（锁会随 TTL 过期）：${it.message}") }
        }
    }

    /**
     * 提交前的行锁自检（见 [com.cloudea.yangbase.txn.LockTable.stillHeld]）。
     *
     * 失效就**明确报 1213 并回滚**：锁可能在 MS 换领导者时被丢掉（§4.4），
     * 而"以为还锁着"地提交等于静默的丢更新。重试是可见的代价，覆盖别人的更新不是。
     */
    suspend fun assertRowLocksHeld() {
        if (rowLocks.isEmpty()) return
        if (!engine.coord.locks.stillHeld(rowLocks)) {
            rollback()
            throw SqlException(
                MySqlErrors.ER_DEADLOCK, "40001",
                "行锁已在事务期间失效（元数据服务的领导者更替会丢掉锁表，见设计文档 §4.4）。" +
                    "本事务已被回滚——请重试。若继续提交，本事务基于「我锁住了这些行」做出的决定" +
                    "可能已被另一笔事务覆盖，而那是静默的丢更新",
            )
        }
    }

    val currentDb get() = currentDbName?.let { engine.catalog.findDatabase(it) }

    fun requireDb(): com.cloudea.yangbase.common.DatabaseSchema = currentDbName?.let {
        // 系统库（information_schema 等）**只读**：它们是内存里物化的，没有主键、
        // 没有分片。走到这里说明有语句想**写**它们（能读的路径不会经过 requireDb）。
        // 必须报"这里不能写"，而不是"没有选中数据库"——后者会让人以为是自己忘了 USE，
        // 然后去反复试 `USE information_schema`（而那一步其实是成功的）
        if (InformationSchema.isSystemSchemaName(it)) {
            throw SqlException(
                MySqlErrors.ER_NOT_SUPPORTED_YET, "42000",
                "'$it' 是只读的系统库（内存里物化，没有分片与主键），不支持写入或改结构",
            )
        }
        engine.catalog.findDatabase(it)
    } ?: throw SqlException(MySqlErrors.ER_BAD_DB_ERROR, "3D000", "No database selected")

    /** 写现在应当进缓冲，还是直接落存储层。 */
    val shouldBuffer: Boolean get() = inTransaction || !autocommit

    /** 确保缓冲存在（懒创建：`autocommit=0` 后第一条语句才真正开事务）。 */
    suspend fun ensureBuffer(): TransactionBuffer {
        txBuffer?.let { return it }
        val buf = TransactionBuffer(engine.currentReadTs().value)
        txBuffer = buf
        return buf
    }

    /**
     * 本语句应使用的读时间戳。
     *
     * 事务内**必须固定快照**并一直复用，否则同一事务内的重复读会看到
     * 别的连接刚提交的数据——那就不是快照隔离了。
     */
    suspend fun readTs(): ReadTs {
        explicitReadTs?.let { return it }
        if (shouldBuffer) {
            val ts = txBuffer?.let { ReadTs(it.startTs) }
                ?: engine.currentReadTs(lastCommitTs).also { explicitReadTs = it }
            explicitReadTs = ts
            return ts
        }
        // 把**本会话上一次提交的 ts** 传下去：并发 2PC 会把读快照压低
        // （见 DatabaseEngine.currentReadTs），若不带上自己的提交，
        // "刚提交紧接着读"会看不到自己的写入——那是使用者一眼就能看见的错
        return engine.currentReadTs(lastCommitTs)
    }

    suspend fun begin() {
        // 上一次事务若异常结束（没有 commit/rollback）就先把它的锁收掉，
        // 否则新事务会继承一批"上一笔遗留的锁"，而它们永远不会被释放
        releaseRowLocks()
        txSerial = nextTxSerial.incrementAndGet()
        val ts = engine.currentReadTs()
        explicitReadTs = ts
        inTransaction = true
        txBuffer = TransactionBuffer(ts.value)
    }

    private val nextTxSerial = java.util.concurrent.atomic.AtomicLong(0)

    /**
     * 提交。返回实际写入的操作数。
     *
     * ⚠️ 跨分片事务的原子性尚未保证——见 [TransactionBuffer] 的说明。
     */
    suspend fun commit(): Int {
        // ★ 提交前先确认行锁还在（见 assertRowLocksHeld 的说明）
        assertRowLocksHeld()
        val buf = txBuffer
        val n = buf?.size ?: 0
        val pending = pendingFullText.toList()
        if (buf != null && !buf.isEmpty) {
            val outcome = engine.commitBuffer(buf)
            // 记下"我提交到哪个 ts 了"：并发 2PC 会压低读快照（见 currentReadTs），
            // 不带这个值的话"刚提交紧接着读"会看不到自己的写入
            if (outcome.commitTs > 0) lastCommitTs = outcome.commitTs
            // 索引维护用**这次提交的** commitTs，而不是此刻的 TSO 水位——
            // 并发下两者不同，用错了索引里的可见性判断就会错位
            if (pending.isNotEmpty()) {
                fullText.applyPending(pending, outcome.commitTs)
            }
        } else if (pending.isNotEmpty()) {
            // 只读了没写：不该有待办；真出现就丢掉，避免污染下一个事务
            pendingFullText.clear()
        }
        endTransaction()
        return n
    }

    /** 回滚：直接丢弃缓冲，存储层从未见过这些写入。 */
    fun rollback() {
        txBuffer?.clear()
        // 回滚连索引维护一起丢弃——存储层从未见过这些写入，
        // 索引里也不该留下它们的痕迹
        pendingFullText.clear()
        endTransaction()
    }

    fun endTransaction() {
        explicitReadTs = null
        inTransaction = false
        txBuffer = null
        pendingFullText.clear()
        // 行锁跟着事务结束一起释放（`endTransaction` 是唯一的事务出口：
        // commit / rollback / DDL 隐式提交都汇流到这里）
        pendingLockRelease = true
    }

    /**
     * 事务结束时释放行锁。
     *
     * 单独一个标志是因为 [endTransaction] 是**非挂起**的（它被很多同步路径调用），
     * 而释放锁要走一次 RPC。调用方在真正挂起的上下文里调 [afterStatement]。
     */
    private var pendingLockRelease = false

    /**
     * 一条语句执行完之后的收尾：按需释放行锁。
     *
     * 两条路径都要覆盖，缺一条就会**把行锁永久留着**（直到 TTL）：
     *
     * 1. **事务刚结束**（commit / rollback / DDL 隐式提交）→ [pendingLockRelease] 被置位；
     * 2. **不在事务里**（autocommit）→ 这一条语句就是它自己的事务，
     *    `SELECT ... FOR UPDATE` 的锁在语句结束时就该没了（MySQL 语义）。
     *
     * 第一版只覆盖了第 2 条，于是 `BEGIN; SELECT ... FOR UPDATE; COMMIT;` 之后
     * 行被锁到 TTL 到期——下一个事务超时报 1205，而错误信息里指向的是**上一个**
     * 事务的 owner，排查时完全看不出是"释放漏了"。
     */
    suspend fun afterStatement() {
        if (pendingLockRelease) {
            pendingLockRelease = false
            releaseRowLocks()
            return
        }
        if (!shouldBuffer) releaseRowLocks()
    }
}

sealed interface ExecResult
class QueryResult(val resultSet: ResultSet) : ExecResult
class OkResult(
    val affectedRows: Long,
    val lastInsertId: Long = 0,
    val info: String = "",
) : ExecResult

/**
 * 语句执行器。
 *
 * 它对上层（MySQL 协议）暴露一个统一的 `execute`，把 AST 落到 [DatabaseEngine]。
 * 优化决策集中在这里，尤其是**减少对象存储请求数**的那几条
 * （设计文档 §10.2 R1–R6）。
 */
/**
 * 本系统**如实提供**的隔离级别（见 [docs/zh/compatibility.md](../../../../../../../docs/zh/compatibility.md) §4.1：
 * 实现是快照隔离，对外报告这一档）。
 *
 * 请求落在这里面不算降级；落在外面（`READ COMMITTED` / `READ UNCOMMITTED`）会被记进
 * `@@yangbase_isolation_degraded` —— 收下但如实回报（原则 P6：显式降级）。
 */
internal val HONORED_ISOLATION = setOf("REPEATABLE-READ", "SERIALIZABLE")

class Executor(private val engine: DatabaseEngine) {

    /**
     * 没有 LIMIT 时全文检索取多少条。
     *
     * 不能取「全部命中」——那会退化成对每个命中文档回表一次。
     * 一个保守的上界比一个正确的下界更符合「不制造意外 IO」的原则（P4）。
     */
    private val DEFAULT_FULLTEXT_TOPK = 1000

    /**
     * 自增 id 的分配器（设计文档 §11.4）。
     *
     * 由**引擎**持有而不是这里：段是本地缓存，按会话各存一份会让每个连接
     * 都去领一个段，既多花请求又把 id 摊得更散。而「权威水位」在对象存储上，
     * 所以多个 CN 之间也不会发出同一个 id。
     */
    private val autoInc get() = engine.autoIncrement

    /** 只用来查 GC 水位——时间旅行必须知道哪些历史已经不可读（设计文档 §6.9）。 */
    private val gc = com.cloudea.yangbase.txn.GarbageCollector(engine.store)

    /** 倒排索引的接入层（设计文档 §8.6）。与会话共享同一份实例。 */
    private val fullText = FullTextSupport.of(engine)

    /**
     * 优化器统计（设计文档 §10.3）。**每个引擎一份**，因为它的缓存必须共享：
     * 按连接各建一份会让同一个表的统计被反复从对象存储读，而且每份缓存各自
     * 失效——同一个查询在不同连接上得到不同的计划。
     */
    private val statsStore = com.cloudea.yangbase.txn.TableStatsStore.of(engine)

    suspend fun execute(session: Session, rawStmt: Statement): ExecResult {
        // 语句时钟复位：`NOW()` 在同一条语句里必须是个常量（见 Session.statementNowMillis）
        session.statementNowMillis = System.currentTimeMillis()
        // ★ 子查询先折算成字面量（见 [Subqueries]）。放在**这里**而不是各条语句里，
        // 是因为它可以出现在任何带表达式的位置（SELECT 的各个子句、UPDATE 的赋值与
        // WHERE、INSERT 的 VALUES），逐个入口去接必然会漏掉某一处——
        // 而漏掉的表现是"那个位置报一句内部错误"，不是数据错误。
        val stmt = Subqueries.rewrite(rawStmt) { sub ->
            val rs = select(session, sub as? SelectStmt ?: throw SqlException(
                MySqlErrors.ER_PARSE_ERROR, "42000", "子查询必须是 SELECT",
            ))
            Subqueries.QueryRows(rs.columns, rs.rows)
        }
        val result = try {
            try {
                executeRewritten(session, stmt)
            } catch (e: SqlException) {
                // ★ 库/表找不到时**刷新一次目录再重试**（只对读语句）。
                //
                // 多 CN 下这是必须的：schema 缓存在**每个 CN 的内存里**，另一个 CN
                // 刚建的表，本 CN 只有等失效通知或 5 秒轮询才看得到。使用者的体验是
                // 「我刚在另一台 CN 上建的表，这边说不存在」——而它确实存在。
                //
                // 只重试**读语句**：写语句如果已经产生了部分副作用，重试就可能写两遍。
                // 而"引用一张不存在的表"的写语句本来也没写成什么，代价只是让使用者
                // 手动重试一次（错误信息会如实说是"表不存在"）。
                // ⚠️ 表不存在是 **1146**（`ER_NO_SUCH_TABLE`），不是 1053
                // （`ER_BAD_TABLE_ERROR` = "Unknown table"，用在 DROP 之类的地方）。
                // 只认 1053 的话，这条重试**永远不会触发**——它看起来写对了，实际是死代码
                val miss = e.errorCode == MySqlErrors.ER_NO_SUCH_TABLE ||
                    e.errorCode == MySqlErrors.ER_BAD_TABLE_ERROR ||
                    e.errorCode == MySqlErrors.ER_BAD_DB_ERROR
                if (!miss || stmt !is SelectStmt) throw e
                engine.catalog.load()
                executeRewritten(session, stmt)
            }
        } catch (e: Throwable) {
            // 语句失败时要把本语句拿到的行锁放掉，否则它们会一直挂到连接断开
            session.rollback()
            session.afterStatement()
            throw e
        }
        // ★ autocommit 模式下这一条语句就是它自己的事务（MySQL 语义）：
        // `SELECT ... FOR UPDATE` 在自动提交下锁只在**语句期间**有效，
        // 语句一结束就释放。事务里（BEGIN 或 autocommit=0）则不释放——
        // 那正是 `FOR UPDATE` 的用途：把行锁到提交为止
        session.afterStatement()
        return result
    }

    private suspend fun executeRewritten(session: Session, stmt: Statement): ExecResult = when (stmt) {
        is CreateDatabaseStmt -> {
            val db = engine.catalog.createDatabase(stmt.name, stmt.ifNotExists)
            // ⚠️ **不要**顺手把会话切到新库上。MySQL 的 `CREATE DATABASE` 只建库，
            // 当前库**保持不变**——而这里曾经写了一句 `session.currentDbName = db.name`，
            // 于是"建库之后不带库名的语句"全部落进了新库：
            //     CREATE DATABASE a;   -- 当前库突然变成 a
            //     CREATE TABLE t (...); -- 建到 a 里去了
            // 不报错、不提示，数据落在另一个库——而这类脚本（初始化、迁移、备份回灌）
            // 恰恰是"建完库接着建表"的写法。真事：mysqldump 的产物里
            // `CREATE DATABASE` 与 `USE` 是分开的两句，这个副作用让
            // `DROP TABLE IF EXISTS t` 之类的语句在错误的库里执行
            OkResult(1, info = "Database '${db.name}' created")
        }

        is DropDatabaseStmt -> {
            val db = engine.catalog.findDatabase(stmt.name)
            if (db == null) {
                if (!stmt.ifExists) throw SqlException(
                    MySqlErrors.ER_BAD_DB_ERROR, "42000", "Unknown database '${stmt.name}'"
                )
                OkResult(0)
            } else {
                engine.catalog.dropDatabase(stmt.name)
                if (session.currentDbName.equals(stmt.name, true)) session.currentDbName = null
                OkResult(0)
            }
        }

        is UseStmt -> {
            // `USE information_schema` 必须成功：真实 MySQL 里它就是一个 schema，
            // 客户端与手敲命令的人都会这么进（进去之后 `SHOW TABLES`、
            // 不带库名的 `SELECT ... FROM tables` 都要能用）。这些库是内存里物化的，
            // 不在 catalog 里，所以要单独放行。
            //
            // 反过来，**不存在的库必须报错**：静默成功会让后面每条语句都报
            // "表不存在"，而真正的原因（库名敲错了）在很远的地方。
            //
            // 判据走 [com.cloudea.yangbase.txn.Catalog.findDatabaseRefreshing]：
            // 多 CN 下目录是异步传播的，「CN1 建库 → CN2 立刻 USE」本来就会落在
            // 一个窗口里。按本地目录直接判会把那个窗口变成一句"库不存在"
            if (!InformationSchema.isSystemSchemaName(stmt.name)) {
                engine.catalog.findDatabaseRefreshing(stmt.name)
                    ?: throw SqlException(
                        MySqlErrors.ER_BAD_DB_ERROR, "42000", "Unknown database '${stmt.name}'",
                    )
            }
            session.currentDbName = stmt.name
            OkResult(0)
        }

        is CreateTableStmt -> createTable(session, stmt)

        is CreateIndexStmt -> {
            val db = session.requireDb()
            val t = resolveTable(session, stmt.table!!)
            // ★ 只有**一条**建索引的实现（[addIndex]）。
            // 这里曾经另写了一份：顺序是「先发布、后取 read_ts」，正好把
            // 在线构建要堵的那个缺口留着——发布到取 read_ts 之间提交的写入
            // 既不在快照里、也没被写入路径维护，索引项永久缺失。
            // 两份实现并存时，改了其中一份另一份不会跟着变，所以合并成一份。
            val indexed = addIndex(
                session, t,
                IndexAst(stmt.name, stmt.columns, stmt.unique, stmt.kind, parser = stmt.parser),
            )
            OkResult(indexed, info = "Index '${stmt.name}' created（回填 $indexed 行）")
        }

        is DropTableStmt -> {
            val db = session.requireDb()
            // 逐个删：`DROP TABLE a, b` 必须真的删两张（曾经只删第一张还返回成功）
            var dropped = 0
            for (name in stmt.names) {
                if (engine.catalog.dropTable(db.databaseId, name)) dropped++
                else if (!stmt.ifExists) {
                    throw SqlException(MySqlErrors.ER_NO_SUCH_TABLE, "42S02", "Table '$name' doesn't exist")
                }
            }
            OkResult(dropped.toLong())
        }

        is CreateViewStmt -> QueryResult(createView(session, stmt).let {
            // MySQL 的 CREATE VIEW 返回的是一个空结果集（不是 OK 包）：驱动按
            // 「有几列」来决定怎么解析后续包，回 OK 会让部分客户端把这次调用当成"未知响应"
            ResultSet(listOf("View"), emptyList())
        })
        is DropViewStmt -> {
            val db = session.requireDb()
            var dropped = 0
            for (name in stmt.names) {
                val existing = engine.catalog.findTable(db.databaseId, name)
                if (existing == null || !existing.isView) {
                    if (!stmt.ifExists) {
                        throw SqlException(
                            MySqlErrors.ER_NO_SUCH_TABLE, "42S02", "View '$name' doesn't exist",
                        )
                    }
                } else {
                    engine.catalog.dropTable(db.databaseId, name)
                    dropped++
                }
            }
            OkResult(dropped.toLong())
        }

        is AlterTableStmt -> alterTable(session, stmt)
        is InsertStmt -> insert(session, stmt)
        is InsertSelectStmt -> insertSelect(session, stmt)
        is SelectStmt -> QueryResult(select(session, stmt))
        is UnionStmt -> QueryResult(union(session, stmt))
        is UpdateStmt -> update(session, stmt)
        is DeleteStmt -> delete(session, stmt)
        is ShowStmt -> QueryResult(show(session, stmt))
        is DescribeStmt -> QueryResult(describe(session, stmt))
        is SetStmt -> setVariable(session, stmt)
        is BeginStmt -> { session.begin(); OkResult(0) }
        CommitStmt -> {
            val n = session.commit()
            OkResult(0, info = "$n 行已提交")
        }
        RollbackStmt -> { session.rollback(); OkResult(0, info = "已回滚") }
        is AnalyzeStmt -> QueryResult(analyze(session, stmt))
        is ExplainStmt -> QueryResult(explain(session, stmt))
        is NoOpStmt -> OkResult(0)
        is UnsupportedStmt -> throw SqlException(
            MySqlErrors.ER_NOT_SUPPORTED_YET, "42000",
            "暂不支持的语句: ${stmt.raw.take(80)}（见设计文档 §11.3 的限制清单）",
        )
    }

    // ── DDL ───────────────────────────────────────────────────────

    private suspend fun createTable(session: Session, stmt: CreateTableStmt): ExecResult {
        val db = session.requireDb()
        val cols = stmt.columns.map { it.column }
        val indexes = stmt.indexes.map { ix ->
            com.cloudea.yangbase.common.IndexDef(
                id = 0, name = ix.name, kind = ix.kind, columns = ix.columns, unique = ix.unique,
            )
        }
        val partitionKind = when (stmt.partitionKindName.uppercase()) {
            "HASH" -> com.cloudea.yangbase.common.PartitionKind.HASH
            "KEY" -> com.cloudea.yangbase.common.PartitionKind.KEY
            "RANGE" -> com.cloudea.yangbase.common.PartitionKind.RANGE
            "LIST" -> com.cloudea.yangbase.common.PartitionKind.LIST
            else -> com.cloudea.yangbase.common.PartitionKind.NONE
        }
        val partitionDefs = stmt.partitions.map { p ->
            com.cloudea.yangbase.common.PartitionDef(
                name = p.name,
                kind = partitionKind,
                rangeUpperBound = p.upperBound,
                listValues = p.values,
            )
        }
        val t = engine.catalog.createTable(
            dbId = db.databaseId,
            name = stmt.name,
            columns = cols,
            primaryKey = stmt.primaryKey,
            indexes = indexes,
            partitionKind = partitionKind,
            partitionCount = if (partitionDefs.isNotEmpty()) partitionDefs.size else stmt.partitionCount,
            partitions = partitionDefs,
            partitionColumn = stmt.partitionColumn,
            partitionExpr = stmt.partitionExpr,
            ifNotExists = stmt.ifNotExists,
        )
        // 立刻打开它的分片，让建表后第一次查询不必再走一次打开路径
        for (shard in engine.allShards(t)) engine.shardStore(shard, t)
        return OkResult(0, info = "Table '${t.name}' created")
    }

    // ── ALTER TABLE / TRUNCATE ────────────────────────────────────

    /**
     * `ALTER TABLE` 与 `TRUNCATE`。
     *
     * 设计文档 §4.5 把 schema 演化划定为「加列/删列这类**无重写**的场景」，
     * 这里严格按那个范围实现：加列只能加在末尾、删列只能删末尾——
     * 于是它们都是纯元数据操作（O(1)），旧行靠行首的列数自然被正确解码。
     * 在中间插删会让列位置整体错位，必须重写全部数据，这里**明确拒绝**。
     */
    private suspend fun alterTable(session: Session, stmt: AlterTableStmt): ExecResult {
        var table = resolveTable(session, stmt.table)
        requireBaseTable(table)          // 视图不可 ALTER，也不能 TRUNCATE
        var total = 0L
        val notes = ArrayList<String>()

        for (action in stmt.actions) {
            when (action) {
                // `ENABLE|DISABLE KEYS`：明确的空操作（见 NoOpAlterAction 的说明）。
                // 不记进 notes、不改表，也不报错
                NoOpAlterAction -> {}
                is AddColumn -> {
                    table = addColumn(session, table, action)
                    notes.add("added column ${action.column.name}")
                }
                is DropColumn -> {
                    table = dropColumn(session, table, action.name)
                    notes.add("dropped column ${action.name}")
                }
                is AddIndexAction -> {
                    total += addIndex(session, table, action.index)
                    table = engine.catalog.requireTable(table.databaseId, table.name)
                    notes.add("added index ${action.index.name}")
                }
                is DropIndexAction -> {
                    dropIndex(table, action.name)
                    table = engine.catalog.requireTable(table.databaseId, table.name)
                    notes.add("dropped index ${action.name}")
                }
                is AddPartitionAction -> {
                    table = addPartition(table, action)
                    notes.add("added ${action.partitions.size} partition(s)")
                }
                is DropPartitionAction -> {
                    total += dropPartition(table, action.names)
                    table = engine.catalog.requireTable(table.databaseId, table.name)
                    notes.add("dropped partition ${action.names.joinToString(",")}")
                }
                is TruncatePartitionAction -> {
                    total += truncatePartition(session, table, action.names)
                    notes.add("truncated partition ${action.names.joinToString(",")}")
                }
                is ReorganizePartitionAction -> {
                    total += reorganizePartition(session, table, action)
                    table = engine.catalog.requireTable(table.databaseId, table.name)
                    notes.add("reorganized ${action.from.joinToString(",")}")
                }
                is RenameTable -> {
                    table = renameTable(table, action.to)
                    notes.add("renamed to ${action.to}")
                }
                TruncateWholeTable -> {
                    total += truncateTable(session, table)
                    notes.add("truncated")
                }
            }
        }
        return OkResult(total, info = notes.joinToString("; "))
    }

    /** 加列。**O(1)**：只改 schema，旧行读时自动补默认值（见 `RowCodec.decode`）。 */
    private suspend fun addColumn(
        session: Session,
        t: TableSchema,
        action: AddColumn,
    ): TableSchema {
        val col = action.column
        if (action.first) {
            throw SqlException(
                MySqlErrors.ER_NOT_SUPPORTED_YET, "0A000",
                "不支持 ADD COLUMN ... FIRST：列的位置会整体错位，必须重写全部数据。" +
                    "本系统只支持在**末尾**加列（那是纯元数据操作，O(1)）。",
            )
        }
        action.after?.let { after ->
            val last = t.columns.lastOrNull()?.name
            if (!after.equals(last, ignoreCase = true)) {
                throw SqlException(
                    MySqlErrors.ER_NOT_SUPPORTED_YET, "0A000",
                    "不支持 ADD COLUMN ... AFTER $after：只有追加到末尾（AFTER ${last ?: "?"}）不需要重写数据",
                )
            }
        }
        if (t.indexOfOrNull(col.name) != null) {
            throw SqlException(MySqlErrors.ER_DUP_FIELDNAME, "42S21", "列 '${col.name}' 已存在")
        }
        // 旧行没有这一列，只能靠默认值补。既不可空又没默认值 → 补出来的是 NULL，
        // 那就直接违反了 NOT NULL。在 DDL 时拦住，而不是留下违反约束的数据。
        // 但**空表**没有旧行，不存在"补不出值"的问题，此时应当允许。
        if (!col.nullable && col.defaultValue == null && engine.scanAll(t, session.readTs()).isNotEmpty()) {
            throw SqlException(
                MySqlErrors.ER_INVALID_USE_OF_NULL, "22004",
                "新增列 '${col.name}' 既不可为 NULL 又没有 DEFAULT：已有行没有它的值。" +
                    "请声明为 NULL 或给出 DEFAULT。",
            )
        }
        if (col.autoIncrement) {
            throw SqlException(
                MySqlErrors.ER_NOT_SUPPORTED_YET, "0A000",
                "不支持把新增列声明为 AUTO_INCREMENT（自增值需要回填已有行）",
            )
        }
        return engine.catalog.updateTable(t.copy(columns = t.columns + col))
    }

    /** 删列。只允许删**末尾**的列，否则列位置错位。 */
    private suspend fun dropColumn(session: Session, t: TableSchema, name: String): TableSchema {
        val idx = t.indexOf(name)
        if (idx != t.columns.size - 1) {
            throw SqlException(
                MySqlErrors.ER_NOT_SUPPORTED_YET, "0A000",
                "只支持删除**末尾**的列：删中间列会让后面的列整体错位，必须重写全部数据。" +
                    "末尾的列是 '${t.columns.last().name}'。",
            )
        }
        if (t.primaryKey.any { it.equals(name, true) }) {
            throw SqlException(MySqlErrors.ER_PARSE_ERROR, "42000", "不能删除主键列 '$name'")
        }
        val usedBy = t.indexes.firstOrNull { ix -> ix.columns.any { it.equals(name, true) } }
        if (usedBy != null) {
            throw SqlException(
                MySqlErrors.ER_PARSE_ERROR, "42000",
                "不能删除列 '$name'：索引 '${usedBy.name}' 用到它。请先 DROP INDEX。",
            )
        }
        return engine.catalog.updateTable(t.copy(columns = t.columns.dropLast(1)))
    }

    /** 加索引（含回填）。与 `CREATE INDEX` 共用同一条路径。 */
    /**
     * `CREATE INDEX`（设计文档 §8.7 的在线构建）。
     *
     * ## 顺序是有讲究的，不能随手调整
     *
     * ```
     *   ① 先取 read_ts        ← 必须**早于**发布索引
     *   ② 发布 BUILDING       ← 这之后的写入才会维护它
     *   ③ 回填 + 追平
     *   ④ 翻 ACTIVE           ← 这之后查询才可以用它
     * ```
     *
     * ①在②之前是**正确性要求**，不是风格：一旦反过来，在
     * 「取 read_ts 之前、发布之后」提交的写入既不在快照里（回填看不到），
     * 又不会写入时维护（发布之前它们已经提交完）——**索引项永久缺失**，
     * 而查询走上索引就不再回退扫描，于是那些行静默地查不出来。
     *
     * ④ 单独成一步，是因为**构建期间的索引必须对查询不可见**：
     * 它只填了一半，拿它取数就是少返回行。`activeIndexes` 是唯一的判据。
     *
     * 唯一索引的重复值自检在回填里做：发现重复就抛异常，索引停在
     * BUILDING（查询不用它，数据不受影响），清理掉重来即可。
     */
    private suspend fun addIndex(session: Session, t: TableSchema, ast: IndexAst): Long {
        val idx = com.cloudea.yangbase.common.IndexDef(
            id = 0, name = ast.name, kind = ast.kind,
            columns = ast.columns, unique = ast.unique, includes = ast.includes,
            parser = ast.parser,
            // 先以 BUILDING 发布，构建完再翻 ACTIVE
            state = com.cloudea.yangbase.common.IndexState.BUILDING,
        )
        // ① 快照时间戳
        val readTs = engine.currentReadTs()
        if (ast.kind == com.cloudea.yangbase.common.IndexKind.FULLTEXT) fullText.analyzerFor(ast.parser)
        // ② 发布
        val updated = engine.catalog.addIndex(t.databaseId, t.name, idx)
        val created = updated.indexes.first { it.name.equals(ast.name, true) }

        // ③ 回填 + 追平
        val built = when (ast.kind) {
            com.cloudea.yangbase.common.IndexKind.FULLTEXT -> {
                val existing = engine.scanAll(updated, readTs)
                // onInsert 走的是全量索引列表（含 BUILDING），所以刚发布、
                // 还处于 BUILDING 的倒排索引在这里也会被回填
                fullText.onInsert(updated, existing, engine.allocateCommitTs())
                existing.size.toLong()
            }
            else -> engine.onlineIndexBuilder.build(updated, created, readTs)
        }

        // ④ 可用
        engine.catalog.setIndexState(
            updated.databaseId, updated.name, created.name,
            com.cloudea.yangbase.common.IndexState.ACTIVE,
        )
        return built
    }

    /** 删索引：从 schema 移除，并清掉它的索引分片对象。 */
    private suspend fun dropIndex(t: TableSchema, name: String) {
        val idx = t.indexes.firstOrNull { it.name.equals(name, true) }
            ?: throw SqlException(MySqlErrors.ER_CANT_DROP_FIELD_OR_KEY, "42000", "索引 '$name' 不存在")
        engine.catalog.updateTable(t.copy(indexes = t.indexes - idx))
        engine.dropIndexData(idx)
    }

    private suspend fun renameTable(t: TableSchema, newName: String): TableSchema {
        if (engine.catalog.findTable(t.databaseId, newName) != null) {
            throw SqlException(MySqlErrors.ER_TABLE_EXISTS_ERROR, "42S01", "表 '$newName' 已存在")
        }
        // 重命名是纯元数据操作：数据分片按 tableId 组织，与名字无关
        val renamed = engine.catalog.updateTable(t.copy(name = newName))
        engine.catalog.dropTable(t.databaseId, t.name)
        return engine.catalog.requireTable(renamed.databaseId, newName)
    }

    private suspend fun truncateTable(session: Session, t: TableSchema): Long {
        val n = engine.deleteAll(t, session.readTs())
        // 索引分片同样要清空——否则索引会残留一堆指向已删除行的项
        for (idx in t.indexes) engine.dropIndexData(idx)
        // MySQL 的 TRUNCATE 会把 AUTO_INCREMENT 重置回 1（「清空重来」的常见用法）。
        // 不复位的话 id 会接着之前的号往下发，与用惯了 MySQL 的人预期不符。
        if (t.columns.any { it.autoIncrement }) autoInc.reset(t.tableId)
        return n
    }

    private suspend fun truncatePartition(session: Session, t: TableSchema, names: List<String>): Long =
        engine.deleteAll(t, session.readTs(), shards = shardsOf(t, names))

    private suspend fun dropPartition(t: TableSchema, names: List<String>): Long {
        val removed = shardsOf(t, names) // 在任何变更前验证所有分区名。
        val kept = t.partitions.filterNot { p -> names.any { it.equals(p.name, true) } }
        if (kept.isEmpty()) {
            throw SqlException(MySqlErrors.ER_PARSE_ERROR, "42000", "不能删除所有分区，请使用 DROP TABLE")
        }
        // 按旧布局删除目标行（包括二级索引），保留分区的物理编号不变。
        engine.deleteAll(t, session0ReadTs(), shards = removed)
        engine.catalog.updateTable(t.copy(partitions = kept))
        return 0
    }

    private suspend fun addPartition(t: TableSchema, action: AddPartitionAction): TableSchema {
        if (!t.partitionKind.isEnumerated) {
            throw SqlException(
                MySqlErrors.ER_PARTITION_MGMT_ON_NONPARTITIONED, "HY000",
                "表 '${t.name}' 不是 RANGE/LIST 分区表，不能 ADD PARTITION",
            )
        }
        if (t.partitions.lastOrNull()?.rangeUpperBound.equals(com.cloudea.yangbase.common.MAXVALUE, true)) {
            throw SqlException(
                MySqlErrors.ER_PARSE_ERROR, "42000",
                "最后一个分区是 MAXVALUE，无法在其后追加。请用 REORGANIZE PARTITION 把它拆开。",
            )
        }
        var updated = t
        for (p in action.partitions) {
            val def = com.cloudea.yangbase.common.PartitionDef(
                name = p.name, kind = t.partitionKind,
                rangeUpperBound = p.upperBound, listValues = p.values,
                partitionId = updated.nextPartitionId,
            )
            updated = updated.copy(partitions = updated.partitions + def, nextPartitionId = updated.nextPartitionId + 1)
        }
        com.cloudea.yangbase.txn.Partitioning.validate(updated)
        return engine.catalog.updateTable(updated)
    }

    /**
     * `REORGANIZE PARTITION`：把若干旧分区换成一组新分区。
     *
     * **这是本系统里唯一需要重写数据的 DDL**——分区函数变了，行的归属就变了，
     * 旧分区里的数据必须搬到新分区。做法是「全表重写 + 换 schema」，
     * 期间**不能有并发写入**（与本项目其他 DDL 一样，尚未做在线 DDL）。
     */
    private suspend fun reorganizePartition(
        session: Session,
        t: TableSchema,
        action: ReorganizePartitionAction,
    ): Long {
        val fromNames = action.from.map { it.lowercase() }.toSet()
        if (t.partitions.none { it.name.lowercase() in fromNames }) {
            throw SqlException(MySqlErrors.ER_PARSE_ERROR, "42000", "分区不存在: ${action.from.joinToString(",")}")
        }
        val kept = t.partitions.filterNot { it.name.lowercase() in fromNames }
        val added = action.into.mapIndexed { i, it ->
            com.cloudea.yangbase.common.PartitionDef(
                name = it.name, kind = t.partitionKind,
                rangeUpperBound = it.upperBound, listValues = it.values,
                partitionId = t.nextPartitionId + i,
            )
        }
        val candidate = t.copy(partitions = kept + added, nextPartitionId = t.nextPartitionId + added.size)
        com.cloudea.yangbase.txn.Partitioning.validate(candidate)

        // 先把数据按**新布局**重写一遍，再切 schema。
        // 顺序不能反：反了会让新 schema 下读到空分区。
        val readTs = session.readTs()
        val rows = engine.scanAll(t, readTs)
        val rewritten = rows.size
        val oldShards = engine.allShards(t).map { it.toString() }.toSet()

        if (rows.isNotEmpty()) {
            // 用新 schema 写：planWrites 会按新的分区定义重新路由
            val writes = engine.planWrites(candidate, rows, tombstone = false)
            engine.applyWrites(candidate, writes)
        }
        engine.catalog.updateTable(candidate)
        // 旧布局里那些**不再被引用**的分区数据要清掉（分区数可能减少、编号会复用）
        engine.dropOrphanShards(candidate, oldShards)
        return rewritten.toLong()
    }

    /** 由分区名解析出分片列表。 */
    private fun shardsOf(t: TableSchema, names: List<String>): List<com.cloudea.yangbase.common.ShardId> {
        val out = ArrayList<com.cloudea.yangbase.common.ShardId>()
        for (n in names) {
            val i = t.partitions.indexOfFirst { it.name.equals(n, true) }
            if (i < 0) {
                throw SqlException(MySqlErrors.ER_PARSE_ERROR, "42000", "分区 '$n' 不存在")
            }
            out.add(engine.shard(t, i))
        }
        return out
    }

    /** 建表之外的路径拿不到会话快照；清空分区用当前水位即可。 */
    private suspend fun session0ReadTs(): ReadTs = engine.currentReadTs()

    // ── INSERT ────────────────────────────────────────────────────

    private suspend fun insert(session: Session, stmt: InsertStmt): ExecResult {
        val db = session.requireDb()
        val table = resolveTable(session, stmt.table!!)
        requireBaseTable(table)          // 视图不可写（见 requireBaseTable 的说明）
        val readTs = session.readTs()

        // ⚠️ IGNORE 模式下**逐行**构造：一行值不合法只跳过那一行，而不是让整条语句失败。
        // MySQL 的 IGNORE 承诺"把逐行的错误降级为警告"——只降级键冲突、不降级值错误
        // 等于承诺了一半。这里跳过的行会计入 `Warnings:`（没有 warning 通道，
        // 但至少行数与提示是诚实的；MySQL 会插入一个"最接近的值"，
        // 那属于静默改数据，本系统不做）
        val rows = ArrayList<List<Datum>>(stmt.rows.size)
        var skipped = 0
        val generatedIds = ArrayList<Long?>()
        for (exprs in stmt.rows) {
            try {
                var generated: Long? = null
                rows.add(buildRow(session, table, stmt.columns, exprs, readTs) { generated = it })
                generatedIds.add(generated)
            } catch (e: SqlException) {
                if (stmt.mode != InsertMode.IGNORE) throw e
                skipped++
            }
        }
        // 普通 INSERT 走整批快路径：一次判重、一次写入。
        // 另外三种（IGNORE / REPLACE / ON DUPLICATE）的结果**依赖当前表里有什么**，
        // 只能逐行处理——但那条路要先把已有行读进来，代价明显更高，不该让最常见的
        // 普通 INSERT 替它买单
        if (stmt.mode == InsertMode.INSERT && stmt.onDuplicate.isEmpty()) {
            checkPrimaryKeys(session, table, rows, readTs)
            checkUnique(session, table, rows, readTs)
            val ts = writeRows(session, table, rows, readTs)
            // 全文索引必须随数据一起维护：只更新数据会让检索返回「曾经匹配、
            // 现在已经改掉」的行——不报错，只给过期结果，比查不到更糟
            syncFullText(session, table, FullTextSupport.Pending.Insert(table, rows), ts)
            val firstGenerated = generatedIds.firstOrNull { it != null }
            firstGenerated?.let { session.lastInsertId = it }
            return OkResult(
                rows.size.toLong(), firstGenerated ?: 0,
                "Records: ${rows.size}  Duplicates: 0  Warnings: $skipped",
            )
        }
        return upsertRows(session, table, rows, stmt, readTs, generatedIds)
    }

    /**
     * `INSERT IGNORE` / `REPLACE` / `ON DUPLICATE KEY UPDATE` 的逐行处理。
     *
     * ## 为什么要先把已有行读进内存
     *
     * 这三种模式的每一行都要问一句"这一行已经存在吗"，而**主键冲突与唯一键冲突
     * 都得查**（MySQL 的 REPLACE 会因唯一键冲突而删掉那一行）。
     * 直接按主键点查只能覆盖主键那一种，于是 `REPLACE INTO` 在唯一键上会
     * 报 1062——而用户写 REPLACE 就是明知会撞键。所以这里读一次全表、
     * 在内存里维护 PK→行 与 唯一键→行 两张表，逐行更新它们。
     *
     * 代价是一次全表读（与 `UPDATE` 无 `WHERE` 时同量级）。收益是这三种模式的
     * 语义**完全正确**，包括同一批里前后行互相冲突的情况。
     */
    private suspend fun upsertRows(
        session: Session,
        table: TableSchema,
        rows: List<List<Datum>>,
        stmt: InsertStmt,
        readTs: ReadTs,
        generatedIds: List<Long?>,
    ): ExecResult {
        val vars = varsOf(session)
        val pkIdx = table.primaryKeyIndexes
        fun pkKey(row: List<Datum>): String =
            java.util.Base64.getEncoder().encodeToString(KeyEncoder.encodeKey(row, table, pkIdx))

        // 当前可见行（含本事务自己的写）按主键索引起来
        val byPk = LinkedHashMap<String, List<Datum>>()
        visibleRows(session, table, readTs).forEach { byPk[pkKey(it)] = it }
        val uniques = table.activeIndexes.filter { it.unique }

        var inserted = 0L
        var changed = 0L
        var firstGenerated: Long? = null
        fun recordGenerated(index: Int) {
            if (firstGenerated == null) generatedIds[index]?.let {
                firstGenerated = it
                session.lastInsertId = it
            }
        }
        for ((rowIndex, newRow) in rows.withIndex()) {
            // 冲突判定：先主键，再唯一键（顺序与 MySQL 一致：主键先命中）
            var conflict: List<Datum>? = byPk[pkKey(newRow)]
            if (conflict == null) {
                for (idx in uniques) {
                    val probe = idx.columns.map { newRow[table.indexOf(it)] }
                    // 含 NULL 的唯一键不参与唯一性（与 checkUnique 同一条规则）
                    if (probe.any { it is Datum.Null }) continue
                    val hit = byPk.values.firstOrNull { candidate ->
                        idx.columns.indices.all {
                            Eval.eqPublic(candidate[table.indexOf(idx.columns[it])], probe[it])
                        }
                    }
                    if (hit != null) { conflict = hit; break }
                }
            }

            when {
                // IGNORE：撞键就跳过这一行（其余行照常插入）
                conflict != null && stmt.mode == InsertMode.IGNORE -> Unit

                // REPLACE：删掉冲突行再插入新行（MySQL 的"先删后插"）
                conflict != null && stmt.mode == InsertMode.REPLACE -> {
                    writeRows(session, table, listOf(conflict), readTs, tombstone = true)
                    syncFullText(session, table, FullTextSupport.Pending.Delete(table, listOf(conflict)), readTs.value)
                    byPk.remove(pkKey(conflict))
                    val ts = writeRows(session, table, listOf(newRow), readTs)
                    syncFullText(session, table, FullTextSupport.Pending.Insert(table, listOf(newRow)), ts)
                    byPk[pkKey(newRow)] = newRow
                    recordGenerated(rowIndex)
                    changed += 2
                }

                // ON DUPLICATE KEY UPDATE：按赋值表达式更新冲突行
                conflict != null -> {
                    val updated = conflict.toMutableList()
                    for ((col, expr) in stmt.onDuplicate) {
                        val idx = table.indexOf(col)
                        // `VALUES(col)` 引用"本来要插入的值"，其余按**旧行**求值
                        val e = substituteValues(expr, table, newRow)
                        updated[idx] = coerce(Eval.eval(e, EvalContext(table, conflict, vars)), table.columns[idx])
                    }
                    val updatedRow = updated.toList()
                    // 更新后的值同样要过唯一约束（不然可以把唯一列改成重复值）
                    checkUnique(session, table, listOf(updatedRow), readTs)
                    // 先按旧行删索引项，再写新行（顺序反了会删掉刚写的索引项）
                    if (table.indexes.isNotEmpty()) {
                        writeRows(session, table, listOf(conflict), readTs, tombstone = true)
                    }
                    val ts = writeRows(session, table, listOf(updatedRow), readTs)
                    syncFullText(session, table, FullTextSupport.Pending.Update(table, listOf(conflict), listOf(updatedRow)), ts)
                    byPk[pkKey(conflict)] = updatedRow
                    // MySQL 的 affectedRows：更新一行算 **2**（语义上是"删掉旧的 + 插上新的"），
                    // 插入算 1。写了 1 的应用（靠行数判断"是插入还是更新"）会得到错误结论
                    changed += 2
                }

                // 没有冲突：普通插入
                else -> {
                    checkUnique(session, table, listOf(newRow), readTs)
                    val ts = writeRows(session, table, listOf(newRow), readTs)
                    syncFullText(session, table, FullTextSupport.Pending.Insert(table, listOf(newRow)), ts)
                    byPk[pkKey(newRow)] = newRow
                    recordGenerated(rowIndex)
                    inserted++
                }
            }
        }
        // affectedRows 的算法与 MySQL 一致：插入算 1、替换/更新算 2
        return OkResult(inserted + changed, firstGenerated ?: 0, "Records: ${rows.size}  Duplicates: ${changed}  Warnings: 0")
    }

    /**
     * 把 `VALUES(col)` 替换成"本来要插入的那个值"。
     *
     * `ON DUPLICATE KEY UPDATE` 的表达式里，裸列名指**旧行**，而 `VALUES(col)`
     * 指**新行**。少了这一步，`col = VALUES(col)`（最常见的写法）
     * 会退化成"把旧值赋给自己"——更新看起来成功了，值却没变。
     */
    private fun substituteValues(e: Expr, table: TableSchema, newRow: List<Datum>): Expr = when (e) {
        is FuncCall -> if (e.name.equals("VALUES", true) && e.args.size == 1) {
            val ref = e.args[0]
            val name = (ref as? ColumnRef)?.name
                ?: throw SqlException(
                    MySqlErrors.ER_PARSE_ERROR, "42000",
                    "VALUES() 的参数必须是列名，实际是 $ref",
                )
            Literal(newRow[table.indexOf(name)])
        } else {
            FuncCall(e.name, e.args.map { substituteValues(it, table, newRow) }, e.distinct)
        }
        is BinaryOp -> BinaryOp(e.op, substituteValues(e.left, table, newRow), substituteValues(e.right, table, newRow))
        is UnaryOp -> UnaryOp(e.op, substituteValues(e.expr, table, newRow))
        is CaseExpr -> CaseExpr(
            e.operand?.let { substituteValues(it, table, newRow) },
            e.whens.map {
                WhenClause(substituteValues(it.condition, table, newRow), substituteValues(it.result, table, newRow))
            },
            e.elseExpr?.let { substituteValues(it, table, newRow) },
        )
        is CastTo -> CastTo(substituteValues(e.expr, table, newRow), e.typeName)
        is Between -> Between(
            substituteValues(e.expr, table, newRow),
            substituteValues(e.low, table, newRow),
            substituteValues(e.high, table, newRow),
            e.negated,
        )
        is IsNull -> IsNull(substituteValues(e.expr, table, newRow), e.negated)
        is Like -> Like(substituteValues(e.expr, table, newRow), substituteValues(e.pattern, table, newRow), e.negated)
        is InList -> InList(substituteValues(e.expr, table, newRow), e.values.map { substituteValues(it, table, newRow) }, e.negated)
        else -> e
    }

    /**
     * `INSERT INTO t [(cols)] SELECT …`。
     *
     * 先跑查询、把结果当字面量行插进去。**列数必须对得上**（MySQL 的 1136），
     * 对不上的话插入的行会整体错位——那是静默的数据损坏，所以宁可报错。
     */
    private suspend fun insertSelect(session: Session, stmt: InsertSelectStmt): ExecResult {
        val table = resolveTable(session, stmt.table)
        requireBaseTable(table)
        val rs = select(session, stmt.query as? SelectStmt ?: throw SqlException(
            MySqlErrors.ER_PARSE_ERROR, "42000", "INSERT ... SELECT 里的查询必须是 SELECT",
        ))
        if (rs.rows.isEmpty()) {
            return OkResult(0, session.lastInsertId, "Records: 0  Duplicates: 0  Warnings: 0")
        }
        val expected = stmt.columns?.size ?: table.columns.size
        if (rs.columns.size != expected) {
            throw SqlException(
                MySqlErrors.ER_WRONG_NUMBER_OF_COLUMNS_IN_SELECT, "21S02",
                "Column count doesn't match value count at row 1" +
                    "（目标列 $expected 个，查询返回 ${rs.columns.size} 个）",
            )
        }
        // 查询结果直接是 Datum，包成字面量表达式交给同一条插入路径
        // （类型转换、唯一性检查、自增、全文索引都只有一份实现）
        val rows = rs.rows.map { row -> row.map { Literal(it) } }
        return insert(session, InsertStmt(stmt.table, stmt.columns, rows, stmt.mode))
    }

    /**
     * **主键判重**：`INSERT` 撞主键必须报 1062，而不是覆盖。
     *
     * LSM 的写入语义是"同 key 覆盖"，所以少了这一步，`INSERT INTO t VALUES (1, …)`
     * 在 id=1 已存在时会**静默改掉那一行**并返回成功——应用拿不到 1062，
     * 也就不会走它本该走的更新分支。唯一索引一直有判重（见 [checkUnique]），
     * 而主键**不在 `activeIndexes` 里**，所以这一路曾经完全没检查。
     *
     * 同一批 VALUES 内部重复也要拦（`INSERT INTO t VALUES (1,…),(1,…)`）。
     */
    private suspend fun checkPrimaryKeys(
        session: Session,
        table: TableSchema,
        rows: List<List<Datum>>,
        readTs: ReadTs,
    ) {
        val pkIdx = table.primaryKeyIndexes
        if (pkIdx.isEmpty()) return
        // ⚡ 空表直接跳过：一张全新的表（批量导入最常见的情形）里不可能有冲突，
        // 而逐行点查在对象存储上是**每行几次 GET**——10 万行的导入会变成 10 万次
        // 请求。唯一性由下面的 seenInBatch 保证（同一批内部的重复仍然拦住）。
        // 计数走元数据路径，**0 次 GET**
        // ⚠️ -1 表示「元数据给不出精确计数」，那时**不能**跳过检查（会漏判冲突）
        if (engine.countAll(table, readTs) == 0L) {
            val seen = HashSet<String>()
            for (row in rows) {
                val pkValues = pkIdx.map { row[it] }
                if (pkValues.any { it is Datum.Null }) continue
                if (!seen.add(java.util.Base64.getEncoder().encodeToString(KeyEncoder.encodeKey(row, table, pkIdx)))) {
                    throw dupPrimaryKey(table, pkValues)
                }
            }
            return
        }
        val seenInBatch = HashSet<String>()
        for (row in rows) {
            val pkValues = pkIdx.map { row[it] }
            // 主键含 NULL 时不做判重：自增列在 buildRow 里已经补过值，
            // 走到这里还有 NULL 说明这张表本来就没有主键约束语义
            if (pkValues.any { it is Datum.Null }) continue
            val encoded = KeyEncoder.encodeKey(row, table, pkIdx)
            val key = java.util.Base64.getEncoder().encodeToString(encoded)
            if (!seenInBatch.add(key)) throw dupPrimaryKey(table, pkValues)
            // 本事务自己的写也要算：同一事务里连续两次插入同一个主键，
            // 第二条会把第一条覆盖掉——那是应用绝对想不到的结果
            val shard = engine.shardFor(table, encoded)
            val inBuffer = session.txBuffer?.opsForShard(shard)?.any { op ->
                op.value != null && op.key.contentEquals(encoded)
            } ?: false
            if (inBuffer) throw dupPrimaryKey(table, pkValues)
            if (engine.lookupByPk(table, row, readTs) != null) throw dupPrimaryKey(table, pkValues)
        }
    }

    /**
     * `UPDATE` 改主键时的判重：新主键不能撞上**别的行**。
     *
     * 与 [checkPrimaryKeys] 的唯一区别是"本语句要更新的那些旧主键"是合法的目标位置——
     * `UPDATE t SET id = id + 1` 这类整表平移必须能跑。
     * 但**同一批更新后的行之间**仍然不能撞（`UPDATE t SET id = 1` 命中两行时）。
     */
    private suspend fun checkUpdatePrimaryKeys(
        session: Session,
        table: TableSchema,
        oldRows: List<List<Datum>>,
        newRows: List<List<Datum>>,
        readTs: ReadTs,
    ) {
        val pkIdx = table.primaryKeyIndexes
        if (pkIdx.isEmpty()) return
        fun enc(row: List<Datum>) = KeyEncoder.encodeKey(row, table, pkIdx)
        val oldKeys = oldRows
            .filter { row -> pkIdx.none { row[it] is Datum.Null } }
            .map { java.util.Base64.getEncoder().encodeToString(enc(it)) }
            .toSet()
        val seen = HashSet<String>()
        for (row in newRows) {
            val pkValues = pkIdx.map { row[it] }
            if (pkValues.any { it is Datum.Null }) continue
            val encoded = enc(row)
            val key = java.util.Base64.getEncoder().encodeToString(encoded)
            if (!seen.add(key)) throw dupPrimaryKey(table, pkValues)
            // 让位给同一语句里其它行的旧主键：那是它们腾出来的位置
            if (key in oldKeys) continue
            val shard = engine.shardFor(table, encoded)
            val inBuffer = session.txBuffer?.opsForShard(shard)?.any { op ->
                op.value != null && op.key.contentEquals(encoded)
            } ?: false
            if (inBuffer) throw dupPrimaryKey(table, pkValues)
            if (engine.lookupByPk(table, row, readTs) != null) throw dupPrimaryKey(table, pkValues)
        }
    }

    private fun dupPrimaryKey(table: TableSchema, values: List<Datum>): SqlException {
        val name = table.primaryKeyIndexes.joinToString(",") { table.columns[it].name }
        return SqlException(
            MySqlErrors.ER_DUP_ENTRY, "23000",
            "Duplicate entry '${values.joinToString("-") { it.asString() }}' for key '$name'",
        )
    }

    /** 按 schema 把表达式求值成完整的一行，并补齐默认值与自增。 */
    private suspend fun buildRow(
        session: Session,
        table: TableSchema,
        explicitColumns: List<String>?,
        exprs: List<Expr>,
        readTs: ReadTs,
        onGenerated: (Long) -> Unit,
    ): List<Datum> {
        val vars = varsOf(session)
        val row = ArrayList<Datum>(table.columns.size)
        for (i in table.columns.indices) row.add(Datum.Null)

        val targetIdx = if (explicitColumns == null) {
            table.columns.indices.toList()
        } else {
            explicitColumns.map { table.indexOf(it) }
        }
        if (targetIdx.size != exprs.size) {
            throw SqlException(
                MySqlErrors.ER_PARSE_ERROR, "21S01",
                "Column count doesn't match value count at row (列 ${targetIdx.size} 个，值 ${exprs.size} 个)",
            )
        }
        for ((k, idx) in targetIdx.withIndex()) {
            val v = Eval.eval(exprs[k], EvalContext(table, null, vars, nowMillis = session.statementNowMillis))
            row[idx] = coerce(v, table.columns[idx])
        }

        // 只为未指定的列填默认值；显式 NULL 必须保留 NULL 的语义。
        for (i in table.columns.indices) {
            if (i in targetIdx) continue
            val col = table.columns[i]
            val default = col.defaultValue ?: continue
            row[i] = coerce(Eval.eval(Parser.parseExprText(default),
                EvalContext(table, null, vars, nowMillis = session.statementNowMillis)), col)
        }

        // 自增列
        for (i in table.columns.indices) {
            val c = table.columns[i]
            if (!c.autoIncrement) continue
            if (row[i] is Datum.Null) {
                val next = nextAutoInc(table)
                row[i] = Datum.I64(next)
                onGenerated(next)
            } else {
                // 显式写了一个自增值：必须把它记成水位的下界。
                // 不记的话，`INSERT INTO t VALUES (100000)` 之后自增发出的
                // id 会从这个值附近重新开始，撞上已经存在的行。
                (row[i] as? Datum.I64)?.let { v ->
                    autoInc.observeExplicit(table.tableId, v.v) { seedAutoInc(table) }
                }
            }
        }
        // 非空校验
        for (i in table.columns.indices) {
            val c = table.columns[i]
            if (!c.nullable && row[i] is Datum.Null && !c.autoIncrement) {
                throw SqlException(
                    MySqlErrors.ER_NO_DEFAULT_FOR_FIELD, "HY000",
                    "Field '${c.name}' doesn't have a default value",
                )
            }
        }
        return row
    }

    private suspend fun nextAutoInc(table: TableSchema): Long =
        autoInc.nextId(table.tableId) { seedAutoInc(table) }

    /**
     * 水位对象不存在时的兜底起点：扫一遍表，取已用过的最大自增值。
     *
     * 只在「这张表从未分配过自增段」时执行**一次**——升级自旧版本的表、
     * 以及从备份/时间点恢复回来的表都属于这种情况，它们已经有数据，
     * 水位却还没建立。不扫这一遍就会从 1 重新发号，直接覆盖已有行。
     *
     * 读过时的旧实现是「游标为 0 就扫一遍」：空表的游标永远是 0，
     * 于是**每一次插入都扫一遍全表**。这里用「水位对象在不在」作判据，
     * 一次就是一次。
     */
    private suspend fun seedAutoInc(table: TableSchema): Long {
        val idx = table.columns.indexOfFirst { it.autoIncrement }
        if (idx < 0) return 0L
        var max = 0L
        for (r in engine.scanAll(table, engine.currentReadTs())) {
            val v = r.getOrNull(idx)
            if (v is Datum.I64 && v.v > max) max = v.v
        }
        return max
    }

    private fun coerce(v: Datum, col: ColumnDef): Datum {
        val type = col.type
        val colName = col.name
        if (v is Datum.Null) return v
        // ENUM / SET / JSON 走单独的校验与规范化：它们不只是"转成字符串"，
        // 还要把值约束在封闭集合里（见各分支的说明）
        if (type == ColumnType.ENUM || type == ColumnType.SET || type == ColumnType.JSON) {
            return coerceClosed(v, col)
        }
        // 长度与宽度校验：MySQL 严格模式（5.7 起的默认）会报 1406 / 1264，
        // 而不是静默截断或回绕。少了它们，`VARCHAR(10)` 列里能塞进 200 个字符、
        // `TINYINT` 列里能存 99999 —— 之后所有依赖"这一列装得下"的代码都会踩空，
        // 而**没有任何一处报错**
        checkWidth(v, col)
        return when {
            type.isInteger || type == ColumnType.BOOLEAN -> when (v) {
                is Datum.I64 -> v
                is Datum.Bool -> Datum.I64(if (v.v) 1 else 0)
                is Datum.F64 -> Datum.I64(v.v.toLong())
                is Datum.Str -> Datum.I64(v.v.toLongOrNull() ?: throw SqlException(
                    MySqlErrors.ER_TRUNCATED_WRONG_VALUE, "22007", "列 '$colName' 收到非法整数 '${v.v}'"
                ))
                else -> v
            }
            type == ColumnType.DOUBLE || type == ColumnType.FLOAT -> when (v) {
                is Datum.F64 -> v
                is Datum.I64 -> Datum.F64(v.v.toDouble())
                is Datum.Str -> Datum.F64(v.v.toDoubleOrNull() ?: throw SqlException(
                    MySqlErrors.ER_TRUNCATED_WRONG_VALUE, "22007", "列 '$colName' 收到非法浮点数 '${v.v}'"
                ))
                else -> v
            }
            type.isString -> when (v) {
                is Datum.Str -> v
                else -> Datum.Str(v.asString())
            }
            // ★ DECIMAL 必须真的转成定点数。
            //
            // 少了这个分支不是"精度差一点"，而是**行格式错位**：`RowCodec.encode`
            // 按 `Datum` 的类型写（F64 写 8 字节 IEEE754），而 `RowCodec.decode`
            // 按**列类型**读（DECIMAL 读 `varint(scale)+zigzag`）。于是
            // `INSERT INTO t(amount) VALUES (12.34)` 之后再读，得到的是
            // 一个由 `12.34` 的二进制位解释出来的 `Dec`，而且只消耗 2 个字节
            // ——这一行**后面所有列全部错位**（或抛"行数据截断"）。
            // 换句话说：DECIMAL 列此前是"写进去就损坏"，且没有任何测试覆盖过真实读写。
            type == ColumnType.DECIMAL -> decOf(v, col)
            // BLOB 同理：BLOB 列按 `Datum.Bin` 编码、按 `Datum.Bin` 解码。
            // 存成 `Str` 会让 `WHERE blob_col = 'abc'` 永远匹配不到
            // （比较时 Str 与 Bin 走"异类"分支，永不相等——不报错，只是查不到）
            type == ColumnType.BLOB -> when (v) {
                is Datum.Bin -> v
                else -> Datum.Bin(v.asString().toByteArray(Charsets.UTF_8))
            }
            type.isTemporal -> when (v) {
                is Datum.Ts -> v
                is Datum.Str -> Datum.Ts(parseTimestamp(v.v))
                is Datum.I64 -> Datum.Ts(v.v)
                else -> v
            }
            // YEAR 在存储上就是个整数（见 ColumnType.YEAR），但它**不在**
            // `isInteger` 里（那个属性用于"整数族之间可以互相转换"的判断）。
            // 漏掉它的后果非常隐蔽：`else` 分支曾把它转成字符串，
            // 而 RowCodec 按列类型用 zigzag 整数解码 —— `1998` 存进去读出来是 `2`
            // （字符串 "1998" 的长度字节被当成了数值）。所以它必须在**这里**显式处理
            type == ColumnType.YEAR -> when (v) {
                is Datum.I64 -> v
                is Datum.Bool -> Datum.I64(if (v.v) 1 else 0L)
                is Datum.F64 -> Datum.I64(v.v.toLong())
                is Datum.Str -> Datum.I64(
                    v.v.trim().toLongOrNull()
                        ?: throw SqlException(
                            MySqlErrors.ER_TRUNCATED_WRONG_VALUE, "22007",
                            "列 '$colName' 收到非法年份 '${v.v}'",
                        ),
                )
                else -> v
            }
            // 走到这里说明有人加了新的列类型却没在 coerce 里处理它。
            // **必须报错而不是"尽力转换"**：RowCodec 是**按列类型**解码的，
            // 值类型与列类型不符时写进去的行读出来就是另一个数
            // （上面 YEAR 那条就是活生生的例子），而且不报任何错
            else -> throw SqlException(
                MySqlErrors.ER_UNKNOWN_ERROR, "HY000",
                "内部错误：列 '$colName' 的类型 ${type.mysqlName} 没有写入转换规则。" +
                    "请连同这句话报告为缺陷",
            )
        }
    }

    /**
     * 列声明的**宽度**校验：`VARCHAR(n)` 的字符数、整数族的取值范围。
     *
     * `n` 按**字符数**算（MySQL 的 `VARCHAR(10)` 在 utf8mb4 下是 10 个字符，
     * 不是 10 字节——按字节算会把中文列误判成超长）。
     *
     * 只在写路径上检查（读路径不检查）：存量数据里哪怕有超长的值，
     * 也必须读得出来——拒绝读会让"当年写进去的数据"突然打不开。
     */
    private fun checkWidth(v: Datum, col: ColumnDef) {
        val t = col.type
        if (t == ColumnType.VARCHAR || t == ColumnType.TEXT) {
            val limit = col.length ?: return
            val text = (v as? Datum.Str)?.v ?: return
            if (text.length > limit) {
                throw SqlException(
                    MySqlErrors.ER_DATA_TOO_LONG, "22001",
                    "Data too long for column '${col.name}' at row 1" +
                        "（列宽 $limit 个字符，实际 ${text.length} 个）",
                )
            }
            return
        }
        // 整数宽度：TINYINT/SMALLINT/INT 有明确范围，BIGINT 是内部表示的上界
        if (!t.isInteger) return
        val n = (v as? Datum.I64)?.v ?: return
        val (lo, hi) = when (t) {
            ColumnType.TINYINT -> -128L to 127L
            ColumnType.SMALLINT -> -32768L to 32767L
            ColumnType.INT -> -2147483648L to 2147483647L
            else -> return
        }
        if (n < lo || n > hi) {
            throw SqlException(
                MySqlErrors.ER_WARN_DATA_OUT_OF_RANGE, "22003",
                "Out of range value for column '${col.name}' at row 1" +
                    "（${t.mysqlName} 的范围是 $lo ~ $hi，实际 $n）",
            )
        }
    }

    /**
     * 把任意值转成列声明的定点数，**按列的标度舍入**。
     *
     * 舍入用 `HALF_UP`（MySQL 的规则：远离零），不是 Kotlin 的 `roundToLong`（银行家舍入）。
     * 标度取列的声明而不是字面量自己的：`DECIMAL(12,2)` 列接受 `1.005` 时应当存 1.01，
     * 而不是带着 3 位小数存进去——那会让同一个列里的值标度不一，
     * 而主键编码（`KeyEncoder`）只按 `unscaled` 比较，标度不一的键顺序与 SQL 顺序会不一致。
     */
    private fun decOf(v: Datum, col: ColumnDef): Datum {
        val scale = col.scale ?: 0
        val unscaled = when (v) {
            is Datum.Dec -> rescale(v.unscaled, v.scale, scale)
            is Datum.I64 -> v.v * Datum.pow10Long(scale)
            is Datum.Bool -> (if (v.v) 1L else 0L) * Datum.pow10Long(scale)
            is Datum.F64 -> fromDouble(v.v, scale)
            is Datum.Str -> {
                val text = v.v.trim()
                val dot = text.indexOf('.')
                val literalScale = if (dot < 0) 0 else text.length - dot - 1
                val digits = text.replace(".", "")
                val unscaledText = digits.toLongOrNull()
                    ?: throw SqlException(
                        MySqlErrors.ER_TRUNCATED_WRONG_VALUE, "22007",
                        "列 '${col.name}' 收到非法定点数 '${v.v}'",
                    )
                if (literalScale == scale) unscaledText
                else if (literalScale < scale) unscaledText * Datum.pow10Long(scale - literalScale)
                else rescale(unscaledText, literalScale, scale)
            }
            else -> throw SqlException(
                MySqlErrors.ER_TRUNCATED_WRONG_VALUE, "22007",
                "列 '${col.name}' 不能接受 ${v::class.simpleName} 类型的值",
            )
        }
        return Datum.Dec(unscaled, scale)
    }

    /** 标度变换（含舍入）。目标标度更大时是精确的，更小时按 HALF_UP 舍入。 */
    private fun rescale(unscaled: Long, fromScale: Int, toScale: Int): Long {
        if (fromScale == toScale) return unscaled
        if (toScale > fromScale) return unscaled * Datum.pow10Long(toScale - fromScale)
        val divisor = Datum.pow10Long(fromScale - toScale)
        val q = unscaled / divisor
        val rem = unscaled % divisor
        // 半值远离零（MySQL 的 ROUND 语义）
        return if (kotlin.math.abs(rem) * 2 >= divisor) q + (if (unscaled < 0) -1 else 1) else q
    }

    /** 双精度 → 定点。走 `BigDecimal`：`12.34.toLong()*100` 会因为二进制误差得到 1233。 */
    private fun fromDouble(d: Double, scale: Int): Long {
        if (d.isNaN() || d.isInfinite()) throw SqlException(
            MySqlErrors.ER_TRUNCATED_WRONG_VALUE, "22007", "无法把 $d 存入定点数列",
        )
        return java.math.BigDecimal.valueOf(d)
            .setScale(scale, java.math.RoundingMode.HALF_UP)
            .movePointRight(scale)
            .longValueExact()
    }

    /**
     * `ENUM` / `SET` / `JSON` 的校验与规范化。
     *
     * 三者的共同点是**值域封闭**，因此必须在写入时拒绝非法值——
     * MySQL 在严格模式下就是这么做的（1265 `Data truncated`）。悄悄收下的话，
     * 之后 `WHERE col = 'a'` 永远查不到那一行，而没有任何一处报错。
     *
     * - `ENUM`：值必须正好是某个成员。
     * - `SET`：逗号分隔的每个成员都必须在表里，**并按声明顺序重新排列**——
     *   MySQL 的 `SET` 显示顺序恒为声明顺序，`'b,a'` 存进去读出来是 `'a,b'`。
     *   不规范化的话，`WHERE s = 'a,b'` 对存进去的 `'b,a'` 会失配。
     * - `JSON`：必须是合法 JSON。用 kotlinx-serialization 的解析器校验——
     *   自己写一个"看起来像 JSON"的判断只会让非法 JSON 悄悄入库。
     */
    private fun coerceClosed(v: Datum, col: ColumnDef): Datum {
        val text = if (v is Datum.Str) v.v else v.asString()
        return when (col.type) {
            ColumnType.ENUM -> {
                if (col.enumValues.isNotEmpty() && text !in col.enumValues) {
                    throw SqlException(
                        MySqlErrors.ER_TRUNCATED_WRONG_VALUE, "01000",
                        "Data truncated for column '${col.name}'：'$text' 不是 " +
                            "ENUM(${col.enumValues.joinToString(",") { "'$it'" }}) 的成员",
                    )
                }
                Datum.Str(text)
            }
            ColumnType.SET -> {
                val parts = if (text.isEmpty()) emptyList()
                else text.split(',').map { it.trim() }.filter { it.isNotEmpty() }
                for (p in parts) {
                    if (col.enumValues.isNotEmpty() && p !in col.enumValues) {
                        throw SqlException(
                            MySqlErrors.ER_TRUNCATED_WRONG_VALUE, "01000",
                            "Data truncated for column '${col.name}'：'$p' 不是 " +
                                "SET(${col.enumValues.joinToString(",") { "'$it'" }}) 的成员",
                        )
                    }
                }
                // 按声明顺序规范化，与 MySQL 的显示顺序一致
                val ordered = if (col.enumValues.isEmpty()) parts.distinct()
                else col.enumValues.filter { it in parts }
                Datum.Str(ordered.joinToString(","))
            }
            ColumnType.JSON -> {
                try {
                    kotlinx.serialization.json.Json.parseToJsonElement(text)
                } catch (e: Exception) {
                    throw SqlException(
                        MySqlErrors.ER_TRUNCATED_WRONG_VALUE, "22032",
                        "Invalid JSON text for column '${col.name}': ${e.message?.take(80)}",
                    )
                }
                Datum.Str(text)
            }
            else -> Datum.Str(text)
        }
    }

    /**
     * 时间字面量 → 毫秒。
     *
     * ⚠️ 时区必须与 [Datum.parseDateTime]（谓词转换、分区边界求值用的那个）一致，
     * 统一为 **UTC**。曾经这里用 `systemDefault` 而分区边界用 UTC，
     * 同一个字符串解出两个不同的时刻——插入的 Ts 与查询条件的 Ts 不相等，
     * 于是 `WHERE dt = '...'` 静默返回空集。
     */
    private fun parseTimestamp(s: String): Long =
        Datum.parseDateTime(s) ?: throw SqlException(MySqlErrors.ER_TRUNCATED_WRONG_VALUE, "22007", "非法的时间格式 '$s'")

    // ── SELECT ────────────────────────────────────────────────────

    /**
     * 系统变量与用户变量。客户端在连接建立与健康检查阶段会大量查询它们，
     * 因此这张表要尽量宽——查不到的返回 NULL 而不是报错。
     */
    private fun varsOf(session: Session): Map<String, Datum> = buildMap {
        // 见 `max_allowed_packet` 那行：报的值必须与读侧真实支持的上限一致

        put("autocommit", Datum.Str(if (session.autocommit) "ON" else "OFF"))
        put("innodb_lock_wait_timeout", Datum.I64(session.lockWaitMillis / 1000))
        put("transaction_isolation", Datum.Str("REPEATABLE-READ"))
        put("tx_isolation", Datum.Str("REPEATABLE-READ"))
        // 如实报告实现细节：本系统是快照隔离，不是 InnoDB 的 next-key lock 实现
        put("yangbase_isolation_detail", Datum.Str("snapshot-isolation"))
        // 客户端请求过、但我们提供不了的隔离级别（没有请求时为 NULL）
        put("yangbase_isolation_degraded", Datum.Str(session.requestedIsolation ?: ""))
        // ⚠️ `max_allowed_packet` 必须与实际能力一致：读侧不做拆包重组之前，
        // 真实上限是 16 MiB，而这个变量报 64 MiB——客户端据此发一条 20 MiB 的
        // INSERT 会被静默截断。现在读侧已经重组（Packets.readPacket），
        // 上限就是这里报的这个值
        
        put("commit_mode", Datum.Str(engine.commitMode.name.lowercase()))
        put("version", Datum.Str(ServerInfo.VERSION))
        put("version_comment", Datum.Str(ServerInfo.VERSION_COMMENT))
        // 如实回报**会话的**字符集，而不是写死 utf8mb4：
        // 客户端 `SET NAMES` 之后拿 `@@character_set_client` 一看还是 utf8mb4，
        // 就会以为设置没生效——而它其实生效了（§11.1.1 陷阱 T1 的同一类问题）
        put("character_set_server", Datum.Str(SessionCharset.DEFAULT.canonical))
        put("character_set_client", Datum.Str(session.charset.canonical))
        put("character_set_connection", Datum.Str(session.charset.canonical))
        put("character_set_results", Datum.Str(session.charset.canonical))
        put("collation_server", Datum.Str(SessionCharset.DEFAULT.collationName))
        put("collation_connection", Datum.Str(session.charset.collationName))
        // ★ `@@collation_database` / `@@character_set_database`：**不能返回 NULL**。
        //
        // 每个库一个默认字符集在 MySQL 里是真实存在的概念；本系统只有一个（建库时
        // 没有 per-database 字符集），所以这里如实回报全局默认值。
        //
        // 为什么这条格外重要：`mysqldump` 连上来之后会读 `select @@collation_database`，
        // 然后**不加 NULL 检查**地把结果拷进一个固定大小的缓冲区
        // （`strncpy(dest, row[0], 30)`）。我们返回 NULL 时，那一句就是
        // `strncpy(dest, NULL, 30)` —— 实测**直接把 mysqldump 打成段错误**
        // （core dump 里 `rdi=<栈地址> rsi=0x0 rdx=0x1e`）。一个 NULL 变量
        // 换来一个客户端崩溃，值得在这里写清楚
        put("collation_database", Datum.Str(SessionCharset.DEFAULT.collationName))
        put("character_set_database", Datum.Str(SessionCharset.DEFAULT.canonical))
        put("max_allowed_packet", Datum.I64(MAX_ALLOWED_PACKET))
        put("lower_case_table_names", Datum.I64(0))
        put("yangbase_protocol_flags", Datum.Str(protocolFlagsOf(session)))
        put("sql_mode", Datum.Str("ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES"))
        put("time_zone", Datum.Str("SYSTEM"))
        put("system_time_zone", Datum.Str("UTC"))
        put("wait_timeout", Datum.I64(28800))
        put("interactive_timeout", Datum.I64(28800))
        put("net_write_timeout", Datum.I64(60))
        put("foreign_key_checks", Datum.I64(1))
        put("unique_checks", Datum.I64(1))
        put("sql_safe_updates", Datum.I64(0))
        // 用户变量用 `@` 前缀存进同一个表：`@a` 与系统变量 `a` 在 MySQL 里是
        // **两个不同的东西**，靠前缀区分才不会互相遮蔽（读取见 Eval 的 VarRef 分支）
        session.userVars.forEach { (k, v) -> put("@$k", v) }
        put("auto_increment_increment", Datum.I64(1))
        put("auto_increment_offset", Datum.I64(1))
        put("init_connect", Datum.Str(""))
        put("license", Datum.Str("Apache-2.0"))
        put("have_ssl", Datum.Str("DISABLED"))
        put("performance_schema", Datum.I64(0))
        put("session_track_gtids", Datum.Str("OFF"))
        put("last_insert_id", Datum.I64(session.lastInsertId))
        put("current_user", Datum.Str("root@%"))
        put("database", session.currentDbName?.let { Datum.Str(it) } ?: Datum.Null)
    }

    /**
     * `SELECT 1` / `SELECT @@version_comment` 这类**无 FROM 的查询**。
     *
     * 它们不是边角情形——Connector/J 在 `createNewIO` 阶段就会发一批，
     * 不支持的话任何 JDBC 客户端都连不上（这一点在端到端测试中被真实地暴露出来）。
     */
    private suspend fun selectConstants(session: Session, stmt: SelectStmt): ResultSet {
        val vars = varsOf(session)
        val ctx = EvalContext(null, null, vars)
        // 无 FROM 在 MySQL 里等价于**一张只有一行的虚拟表**：`WHERE` 决定这一行在不在，
        // 其余子句（聚合 / GROUP BY / HAVING / ORDER BY / LIMIT / OFFSET）与有表查询同义。
        // 实测（MySQL 8.0）：`SELECT 1 WHERE 0` → 0 行；`SELECT COUNT(*)` → 1 行 1 值；
        // `SELECT COUNT(*) WHERE 0` → 1 行 0 值；`SELECT 1 LIMIT 0` → 0 行。
        //
        // 所以这里**不再自己拼一行**，而是把这一行交给与有表查询共用的
        // [finishSelect]——那份实现已经正确处理好 LIMIT / ORDER BY / 聚合 / HAVING。
        // 自己拼一行的版本曾经把 `LIMIT`、`WHERE`、`GROUP BY`、`HAVING` **静默忽略**，
        // 而"静默忽略子句"正是本项目最反对的一类缺陷（结果看起来完全正常）。
        val alive = stmt.where == null || Eval.truthy(Eval.eval(stmt.where, ctx))
        return finishSelect(
            session, stmt, table = NO_TABLE,
            scanRows = if (alive) listOf(emptyList()) else emptyList(),
            vars = vars,
        )
    }

    /**
     * 无 FROM 的常量查询用的"空表"：**零列**。
     *
     * 用它把常量查询送进与有表查询**共用的那条管道**，而不是另写一份子句处理——
     * 另写的那份曾经把 `LIMIT` / `WHERE` / `GROUP BY` / `HAVING` 静默忽略，
     * 而"静默忽略子句"会给出一个看起来完全正常的结果。
     * 零列意味着任何列名都解析不出来（`SELECT x` 无 FROM 本来就该报未知列）。
     */
    private val NO_TABLE = TableSchema(
        tableId = -1, databaseId = -1, name = "", version = 0,
        columns = emptyList(), primaryKey = emptyList(),
    )

    /**
     * 解析表名。支持 `db.table` 形式的限定名，以及 `USE` 选中的当前库。
     *
     * 限定名里的库名优先于当前库——这与 MySQL 一致，也是各类客户端
     * 做 introspection 时的常见写法（`SELECT ... FROM mydb.mytable`）。
     */
    private fun resolveTable(session: Session, rawName: String): TableSchema {
        // 当前库是系统库时，裸表名要先补成限定名才能被认出来（见 [qualifiedUnderCurrent]）
        val name = qualifiedUnderCurrent(session, rawName)
        val dot = name.indexOf('.')
        if (dot > 0 && InformationSchema.isSystemSchemaName(name.substring(0, dot))) {
            // 系统库是内存里物化的**只读**视图，没有主键、没有分片。写它必须报错，
            // 而且要说清是"这里不能写"，不是"表不存在"——后者会让人去找建表语句
            throw SqlException(
                MySqlErrors.ER_NOT_SUPPORTED_YET, "42000",
                "'${name.substring(0, dot)}' 是只读的系统库（内存里物化，没有分片），不支持写入或改结构",
            )
        }
        if (dot < 0) {
            val db = session.requireDb()
            return engine.catalog.requireTable(db.databaseId, name)
        }
        val schema = name.substring(0, dot)
        val table = name.substring(dot + 1)
        val db = engine.catalog.requireDatabase(schema)
        return engine.catalog.requireTable(db.databaseId, table)
    }

    /**
     * 本语句要用的读时间戳。`AS OF` 优先于会话快照。
     *
     * ⚠️ 时间旅行必须先与 GC 水位比对：早于水位的历史已经被回收，
     * 那部分版本链是断的。**必须报错，不能返回残缺结果**——
     * 一个「看起来正常」的残缺结果会让使用者以为那个时刻的数据就是那样。
     */
    private suspend fun readTsFor(session: Session, stmt: SelectStmt): ReadTs {
        val asOf = stmt.asOf ?: return session.readTs()
        val ts = com.cloudea.yangbase.txn.TimeTravel.parse(asOf.kind, asOf.text)
        val safe = gc.safeReadTs()
        com.cloudea.yangbase.txn.TimeTravel.requireAboveWatermark(ts, safe)
        return ReadTs(ts)
    }

    /**
     * `UNION` / `UNION ALL`。
     *
     * 两侧各自执行，然后拼接：`UNION ALL` 原样拼，`UNION` 去重。
     *
     * **列数必须一致**（MySQL 的 1222），列名取左侧那一份——与 MySQL 一致，
     * 也是唯一能让 `rs.getString(1)` 有意义的选择。
     *
     * 去重按**整行的值**做：用 `List<Datum>` 做键，因此 `1` 与 `'1'`
     * 是两行（值类型不同）。MySQL 会按列类型做隐式转换后再比，本系统不做隐式转换
     * （见 §11.3 的限制清单）——这是差异，但它偏"少去重"而不偏"少返回行"，
     * 两种都不报错，宁可偏保守。
     */
    private suspend fun union(session: Session, stmt: UnionStmt): ResultSet {
        val left = resultOf(session, stmt.left)
        val right = resultOf(session, stmt.right)
        if (left.columns.size != right.columns.size) {
            throw SqlException(
                MySqlErrors.ER_WRONG_NUMBER_OF_COLUMNS_IN_SELECT, "21000",
                "The used SELECT statements have a different number of columns" +
                    "（左 ${left.columns.size} 列，右 ${right.columns.size} 列）",
            )
        }
        // ★ 右侧的值按**左侧的类型**归一之后再合并。
        //
        // MySQL 的 UNION 会先做"结果类型聚合"：`SELECT 10 UNION SELECT '10'`
        // 的结果类型取自第一支（整数），'10' 被转成 10，于是**只有一行**。
        // 少了这一步，去重是按"值自身的类型"做的（`10` 与 `'10'` 的 key 不同），
        // 于是返回**两行看起来重复的值**——去重"声称做了、实际漏了"，
        // 而且与本系统自己的 `WHERE '10' = 10` 判决相反。
        val rightRows = right.rows.map { row ->
            row.mapIndexed { i, v ->
                val l = left.rows.firstOrNull()?.getOrNull(i) ?: return@mapIndexed v
                coerceUnionValue(v, l)
            }
        }
        var rows = if (stmt.all) left.rows + rightRows else distinctRows(left.rows + rightRows)

        // 并集级的 ORDER BY / LIMIT 作用于**整个并集**（这也是它们必须挂在
        // UnionStmt 上、而不是某个分支上的原因）。
        //
        // 排序要有一张"表"才能解析列名，而并集没有表——这里合成一张：
        // 列名取左侧分支的输出名，类型一律按 VARCHAR（比较走 `Datum.compareTo`，
        // 它认的是值自身的类型，与声明类型无关，所以排序结果是对的）
        if (stmt.orderBy.isNotEmpty() || stmt.limit != null || stmt.offset != null) {
            val synth = TableSchema(
                tableId = 0, databaseId = 0, name = "(union)", version = 0,
                columns = left.columns.map { ColumnDef(it, ColumnType.VARCHAR) },
                primaryKey = emptyList(),
            )
            if (stmt.orderBy.isNotEmpty()) {
                val outputs = left.columns.map { SelectItem.Field(ColumnRef(it), null) as SelectItem }
                val comparators = resolveOrdinals(stmt.orderBy, outputs, synth)
                    .map { oi ->
                        Triple((oi.expr as? ColumnRef)?.let { synth.indexOfOrNull(it.name) }, oi.ascending, oi.expr)
                    }
                rows = rows.sortedWith { a, b -> compareRows(a, b, comparators, synth, emptyMap(), null) }
            }
            val offset = stmt.offset ?: 0
            if (offset > 0 || stmt.limit != null) {
                val end = if (stmt.limit != null) minOf(offset + stmt.limit, rows.size) else rows.size
                rows = if (offset >= rows.size) emptyList() else rows.subList(offset, end)
            }
        }
        return ResultSet(left.columns, rows)
    }

    private suspend fun resultOf(session: Session, stmt: Statement): ResultSet = when (stmt) {
        is SelectStmt -> select(session, stmt)
        // 嵌套的 UNION（`a UNION b UNION c`）已经在解析阶段折成左结合的树
        is UnionStmt -> union(session, stmt)
        else -> throw SqlException(
            MySqlErrors.ER_PARSE_ERROR, "42000", "UNION 的两侧必须是 SELECT",
        )
    }

    /**
     * 当前库是**系统库**时，把裸表名补成限定名（`tables` → `information_schema.tables`）。
     *
     * 存在的原因是 `USE information_schema; SELECT * FROM tables` 是**真实 MySQL 的
     * 常规用法**（也是不少客户端的做法）：那些库名在 catalog 里根本不存在，
     * 它们是内存里物化的，所以必须先补全才能被认出来。
     */
    private fun qualifiedUnderCurrent(session: Session, raw: String): String {
        if (raw.contains('.')) return raw
        val cur = session.currentDbName ?: return raw
        return if (InformationSchema.isSystemSchemaName(cur)) "$cur.$raw" else raw
    }

    private suspend fun select(session: Session, stmt: SelectStmt): ResultSet {
        if (stmt.table == null) return selectConstants(session, stmt)
        // 系统库里的裸表名先补全（见 qualifiedUnderCurrent）：`SelectStmt` 是 data class，
        // 直接 copy 一份改表名，比在下面每个分支里各判一次干净
        val rawTable = stmt.table!!
        if (!rawTable.contains('.')) {
            val qualified = qualifiedUnderCurrent(session, rawTable)
            if (qualified != rawTable) return select(session, stmt.copy(table = qualified))
        }

        // information_schema / performance_schema 等系统库不是真实的表：
        // 它们由内存中的 catalog 直接物化。没有这一层，DataGrip / Navicat
        // 这类客户端连不上——它们连接后第一件事就是查 information_schema。
        InformationSchema.tableFor(stmt.table!!)?.let { vt ->
            return finishSelect(session, stmt, vt.schema, vt.rows(session, engine), varsOf(session))
        }

        val table = resolveTable(session, stmt.table!!)
        val readTs = readTsFor(session, stmt)
        val vars = varsOf(session)

        // ★ 视图：先把视图内层查询跑出来，再在这批行上执行外层子句。
        // 见 [selectView] 的说明（为什么不把视图改写成连接）。
        if (table.isView) {
            if (stmt.joins.isNotEmpty()) {
                throw SqlException(
                    MySqlErrors.ER_NOT_SUPPORTED_YET, "42000",
                    "暂不支持把视图放进 JOIN（视图 '${table.name}'）。" +
                        "视图是「已存查询」，参与连接需要把它改写成派生表——" +
                        "本系统暂时是把视图结果取出来再套外层子句，先只支持单表位置",
                )
            }
            return selectView(session, stmt, table, readTs, vars)
        }

        // 限定名必须指向**这张表**（或它的别名）。
        // 少了这一步，`SELECT x.id FROM t` 会静默按 `id` 解析——而更危险的是
        // **相关子查询**：`EXISTS (SELECT 1 FROM orders o WHERE o.user_id = u.id)` 里的
        // `u.id` 会退化成内层的 `id`（orders 自己也有 id！），于是子查询照常执行、
        // 返回一批**错误**的行，而使用者完全看不出来（那本该是一个"相关子查询不支持"的提示）
        if (stmt.joins.isEmpty()) {
            val allowed = setOf(
                table.name.lowercase(),
                (stmt.tableAlias ?: table.name).lowercase(),
                stmt.table!!.lowercase(),
            )
            validateQualifiers(stmt, allowed)
        }

        // ★ 连接查询走独立的一层（见 [JoinEngine]）：它把多张表拼成一张合成 schema 的行，
        // 之后的分组/排序/投影与单表查询共用同一套代码。
        // 放在这里而不是更靠后，是因为连接**换了"行是什么"**——
        // 索引选择、分区裁剪、COUNT 的元数据路径全都建立在"单表"这个前提上，
        // 它们对连接查询一概不成立，混在一起会给出错误的结果（比如拿单表的行数当连接的计数）。
        if (stmt.joins.isNotEmpty() && stmt.lockMode != null) {
            throw SqlException(
                MySqlErrors.ER_NOT_SUPPORTED_YET, "42000",
                "暂不支持在 JOIN 查询里使用 SELECT ... FOR UPDATE。" +
                    "MySQL 会锁住查询里读到的**每一张表**的行，而本系统的行锁按基表主键加锁，" +
                    "对多表连接的语义还没有定义——给一个「只锁了一半」的实现会让使用者" +
                    "误以为整条查询是安全的",
            )
        }
        if (stmt.joins.isNotEmpty()) {
            // 标量位置的相关子查询在连接查询里没有落点：它的实现要靠"外层表自己的列"
            // 按关联键建表（见 [CorrelatedScalars]），而连接之后"一行"是合成行。
            // 这里明确报错——不拦的话它会走到求值期，报一句"请报告为缺陷"的内部错
            rejectScalarSubqueryInJoin(stmt)
            return selectJoined(session, stmt, table, readTs, vars)
        }

        // ★ 相关子查询改写成半/反连接（见 [CorrelatedSubqueries]）。
        // 必须在**任何取数动作之前**做：改写的产物是一条连接，而连接换掉了"行是什么"，
        // 索引选择、分区裁剪、COUNT 的元数据路径都建立在单表前提上。
        val correlated = CorrelatedSubqueries.extract(
            stmt.where,
            (stmt.tableAlias ?: table.name).lowercase(),
        ) { name -> runCatching { resolveTable(session, name) }.getOrNull() }
        // ⚠️ `rejectNonEqui` 要在 `isEmpty` 判断**之前**跑：一条**非等值**关联的
        // 相关子查询根本抽不出 Spec（没有等值可认），于是它既不会进改写路径、
        // 也不会被这里拦住——而是掉进求值期报一句含糊的"子查询没能被提前求值"。
        // 使用者写的是完全合法的 SQL，报错必须说清楚"为什么不支持"
        CorrelatedSubqueries.rejectNonEqui(
            stmt.where, (stmt.tableAlias ?: table.name).lowercase(),
        ) { name -> runCatching { resolveTable(session, name) }.getOrNull() }
        // ★ 标量位置的相关子查询（`SELECT (SELECT … WHERE i.k = o.k) FROM o`）：
        // 内层按关联键**扫一次**建表，外层每行按键查表（见 [CorrelatedScalars]）。
        // 与谓词位置的一起走同一条路径——两者都要"先取外层行、再补上算不出来的值"
        val innerResolver = { name: String -> runCatching { resolveTable(session, name) }.getOrNull() }
        val outerNames = setOf(
            table.name.lowercase(),
            (stmt.tableAlias ?: table.name).lowercase(),
            stmt.table!!.lowercase(),
        )
        val scalars = CorrelatedScalars.extract(stmt, outerNames, innerResolver)
        if (correlated.isNotEmpty() || scalars.isNotEmpty()) {
            return selectCorrelated(session, stmt, table, readTs, vars, correlated, scalars)
        }

        // ★ ONLY_FULL_GROUP_BY 等分组语义的检查。放在**取数之前**：
        // 它只看 SQL 文本与表结构，与数据无关，早报错就少一次白跑的扫描。
        //
        // 这里必须真做检查：`sql_mode` 如实回报了 `ONLY_FULL_GROUP_BY`，
        // 报了却不做等于用"看起来更兼容"换一个**每组随便挑一行**的不确定结果
        Aggregates.validate(table, stmt, aliasMapOf(stmt))
        // ★ 窗口函数不能出现在 WHERE / HAVING / GROUP BY / ON 里：它在 HAVING 之后才求值，
        // 写在那些位置根本没有值可取。同样在取数之前报出来
        Window.rejectMisplaced(stmt)

        // ★ R6：COUNT(*) 优先走元数据路径，不做代价比较（设计文档 §8.9.6）
        //
        // ⚠️ 只有"整张表一个数"才成立：`GROUP BY` / `HAVING` / `DISTINCT` 下
        // 要按组各数一次，元数据路径给的是**全表一个数**——查询照常返回一行，
        // 数字看起来也很正常（比如 3），而答案是错的。这类错误最难被发现：
        // 它只在"元数据路径可用"时出现（事务内有未提交写入时又会回退到扫描，
        // 于是同一句 SQL 时而对时而错）
        val onlyCount = stmt.projections.size == 1 &&
            (stmt.projections[0] as? SelectItem.Field)?.expr is CountCall &&
            stmt.groupBy.isEmpty() && stmt.having == null && !stmt.distinct

        if (onlyCount) {
            val countExpr = (stmt.projections[0] as SelectItem.Field).expr as CountCall
            val rs = tryCount(
                session, table, stmt.where, countExpr, readTs, vars,
                limit = stmt.limit, offset = stmt.offset,
            )
            if (rs != null) return rs
        }

        // ★ 全文检索优先：它是一个**访问路径**（先用倒排索引取候选主键再回表），
        // 不是一个逐行谓词——BM25 打分依赖全语料统计量，逐行判定在语义上做不到。
        val match = fullText.findMatch(stmt.where)
        if (match != null) {
            // LIMIT 是 top-k 的自然上界；没有 LIMIT 时给一个保守的默认值，
            // 否则「取全部命中文档」会退化成全表回表
            val topK = (stmt.limit ?: DEFAULT_FULLTEXT_TOPK) + (stmt.offset ?: 0)
            val hits = fullText.search(table, match, topK, readTs)
                ?: throw SqlException(
                    MySqlErrors.ER_NOT_SUPPORTED_YET, "42000",
                    "全文查询需要先在 '${table.name}.${match.columns.firstOrNull()}' 上建索引：" +
                        "CREATE FULLTEXT INDEX ... ON ${table.name}(${match.columns.firstOrNull()})",
                )
            // 摘掉全文谓词再过滤——留着它会在逐行求值时抛错
            val rest = fullText.removeMatch(stmt.where, match)
            return finishSelect(session, stmt, table, hits, vars, onlyCount, rest)
        }

        // ★ 访问路径选择（设计文档 §10.2）：有统计时按「预计对象存储请求数」比价，
        // 没有统计时退回硬编码优先级链（与优化器引入之前**逐字一致**）。
        val chosen = choosePlan(
            session, table, stmt.where, readTs, vars,
            hasOrderBy = stmt.orderBy.isNotEmpty(),
            hasGrouping = stmt.groupBy.isNotEmpty() || stmt.having != null ||
                Aggregates.collect(stmt).isNotEmpty(),
        )
        val scanRows = executePlan(session, table, chosen, readTs, vars)

        return finishSelect(session, stmt, table, scanRows, vars, onlyCount)
    }

    /**
     * 查视图。
     *
     * ## 为什么不把它改写成连接 / 派生表
     *
     * 视图是「已存查询」。正统做法是把它展开进外层的 FROM（派生表），
     * 那样外层谓词可以下推进视图、优化器也能做代价选择。本系统没有派生表这一层，
     * 所以这里选了另一条**正确但简单**的路：把视图的 SELECT 先执行完，
     * 拿到一批行，再在这批行上套外层的 `WHERE` / 分组 / 排序 / 投影。
     *
     * 代价要说清楚：**外层谓词下推不进视图**——`SELECT * FROM v WHERE id = 1`
     * 会先把视图整个算出来、再过滤。在对象存储上那不是零成本（视图内层可能是一次全表扫描）。
     * 之所以仍然这样做：它不会给出错误结果，而"展开成派生表"是一个独立的、
     * 需要动查询规划器的工作；先有正确的视图，再有快的视图。
     *
     * 视图的行用一张**合成 schema**（列名 = 视图的输出列）承载，
     * 于是 `Eval` / 排序 / 分组全都照常工作，不需要为视图另写一套求值。
     */
    private suspend fun selectView(
        session: Session,
        stmt: SelectStmt,
        view: TableSchema,
        readTs: ReadTs,
        vars: Map<String, Datum>,
    ): ResultSet {
        val inner = Parser.parse(view.viewSql!!)
        val rs = when (inner) {
            is SelectStmt -> select(session, inner)
            is UnionStmt -> union(session, inner)
            else -> throw SqlException(
                MySqlErrors.ER_PARSE_ERROR, "42000", "视图 '${view.name}' 的定义不是一条 SELECT",
            )
        }
        // 合成 schema 描述视图的**输出行**：列名取自视图定义（建视图时推导并落盘），
        // 类型一律按 VARCHAR——比较走的是值自身的类型，声明类型只影响 introspection
        val synth = TableSchema(
            tableId = view.tableId,
            databaseId = view.databaseId,
            name = view.name,
            version = view.version,
            // ⚠️ 列类型必须按**实际值**推断，不能一律 VARCHAR。
            // 声明成 VARCHAR 会让 `Eval` 的字面量转换路径把 `WHERE id = 10` 里的
            // `10` 转成字符串 `'10'`，然后 `I64(10) == Str("10")` 不相等——
            // 查询**静默返回空集**（不报错，看起来就是"视图里没有这行"）。
            // 实测踩到过。
            columns = rs.columns.mapIndexed { i, name -> ColumnDef(name, inferType(rs.rows, i)) },
            primaryKey = emptyList(),
        )
        return finishSelect(session, stmt, synth, rs.rows, vars)
    }

    /**
     * `CREATE [OR REPLACE] VIEW name AS SELECT ...`。
     *
     * ## 视图的输出列怎么来
     *
     * 把内层的 SELECT 跑一遍**结构**（不取数）：解析它、把它 `*` 展开成实际列名，
     * 得到输出列清单。这份清单写进 catalog，于是 `information_schema.COLUMNS`、
     * `DESC v`、客户端的 introspection 都能正常工作——**不写它，图形化客户端
     * 打开视图会看到零列**。
     *
     * ## 建视图时**不**执行内层查询
     *
     * MySQL 也是这么做的（视图是"已存查询"，不是物化结果）。这里更不能执行：
     * 那会在建视图时产生一次全表扫描，而代价与视图本身的正确性无关。
     * 语法错误在这里就会报出来（解析一次），这是"早失败"该有的样子。
     */
    private suspend fun createView(session: Session, stmt: CreateViewStmt): Int {
        val db = session.requireDb()
        val parsed = Parser.parse(stmt.sql)
        val columns = viewColumns(session, parsed)
        val existing = engine.catalog.findTable(db.databaseId, stmt.name)
        if (existing != null) {
            if (!stmt.orReplace) {
                throw SqlException(
                    MySqlErrors.ER_TABLE_EXISTS_ERROR, "42S01", "Table '${stmt.name}' already exists",
                )
            }
            if (!existing.isView) {
                throw SqlException(
                    MySqlErrors.ER_TABLE_EXISTS_ERROR, "42S01",
                    "'${stmt.name}' 是一张**表**，不能用 OR REPLACE VIEW 覆盖它",
                )
            }
        }
        val schema = TableSchema(
            tableId = existing?.tableId ?: 0,
            databaseId = db.databaseId,
            name = stmt.name,
            version = 0,
            columns = columns,
            primaryKey = emptyList(),
            viewSql = stmt.sql,
        )
        if (existing != null) {
            engine.catalog.replaceTable(schema)
        } else {
            engine.catalog.createTable(
                db.databaseId, stmt.name, columns, emptyList(),
                viewSql = stmt.sql,
            )
        }
        return 0
    }

    /**
     * 按**实际值**推断一列的类型。
     *
     * 取第一个非 NULL 值：视图/并集的输出列没有声明类型可用（它们来自任意表达式），
     * 而"一律当成 VARCHAR"会让字面量比较走错路（见 [selectView] 的说明）。
     * 全列都是 NULL 时退回 VARCHAR——那种列怎么比较都不影响结果。
     */
    private fun inferType(rows: List<List<Datum>>, col: Int): ColumnType {
        for (r in rows) {
            when (val v = r.getOrNull(col)) {
                null, is Datum.Null -> continue
                is Datum.I64 -> return ColumnType.BIGINT
                is Datum.Bool -> return ColumnType.BOOLEAN
                is Datum.F64 -> return ColumnType.DOUBLE
                is Datum.Ts -> return ColumnType.DATETIME
                is Datum.Dec -> return ColumnType.DECIMAL
                is Datum.Bin -> return ColumnType.BLOB
                is Datum.Str -> return ColumnType.VARCHAR
            }
        }
        return ColumnType.VARCHAR
    }

    /**
     * 写路径的守卫：**视图不可写**。
     *
     * 不拦的话，`INSERT INTO v` / `UPDATE v` / `DELETE FROM v` 会**照常成功**，
     * 把数据写进视图的"表分片"里——而视图的行是由内层查询现算的，
     * 那些写入永远不会出现在任何查询结果里，也永远不会报错。
     * 使用者以为写成功了，实际数据进了一个黑洞。
     *
     * 错误码用 1347（`ER_WRONG_OBJECT`），与 MySQL 的 `'v' is not BASE TABLE` 对齐。
     */
    private fun requireBaseTable(table: TableSchema) {
        if (table.isView) {
            throw SqlException(
                MySqlErrors.ER_WRONG_OBJECT, "HY000",
                "'${table.name}' is not BASE TABLE（它是视图）。" +
                    "本系统的视图是**只读**的：视图的行由内层查询现算，写进去的数据不会出现在任何查询里，" +
                    "所以这里明确报错而不是让它悄悄成功",
            )
        }
    }

    /** 投影若是基础表上的普通列引用，取它的类型；否则 VARCHAR。 */
    private fun baseColumnTypeOrVarchar(session: Session, sel: SelectStmt, e: Expr): ColumnType {
        val ref = e as? ColumnRef ?: return ColumnType.VARCHAR
        val base = sel.table ?: return ColumnType.VARCHAR
        val t = runCatching { resolveTable(session, base) }.getOrNull() ?: return ColumnType.VARCHAR
        val idx = ref.table?.let { t.indexOfOrNull(ref.name) } ?: t.indexOfOrNull(ref.name)
        return idx?.let { t.columns[it].type } ?: ColumnType.VARCHAR
    }

    /** 视图的输出列：解析内层 SELECT 的投影，必要时展开 `*`。 */
    private fun viewColumns(session: Session, parsed: Statement): List<ColumnDef> {
        val sel = when (parsed) {
            is SelectStmt -> parsed
            is UnionStmt -> (parsed.left as? SelectStmt)
                ?: throw SqlException(
                    MySqlErrors.ER_NOT_SUPPORTED_YET, "42000",
                    "视图的定义以 UNION 开头时暂不支持（列名推导需要左侧分支）",
                )
            else -> throw SqlException(
                MySqlErrors.ER_PARSE_ERROR, "42000", "视图的定义必须是一条 SELECT",
            )
        }
        val out = ArrayList<ColumnDef>()
        for ((i, item) in sel.projections.withIndex()) {
            when (item) {
                is SelectItem.Field -> out.add(
                    // 投影是**普通列引用**时沿用基础列的类型（`SELECT id, name FROM users`
                    // 是最常见的视图形态），这样 `information_schema.COLUMNS` 里的
                    // DATA_TYPE 是诚实的；表达式列只能按 VARCHAR 记
                    ColumnDef(
                        item.alias ?: exprLabel(item.expr),
                        baseColumnTypeOrVarchar(session, sel, item.expr),
                        nullable = true,
                    )
                )
                is SelectItem.All -> {
                    val base = sel.table
                        ?: throw SqlException(
                            MySqlErrors.ER_PARSE_ERROR, "42000", "`SELECT *` 需要 FROM",
                        )
                    val t = resolveTable(session, base)
                    for (c in t.columns) out.add(c.copy(name = c.name))
                }
            }
        }
        if (out.isEmpty()) {
            throw SqlException(MySqlErrors.ER_PARSE_ERROR, "42000", "视图至少要有输出列")
        }
        return out
    }

    /**
     * 带相关子查询的查询：把每个子查询谓词当成一次**半/反连接**来过滤外层行。
     *
     * 逐个谓词串行处理（而不是一次连接里做多级），是因为每一步的合成 schema 都是
     * 「外层 + 那一个内表」——半连接只输出外层行，所以下一步的行宽与它自己的
     * schema 依然对齐。一次做多级的话，第二步的 ON 会去引用"已经被半连接丢掉的
     * 中间列"，布局对不上。
     *
     * 每个内表**只扫描一次**（不是每行一次），这是这套改写存在的全部意义。
     */
    private suspend fun selectCorrelated(
        session: Session,
        stmt: SelectStmt,
        table: TableSchema,
        readTs: ReadTs,
        vars: Map<String, Datum>,
        specs: List<CorrelatedSubqueries.Spec>,
        scalars: List<CorrelatedScalars.Spec>,
    ): ResultSet {
        val outerQualifier = (stmt.tableAlias ?: table.name).lowercase()
        // 外层行。**不做分区裁剪与索引选择**：下面那两条路径都建立在"谓词只涉及本表"
        // 的前提上，而这里的查询里还有引用别的表的子查询。走全表扫描是保守但正确的
        var rows = engine.scanAll(table, readTs)
        for (spec in specs) {
            val clause = CorrelatedSubqueries.toJoinClause(spec)
            val joined = JoinEngine.schemaOf(table, outerQualifier, listOf(clause to spec.innerTable))
            val inner = engine.scanAll(spec.innerTable, readTs)
            rows = JoinEngine.join(
                joined, rows, listOf(inner), listOf(clause.type), listOf(clause.on), vars,
                listOf(CorrelatedSubqueries.antiSemanticsOf(spec.node)),
            )
        }
        // 标量相关子查询：内层各扫一次，得到"每外层行 → {子查询键: 值}"。
        // 这一步必须在**连接改写之后**：半/反连接会筛掉一部分外层行，
        // 那些行上的子查询不必算（算了也不影响结果，只是白花钱）
        val precomputed = if (scalars.isEmpty()) null
        else CorrelatedScalars.evaluate(scalars, rows, table, vars) { inner -> engine.scanAll(inner, readTs) }
        // 谓词已经变成连接了，交给下层过滤的 WHERE 必须把它们摘掉——
        // 留着的话逐行求值会撞上"子查询没能被提前求值"的内部错误
        return finishSelect(
            session, stmt, table, rows, vars,
            effectiveWhere = CorrelatedSubqueries.removeSpecs(stmt.where, specs),
            precomputed = precomputed,
        )
    }

    /**
     * 校验表达式里所有**限定列引用**的表名。
     *
     * 只认 [allowed] 里的名字（表名、别名），其余报 1053——与 MySQL 一致。
     * 这是"相关子查询"能被识别的关键：子查询里引用了外层别名时，
     * 内层这张表的 allowed 集合里没有那个名字，于是它会被明确报出来，
     * 而不是退化成同名的内层列（见 [Subqueries] 的说明）。
     */
    private fun validateQualifiers(stmt: SelectStmt, allowed: Set<String>) {
        fun walk(e: Expr?) {
            if (e == null) return
            if (e is ColumnRef && e.table != null && e.table.lowercase() !in allowed) {
                throw SqlException(
                    MySqlErrors.ER_BAD_TABLE_ERROR, "42S02",
                    "Unknown table '${e.table}'（在 ${allowed.joinToString("/")} 上执行）。" +
                        "如果这是一个**相关子查询**，本系统暂不支持——相关子查询的结果逐行不同，" +
                        "无法提前算一次，可以改写成 JOIN",
                )
            }
            childrenOf(e).forEach { walk(it) }
        }
        stmt.projections.forEach { if (it is SelectItem.Field) walk(it.expr) }
        walk(stmt.where)
        stmt.groupBy.forEach { walk(it) }
        walk(stmt.having)
        stmt.orderBy.forEach { walk(it.expr) }
    }

    /**
     * 连接查询的执行。
     *
     * ## 取数策略：两边都全量取，连接在内存里做
     *
     * 这是**刻意的第一版**，不是遗漏：把一侧的谓词下推、或按连接键做索引探查，
     * 都需要先知道"哪一侧是驱动表、连接键能不能用上索引"，那是优化器的活。
     * 现在先保证**结果对**，并在 `EXPLAIN` 里如实说"两侧都是全表扫描"——
     * 说清楚比假装优化过强。
     *
     * ## `WHERE` 在连接之后
     *
     * 顺序是 `ON` → 连接 → `WHERE`。`ON` 决定"哪一对算匹配"、`WHERE` 决定
     * "匹配完的行要不要"，两者混淆会让外连接退化成内连接（结果少行且不报错）。
     */
    private suspend fun selectJoined(
        session: Session,
        stmt: SelectStmt,
        primary: TableSchema,
        readTs: ReadTs,
        vars: Map<String, Datum>,
    ): ResultSet {
        val resolved = stmt.joins.map { j -> j to resolveTable(session, j.table) }
        val joined = JoinEngine.schemaOf(primary, stmt.tableAlias ?: primary.name, resolved)
        JoinEngine.validateRefs(joined, stmt)

        // 每个来源各自全量扫描。用各自的 schema 扫，行再按来源顺序拼起来
        val leftRows = engine.scanAll(primary, readTs)
        val rightRows = resolved.map { (_, t) -> engine.scanAll(t, readTs) }

        val merged = JoinEngine.join(
            joined, leftRows, rightRows,
            resolved.map { it.first.type },
            resolved.map { it.first.on },
            vars,
        )

        // 拼出来的行只有"真实列"，而合成 schema 里还带着裸名列的映射，
        // 所以这里要按真实宽度裁一下，否则投影会读到越界（→ 静默 NULL）
        val realWidth = joined.realWidth
        val rows = merged.map { if (it.size == realWidth) it else it.subList(0, realWidth) }

        val rs = finishSelect(session, stmt, joined.schema, rows, vars)
        // `SELECT *` 的输出名要去掉限定前缀（内部表示不该泄漏给客户端）
        return if (stmt.projections.size == 1 && stmt.projections[0] is SelectItem.All) {
            ResultSet(JoinEngine.outputNames(joined), rs.rows)
        } else {
            rs
        }
    }

    // ── 访问路径：选择与执行（设计文档 §10.2）──────────────────────

    /** 一次 SELECT 的路径决策结果。规划与执行共用它，因此两者不可能不一致。 */
    private class ChosenPlan(
        val plan: PathPlan,
        /** 主键键区间（CONST/RANGE 用）。 */
        val keyRange: com.cloudea.yangbase.sql.KeyRange? = null,
        /** 命中的二级索引候选（REF 用）。 */
        val candidate: IndexCandidate? = null,
        /** 过滤时使用的完整 WHERE。 */
        val where: Expr? = null,
        /** 规划输入（EXPLAIN 要用它渲染 `Extra` 的收尾部分）。 */
        val input: PlannerInput? = null,
    )

    /**
     * 选路。**全程零对象存储请求**（原则 P4）：统计来自内存缓存
     * （[com.cloudea.yangbase.txn.TableStatsStore.load] 命中后 0 次请求），
     * 约束来自纯语法分析。唯一可能发请求的是首次加载某表的统计对象（1 次 GET，
     * 之后进负缓存或正缓存）。
     *
     * `EXPLAIN` 与执行都走这里——同一条决策函数保证「EXPLAIN 说的」就是
     * 「执行做的」。分成两份实现迟早会失同步，而那时 EXPLAIN 就在说谎。
     */
    private suspend fun choosePlan(
        session: Session,
        table: TableSchema,
        where: Expr?,
        readTs: ReadTs,
        vars: Map<String, Datum>,
        /** `COUNT(*)` 的元数据路径是否可用，以及它给出的精确行数（`EXPLAIN` 用）。 */
        metadataCount: MetadataCount? = null,
        /**
         * 这条语句有没有 `ORDER BY`（`EXPLAIN` 的 `Using filesort` 用）。
         *
         * 由调用方传进来而不是在这里去看 `stmt`：`choosePlan` 是**规划与执行共用**
         * 的那个函数，签名里没有 stmt，硬塞一个进去会让它同时依赖两种调用场景。
         */
        hasOrderBy: Boolean = false,
        /** 这条查询是否有分组/聚合（`EXPLAIN` 的 `Using temporary` 用）。 */
        hasGrouping: Boolean = false,
    ): ChosenPlan {
        val stats = statsStore.load(table)
        val keyRange = Eval.extractKeyRange(where, table)

        // ⚠️ `KeyRange.isUsable` 只认「有前缀或全列等值」，因此单列主键的
        // `WHERE id > 5` 会被判为不可用而退化成全表扫描——在对象存储上那是
        // 1 次 GET 与上百次 GET 的差别（R1 最想消除的那一类开销）。
        // 这里把「只有上下界」也算作可用（键区间本身即一段连续区间）。
        val rangeUsable = keyRange != null &&
            (keyRange.isUsable || keyRange.low != null || keyRange.high != null)

        val candidates = CostModel.indexCandidates(table, where, vars, stats)
        val pruned = prunedShards(table, where)
        val rowCount = stats?.rowCount ?: (engine.peekRowCount(table) ?: TableStats.FALLBACK_ROWS_PER_BLOCK.toLong())

        val input = PlannerInput(
            table = table,
            stats = stats,
            rowCount = rowCount,
            shards = (pruned ?: engine.allShards(table)).size.coerceAtLeast(1),
            totalShards = engine.allShards(table).size.coerceAtLeast(1),
            where = where,
            constPk = keyRange?.exactPk != null,
            rangeSelectivity = if (rangeUsable) CostModel.keyRangeSelectivity(keyRange!!, table, stats, rowCount) else null,
            rangeColumns = keyRange?.let { r -> table.primaryKey.take(maxOf(1, r.prefix.size + 1)) } ?: emptyList(),
            indexCandidates = candidates,
            metadataCountRows = metadataCount?.rows,
            metadataCountBlocked = metadataCount?.blockedReason,
            // MATCH(...) AGAINST(...) 不是逐行谓词，而是一条**取候选集的访问路径**：
            // 不给它一个独立的 type，EXPLAIN 就无法区分「全文路径」与「扫描 + 事后过滤」
            fullText = fullText.findMatch(where) != null,
            // 执行器目前**总是**把结果集捞进内存再排序（见 `finishSelect` 的 `stmt.orderBy` 分支），
            // 没有利用「聚簇扫描本来就是主键序」这一事实去省掉排序。既然它真的会排，
            // 就如实写 `Using filesort`——包括 `ORDER BY 主键` 这种看起来多此一举的情形。
            // 将来若把「主键序扫描免排序」做掉，这里的判断条件要跟着变成按列的判断。
            orderBy = hasOrderBy,
            blockPruning = PruneRanges.extract(where, table).isNotEmpty(),
            grouped = hasGrouping,
        )
        val plan = CostModel.choose(input)
        return ChosenPlan(
            plan = plan,
            keyRange = if (plan.type == AccessType.CONST || plan.type == AccessType.RANGE) keyRange else null,
            candidate = input.indexCandidates.firstOrNull().takeIf { plan.type == AccessType.REF },
            where = where,
            input = input,
        )
    }

    /**
     * 执行选定的路径。
     *
     * 每条分支只负责「怎么把行取出来」，过滤/排序/投影/限制仍然由
     * [finishSelect] 统一完成——路径不改变结果集，只改变取数方式。
     */
    private suspend fun executePlan(
        session: Session,
        table: TableSchema,
        chosen: ChosenPlan,
        readTs: ReadTs,
        vars: Map<String, Datum>,
    ): List<List<Datum>> = when (chosen.plan.type) {
        AccessType.CONST -> {
            val pk = encodePk(table, chosen.keyRange!!.exactPk!!)
            visibleRow(session, table, pk, readTs)?.let { listOf(it) } ?: emptyList()
        }
        AccessType.RANGE -> scanKeyRange(table, chosen.keyRange!!, readTs, session)
        AccessType.REF -> {
            val c = chosen.candidate!!
            indexLookup(session, table, c, chosen.where, readTs, vars)
                ?: visibleRows(session, table, readTs, chosen.where)
        }
        // 全文路径由 select() 先行处理（它需要 top-k 与相关性打分，不属于「取行」这一层）。
        // 走到这里说明取数顺序被改坏了——**报错，而不是静默退化成全表扫描**：
        // 退化成扫描的结果仍然"正确"，因此这条错误会很久都没人发现。
        AccessType.FULLTEXT -> throw SqlException(
            MySqlErrors.ER_UNKNOWN_ERROR, "HY000",
            "内部错误：全文检索路径必须由 MATCH 分支处理，不能走通用取数",
        )
        else -> visibleRows(session, table, readTs, chosen.where)
    }

    /**
     * SELECT 的收尾：过滤 → COUNT → 排序 → 投影 → LIMIT。
     *
     * 抽出来是因为 information_schema 的虚拟表要走**完全相同**的收尾逻辑——
     * 否则系统库上的 WHERE / ORDER BY / LIMIT 就得再实现一遍，
     * 而两份实现迟早会不一致。
     */
    private suspend fun finishSelect(
        session: Session,
        stmt: SelectStmt,
        table: TableSchema,
        scanRows: List<List<Datum>>,
        vars: Map<String, Datum>,
        onlyCount: Boolean = false,
        /** 覆盖 `stmt.where`。全文检索路径用它传入「已摘掉 MATCH 的剩余谓词」。 */
        effectiveWhere: Expr? = stmt.where,
        /**
         * 每行预先算好的"不在任何一行里"的值（标量相关子查询的结果），与 [scanRows] 一一对应。
         *
         * ⚠️ 它必须**跟着行一起被筛**：`WHERE` 过滤会丢掉一些行，按下标去取就会错位，
         * 于是某一行拿到**另一行**的子查询值——行数对、列数对、数字全错。
         */
        precomputed: List<Map<String, Datum>>? = null,
    ): ResultSet {
        // 过滤（WHERE 在**分组之前**——这是 SQL 的求值顺序，不能挪到后面）
        var rows = scanRows
        var pre = precomputed
        if (effectiveWhere != null) {
            val keep = ArrayList<Int>(rows.size)
            for (i in rows.indices) {
                val ctx = EvalContext(table, rows[i], vars, pre?.getOrNull(i) ?: emptyMap(), session.statementNowMillis)
                if (Eval.truthy(Eval.eval(effectiveWhere, ctx))) keep.add(i)
            }
            rows = keep.map { rows[it] }
            pre = pre?.let { p -> keep.map { p[it] } }
        }

        // ★ `SELECT ... FOR UPDATE` / `LOCK IN SHARE MODE`（设计文档 §4.4）
        //
        // 锁的是**通过 WHERE 的行**（在 LIMIT 之前）。MySQL 在 LIMIT 下可能只锁读到
        // 的那部分，多锁一些是保守的一侧：只会让别人多等，不会让数据出错。
        if (stmt.lockMode != null && rows.isNotEmpty()) {
            acquireRowLocks(session, table, rows, stmt.lockMode)
        }

        // 扫描路径上的 COUNT：数出来的行数本身就是答案。
        // （元数据路径不可用时——例如事务内有未提交写入——会走到这里）
        //
        // ⚠️ 只在「没有分组」时成立：`SELECT COUNT(*) FROM t GROUP BY x` 要按组各数一次，
        // 拿总行数当答案会给出一个**看起来正常**的错误数字。
        // ⚠️ 只对 `COUNT(*)` 成立：`COUNT(列)` 数的是**非 NULL 的列值**，
        // 拿总行数当答案是另一个数字（而且看起来完全正常）。
        // 这个捷径曾经把 `COUNT(amount)` 与 `COUNT(*)` 混为一谈
        val plainCount = stmt.groupBy.isEmpty() && stmt.having == null &&
            (onlyCount || (stmt.projections.size == 1 &&
                (stmt.projections[0] as? SelectItem.Field)?.expr is CountCall))
        if (plainCount) {
            val ce = (stmt.projections[0] as SelectItem.Field).expr as CountCall
            if (!ce.distinct && ce.arg == null) {
                // ⚠️ 捷径也要走 LIMIT：`SELECT COUNT(*) … LIMIT 0` 在 MySQL 里返回 **0 行**
                // （这是客户端探测"这条查询是否存在"的常用手法）。曾经这条捷径直接返回，
                // 于是同一个 LIMIT 0 在 `COUNT(*)` 上给 1 行、在 `COUNT(列)` 上给 0 行
                // ——两条路径互相矛盾，而调用方多半在数行数
                val only = applyLimit(listOf(listOf(Datum.I64(rows.size.toLong()))), stmt)
                return ResultSet(listOf("COUNT(*)"), only)
            }
        }

        val aliases = aliasMapOf(stmt)
        if (stmt.distinct) rejectOrderByNotSelected(stmt, aliases, table)
        val aggExprs = Aggregates.collect(stmt)
        val grouped = stmt.groupBy.isNotEmpty() || stmt.having != null || aggExprs.isNotEmpty()

        // ── 不分组：**原路径原样保留** ──────────────────────────────
        //
        // 刻意不去共用下面那条分组路径的管道：这是绝大多数查询走的路，
        // `SELECT * FROM t` 目前连"每行包一层"的分配都没有，
        // 为了少写十行代码而给它加一次逐行分配不值得。
        // 两条路径共用的部分是 `project` / `orderKeyOf` / `applyLimit` 这几个小函数，
        // 真正的逻辑没有第二份实现。
        if (!grouped) {
            val winCalls = Window.collect(stmt)
            if (winCalls.isNotEmpty()) {
                return finishWithWindows(stmt, table, rows, vars, winCalls, pre)
            }
            if (stmt.orderBy.isNotEmpty()) {
                val comparators = resolveOrdinals(stmt.orderBy, stmt.projections, table).map { oi ->
                    val e = resolveAliases(oi.expr, aliases)
                    Triple((e as? ColumnRef)?.let { table.indexOfOrNull(it.name) }, oi.ascending, e)
                }
                if (pre == null) {
                    rows = rows.sortedWith { a, b ->
                        compareRows(a, b, comparators, table, vars, null)
                    }
                } else {
                    // 有"每行的值表"时按**下标**排序：值表要跟着行走，
                    // 按值排序会让它和行脱节（见 finishWithWindows 的同一处说明）
                    val perRow = pre
                    val order = rows.indices.sortedWith(
                        Comparator { i, j ->
                            compareRows(rows[i], rows[j], comparators, table, vars, perRow[i], perRow[j])
                        },
                    )
                    val moved = order.map { rows[it] }
                    pre = order.map { perRow[it] }
                    rows = moved
                }
            }
            val (colNames, projected) = project(table, stmt.projections, rows, vars, null, pre)
            return ResultSet(
                colNames,
                applyLimit(dedupe(projected, stmt), stmt),
                declaredTypesOf(stmt.projections, table),
            )
        }

        // ── 分组路径 ────────────────────────────────────────────────
        // 标量相关子查询在这里没有落点：分组之后"这一行"是**一整组**，
        // 而按行算好的值表是按原始行建的。要支持它得先把关联键对齐到分组键上，
        // 那是一个独立的工作量——所以明确报错，而不是让每组拿到"组里第一行"的值
        if (precomputed != null) {
            throw SqlException(
                MySqlErrors.ER_NOT_SUPPORTED_YET, "42000",
                "暂不支持在 GROUP BY / HAVING / 聚合查询里使用**标量相关子查询**。" +
                    "按行算好的子查询结果对不上分组后的行——请把它改写成 JOIN，" +
                    "或去掉分组",
            )
        }
        var cells: List<Aggregates.GroupCell> = if (stmt.groupBy.isEmpty()) {
            // 有聚合但没有 GROUP BY：整张表就是**一组**。
            // 空表时也必须返回一行（`COUNT(*)` 给 0、`SUM` 给 NULL），
            // 这是 SQL 里少数"零行也要出行"的地方之一
            val computed = aggExprs.associate {
                aggregateKey(it) to Aggregates.compute(it, rows, table, vars, nowMillis = session.statementNowMillis)
            }
            Aggregates.singleGroup(rows, computed)
        } else {
            Aggregates.group(table, rows, stmt.groupBy, vars).map { (_, members) ->
                val computed = aggExprs.associate {
                    aggregateKey(it) to
                        Aggregates.compute(it, members, table, vars, nowMillis = session.statementNowMillis)
                }
                Aggregates.GroupCell(members[0], computed)
            }
        }

        // HAVING：分组**之后**的过滤。可以引用聚合，也可以引用分组键
        if (stmt.having != null) {
            val having = resolveAliases(stmt.having, aliases)
            cells = cells.filter {
                Eval.truthy(Eval.eval(having, EvalContext(table, it.values, vars, it.aggs, session.statementNowMillis)))
            }
        }

        // ★ 窗口函数：作用在**分组之后**的这些行上（每组一行），而不是原始行。
        // 算出来的值并进每格自己的聚合表——投影与 ORDER BY 都是按"从表里查"来求值的，
        // 所以两者共用同一条路径，不需要为窗口单开一份
        val winCalls = Window.collect(stmt)
        if (winCalls.isNotEmpty()) {
            val win = Window.compute(cells.map { it.values }, cells.map { it.aggs }, table, vars, winCalls)
            cells = cells.mapIndexed { i, cell -> Aggregates.GroupCell(cell.values, cell.aggs + win[i]) }
        }

        if (stmt.orderBy.isNotEmpty()) {
            val comparators = resolveOrdinals(stmt.orderBy, stmt.projections, table).map { oi ->
                val e = resolveAliases(oi.expr, aliases)
                Triple((e as? ColumnRef)?.let { table.indexOfOrNull(it.name) }, oi.ascending, e)
            }
            cells = cells.sortedWith { a, b ->
                compareRows(a.values, b.values, comparators, table, vars, a.aggs, b.aggs)
            }
        }

        // 投影：聚合值与窗口值都从每格自带的表里查（见 EvalContext.aggregates）
        val projected = cells.map { cell ->
            stmt.projections.flatMap { item ->
                when (item) {
                    is SelectItem.All -> cell.values
                    is SelectItem.Field -> listOf(
                        Eval.eval(item.expr, EvalContext(table, cell.values, vars, cell.aggs, session.statementNowMillis)),
                    )
                }
            }
        }
        return ResultSet(
            projectionNames(stmt.projections, table),
            applyLimit(dedupe(projected, stmt), stmt),
            declaredTypesOf(stmt.projections, table),
        )
    }

    /**
     * 带窗口函数的**非分组**查询。
     *
     * 与不带窗口的那条路（`project`）分开写，是因为窗口值必须**跟着行走**：
     * 排序会打乱行号，先按行号算好窗口值、排序之后再按下标去取，取到的是**别的行**的值。
     * 所以这里排的是**下标**（`order`），窗口表与原始行都按同一个下标访问。
     *
     * 顺带把 `SELECT *` 展开成真实的列：与 `project` 的单 `*` 快路径同一套语义，
     * 而 `SELECT *, ROW_NUMBER() OVER ()` 这种混写在这里也是对的。
     */
    private fun finishWithWindows(
        stmt: SelectStmt,
        table: TableSchema,
        rows: List<List<Datum>>,
        vars: Map<String, Datum>,
        winCalls: List<WindowCall>,
        pre: List<Map<String, Datum>>?,
    ): ResultSet {
        val win = Window.compute(rows, null, table, vars, winCalls)
        var order = rows.indices.toList()
        if (stmt.orderBy.isNotEmpty()) {
            val aliases = aliasMapOf(stmt)
            val comparators = resolveOrdinals(stmt.orderBy, stmt.projections, table).map { oi ->
                val e = resolveAliases(oi.expr, aliases)
                Triple((e as? ColumnRef)?.let { table.indexOfOrNull(it.name) }, oi.ascending, e)
            }
            order = order.sortedWith(
                Comparator { i, j ->
                    compareRows(rows[i], rows[j], comparators, table, vars, win[i], win[j])
                },
            )
        }
        val projected = order.map { i ->
            val row = rows[i]
            // 这条路径（带窗口函数的投影）拿不到 session，`NOW()` 退化为"构造这一刻"。
            // 与其它路径的语句级时钟有一点差别，但同一行内的多个 NOW() 仍然一致
            val ctx = EvalContext(table, row, vars, pre?.getOrNull(i)?.plus(win[i]) ?: win[i])
            stmt.projections.flatMap { item ->
                when (item) {
                    is SelectItem.All -> row
                    is SelectItem.Field -> listOf(Eval.eval(item.expr, ctx))
                }
            }
        }
        val names = projectionNames(stmt.projections, table)
        return ResultSet(names, applyLimit(dedupe(projected, stmt), stmt), declaredTypesOf(stmt.projections, table))
    }


    /**
     * 投影列的**声明类型**（能确定时）。
     *
     * 只认三种能一眼看出的形态：裸列（查 schema）、`CAST(x AS DATE)`、
     * 以及少数几个结果类型固定的日期函数。其余（表达式、聚合、窗口）返回 null，
     * 由协议层退回"按值推断"——那种推断出的类型不够精确，但对结果本身没有影响。
     */
    private fun declaredTypesOf(items: List<SelectItem>, table: TableSchema?): List<ColumnType?> {
        val out = ArrayList<ColumnType?>()
        for (item in items) {
            when (item) {
                is SelectItem.All -> {
                    if (table == null) return emptyList()
                    table.columns.forEach { out.add(it.type) }
                }
                is SelectItem.Field -> out.add(declaredTypeOf(item.expr, table))
            }
        }
        return out
    }

    private fun declaredTypeOf(e: Expr, table: TableSchema?): ColumnType? = when {
        e is ColumnRef && table != null -> table.columns.getOrNull(table.indexOfOrNull(e.name) ?: -1)?.type
        e is CastTo && e.typeName.uppercase().startsWith("DATE") &&
            !e.typeName.uppercase().startsWith("DATETIME") -> ColumnType.DATE
        e is FuncCall && e.name.uppercase() in setOf("DATE", "CURDATE", "CURRENT_DATE", "UTC_DATE", "LAST_DAY") ->
            ColumnType.DATE
        else -> null
    }

    /**
     * UNION 合并时的取值归一：右侧的值向左侧同位置的值靠。
     *
     * 只在**跨族**时转换（数值 ↔ 字符串、时间 ↔ 字符串），同族内保持原样——
     * 后者的差异（`10` 与 `10.0`）由 `distinctRows` 自己归一，不在这里做，
     * 否则 `SELECT DISTINCT` 会把字符串列里的 `'10'` 与 `'10.0'` 折成一个，
     * 而 MySQL 在同一个 VARCHAR 列里不会折。
     */
    private fun coerceUnionValue(v: Datum, like: Datum): Datum {
        if (v is Datum.Null || like is Datum.Null) return v
        val vNum = v is Datum.I64 || v is Datum.F64 || v is Datum.Bool || v is Datum.Dec
        val lNum = like is Datum.I64 || like is Datum.F64 || like is Datum.Bool || like is Datum.Dec
        return when {
            lNum && v is Datum.Str -> v.v.trim().toDoubleOrNull()
                ?.let { if (like is Datum.I64 || like is Datum.Bool) Datum.I64(it.toLong()) else Datum.F64(it) }
                ?: v
            like is Datum.Str && vNum -> Datum.Str(v.asString())
            like is Datum.Ts && v is Datum.Str -> v.coerceTo(ColumnType.DATETIME)
            else -> v
        }
    }

    /** 投影列名。`*` 展开成实际的列名——名字与值是两处独立的展开，必须一致。 */
    private fun projectionNames(items: List<SelectItem>, table: TableSchema): List<String> =
        items.flatMap { item ->
            when (item) {
                is SelectItem.All -> table.columns.map { it.name }
                is SelectItem.Field -> listOf(item.alias ?: exprLabel(item.expr))
            }
        }

    /**
     * 给这批行加行锁（`SELECT ... FOR UPDATE`）。
     *
     * ## 为什么必须走协调服务，而不是在本进程里记一个集合
     *
     * 行锁要挡住的是**别的 CN** 上的事务。本进程内的一个并发集合对另一个进程
     * 毫无约束力——那会得到一个"看起来加锁了、实际没有任何互斥"的实现，
     * 而这正是最坏的一类缺陷：它让使用者以为"读了改"是安全的。
     *
     * ## 单表之外的形状明确报错
     *
     * `FOR UPDATE` 在连接查询里要锁住**每个被读到的表**的行；本系统现在的实现
     * 是按基表的主键加锁，对连接/视图的语义还没有定义。宁可报错，也不要给一个
     * "只锁了一半"的实现——那会让使用者以为整条查询是安全的。
     */
    private suspend fun acquireRowLocks(
        session: Session,
        table: TableSchema,
        rows: List<List<Datum>>,
        mode: com.cloudea.yangbase.txn.LockMode,
    ) {
        if (table.isView) {
            throw SqlException(
                MySqlErrors.ER_NOT_SUPPORTED_YET, "42000",
                "视图上不支持 SELECT ... FOR UPDATE（视图背后可能有多张表，" +
                    "而锁的语义是对**真实的行**加锁）",
            )
        }
        if (table.tableId <= 0) {
            // 系统库的虚拟表（information_schema 等）没有真实的行可锁
            throw SqlException(
                MySqlErrors.ER_NOT_SUPPORTED_YET, "42000",
                "系统库的虚拟表上不支持 SELECT ... FOR UPDATE（${table.name}）",
            )
        }
        val timeout = session.lockWaitMillis
        // 同一行只加一次：`FOR UPDATE` 的语义是"这些行归我"，
        // 对同一行重复加锁既多花 RTT，也会在共享锁上把自己算成两个持有者
        val seen = HashSet<String>()
        for (row in rows) {
            val pk = rowPk(row, table)
            if (!seen.add(pk)) continue
            val key = com.cloudea.yangbase.txn.LockKey.row(table.tableId, pk.toByteArray(Charsets.ISO_8859_1))
            // ★ 锁的 TTL 必须**明显长于**等待上限。
            //
            // 否则会出现这样一条时间线：A 拿锁（TTL 30s）→ A 卡住 → B 一直等到 30s，
            // 恰好等到 A 的锁过期，于是 B 拿到锁并改了那一行 → A 恢复后仍以为自己锁着。
            // 取 2 倍 + 下限 30 秒，让"等待者等到持有者的锁过期"这件事在默认配置下不会发生；
            // 而真正的兜底是提交前的自检（stillHeld）：那时会报 1213，而不是静默覆盖
            val ttl = maxOf(30_000L, timeout * 2)
            val handle = engine.coord.locks.lock(key, mode, session.lockOwner, ttlMillis = ttl, waitMillis = timeout)
            session.rememberRowLock(handle)
        }
    }

    /**
     * `SELECT DISTINCT` 的去重：在**投影之后、`LIMIT` 之前**。
     *
     * 这个顺序不是细节：`DISTINCT ... LIMIT 1` 先截断再去重会把该有的行丢掉
     * （截到的那一行恰好是重复行时，结果里就少了别的值）。
     * 而且这里去重的是**投影后的值**——`SELECT DISTINCT a, b` 判重看的是 `(a, b)`，
     * 不是只按某一列。
     */
    private companion object {
        /** 2^53：Double 能**精确**表示整数的上界。超过它的浮点值不能当作整数键。 */
        const val EXACT_DOUBLE = 9.007199254740992E15
    }

    private fun dedupe(projected: List<List<Datum>>, stmt: SelectStmt): List<List<Datum>> =
        if (stmt.distinct) distinctRows(projected) else projected

    /**
     * 按 **SQL 的等值语义**去重，而不是按 Kotlin 的结构相等。
     *
     * 两者的差别只在跨数值类型时出现：`Datum.I64(10)` 与 `Datum.F64(10.0)` 是两个
     * 不同的对象（结构上不相等），而 SQL 里 `10 = 10.0` 为真。用结构相等去重的话，
     * `SELECT amount FROM t1 UNION SELECT amount FROM t2`（一边 `INT` 一边 `DOUBLE`）
     * 会返回两行看起来一模一样的值——去重**声称做了，实际没做**，而结果集看起来很正常。
     *
     * 键逐值构造：整数族用**精确**的十进制形式（不经过 Double，否则
     * 大于 2^53 的两个 BIGINT 会被折叠成一个），字符串与二进制各带类型前缀。
     */
    private fun distinctRows(rows: List<List<Datum>>): List<List<Datum>> {
        if (rows.size < 2) return rows
        val seen = HashSet<String>(rows.size * 2)
        val out = ArrayList<List<Datum>>(rows.size)
        for (r in rows) {
            val key = rowKey(r)
            if (seen.add(key)) out.add(r)
        }
        return out
    }

    private fun rowKey(row: List<Datum>): String = buildString(row.size * 8) {
        for (d in row) {
            val k = valueKey(d)
            // 长度前缀：分隔符可能出现在字符串值里，用长度才不会有歧义
            append(k.length).append(':').append(k)
        }
    }

    private fun valueKey(d: Datum): String = when (d) {
        is Datum.Null -> "N"
        is Datum.Bool -> if (d.v) "I1" else "I0"
        is Datum.I64 -> "I${d.v}"
        // 时间戳与整数**不混同**：它们在 `Datum.compareTo` 里按数值比大小，
        // 但一列的值类型是固定的，去重不该把 5 秒和整数 5 合成一行
        is Datum.Ts -> "T${d.millis}"
        is Datum.F64 -> if (d.v % 1.0 == 0.0 && d.v >= -EXACT_DOUBLE && d.v <= EXACT_DOUBLE) {
            "I${d.v.toLong()}"          // 整数值的浮点数按整数走，于是 I64(10) 与 F64(10.0) 是同一个键
        } else {
            "F${d.v}"
        }
        is Datum.Dec -> {
            // 归一化：去掉尾随的 0，于是 DECIMAL 10.0 与整数 10 是同一个键
            var unscaled = d.unscaled
            var scale = d.scale
            while (scale > 0 && unscaled % 10L == 0L) { unscaled /= 10; scale-- }
            if (scale == 0) "I$unscaled" else "D${unscaled}e-$scale"
        }
        is Datum.Str -> "S${d.v}"
        is Datum.Bin -> "B" + d.v.joinToString("") { "%02x".format(it) }
    }

    /**
     * `SELECT DISTINCT` 下，`ORDER BY` 只能引用结果里有的列（MySQL 的 3065）。
     *
     * 不检查的话，按"结果里没有的列"排序会**照常返回一个结果**——
     * 但那一列在去重之后已经没有意义了（同一个 `city` 对应哪个 `name`？
     * 取决于哪一行先出现），于是排序是任意的、且看起来完全正常。
     * MySQL 在这里直接报错，本系统也一样。
     */
    /**
     * 连接查询里出现**标量相关子查询**时明确报错（见 `select()` 里的调用点）。
     *
     * 非相关的标量子查询不受影响——它们在取数之前就被折成了字面量。
     */
    private fun rejectScalarSubqueryInJoin(stmt: SelectStmt) {
        fun found(e: Expr?): Boolean = when {
            e == null -> false
            e is ScalarSubquery -> true
            e is ExistsSubquery || e is InSubquery -> false
            else -> childrenOf(e).any { found(it) }
        }
        val hit = stmt.projections.any { it is SelectItem.Field && found(it.expr) } ||
            found(stmt.where) || found(stmt.having) || stmt.orderBy.any { found(it.expr) }
        if (hit) {
            throw SqlException(
                MySqlErrors.ER_NOT_SUPPORTED_YET, "42000",
                "暂不支持在 JOIN 查询里使用**标量相关子查询**。" +
                    "它的实现需要按外层表的关联键建一张「键 → 值」的表，而连接之后" +
                    "「一行」是多张表拼起来的合成行，关联键对不上。" +
                    "可以改写成 JOIN（`LEFT JOIN` 一个按关联键分组的派生查询），或把它拆成两条查询",
            )
        }
    }

    private fun rejectOrderByNotSelected(stmt: SelectStmt, aliases: Map<String, Expr>, table: TableSchema) {
        if (stmt.orderBy.isEmpty()) return
        val labels = HashSet<String>()
        for (item in stmt.projections) {
            when (item) {
                is SelectItem.All -> labels.add("*")
                is SelectItem.Field -> {
                    labels.add(exprLabel(item.expr))
                    item.alias?.let { labels.add(it.lowercase()) }
                }
            }
        }
        // `SELECT *` 包含所有列，此时任何列都在结果里
        if ("*" in labels) return
        for (oi in stmt.orderBy) {
            // 序号（`ORDER BY 1`）指向投影里的某一项，永远合法
            val ordinal = (oi.expr as? Literal)?.value as? Datum.I64
            if (ordinal != null && ordinal.v in 1..stmt.projections.size.toLong()) continue
            val resolved = resolveAliases(oi.expr, aliases)
            if (exprLabel(resolved) in labels) continue
            throw SqlException(
                MySqlErrors.ER_FIELD_IN_ORDER_NOT_SELECT, "42000",
                "Expression #${stmt.orderBy.indexOf(oi) + 1} of ORDER BY clause is not in SELECT list, " +
                    "references '${exprLabel(resolved)}' which is not in SELECT list; " +
                    "this is incompatible with DISTINCT" +
                    "（`DISTINCT` 之后这一列已经没有确定的值：同一个结果行可能对应多个原值）",
            )
        }
    }


    /**
     * 比较两行。`aggsA` / `aggsB` 非 null 时是分组查询，聚合值从那里查。
     *
     * 抽出来是因为普通路径与分组路径都要用它，而"两处各写一份比较"必然漂移——
     * 那会导致 `ORDER BY` 在分组查询上与普通查询行为不同。
     */
    /**
     * 把 `ORDER BY 1` 这种**序号**换成对应的输出列表达式。
     *
     * MySQL 里 `ORDER BY <整数>` 指的是「第 N 个输出列」，不是常量。不处理的话
     * 我们会对一个常量排序——**顺序完全不变，而查询看起来正常返回**。
     * 那是最难被发现的一类缺陷：结果集对了，只是顺序错了，
     * 而调用方往往正是靠 `ORDER BY 1` + `LIMIT` 取 top-N。
     *
     * 序号越界要报错（MySQL 的 1054），不能悄悄忽略。
     */
    private fun resolveOrdinals(
        orderBy: List<OrderItem>,
        /** 输出列：普通查询传投影项，并集查询传各输出列的名字。 */
        outputs: List<SelectItem>,
        table: TableSchema?,
    ): List<OrderItem> = orderBy.map { item ->
        val n = (item.expr as? Literal)?.value?.let { v ->
            when (v) {
                is Datum.I64 -> v.v
                is Datum.Dec -> v.unscaled
                else -> null
            }
        }
        if (n == null) return@map item
        if (n < 1 || n > outputs.size) {
            throw SqlException(
                MySqlErrors.ER_BAD_FIELD_ERROR, "42S22",
                "Unknown column '$n' in 'order clause'：ORDER BY 的序号必须在 1..${outputs.size} 之间",
            )
        }
        when (val p = outputs[(n - 1).toInt()]) {
            is SelectItem.Field -> item.copy(expr = p.expr)
            // `SELECT *` 时第 N 个输出列就是表里的第 N 列
            is SelectItem.All -> {
                val name = table?.columns?.getOrNull((n - 1).toInt())?.name
                if (name == null) item else item.copy(expr = ColumnRef(name))
            }
        }
    }

    private fun compareRows(
        a: List<Datum>,
        b: List<Datum>,
        comparators: List<Triple<Int?, Boolean, Expr>>,
        table: TableSchema,
        vars: Map<String, Datum>,
        aggsA: Map<String, Datum>?,
        aggsB: Map<String, Datum>? = aggsA,
    ): Int {
        for ((idx, asc, expr) in comparators) {
            val av = if (idx != null) a.getOrNull(idx) ?: Datum.Null
            else Eval.eval(expr, EvalContext(table, a, vars, aggsA ?: emptyMap()))
            val bv = if (idx != null) b.getOrNull(idx) ?: Datum.Null
            else Eval.eval(expr, EvalContext(table, b, vars, aggsB ?: emptyMap()))
            val c = av.compareTo(bv)
            if (c != 0) return if (asc) c else -c
        }
        return 0
    }

    /** `LIMIT` / `OFFSET`。放在投影之后，避免对不需要的行做表达式求值。 */
    /**
     * 计数路径的 `LIMIT`/`OFFSET`（见 [tryCount] 的同名参数）。
     *
     * 计数结果是**一行**，所以语义很简单：`LIMIT 0` 或 `OFFSET ≥ 1` → 空集。
     */
    private fun limitRows(rows: List<List<Datum>>, limit: Int?, offset: Int?): List<List<Datum>> {
        if (limit == null && (offset ?: 0) <= 0) return rows
        val off = offset ?: 0
        val lim = limit ?: Int.MAX_VALUE
        if (off >= rows.size) return emptyList()
        val end = minOf(off.toLong() + lim.toLong(), rows.size.toLong()).toInt()
        return rows.subList(off, end)
    }

    private fun applyLimit(projected: List<List<Datum>>, stmt: SelectStmt): List<List<Datum>> {
        val offset = stmt.offset ?: 0
        if (offset <= 0 && stmt.limit == null) return projected
        val end = if (stmt.limit != null) minOf(offset + stmt.limit, projected.size) else projected.size
        return if (offset >= projected.size) emptyList() else projected.subList(offset, end)
    }

    /**
     * 计数。
     *
     * 返回 null 表示「无法从元数据精确得出」——此时会回退到扫描，
     * **绝不返回估算值**（设计文档 §8.9.7）。
     */
    private suspend fun tryCount(
        session: Session,
        table: TableSchema,
        where: Expr?,
        countExpr: CountCall,
        readTs: ReadTs,
        vars: Map<String, Datum>,
        /**
         * 这条查询的 `LIMIT` / `OFFSET`（null 表示没写）。
         *
         * 必须传进来：元数据计数是"一行结果"，而 `LIMIT 0` 要求**零行**——
         * 不走 LIMIT 会让同一个查询在不同计划下给出不同的行数
         */
        limit: Int? = null,
        offset: Int? = null,
    ): ResultSet? {
        if (countExpr.distinct) return null     // COUNT(DISTINCT x) 需要扫描
        // `COUNT(列)` 数的是非 NULL 值，元数据里没有这个信息（只知道行数），
        // 必须回退到扫描。走元数据会把 `COUNT(amount)` 当成 `COUNT(*)` 返回行数——
        // 一个安静的错误数字
        if (countExpr.arg != null) return null

        // ★ 事务内有未提交的写入时，元数据计数不成立——它只反映**已提交**的状态。
        // 若在这里返回元数据值，就会出现「事务内 SELECT 看得到 3 行、COUNT 却说 0 行」
        // 这种自相矛盾的结果。此时必须回退到能看到事务缓冲的扫描路径。
        if ((session.txBuffer?.sizeForTable(table.tableId) ?: 0) > 0) return null

        // 分区裁剪只有在扫描路径上才省得下 IO；元数据路径本来就不读数据块，
        // 但裁掉的分区连元数据都不必加载，所以两条路径都传。
        val pruned = prunedShards(table, where)
        if (pruned != null && pruned.isEmpty()) {
            return ResultSet(listOf("COUNT(*)"), limitRows(listOf(listOf(Datum.I64(0L))), limit, offset))
        }
        val shards = pruned ?: engine.allShards(table)

        if (where == null) {
            val n = engine.countAll(table, readTs, shards)
            if (n >= 0) {
                // ⚠️ 三条 COUNT 捷径（元数据总行数、空分区、跳数计数）都必须
                // 遵守 `LIMIT`——`LIMIT 0` 是客户端探测"这条查询存不存在"的常用手法，
                // 而在这些路径上无视它会让同一个查询在不同计划下给出不同的行数
                return ResultSet(listOf("COUNT(*)"), limitRows(listOf(listOf(Datum.I64(n))), limit, offset))
            }
            return null
        }

        // 尝试 `低基数列 = 常量` 的元数据计数（设计文档 §8.9.4）
        val eq = where as? BinaryOp
        if (eq != null && eq.op == "=") {
            val (col, valExpr) = when {
                eq.left is ColumnRef && eq.right !is ColumnRef -> eq.left to eq.right
                eq.right is ColumnRef && eq.left !is ColumnRef -> eq.right to eq.left
                else -> return null
            }
            val colIdx = table.indexOfOrNull(col.name) ?: return null
            val v = try {
                Eval.eval(valExpr, EvalContext(table, null, vars))
            } catch (_: Exception) { return null }
            if (v is Datum.Null) return null
            // 按**列类型**解释这个值再编码。不转换的话 `WHERE status = 200`
            // （status 是 VARCHAR 计数列）会用一个整数编码去查一张按字符串编码的
            // 值→计数表，查不到就是 0——而扫描路径按列类型把 200 转成 '200'，
            // 能查到 2 行。两条路径对同一句 SQL 给出不同数字，且都不报错
            val encoded = KeyEncoder.encodeDatum(v.coerceTo(table.columns[colIdx].type), table.columns[colIdx].type)
            val n = engine.countByColumnEquals(table, colIdx, encoded, readTs, shards)
            if (n >= 0) return ResultSet(listOf("COUNT(*)"), limitRows(listOf(listOf(Datum.I64(n))), limit, offset))
        }
        return null
    }

    // ── 统计信息（ANALYZE TABLE）与 EXPLAIN ───────────────────────

    /**
     * `ANALYZE [LOCAL] TABLE t [, t2 ...]`（设计文档 §10.3、§13.3）。
     *
     * ## 它产生多少次对象存储请求
     *
     * **数据块读恒为 0**：只碰 `.idx`（常驻内存）与 manifest，绝不读 `.dat`。
     * 写：每个分区 1 个 `stats/column/...` 加 1 个表级 `stats/table/...`，
     * 另有 `Catalog.updateTable` 的 3 次 PUT——版本号 +1 就是统计的可见性开关，
     * 见 [com.cloudea.yangbase.txn.TableStatsStore]。
     *
     * 结果集是 MySQL 的四列形状（`Table / Op / Msg_type / Msg_text`）。
     * `Msg_text` 里如实写出这次采集的**规模**（版本、分区数、`.idx` 文件数、
     * 写出的对象数与字节数）：统计信息有多大，直接决定它能否整体常驻内存
     * （原则 P3），所以它不该是黑箱。
     */
    private suspend fun analyze(session: Session, stmt: AnalyzeStmt): ResultSet {
        val rows = ArrayList<List<Datum>>()
        for (name in stmt.tables) {
            val table = resolveTable(session, name)
            val r = statsStore.analyze(table)
            rows.add(
                listOf(
                    Datum.Str(r.tableName), Datum.Str("analyze"), Datum.Str("status"),
                    Datum.Str(
                        "OK  统计版本 v${r.statsVersion} · ${r.partitionCount} 个分区 · ${r.rowCount} 行" +
                            "（已落盘 ${r.flushedRows}）· ${r.columnCount} 列 · " +
                            "读 ${r.indexFiles} 个 .idx（0 次数据块读）· " +
                            "写 ${r.objectsWritten} 个对象 / ${r.bytesWritten} 字节"
                    ),
                )
            )
            // 采集的**边界**必须跟着结果一起返回：哪些数字是精确的、哪些是下界或上界，
            // 使用者要能分辨，否则会把一个上界当成真实基数来解读
            for (n in r.notes) {
                rows.add(listOf(Datum.Str(r.tableName), Datum.Str("analyze"), Datum.Str("note"), Datum.Str(n)))
            }
        }
        return ResultSet(listOf("Table", "Op", "Msg_type", "Msg_text"), rows)
    }

    /**
     * 元数据计数的可用性，以及它给出的**精确**行数（`EXPLAIN` 用）。
     *
     * `rows` 非 null 表示这条路真的可用——并且这个数字就是执行 `COUNT(*)` 会返回的值。
     */
    private class MetadataCount(val rows: Long?, val blockedReason: String?)

    /**
     * `EXPLAIN SELECT ...`（设计文档 §8.9.6）。
     *
     * ## 为什么它与执行**共用**同一条选路函数
     *
     * `EXPLAIN` 与执行都调 [choosePlan]，所以「EXPLAIN 说的」就是「执行做的」。
     * 两份实现迟早会失同步，而失同步的 EXPLAIN 是在**说谎**——比没有 EXPLAIN 更糟：
     * 使用者会据此以为 0 次 GET 的计数路径生效了，实际却没有。
     *
     * ## `COUNT(*)` 为什么真的去走一次元数据路径
     *
     * 计数路径上 `rows` 填的是元数据给出的**精确行数**，也就是执行 `COUNT(*)` 真会
     * 返回的值。用估算填会让 EXPLAIN 与结果对不上，而 `COUNT(*)` 恰恰是使用者
     * 拿来分页与对账的（§8.9.7）。这条路 0 次 GET，所以「为了 EXPLAIN 真跑一次」
     * 不吃任何 IO。
     */
    private suspend fun explain(session: Session, stmt: ExplainStmt): ResultSet {
        val inner = stmt.statement
        if (inner !is SelectStmt) throw SqlException(
            MySqlErrors.ER_NOT_SUPPORTED_YET, "42000",
            "EXPLAIN 目前只支持 SELECT（§8.9.6 要求它至少能显示 COUNT(*) 的 METADATA_COUNT 路径）",
        )

        val rawTable = inner.table
        if (rawTable == null) {
            return ResultSet(
                explainColumns,
                listOf(explainRow(null, AccessType.NO_TABLE.explainName, rows = 1L, filtered = 100.0, extra = listOf("No tables used"))),
            )
        }

        // 系统库是**内存物化**的虚拟表：一次对象存储请求都不产生。
        // 这一行必须如实写出来，否则使用者会以为 EXPLAIN 漏了什么。
        if (InformationSchema.tableFor(rawTable) != null) {
            return ResultSet(
                explainColumns,
                listOf(
                    explainRow(
                        rawTable, AccessType.ALL.explainName,
                        rows = null, filtered = 100.0,
                        extra = listOf("virtual table: materialized in memory from the catalog (0 object storage requests)"),
                    )
                ),
            )
        }

        val table = resolveTable(session, rawTable)
        val readTs = readTsFor(session, inner)
        val vars = varsOf(session)
        val where = inner.where

        // ★ 连接查询：**每个来源输出一行**（MySQL 的形状）。
        // 挤成一行会让使用者以为只读了一张表，而连接的真实代价恰恰在"读了几张表"上。
        // 连接路径不做索引选择、也不做分区裁剪（见 selectJoined 的说明），
        // 所以这里如实写 ALL + 「全表扫描」——**把没做的优化说出来**，
        // 比给一个好看但不成立的 key 更有用
        if (inner.joins.isNotEmpty()) {
            val clauses = inner.joins.map { j -> j to resolveTable(session, j.table) }
            val joined = JoinEngine.schemaOf(table, inner.tableAlias ?: table.name, clauses)
            // 真跑一次连接的**策略判定**（只算键、不取数）：EXPLAIN 说 `Using hash join`
            // 就必须真的是哈希，否则那一列就成了装饰
            for ((i, pair) in clauses.withIndex()) {
                JoinEngine.planStrategy(joined, i, pair.first.on)
            }
            val rows = ArrayList<List<Datum>>()
            for ((i, src) in joined.sources.withIndex()) {
                val on = inner.joins.getOrNull(i - 1)?.on
                val extra = buildList {
                    if (i == 0) {
                        add("joined with " + joined.sources.drop(1).joinToString(", ") { it.qualifier })
                    }
                    if (on != null) add("ON $on")
                    add("full scan (join path does not use index or partition pruning yet)")
                    // 这一步用的是哈希还是双层循环，由真实算出来的键决定
                    if (i > 0) add("Using ${joined.lastStrategy} join")
                    if (inner.orderBy.isNotEmpty()) add("Using filesort")
                    if (inner.groupBy.isNotEmpty() || inner.having != null ||
                        Aggregates.collect(inner).isNotEmpty()
                    ) {
                        add("Using temporary (group by / aggregate)")
                    }
                }
                rows.add(
                    explainRow(
                        table = src.qualifier,
                        type = AccessType.ALL.explainName,
                        partitions = explainPartitions(src.schema, where),
                        possibleKeys = emptyList(),
                        key = null,
                        keyLen = null,
                        ref = null,
                        rows = engine.peekRowCount(src.schema),
                        filtered = 100.0,
                        extra = extra,
                    )
                )
            }
            return ResultSet(explainColumns, rows)
        }

        // ★ R6：COUNT(*) 的元数据路径。这里**真去走一次**（0 次 GET）而不是估算。
        val countExpr = (inner.projections.singleOrNull() as? SelectItem.Field)?.expr as? CountCall
        var metadata: MetadataCount? = null
        if (countExpr != null && !countExpr.distinct) {
            val counted = tryCount(session, table, where, countExpr, readTs, vars)
            metadata = if (counted != null) {
                MetadataCount((counted.rows.first().first() as Datum.I64).v, null)
            } else {
                MetadataCount(null, metadataBlockedReason(session, table, where))
            }
        }

        val chosen = choosePlan(
            session, table, where, readTs, vars, metadata,
            // 元数据计数一旦可用，`select` 会在 `tryCount` 处直接返回（`if (rs != null) return rs`），
            // 根本不进 `finishSelect` 的排序分支——此时还写 `Using filesort` 就是假话。
            // 它不可用时才会继续规划、扫描、排序，那时才如实报。
            hasOrderBy = inner.orderBy.isNotEmpty() && metadata?.rows == null,
            hasGrouping = inner.groupBy.isNotEmpty() || inner.having != null ||
                Aggregates.collect(inner).isNotEmpty(),
        )
        val plan = chosen.plan
        return ResultSet(
            explainColumns,
            listOf(
                explainRow(
                    table = table.name,
                    type = plan.type.explainName,
                    partitions = explainPartitions(table, where),
                    possibleKeys = plan.possibleKeys,
                    key = plan.key,
                    keyLen = explainKeyLen(table, chosen),
                    ref = if (plan.type == AccessType.CONST || plan.type == AccessType.REF) "const" else null,
                    rows = plan.rows,
                    filtered = plan.filteredPercent,
                    // Extra 是本项目里信息密度最高的一列：路径的**来历**（统计还是兜底链）
                    // 与计数路径的文案都在这里，别处看不出来
                    extra = plan.extra + CostModel.trailingExtras(chosen.input!!),
                ),
            ),
        )
    }

    /** `EXPLAIN` 的列，顺序与 MySQL 一致（各客户端按名字与位置渲染）。 */
    private val explainColumns = listOf(
        "id", "select_type", "table", "partitions", "type",
        "possible_keys", "key", "key_len", "ref", "rows", "filtered", "Extra",
    )

    private fun explainRow(
        table: String?,
        type: String,
        partitions: String? = null,
        possibleKeys: List<String> = emptyList(),
        key: String? = null,
        keyLen: Long? = null,
        ref: String? = null,
        rows: Long?,
        filtered: Double,
        extra: List<String> = emptyList(),
    ): List<Datum> = listOf(
        Datum.I64(1),
        // 本 SQL 子集没有 JOIN / 子查询，因此不存在 MySQL 的 DEPENDENT SUBQUERY 等取值
        Datum.Str("SIMPLE"),
        table?.let { Datum.Str(it) } ?: Datum.Null,
        partitions?.let { Datum.Str(it) } ?: Datum.Null,
        Datum.Str(type),
        possibleKeys.takeIf { it.isNotEmpty() }?.joinToString(",")?.let { Datum.Str(it) } ?: Datum.Null,
        key?.let { Datum.Str(it) } ?: Datum.Null,
        keyLen?.let { Datum.I64(it) } ?: Datum.Null,
        ref?.let { Datum.Str(it) } ?: Datum.Null,
        rows?.let { Datum.I64(it) } ?: Datum.Null,
        Datum.F64(filtered),
        Datum.Str(extra.joinToString("; ")),
    )

    /** 分区裁剪的结果（`partitions` 列）。推不出约束时为 NULL，表示全部分区。 */
    private fun explainPartitions(table: TableSchema, where: Expr?): String? {
        if (!table.isEnumeratedPartition && !com.cloudea.yangbase.txn.Partitioning.hasHashColumn(table)) return null
        val shards = prunedShards(table, where) ?: return null
        if (shards.isEmpty()) return null
        return shards.joinToString(",") { shard ->
            table.partitions.firstOrNull { it.partitionId == shard.partition.value }?.name ?: "p${shard.partition.value}"
        }
    }

    /**
     * `key_len`：索引键里**真正参与比较**的字节数。
     *
     * 只有等值/范围两条主键路径能给出精确值（键就来自 [rangeBound] / [encodePk]，
     * 与实际扫描用的是同一个编码）；二级索引的键编码在 [com.cloudea.yangbase.txn.IndexCodec]
     * 内部，这里不重复实现——宁可留 NULL，也不给一个算不准的数。
     */
    private fun explainKeyLen(table: TableSchema, chosen: ChosenPlan): Long? = when (chosen.plan.type) {
        AccessType.CONST -> chosen.keyRange?.exactPk?.let { encodePk(table, it).size.toLong() }
        AccessType.RANGE -> chosen.keyRange?.let { r ->
            val from = rangeBound(table, r, r.low)
            maxOf(from.size, r.high?.let { rangeBound(table, r, it).size } ?: from.size).toLong()
        }
        else -> null
    }

    /**
     * 元数据计数为什么不可用——`EXPLAIN` 要如实写出来，而不是假装它能用。
     *
     * 几种原因**都不是缺陷**，但使用者必须能分辨「这条路本来就该走扫描」
     * 与「本该 0 次 GET 却退化了」：前者是语义决定的，后者才是问题。
     */
    private fun metadataBlockedReason(session: Session, table: TableSchema, where: Expr?): String = when {
        (session.txBuffer?.sizeForTable(table.tableId) ?: 0) > 0 ->
            "metadata count unavailable: transaction has uncommitted writes (metadata reflects committed state only)"
        where == null ->
            "metadata count unavailable: exact row count not maintained for this table (falling back to scan)"
        else ->
            "metadata count unavailable for this predicate: only COUNT(*) and column equality are answerable from metadata"
    }

    // ── 事务缓冲的读写接入 ────────────────────────────────────────

    /**
     * 写一批行。事务中进缓冲（COMMIT 才落地），否则直接写存储层。
     */
    /**
     * 写入一批行。
     *
     * @return 直接落地时的 commitTs；进入事务缓冲时返回 -1（真正的提交时间戳
     *         要到 COMMIT 才知道）。调用方据此决定是立刻维护全文索引还是排队。
     */
    private suspend fun writeRows(
        session: Session,
        table: TableSchema,
        rows: List<List<Datum>>,
        readTs: ReadTs,
        tombstone: Boolean = false,
    ): Long {
        if (rows.isEmpty()) return -1
        // 规划出的操作包含**数据分片与索引分片**两部分——
        // 索引维护不是一个独立的步骤，它和主数据在同一个 WAL 批次里
        val writes = engine.planWrites(table, rows, tombstone)
        if (!session.shouldBuffer) {
            return engine.applyWrites(table, writes).commitTs
        }
        val buf = session.ensureBuffer()
        for ((shard, ops) in writes) for (op in ops) buf.put(shard, op.key, op.value)
        return -1
    }

    /**
     * 维护全文索引：不在事务里就立刻做，在事务里就排队到 COMMIT。
     *
     * 排队是必须的——立刻索引会让未提交的行可以被检索到（脏读）。
     */
    private suspend fun syncFullText(session: Session, table: TableSchema, op: FullTextSupport.Pending, commitTs: Long) {
        if (fullText.fullTextIndexes(table).isEmpty()) return
        if (commitTs >= 0) fullText.applyPending(listOf(op), commitTs)
        else session.pendingFullText.add(op)
    }

    /**
     * 唯一约束校验。
     *
     * 必须在**写入之前**做，而且要同时看两处：
     *  - 已提交的数据（走索引查找）
     *  - 本事务缓冲里尚未提交的索引项
     *
     * 只看其中一处就会漏——事务内连续插入两个相同唯一键必须被拦住。
     */
    private suspend fun checkUnique(
        session: Session,
        table: TableSchema,
        rows: List<List<Datum>>,
        readTs: ReadTs,
    ) {
        // 只看 ACTIVE：BUILDING 的唯一索引还没自检完，拿它拦写入会把
        // 合法的插入判成重复（引擎侧的判重同样只看 ACTIVE）
        val uniques = table.activeIndexes.filter { it.unique }
        if (uniques.isEmpty()) return

        val selfKeys = HashSet<String>()
        for (row in rows) {
            for (idx in uniques) {
                val values = idx.columns.map { row[table.indexOf(it)] }
                // 含 NULL 的唯一键在 SQL 中不参与唯一性约束（与 MySQL 一致）
                if (values.any { it is Datum.Null }) continue

                val entryKey = com.cloudea.yangbase.txn.IndexCodec.entryKey(table, idx, row)
                val valueOnly = com.cloudea.yangbase.txn.IndexCodec.valuePrefix(table, idx, values)
                val mark = "${idx.id}:" + java.util.Base64.getEncoder().encodeToString(valueOnly)

                // (a) 同一批里重复
                if (!selfKeys.add(mark)) {
                    throw dupEntry(idx, values)
                }
                // (b) 缓冲里已有别的行占用。
                // ⚠️ 索引项落在**索引分片**上，不是表分片——查表分片会什么都找不到，
                // 于是事务内连续插入同一个唯一键不会被拦住（踩过的坑）。
                val buf = session.txBuffer
                if (buf != null) {
                    val idxShard = com.cloudea.yangbase.txn.IndexCodec.shardOf(idx)
                    for (op in buf.opsForShard(idxShard)) {
                        if (op.value == null) continue
                        if (!com.cloudea.yangbase.txn.IndexCodec.matchesPrefix(op.key, valueOnly)) continue
                        // 索引项 key 相同但主键不同 → 冲突
                        if (!op.key.contentEquals(entryKey)) throw dupEntry(idx, values)
                    }
                }
                // (c) 已提交数据里已有别的行占用
                val conflict = engine.findUniqueConflict(table, idx, row, readTs)
                if (conflict != null) throw dupEntry(idx, values)
            }
        }
    }

    private fun dupEntry(idx: com.cloudea.yangbase.common.IndexDef, values: List<Datum>): SqlException =
        SqlException(
            MySqlErrors.ER_DUP_ENTRY, "23000",
            "Duplicate entry '${values.joinToString("-") { it.toString() }}' for key '${idx.name}'",
        )

    private fun pkString(pk: ByteArray): String = java.util.Base64.getEncoder().encodeToString(pk)

    private fun rowPk(row: List<Datum>, table: TableSchema): String =
        pkString(KeyEncoder.encodeKey(row, table, table.primaryKeyIndexes))

    /**
     * 表在 [readTs] 处的可见行，**叠加本事务尚未提交的写入**。
     *
     * 这是「读自己的写」：事务内 `INSERT` 之后立刻 `SELECT` 必须看得到，
     * 否则事务语义就是不成立的。
     */
    /**
     * WHERE 能排除掉的分区。
     *
     * 返回 null = 推不出约束，必须扫全部分区；返回**空列表 = 该查询必然无结果**，
     * 此时一次对象存储请求都不该发出。这个区分很重要：空列表是最有价值的裁剪结果，
     * 如果把它当成 null 处理，就退化成了全表扫描。
     */
    private fun prunedShards(table: TableSchema, where: Expr?): List<com.cloudea.yangbase.common.ShardId>? {
        val c = PartitionPrune.constraintFor(table, where) ?: return null
        val keep = com.cloudea.yangbase.txn.Partitioning.prune(table, c) ?: return null
        return keep.sorted().map { engine.shard(table, it) }
    }

    private suspend fun visibleRows(
        session: Session,
        table: TableSchema,
        readTs: ReadTs,
        where: Expr? = null,
    ): List<List<Datum>> {
        val shards = prunedShards(table, where)
        // 空列表 = 所有分区都被裁掉，直接返回空（零 IO）
        if (shards != null && shards.isEmpty()) return emptyList()
        // ★ 块级剪枝：把 WHERE 里的列约束下推到存储层。
        // 被剪掉的块**一次 GET 都不产生**——这是跳数索引省下 IO 的唯一机制
        val all = engine.scanAll(
            table, readTs,
            shards = shards ?: engine.allShards(table),
            pruneRanges = PruneRanges.extract(where, table),
        )

        val buf = session.txBuffer
        if (buf == null || buf.sizeForTable(table.tableId) == 0) return all

        val merged = LinkedHashMap<String, List<Datum>>()
        for (r in all) merged[rowPk(r, table)] = r

        for (op in buf.opsForTable(table.tableId)) {
            val k = pkString(op.key)
            val v = op.value
            if (v == null) merged.remove(k) else merged[k] = RowCodec.decode(v, table)
        }
        return merged.values.toList()
    }

    /**
     * 用二级索引取数（设计文档 §10.2 R2）。
     *
     * 候选（哪个索引、哪一列等于什么）由 [CostModel.indexCandidates] 在**代价模型里**
     * 挑出来，这里只负责执行。索引项的值就是**整行**（见 [IndexCodec]），所以命中后
     * 不需要回表——`EXPLAIN` 里写 `Using index` 是据此写的，不是抄 MySQL 的措辞。
     *
     * ⚠️ **过滤必须用完整 `WHERE`，而不是「索引首列 = 常量」那一个谓词。**
     * 索引路径是从 `AND` 合取项里挑出一个等值前缀，其余谓词索引一概不管；
     * 只用首列谓词过滤会**静默多返回行**。
     *
     * 返回 null 表示这条路在运行时不可用，调用方回退到扫描。
     */
    private suspend fun indexLookup(
        session: Session,
        table: TableSchema,
        candidate: IndexCandidate,
        where: Expr?,
        readTs: ReadTs,
        vars: Map<String, Datum>,
    ): List<List<Datum>>? {
        val fromIndex = engine.lookupByIndex(table, candidate.index, listOf(candidate.value), readTs)

        val filtered = if (where == null) fromIndex else fromIndex.filter {
            Eval.truthy(Eval.eval(where, EvalContext(table, it, vars, nowMillis = session.statementNowMillis)))
        }

        // 叠加事务缓冲（读自己的写）
        val buf = session.txBuffer
        if (buf == null || buf.sizeForTable(table.tableId) == 0) return filtered

        val merged = LinkedHashMap<String, List<Datum>>()
        for (r in filtered) merged[rowPk(r, table)] = r
        for (op in buf.opsForTable(table.tableId)) {
            val k = pkString(op.key)
            val v = op.value
            if (v == null) {
                merged.remove(k)
                continue
            }
            val row = RowCodec.decode(v, table)
            // 缓冲里的版本**取代**索引里的版本：不满足谓词时也必须把旧项删掉，
            // 否则会返回一行「按未提交的新值已经不匹配」的旧数据。
            if (where == null || Eval.truthy(Eval.eval(where, EvalContext(table, row, vars, nowMillis = session.statementNowMillis)))) {
                merged[k] = row
            } else {
                merged.remove(k)
            }
        }
        return merged.values.toList()
    }

    /** 点查，叠加事务缓冲。 */
    private suspend fun visibleRow(
        session: Session,
        table: TableSchema,
        pk: ByteArray,
        readTs: ReadTs,
    ): List<Datum>? {
        session.txBuffer?.let { buf ->
            if (buf.sizeForTable(table.tableId) > 0) {
                when (val r = buf.lookup(engine.shardFor(table, pk), pk)) {
                    is com.cloudea.yangbase.txn.LookupResult.Found -> return RowCodec.decode(r.value, table)
                    com.cloudea.yangbase.txn.LookupResult.Deleted -> return null
                    null -> {}
                }
            }
        }
        return engine.get(table, pk, readTs)
    }

    /** 把按主键列顺序给出的值编码成完整主键。 */
    private fun encodePk(table: TableSchema, values: List<Datum>): ByteArray {
        val pkIdx = table.primaryKeyIndexes
        val row = ArrayList<Datum>(table.columns.size)
        for (i in table.columns.indices) row.add(Datum.Null)
        for ((k, idx) in pkIdx.withIndex()) row[idx] = values[k]
        return KeyEncoder.encodeKey(row, table, pkIdx)
    }

    /**
     * 复合主键上的键范围扫描。
     *
     * 键是**逐列拼接**的，所以 `(id, region)` 上的 `WHERE id = 5` 得到的是
     * 「主键以 id=5 开头」的一段连续区间——这正是 LSM 里一次范围扫描就能取全的东西。
     * 在此之前复合主键的约束被整个丢弃，退化成全表扫描。
     *
     * 上下界一律**取得比约束更宽**：真正的过滤由 WHERE 兜底。
     * 放松只会多读几行，收紧会漏行——两者代价不对等。
     */
    private suspend fun scanKeyRange(
        table: TableSchema,
        range: KeyRange,
        readTs: ReadTs,
        session: Session,
    ): List<List<Datum>> {
        val from = rangeBound(table, range, range.low)
        // 上界分三种情况：
        //  - 有显式上界 ⇒ 用它；
        //  - 无上界但有等值前缀 ⇒ 「前缀 + 1」（[prefixUpperBound]）；
        //  - 无上界且**没有前缀**（单列主键的 `WHERE id > 5`）⇒ **没有上界**。
        //
        // 第三种不能退化成 `prefixUpperBound(from)`：`from` 是**恰好一行**的键，
        // 把它整体 +1 得到的是「那一行之后紧邻的一小段」——扫描会在表尾之前停下，
        // **静默丢行**。宁可多扫，也不能少行。
        val to = when {
            range.high != null -> rangeBound(table, range, range.high)
            range.prefix.isEmpty() -> null
            else -> prefixUpperBound(from)
        }

        val out = ArrayList<List<Datum>>()
        val shards = prunedShards(table, null)?.takeIf { it.isNotEmpty() } ?: engine.allShards(table)
        for (shard in shards) {
            val rvs = engine.scanShard(
                table, shard, from, to, readTs,
                toInclusive = range.high != null && !range.highExclusive,
            )
            for (rv in rvs) out.add(RowCodec.decode(rv.value, table))
        }
        mergeTxBuffer(session, table, out, readTs)
        return out
    }

    /**
     * 键区间的一个端点：等值前缀 + 可选的下一列取值。
     *
     * `next` 为 null 时只编码等值前缀——这正是「前缀扫描」的下界。
     */
    private fun rangeBound(table: TableSchema, range: KeyRange, next: Datum?): ByteArray {
        val pkIdx = table.primaryKeyIndexes
        val stop = range.prefix.size
        val row = ArrayList<Datum>(table.columns.size)
        for (i in table.columns.indices) row.add(Datum.Null)
        for ((k, idx) in range.prefix.withIndex()) row[pkIdx[k]] = range.prefix[k]
        val withNext = next != null && stop < pkIdx.size
        if (withNext) row[pkIdx[stop]] = next
        return KeyEncoder.encodeKey(
            row, table,
            if (withNext) pkIdx.subList(0, stop + 1) else pkIdx.subList(0, stop),
        )
    }

    /** 把前缀的最后一个字节 +1，得到「所有以此前缀开头的键」的半开上界。 */
    private fun prefixUpperBound(prefix: ByteArray): ByteArray {
        val out = prefix.copyOf()
        for (i in out.indices.reversed()) {
            val v = out[i].toInt() and 0xFF
            if (v != 0xFF) { out[i] = (v + 1).toByte(); return out.copyOf(i + 1) }
        }
        return ByteArray(0)   // 全 0xFF：退化为无上界
    }

    /**
     * 把事务缓冲里尚未提交的写入并进扫描结果（读自己的写）。
     *
     * `predicate` 为 null 表示「调用方自己会再过滤」，此处只负责合并与去重。
     */
    private fun mergeTxBuffer(
        session: Session,
        table: TableSchema,
        into: MutableList<List<Datum>>,
        @Suppress("UNUSED_PARAMETER") readTs: ReadTs,
    ) {
        val buf = session.txBuffer ?: return
        if (buf.sizeForTable(table.tableId) == 0) return
        val at = HashMap<String, Int>()
        for ((i, r) in into.withIndex()) at[rowPk(r, table)] = i
        val removed = HashSet<Int>()
        for (op in buf.opsForTable(table.tableId)) {
            val key = pkString(op.key)
            val v = op.value
            if (v == null) {
                at[key]?.let { removed.add(it) }
            } else {
                val row = RowCodec.decode(v, table)
                val i = at[key]
                if (i == null) { at[key] = into.size; into.add(row) } else into[i] = row
            }
        }
        if (removed.isNotEmpty()) {
            val kept = into.filterIndexed { i, _ -> i !in removed }
            into.clear(); into.addAll(kept)
        }
    }

    private suspend fun scanRange(
        table: TableSchema,
        range: PkRange,
        colIdx: Int,
        readTs: ReadTs,
        session: Session,
    ): List<List<Datum>> {
        fun keyOf(d: Datum?): ByteArray = if (d == null) ByteArray(0) else
            KeyEncoder.encodeKey(
                List(table.columns.size) { i -> if (i == colIdx) d else Datum.Null },
                table, listOf(colIdx),
            )

        // 下界：开区间需要把 key 加一（保序编码下无法简单 +1），
        // 因此对开区间采用「包含下界扫描 + 事后过滤」的策略，正确性由 WHERE 兜底。
        val from = range.low?.let { keyOf(it) }
        val to = range.high?.let { keyOf(it) }

        val out = ArrayList<List<Datum>>()
        // 分区裁剪：RANGE/LIST 分区下，一个主键范围通常只落在少数几个分区里。
        // 哈希分区裁不动（一个范围本就散落在所有分区）——那不是缺陷，是哈希分区的固有性质。
        val shards = prunedShards(table, null)?.takeIf { it.isNotEmpty() } ?: engine.allShards(table)
        for (shard in shards) {
            // 分区表在有主键范围时仍要扫所有分区：分区是按主键哈希的，
            // 一个范围可能落在任意分区。这与设计文档 §9.2 的说明一致——
            // 哈希分区**只能做等值裁剪**，范围裁剪需要 RANGE/LIST 分区。
            if (shards.size > 1 && range.exact == null) {
                val lsm = engine.shardStore(shard, table)
                for (rv in lsm.scan(null, null, readTs)) {
                    val row = RowCodec.decode(rv.value, table)
                    val v = row[colIdx]
                    if (!inRange(v, range)) continue
                    out.add(row)
                }
            } else {
                val rvs = engine.scanShard(table, shard, from, to, readTs, toInclusive = !range.highExclusive)
                for (rv in rvs) {
                    val row = RowCodec.decode(rv.value, table)
                    if (!inRange(row[colIdx], range)) continue
                    out.add(row)
                }
            }
        }
        // 事务内的未提交写入也必须出现在结果里（读自己的写）
        val buf = session.txBuffer
        if (buf != null && buf.sizeForTable(table.tableId) > 0) {
            val seen = HashSet(out.map { rowPk(it, table) })
            for (op in buf.opsForTable(table.tableId)) {
                val v = op.value ?: continue
                val k = pkString(op.key)
                if (k in seen) continue
                val row = RowCodec.decode(v, table)
                if (inRange(row[colIdx], range)) out.add(row)
            }
            // 缓冲里被删掉的行要从结果中剔除
            val deleted = buf.opsForTable(table.tableId).filter { it.value == null }
                .map { pkString(it.key) }.toSet()
            if (deleted.isNotEmpty()) out.removeIf { rowPk(it, table) in deleted }
        }
        return out
    }

    private fun inRange(v: Datum, r: PkRange): Boolean {
        if (r.exact != null) return Eval.eq(v, r.exact)
        r.low?.let {
            val c = v.compareTo(it)
            if (c < 0 || (c == 0 && r.lowExclusive)) return false
        }
        r.high?.let {
            val c = v.compareTo(it)
            if (c > 0 || (c == 0 && r.highExclusive)) return false
        }
        return true
    }

    private fun project(
        table: TableSchema,
        items: List<SelectItem>,
        rows: List<List<Datum>>,
        vars: Map<String, Datum>,
        aggs: Map<String, Datum>?,
        /** 每行各自的值表（标量相关子查询的结果）。非 null 时优先于 [aggs]。 */
        perRow: List<Map<String, Datum>>? = null,
    ): Pair<List<String>, List<List<Datum>>> {
        if (items.size == 1 && items[0] is SelectItem.All) {
            return table.columns.map { it.name } to rows
        }
        val names = projectionNames(items, table)
        val out = rows.mapIndexed { rowIdx, row ->
            val values = perRow?.getOrNull(rowIdx) ?: aggs ?: emptyMap()
            // `*` 展开成**本行的真实列值**：它不是"某一列"，而是若干列。
            // 给一列 NULL（曾经的做法）会让 `SELECT *, 1 FROM t` 的前几列全是 NULL，
            // 而列名还叫 "*"——结果集看起来正常，数据全是空的
            items.flatMap { item ->
                when (item) {
                    is SelectItem.All -> row
                    is SelectItem.Field ->
                        listOf(Eval.eval(item.expr, EvalContext(table, row, vars, values)))
                }
            }
        }
        return names to out
    }

    private fun exprLabel(e: Expr): String = when (e) {
        is ColumnRef -> e.name
        is CountCall -> if (e.arg == null) "COUNT(*)" else "COUNT(${exprLabel(e.arg)})"
        is AggCall -> if (e.distinct) "${e.name}(DISTINCT ${exprLabel(e.arg)})" else "${e.name}(${exprLabel(e.arg)})"
        else -> e.toString()
    }

    // ── UPDATE / DELETE ───────────────────────────────────────────

    private suspend fun update(session: Session, stmt: UpdateStmt): ExecResult {
        val db = session.requireDb()
        val table = resolveTable(session, stmt.table!!)
        requireBaseTable(table)          // 视图不可写
        val readTs = session.readTs()
        val vars = varsOf(session)

        val candidates = visibleRows(session, table, readTs)
        val targets = candidates.filter { row ->
            stmt.where == null || Eval.truthy(Eval.eval(stmt.where, EvalContext(table, row, vars)))
        }
        if (targets.isEmpty()) return OkResult(0)

        val updated = targets.map { row ->
            val newRow = row.toMutableList()
            for ((col, expr) in stmt.assignments) {
                val idx = table.indexOf(col)
                newRow[idx] = coerce(Eval.eval(expr, EvalContext(table, row, vars)), table.columns[idx])
            }
            newRow.toList()
        }
        // 更新后的值也必须过唯一约束——否则可以把唯一列改成重复值
        checkUnique(session, table, updated, readTs)
        // ★ 主键也一样：`UPDATE t SET id = 2 WHERE id = 1` 在 id=2 已存在时必须报 1062。
        // 少了这一步，LSM 的"同 key 覆盖"语义会把**别人的那一行**静默改掉，
        // 而应用拿到的是"更新成功、影响 1 行"
        checkUpdatePrimaryKeys(session, table, targets, updated, readTs)

        // ── 旧行的墓碑：两个理由，都必须处理 ─────────────────────────
        //
        //  1. **有二级索引**：索引项的 key 由旧值算出，不删就留下指向旧值的索引项
        //  2. **主键变了**：LSM 的键就是主键，旧 key 不写墓碑就是一行**幽灵行**
        //     （`SELECT *` 还看得见它、`COUNT(*)` 也把它算进去，而它不该存在）
        //
        // 曾经这里只有第 1 条（`if (table.indexes.isNotEmpty())`），于是
        // 「只有主键、没有二级索引」的表改一次主键就多一行幽灵数据。
        // 顺序不能反：先删旧索引项 / 旧 key，再写新行——反了会把新写入的当成旧的删掉
        val pkIdx = table.primaryKeyIndexes
        val staleIdx = LinkedHashSet<Int>()
        if (table.indexes.isNotEmpty()) targets.indices.forEach { staleIdx.add(it) }
        if (pkIdx.isNotEmpty()) {
            targets.indices.forEach { i ->
                if (!KeyEncoder.encodeKey(targets[i], table, pkIdx)
                        .contentEquals(KeyEncoder.encodeKey(updated[i], table, pkIdx))
                ) {
                    staleIdx.add(i)
                }
            }
        }
        if (staleIdx.isNotEmpty()) {
            writeRows(session, table, staleIdx.map { targets[it] }, readTs, tombstone = true)
        }
        val ts = writeRows(session, table, updated, readTs)
        // 全文索引按「删旧 + 插新」维护（顺序不能反，见 FullTextSupport.onUpdate）
        syncFullText(session, table, FullTextSupport.Pending.Update(table, targets, updated), ts)
        return OkResult(updated.size.toLong())
    }

    private suspend fun delete(session: Session, stmt: DeleteStmt): ExecResult {
        val db = session.requireDb()
        val table = resolveTable(session, stmt.table!!)
        requireBaseTable(table)          // 视图不可写
        val readTs = session.readTs()
        val vars = varsOf(session)

        val pkIdx = table.primaryKeyIndexes
        // 扫描必须看到本事务自己的写（读自己的写）
        val candidates = visibleRows(session, table, readTs)
        val targets = if (stmt.where == null) candidates else candidates.filter { row ->
            Eval.truthy(Eval.eval(stmt.where, EvalContext(table, row, vars)))
        }
        if (targets.isEmpty()) return OkResult(0)

        // 传整行而不是主键——否则算不出索引项 key，索引会残留脏项
        val ts = writeRows(session, table, targets, readTs, tombstone = true)
        syncFullText(session, table, FullTextSupport.Pending.Delete(table, targets), ts)
        return OkResult(targets.size.toLong())
    }

    // ── SHOW / DESCRIBE / SET ─────────────────────────────────────

    /** `SHOW TABLE STATUS` 的列（与 MySQL 8.0 同名同序）。 */
    private val TABLE_STATUS_COLUMNS = listOf(
        "Name", "Engine", "Version", "Row_format", "Rows", "Avg_row_length",
        "Data_length", "Max_data_length", "Index_length", "Data_free", "Auto_increment",
        "Create_time", "Update_time", "Check_time", "Collation", "Checksum",
        "Create_options", "Comment",
    )

    private suspend fun show(session: Session, stmt: ShowStmt): ResultSet = when (stmt.what) {
        // ★ SHOW CREATE TABLE —— mysqldump 的第一步。
        // 没有它，任何导出工具都会在第一张表上失败。
        "CREATE" -> {
            val t = resolveTable(session, stmt.name
                ?: throw SqlException(MySqlErrors.ER_PARSE_ERROR, "42000", "SHOW CREATE TABLE 需要表名"))
            // ⚠️ 视图要按视图输出。解析 `SHOW CREATE VIEW v` 时对象类型词（VIEW）
            // 与 TABLE 一样被吃掉，所以**不能**按用户写了哪个词来分派——
            // 按对象的实际类型分派。这也正是 MySQL 的行为：
            // `SHOW CREATE TABLE v`（v 是视图）同样返回 Create View 两列。
            //
            // 不分派的话，视图会得到一份 `CREATE TABLE v (...)` 的**假 DDL**：
            // mysqldump 拿它去重建，会把视图变成一张空表，而且不报错。
            if (t.isView) {
                ResultSet(
                    listOf("View", "Create View", "character_set_client", "collation_connection"),
                    listOf(
                        listOf(
                            Datum.Str(t.name),
                            Datum.Str("CREATE VIEW `${t.name}` AS ${t.viewSql}"),
                            Datum.Str("utf8mb4"),
                            Datum.Str("utf8mb4_0900_ai_ci"),
                        )
                    ),
                )
            } else {
                ResultSet(
                    listOf("Table", "Create Table"),
                    listOf(listOf(Datum.Str(t.name), Datum.Str(createTableDdl(t)))),
                )
            }
        }

        // SHOW INDEX / SHOW KEYS —— 与 information_schema.STATISTICS 同源
        "INDEX", "INDEXES", "KEYS" -> {
            val t = resolveTable(session, stmt.name
                ?: throw SqlException(MySqlErrors.ER_PARSE_ERROR, "42000", "SHOW INDEX 需要表名"))
            val rows = ArrayList<List<Datum>>()
            val dbName = session.currentDbName ?: ""
            if (t.primaryKey.isNotEmpty()) {
                t.primaryKey.forEachIndexed { i, col ->
                    rows.add(indexRow(dbName, t.name, "PRIMARY", 0L, i, col, "BTREE"))
                }
            }
            // SHOW INDEX / SHOW CREATE TABLE 只列可用的索引：构建中的索引
            // 对查询不可见，对客户端也应如此（与 MySQL 的在线 DDL 一致）
            for (ix in t.activeIndexes) {
                val uniq = if (ix.unique) 0L else 1L
                val type = when (ix.kind) {
                    com.cloudea.yangbase.common.IndexKind.HASH -> "HASH"
                    com.cloudea.yangbase.common.IndexKind.SKIP_MINMAX,
                    com.cloudea.yangbase.common.IndexKind.SKIP_SET,
                    com.cloudea.yangbase.common.IndexKind.SKIP_BLOOM,
                    com.cloudea.yangbase.common.IndexKind.SKIP_NGRAM_BLOOM -> "SKIP"
                    com.cloudea.yangbase.common.IndexKind.FULLTEXT -> "FULLTEXT"
                    else -> "BTREE"
                }
                ix.columns.forEachIndexed { i, col -> rows.add(indexRow(dbName, t.name, ix.name, uniq, i, col, type)) }
            }
            ResultSet(
                listOf("Table", "Non_unique", "Key_name", "Seq_in_index", "Column_name",
                    "Collation", "Cardinality", "Sub_part", "Packed", "Null", "Index_type", "Comment", "Index_comment"),
                rows,
            )
        }

        // SHOW COLUMNS / SHOW FIELDS —— 与 DESC 同一份实现
        "COLUMNS", "FIELDS" -> describe(
            session,
            DescribeStmt(stmt.name
                ?: throw SqlException(MySqlErrors.ER_PARSE_ERROR, "42000", "SHOW COLUMNS 需要表名")),
        )

        "DATABASES", "SCHEMAS" -> {
            // 系统库也要列出来：MySQL 的 `SHOW DATABASES` 就包含 information_schema
            // 与 performance_schema。少了它们，客户端（以及靠"看得见的库列表"工作的
            // 工具）会以为这台服务器没有系统库——`mysqldump --all-databases` 更是
            // **靠这个名字来跳过它们**的
            val names = listOf("information_schema", "performance_schema") +
                engine.catalog.listDatabases().map { it.name }
            val filtered = names.filter {
                stmt.like == null || Eval.likeMatch(Datum.Str(it), Datum.Str(stmt.like))
            }
            ResultSet(listOf("Database"), filtered.map { listOf(Datum.Str(it)) })
        }
        // ★ `SHOW TABLE STATUS` —— **mysqldump 逐表取元数据**的那一步。
        //
        // 列名与顺序按 MySQL 8.0 的 18 列原样给出（客户端按**名字**读，但顺序错了
        // 有些工具会错位）。值分三类，注释里标清楚了：
        //  · **真实值**：Name / Rows / Data_length / Index_length / Update_time / Comment
        //    —— 全部来自内存里的 catalog 与 manifest，**不产生任何数据读取**
        //  · **如实填写但不假装是 InnoDB**：Engine="YangBase"、Row_format="LSM"
        //  · **占位**：MySQL 里那些描述 InnoDB 内部结构的字段（Data_free、Checksum…）
        //    在本系统里没有对应物，填 NULL/0，而不是抄一个 InnoDB 的数字糊弄工具
        "TABLE_STATUS" -> {
            val db = session.requireDb()
            val readTs = session.readTs()
            val tables = engine.catalog.listTables(db.databaseId)
                .filter { stmt.like == null || Eval.likeMatch(Datum.Str(it.name), Datum.Str(stmt.like)) }
            val rows = ArrayList<List<Datum>>(tables.size)
            for (t in tables) {
                var dataBytes = 0L
                var indexBytes = 0L
                var exactRows = 0L
                var updatedAt = 0L
                if (!t.isView) {
                    for (shard in engine.allShards(t)) {
                        val lsm = engine.shardStore(shard, t)
                        dataBytes += lsm.dataBytes()
                        indexBytes += lsm.stats().residentIndexBytes
                        val n = lsm.countAll(readTs)
                        if (n > 0) exactRows += n
                        updatedAt = maxOf(updatedAt, lsm.lastUpdatedMillis())
                    }
                }
                val rowsCnt = if (t.isView) Datum.Null else Datum.I64(exactRows)
                val avg = if (exactRows > 0) Datum.I64(dataBytes / exactRows) else Datum.I64(0)
                rows.add(
                    listOf(
                        Datum.Str(t.name),                                  // Name
                        Datum.Str(if (t.isView) "" else "YangBase"),        // Engine（视图没有引擎）
                        Datum.I64(if (t.isView) 0 else 1),                  // Version
                        Datum.Str(if (t.isView) "" else "LSM"),             // Row_format
                        rowsCnt,                                            // Rows（元数据精确值，0 次 GET）
                        avg,                                                // Avg_row_length
                        Datum.I64(dataBytes),                               // Data_length
                        Datum.I64(0),                                       // Max_data_length
                        Datum.I64(indexBytes),                              // Index_length
                        Datum.I64(0),                                       // Data_free
                        // Auto_increment 需要读水位对象（1 次 GET），而 mysqldump 只把它
                        // 当作参考；这里如实给 NULL 而不是编一个数字
                        Datum.Null,
                        if (updatedAt > 0) Datum.Ts(updatedAt) else Datum.Null,  // Create_time（未单独记录）
                        if (updatedAt > 0) Datum.Ts(updatedAt) else Datum.Null,  // Update_time
                        Datum.Null,                                         // Check_time
                        Datum.Str(SessionCharset.DEFAULT.collationName),    // Collation
                        Datum.Null,                                         // Checksum
                        Datum.Str(""),                                      // Create_options
                        // mysqldump 靠这一列识别视图（Comment 含 "VIEW"）
                        Datum.Str(if (t.isView) "VIEW" else ""),            // Comment
                    ),
                )
            }
            ResultSet(TABLE_STATUS_COLUMNS, rows)
        }
        "TABLES" -> {
            // `SHOW TABLES [FROM db]`：**FROM 指定的库名优先于当前库**（与 MySQL 一致）。
            // 此前 `FROM db` 被解析出来却没人用，于是 `SHOW TABLES FROM a`（当前库为 b）
            // 要么报"没有选中数据库"、要么答非所问地列出 b 的表
            //
            // 系统库（`information_schema` 等）不在 catalog 里，它们是**内存里物化**的：
            // 这里列出它们的表名，好让 `USE information_schema; SHOW TABLES` 像 MySQL 一样工作
            val sysDb = stmt.name ?: session.currentDbName
            if (sysDb != null && InformationSchema.isSystemSchemaName(sysDb)) {
                val known = if (sysDb.equals("information_schema", true)) InformationSchema.TABLE_NAMES
                else emptyList()
                val names = known.filter {
                    stmt.like == null || Eval.likeMatch(Datum.Str(it), Datum.Str(stmt.like))
                }
                ResultSet(
                    if (stmt.full) listOf("Tables_in_$sysDb", "Table_type")
                    else listOf("Tables_in_$sysDb"),
                    names.map {
                        if (stmt.full) listOf(Datum.Str(it), Datum.Str("SYSTEM VIEW"))
                        else listOf(Datum.Str(it))
                    },
                )
            } else {
                val db = if (stmt.name != null) {
                    engine.catalog.findDatabase(stmt.name)
                        ?: throw SqlException(
                            MySqlErrors.ER_BAD_DB_ERROR, "42000", "Unknown database '${stmt.name}'",
                        )
                } else {
                    session.requireDb()
                }
                val ts = engine.catalog.listTables(db.databaseId)
                    .filter { stmt.like == null || Eval.likeMatch(Datum.Str(it.name), Datum.Str(stmt.like)) }
                ResultSet(
                    // `SHOW FULL TABLES` 多一列 Table_type —— 客户端（尤其 mysqldump）
                    // 靠它区分表与视图。少这一列会让它们读到 NULL 并崩溃
                    if (stmt.full) listOf("Tables_in_${db.name}", "Table_type")
                    else listOf("Tables_in_${db.name}"),
                    ts.map {
                        if (stmt.full) {
                            listOf(Datum.Str(it.name), Datum.Str(if (it.isView) "VIEW" else "BASE TABLE"))
                        } else {
                            listOf(Datum.Str(it.name))
                        }
                    },
                )
            }
        }
        "VARIABLES" -> {
            val vars = listOf(
                listOf(Datum.Str("commit_mode"), Datum.Str(engine.commitMode.name.lowercase())),
                listOf(Datum.Str("autocommit"), Datum.Str(if (session.autocommit) "ON" else "OFF")),
                listOf(Datum.Str("innodb_lock_wait_timeout"), Datum.I64(session.lockWaitMillis / 1000)),
                listOf(Datum.Str("transaction_isolation"), Datum.Str("REPEATABLE-READ")),
                // 如实报告实现细节：本系统是快照隔离，不是 InnoDB 的 next-key lock 实现
                listOf(Datum.Str("yangbase_isolation_detail"), Datum.Str("snapshot-isolation")),
                listOf(Datum.Str("version"), Datum.Str(ServerInfo.VERSION)),
                listOf(Datum.Str("character_set_server"), Datum.Str(SessionCharset.DEFAULT.canonical)),
                listOf(Datum.Str("character_set_client"), Datum.Str(session.charset.canonical)),
                listOf(Datum.Str("character_set_connection"), Datum.Str(session.charset.canonical)),
                listOf(Datum.Str("character_set_results"), Datum.Str(session.charset.canonical)),
                listOf(Datum.Str("collation_server"), Datum.Str(SessionCharset.DEFAULT.collationName)),
                listOf(Datum.Str("collation_connection"), Datum.Str(session.charset.collationName)),
                listOf(Datum.Str("max_allowed_packet"), Datum.Str("67108864")),
                listOf(Datum.Str("lower_case_table_names"), Datum.Str("0")),
                listOf(Datum.Str("yangbase_protocol_flags"), Datum.Str(protocolFlagsOf(session))),
                listOf(Datum.Str("sql_mode"), Datum.Str("ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES")),
            )
            ResultSet(
                listOf("Variable_name", "Value"),
                vars.filter { stmt.like == null || Eval.likeMatch(it[0], Datum.Str(stmt.like)) },
            )
        }
        "ENGINES" -> ResultSet(
            listOf("Engine", "Support", "Comment"),
            listOf(listOf(Datum.Str("YangBase"), Datum.Str("DEFAULT"), Datum.Str("Object-storage native LSM"))),
        )
        "STATUS" -> {
            val st = engine.stats()
            ResultSet(
                listOf("Variable_name", "Value"),
                listOf(
                    listOf(Datum.Str("Open_shards"), Datum.Str(st.openShards.toString())),
                    listOf(Datum.Str("Cache_hit_rate"), Datum.Str("%.4f".format(st.cacheHitRate))),
                ),
            )
        }
        "WARNINGS", "ERRORS" -> ResultSet(
            listOf("Level", "Code", "Message"), emptyList()
        )
        // `SHOW TRIGGERS` —— mysqldump 逐表问一次"这张表有哪些触发器"。
        //
        // 返回**空结果集**而不是"暂不支持"的错误，理由是这里没有欺骗性：
        // 本系统根本不支持 `CREATE TRIGGER`（写它会明确报错），所以"触发器列表"
        // 的正确内容就是**空的**——MySQL 在一张没有触发器的表上返回的也是空集。
        // 反过来，如果这里报错，任何导出/备份工具都会在第一张表上停住，
        // 而挡住它们的是一个**并不存在的功能**。
        "TRIGGERS" -> ResultSet(
            listOf(
                "Trigger", "Event", "Table", "Statement", "Timing", "Created",
                "sql_mode", "Definer", "character_set_client", "collation_connection",
                "Database Collation",
            ),
            emptyList(),
        )
        // ⚠️ 兜底必须是**错误**，不能是一个「内容写着暂未实现」的成功结果集。
        // 后者会让客户端与工具链以为「查到了」，只是内容为空——
        // mysqldump 之类会拿它当真实 DDL 用下去，然后在很远的地方以更奇怪的方式失败。
        else -> throw SqlException(
            MySqlErrors.ER_NOT_SUPPORTED_YET, "42000",
            "暂不支持 SHOW ${stmt.what}（设计文档 §11.3 的限制清单）",
        )
    }

    private fun describe(session: Session, stmt: DescribeStmt): ResultSet {
        val t = resolveTable(session, stmt.table!!)
        val rows = t.columns.map { c ->
            listOf<Datum>(
                Datum.Str(c.name),
                Datum.Str(typeLabel(c.type, c.length, c.precision, c.scale, c.enumValues)),
                Datum.Str(if (c.nullable) "YES" else "NO"),
                Datum.Str(if (t.primaryKey.any { it.equals(c.name, true) }) "PRI" else ""),
                c.defaultValue?.let { Datum.Str(it) } ?: Datum.Null,
                Datum.Str(if (c.autoIncrement) "auto_increment" else ""),
            )
        }
        return ResultSet(listOf("Field", "Type", "Null", "Key", "Default", "Extra"), rows)
    }

    private fun indexRow(
        db: String, table: String, key: String, nonUnique: Long,
        seq: Int, column: String, type: String,
    ): List<Datum> = listOf(
        Datum.Str(table), Datum.I64(nonUnique), Datum.Str(key), Datum.I64((seq + 1).toLong()),
        Datum.Str(column), Datum.Str("A"),
        Datum.Null,   // Cardinality：本系统不维护索引基数统计
        Datum.Null, Datum.Null,
        Datum.Str(""), Datum.Str(type), Datum.Str(""), Datum.Str(""),
    )

    /**
     * 由 schema 重建 `CREATE TABLE` 语句。
     *
     * 它必须**能被自己解析回去**——这是 `mysqldump` 往返（导出再导入）的前提。
     * 因此这里只输出解析器真正支持的语法，不写任何"看起来更漂亮"的东西。
     */
    private fun createTableDdl(t: TableSchema): String {
        val sb = StringBuilder("CREATE TABLE `").append(t.name).append("` (\n")
        for (c in t.columns) {
            sb.append("  `").append(c.name).append("` ").append(typeLabel(c.type, c.length, c.precision, c.scale, c.enumValues))
            if (!c.nullable) sb.append(" NOT NULL")
            c.defaultValue?.let { sb.append(" DEFAULT ").append(it) }
            if (c.autoIncrement) sb.append(" AUTO_INCREMENT")
            c.comment?.let { sb.append(" COMMENT '").append(it.replace("'", "''")).append("'") }
            sb.append(",\n")
        }
        if (t.primaryKey.isNotEmpty()) {
            sb.append("  PRIMARY KEY (").append(t.primaryKey.joinToString(", ") { "`$it`" }).append("),\n")
        }
        for (ix in t.activeIndexes) {
            sb.append("  ").append(if (ix.unique) "UNIQUE KEY" else "KEY").append(" `").append(ix.name).append("` (")
            sb.append(ix.columns.joinToString(", ") { "`$it`" })
            sb.append(")")
            when (ix.kind) {
                com.cloudea.yangbase.common.IndexKind.HASH -> sb.append(" USING HASH")
                com.cloudea.yangbase.common.IndexKind.SKIP_MINMAX -> sb.append(" USING MINMAX")
                com.cloudea.yangbase.common.IndexKind.SKIP_SET -> sb.append(" USING SET")
                com.cloudea.yangbase.common.IndexKind.SKIP_BLOOM -> sb.append(" USING BLOOM")
                com.cloudea.yangbase.common.IndexKind.SKIP_NGRAM_BLOOM -> sb.append(" USING NGRAM")
                com.cloudea.yangbase.common.IndexKind.FULLTEXT -> sb.append(" WITH PARSER ").append(ix.parser ?: "standard")
                else -> {}
            }
            sb.append(",\n")
        }
        // 末尾多余的逗号要去掉
        if (sb.endsWith(",\n")) sb.setLength(sb.length - 2)
        sb.append("\n)")
        if (t.partitionKind != com.cloudea.yangbase.common.PartitionKind.NONE) {
            sb.append("\n").append(partitionClause(t))
        }
        return sb.toString()
    }

    private fun partitionClause(t: TableSchema): String {
        val kind = t.partitionKind.name
        val col = t.partitionColumn ?: ""
        val expr = t.partitionExpr ?: col
        return when {
            t.partitionKind.isEnumerated -> buildString {
                append("PARTITION BY ").append(kind).append(" (").append(expr).append(") (\n")
                t.partitions.forEachIndexed { i, p ->
                    append("  PARTITION ").append(p.name).append(" VALUES ")
                    if (t.partitionKind == com.cloudea.yangbase.common.PartitionKind.RANGE) {
                        append("LESS THAN (")
                        append(if (p.rangeUpperBound.equals(com.cloudea.yangbase.common.MAXVALUE, true)) "MAXVALUE"
                        else p.rangeUpperBound ?: "MAXVALUE")
                        append(")")
                    } else {
                        append("IN (").append(p.listValues.joinToString(", ")).append(")")
                    }
                    if (i < t.partitions.size - 1) append(",")
                    append("\n")
                }
                append(")")
            }
            else -> "PARTITION BY $kind ($col) PARTITIONS ${t.partitionCount}"
        }
    }

    private fun typeLabel(t: ColumnType, len: Int?, p: Int?, s: Int?, members: List<String> = emptyList()): String = when (t) {
        ColumnType.VARCHAR -> "varchar(${len ?: 255})"
        ColumnType.DECIMAL -> "decimal(${p ?: 10},${s ?: 0})"
        ColumnType.ENUM -> "enum(${members.joinToString(",") { "'$it'" }})"
        ColumnType.SET -> "set(${members.joinToString(",") { "'$it'" }})"
        else -> t.mysqlName
    }

    /**
     * 从 `SET` 的值里取出字符集。
     *
     * ⚠️ `SET character_set_results = NULL` 是 **Connector/J 在建立连接时就会发的**
     * （设计文档 §11.1.1 陷阱 T1）。它的语义是「不做转换，按列自身字符集给原始字节」。
     * 本系统所有字符串列都是 utf8mb4，所以「不转换」与「按 utf8mb4」等价——
     * 这是那条陷阱在本系统里的正确应对，而不是把 NULL 当成"空字符串"去解析
     * （那会得到一个 `Unknown character set: ''`，连接直接建不起来）。
     */
    private fun charsetOf(v: Datum): SessionCharset =
        if (v is Datum.Null) SessionCharset.DEFAULT else SessionCharset.parse(v.asString())

    /** 把能力位翻成可读的名字，见 [Session.protocolFlags]。 */
    private fun protocolFlagsOf(session: Session): String {
        val f = session.protocolFlags
        val names = ArrayList<String>()
        if (f and 0x01000000 != 0) names.add("deprecate_eof")
        if (f and 0x00010000 != 0) names.add("multi_statements")
        if (f and 0x00020000 != 0) names.add("multi_results")
        if (f and 0x00080000 != 0) names.add("ps_multi_results")
        return if (names.isEmpty()) "(none)" else names.joinToString(",")
    }

    private fun setVariable(session: Session, stmt: SetStmt): ExecResult {
        // ★ 用户变量（`@x`）与系统变量（`@@x` / `x`）是**两回事**：前者是会话里的
        // 一个普通存储位，任何表达式都能赋；后者是服务端配置项。先分流，否则
        // `@a` 会被 lower/去前缀之后当成一个叫 "a" 的系统变量而被静默忽略
        val raw = stmt.name
        if (raw.startsWith("@") && !raw.startsWith("@@")) {
            // 这里**不吞异常**：`SET @a = foo`（foo 不存在）在 MySQL 里报 1054，
            // 我们同样报——把它吞成 NULL 就是那种"不报错但值为空"的老问题
            val value = Eval.eval(stmt.value, EvalContext(null, null, varsOf(session)))
            session.userVars[raw.removePrefix("@").lowercase()] = value
            return OkResult(0)
        }
        val name = raw.lowercase().removePrefix("@@").removePrefix("session.").removePrefix("global.")
        val v = try { Eval.eval(stmt.value, EvalContext(null, null)) } catch (_: Exception) { Datum.Str("") }
        when (name) {
            "autocommit" -> session.autocommit = Eval.truthy(v)
            "commit_mode" -> {
                val mode = v.asString().lowercase()
                if (mode !in setOf("sync", "batch", "async")) {
                    throw SqlException(
                        MySqlErrors.ER_UNKNOWN_SYSTEM_VARIABLE, "HY000",
                        "commit_mode 只能是 sync / batch / async，收到 '$mode'",
                    )
                }
                // ★ 这里是**部署级**设置，不是会话级的，`SET` 改不动它。
                //
                // 组提交的 WalWriter 是每个分片一个、被所有会话共享的。某个会话
                // 单独切档在架构上无法表达：同一批里混着不同持久性承诺的事务，
                // 攒批就没法定义。所以 `SET commit_mode` 只能如实拒绝——
                // 悄悄接受会让使用者以为自己的写入变成了 async，实际没有。
                val actual = engine.commitMode.name.lowercase()
                if (mode != actual) {
                    throw SqlException(
                        MySqlErrors.ER_NOT_SUPPORTED_YET, "0A000",
                        "commit_mode 是部署级设置，当前实例为 '$actual'，无法按会话切换" +
                            "（组提交写者按分片共享，不同持久性承诺的事务无法共用一批）。" +
                            "请用服务端 --commit-mode 启动参数设置。",
                    )
                }
            }
            // ★ 字符集**不能**像别的一样收下就完事。
            // 收下不作声 = 客户端按 gbk 发、服务端按 utf8mb4 解，中文变乱码，
            // 而整条链路上没有任何一处报错（§11.1.1 陷阱 T1）。
            // 所以：支持的名字记下来并真的生效，不支持的**当场报 1115**。
            "names" -> session.charset = charsetOf(v)
            "character_set_client", "character_set_connection", "character_set_results" ->
                session.charset = charsetOf(v)
            "collation_connection", "collation_server" -> session.charset = charsetOf(v)
            // ★ 行锁的等待上限（设计文档 §4.4：与 `innodb_lock_wait_timeout` 语义对齐）。
            // **必须真的生效**：应用靠它控制"等多久就放弃"，而收下不生效的表现是
            // 一句 `SET` 成功、随后查询却按默认的 50 秒挂住——排查的人会去怀疑锁、
            // 怀疑网络，唯独不会怀疑这个变量没生效
            "innodb_lock_wait_timeout" -> {
                val secs = v.asLong()
                if (secs < 1 || secs > 3600) {
                    throw SqlException(
                        MySqlErrors.ER_WRONG_ARGUMENTS, "HY000",
                        "innodb_lock_wait_timeout 必须在 1..3600 秒之间，收到 $secs",
                    )
                }
                session.lockWaitMillis = secs * 1000
            }
            // ★ 隔离级别：**收下，但要如实说明降级**。
            //
            // 本系统只有快照隔离一种实现。客户端（Connector/J 的 `transactionIsolation`
            // 配置、以及各种连接池的初始化 SQL）会在连接建立时发
            // `SET SESSION TRANSACTION ISOLATION LEVEL …`——**直接拒绝会让这些客户端连不上**，
            // 所以必须收下；但收下不说话就是"配了 READ COMMITTED 却按快照隔离跑"，
            // 而那正是本项目最反对的静默不一致（原则 P6：显式降级）。
            //
            // 做法：接受、记下请求值，并把它如实回报在
            // `@@yangbase_isolation_degraded` 里；只要请求的不是
            // REPEATABLE-READ / SERIALIZABLE，服务端日志会打一行警告
            "transaction_isolation", "tx_isolation" -> {
                val want = v.asString().uppercase().replace('_', '-').trim()
                // 只记"没能满足"的请求：`@@yangbase_isolation_degraded` 回答的是
                // 「你请求的哪一档我没给」，而不是「你请求了什么」。
                // 请求 REPEATABLE-READ 时我们**正好**提供这一档（对外如此报告），
                // 报成降级会让真正需要关注的降级（READ COMMITTED 等）淹没在噪声里
                session.requestedIsolation = if (want in HONORED_ISOLATION) null else want
                if (want !in HONORED_ISOLATION) {
                    System.err.println(
                        "[yb] 警告: 客户端请求隔离级别 $want，但本系统只提供快照隔离" +
                            "（对外报告 REPEATABLE-READ）。写偏斜需要显式加锁（SELECT … FOR UPDATE）",
                    )
                }
            }
            "sql_mode", "time_zone",
            "foreign_key_checks", "unique_checks", "sql_safe_updates" -> {}
            else -> {
                // MySQL 客户端会设置大量无关变量；对未知变量保持宽容，
                // 但对影响正确性的那几个（已在上方列出）必须处理
            }
        }
        return OkResult(0)
    }
}

object ServerInfo {
    const val VERSION = "8.0.32-YangBase-0.1.0"
    const val VERSION_COMMENT = "YangBase: object-storage-native distributed TP database"
}
