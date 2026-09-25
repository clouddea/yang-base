package com.cloudea.yangbase.mysql

import com.cloudea.yangbase.common.ColumnType
import com.cloudea.yangbase.common.Datum
import com.cloudea.yangbase.common.MySqlErrors
import com.cloudea.yangbase.common.SqlException
import com.cloudea.yangbase.common.TableSchema
import com.cloudea.yangbase.sql.ExecResult
import com.cloudea.yangbase.sql.Executor
import com.cloudea.yangbase.sql.SessionCharset
import com.cloudea.yangbase.sql.OkResult
import com.cloudea.yangbase.sql.Parser
import com.cloudea.yangbase.sql.QueryResult
import com.cloudea.yangbase.sql.ResultSet
import com.cloudea.yangbase.sql.ServerInfo
import com.cloudea.yangbase.sql.Session
import com.cloudea.yangbase.sql.Statement
import com.cloudea.yangbase.txn.DatabaseEngine
import java.io.BufferedInputStream
import java.io.BufferedOutputStream
import java.net.InetSocketAddress
import java.net.ServerSocket
import java.net.Socket
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger

class MysqlServerConfig(
    val host: String = "0.0.0.0",
    val port: Int = 3306,
    /** 允许任意密码登录（仅限开发）。生产必须置为 false。 */
    val allowAnyPassword: Boolean = true,
    val maxConnections: Int = 512,
)

/**
 * MySQL 协议服务端。
 *
 * **并发模型（设计文档 §15.1）**：每条连接一个**虚拟线程** + 阻塞式 socket。
 * 这里刻意不用 Netty——协议是有状态的、每连接串行的，而连接数在这个
 * 目标场景（低频应用）下不多。虚拟线程让阻塞 IO 也能支撑大量连接，
 * 同时把代码复杂度压到最低。
 *
 * 注意这与「本地缓存引擎的 JNI 调用」是两回事：那里的虚拟线程会被原生帧钉住，
 * 而 socket IO 不会。
 */
class MysqlServer(
    private val engine: DatabaseEngine,
    private val config: MysqlServerConfig = MysqlServerConfig(),
    /**
     * 语句观测点（观测用，不影响协议行为）。默认不观测——
     * 生产由 CN 装配可观测性实现（见 ServerMetrics）。
     */
    private val observer: QueryObserver? = null,
) {
    private val executor = Executor(engine)
    private val connectionCounter = AtomicInteger(1)
    private val running = AtomicBoolean(false)
    private val liveConnections = ConcurrentHashMap<Int, MysqlConnection>()

    @Volatile
    private var serverSocket: ServerSocket? = null

    /** 实际绑定的端口（传 0 时由系统分配，测试用）。 */
    var boundPort: Int = -1
        private set

    /**
     * 当前活跃连接数。**只读一个 `ConcurrentHashMap` 的 size**——
     * 观测端点（`/healthz`、`yb_mysql_connections`）会随时读它，
     * 因此这里既不能加锁也不能遍历。
     */
    val liveConnectionCount: Int get() = liveConnections.size

    fun start() {
        if (!running.compareAndSet(false, true)) return
        val ss = ServerSocket()
        ss.reuseAddress = true
        // ★ 端口被占用是**最常见的启动失败**（本机已经跑着 MySQL / 另一个 CN），
        // 而裸的 `BindException: 地址已在使用` 既不说是谁占的、也不说该怎么办。
        // 这里把它翻译成一条能直接照着做的错误：原因 + 怎么查 + 怎么改。
        try {
            ss.bind(InetSocketAddress(config.host, config.port), 128)
        } catch (e: java.net.BindException) {
            running.set(false)
            runCatching { ss.close() }
            throw IllegalStateException(
                "MySQL 端口 ${config.port} 已被占用，无法启动。\n" +
                    "  · 3306 是本机 MySQL/MariaDB 的默认端口，也可能是上一个没退干净的 YangBase 进程；\n" +
                    "  · 查是谁占用：ss -ltnp | grep :${config.port}（或 lsof -i :${config.port}）\n" +
                    "  · 换一个端口再起：--port 3307（或任何未占用的端口）",
                e,
            )
        }
        serverSocket = ss
        boundPort = ss.localPort

        Thread.ofPlatform().name("yb-mysql-accept").daemon(true).start {
            while (running.get()) {
                try {
                    val socket = ss.accept()
                    if (liveConnections.size >= config.maxConnections) {
                        socket.close()
                        continue
                    }
                    socket.tcpNoDelay = true
                    socket.soTimeout = 0
                    val id = connectionCounter.getAndIncrement()
                    // 每条连接一个虚拟线程：阻塞式协议实现，但能支撑大量连接
                    Thread.ofVirtual().name("yb-conn-$id").start {
                        val conn = MysqlConnection(id, socket, engine, executor, config, observer)
                        liveConnections[id] = conn
                        try {
                            conn.run()
                        } catch (t: Throwable) {
                            // 连接级异常不应影响**服务端**，但绝不能连痕迹都不留。
                            // 曾因此把一个「mysql 命令行客户端连不上」的问题查了很久——
                            // 服务端日志干净得像什么都没发生。
                            // 客户端主动断开（socket 关闭）是常态，降噪处理。
                            val quiet = t is java.io.EOFException ||
                                (t is java.net.SocketException && t.message?.contains("reset") == true)
                            if (!quiet) {
                                System.err.println("[yb-mysql] 连接 #$id(${conn.peerName()}) 异常终止: ${t::class.simpleName}: ${t.message}")
                                if (System.getProperty("yb.mysql.trace") != null) t.printStackTrace()
                            }
                        } finally {
                            liveConnections.remove(id)
                        }
                    }
                } catch (_: Exception) {
                    if (running.get()) continue else break
                }
            }
        }
    }

    fun stop() {
        running.set(false)
        runCatching { serverSocket?.close() }
        for (c in liveConnections.values) runCatching { c.close() }
        liveConnections.clear()
    }

    val connectionCount: Int get() = liveConnections.size
}

/** 一条连接的协议状态机。 */
class MysqlConnection(
    private val id: Int,
    private val socket: Socket,
    private val engine: DatabaseEngine,
    private val executor: Executor,
    private val config: MysqlServerConfig,
    private val observer: QueryObserver? = null,
) {
    private val input = Packets.Reader(BufferedInputStream(socket.getInputStream(), 32 * 1024))
    private val output = Packets.Writer(BufferedOutputStream(socket.getOutputStream(), 32 * 1024))

    val session = Session(engine)

    private var capabilities: Int = 0
    private var user: String = ""
    private var database: String? = null

    /** 预编译语句表。stmt_id → SQL。 */
    private val prepared = ConcurrentHashMap<Int, PreparedStatement>()
    private val stmtCounter = AtomicInteger(1)

    /**
     * 一条预编译语句。
     *
     * [cursor] 非 null 表示这条语句以「只读游标」的方式执行
     * （`COM_STMT_EXECUTE` 的 `CURSOR_TYPE_READ_ONLY`，设计文档 §11.1.1 陷阱 T6）：
     * 结果集**先不回给客户端**，而是留在这里等 `COM_STMT_FETCH` 分批取。
     * Connector/J 的 `useCursorFetch=true` 走的就是这条路；
     * 不支持它时客户端会在取大结果集时报错。
     */
    private class PreparedStatement(
        val id: Int,
        val sql: String,
        val paramCount: Int,
        var cursor: CursorState? = null,
    )

    /** 服务端游标的剩余内容（[rows] 的 [pos] 之后部分）。 */
    private class CursorState(
        val resultSet: ResultSet,
        val columnTypes: List<com.cloudea.yangbase.common.ColumnType>,
        var pos: Int = 0,
    )

    fun run() {
        try {
            // 协议本身是「每连接串行」的，而执行器是挂起的；
            // 用 runBlocking 把两者接起来最直接——每条连接已经独占一个虚拟线程，
            // 阻塞它没有代价。
            kotlinx.coroutines.runBlocking {
                sendHandshake()
                if (!readHandshakeResponse()) return@runBlocking
                commandLoop()
            }
        } finally {
            runCatching { socket.close() }
            // ★ 连接断开必须**结束它的事务并释放行锁**。
            //
            // 曾经这里只关 socket：客户端在 `BEGIN; SELECT … FOR UPDATE;` 之后
            // 掉线（或被 kill -9），它持有的行锁只能等 TTL 自然过期（默认 30 秒），
            // 期间其它事务在同一批行上阻塞——而 `innodb_lock_wait_timeout` 默认 50 秒，
            // 所以表现是"莫名很慢"而不是一个明确的 1205。
            //
            // 释放走的是与 `ROLLBACK` 同一条路径（未提交的写入丢弃、行锁释放），
            // 失败也不影响连接关闭——但**不能静默**：它是"锁没释放"的唯一线索
            runCatching {
                kotlinx.coroutines.runBlocking {
                    // 行锁必须**显式**释放：`Session.endTransaction` 只是置了一个
                    // "下次语句开始时释放"的标记，而这条连接已经没有下一次语句了。
                    // 先释放锁，再复位事务状态（`txBuffer = null` 会丢掉未提交的写）
                    session.releaseRowLocks()
                    session.endTransaction()
                }
            }.onFailure {
                System.err.println("[yb-mysql] 连接 #$id 关闭时释放行锁失败（锁会随 TTL 过期）：${it.message}")
            }
        }
    }

    fun close() = runCatching { socket.close() }.let { }

    /** 对端地址，仅用于日志。 */
    fun peerName(): String = runCatching { socket.remoteSocketAddress.toString() }.getOrDefault("?")

    // ── 握手 ──────────────────────────────────────────────────────

    private fun serverCapabilities(): Int =
        Packets.CLIENT_LONG_PASSWORD or Packets.CLIENT_FOUND_ROWS or Packets.CLIENT_LONG_FLAG or
            Packets.CLIENT_CONNECT_WITH_DB or Packets.CLIENT_PROTOCOL_41 or
            Packets.CLIENT_TRANSACTIONS or Packets.CLIENT_SECURE_CONNECTION or
            Packets.CLIENT_MULTI_STATEMENTS or Packets.CLIENT_MULTI_RESULTS or
            Packets.CLIENT_PLUGIN_AUTH or Packets.CLIENT_CONNECT_ATTRS or
            Packets.CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA or
            Packets.CLIENT_DEPRECATE_EOF

    private fun sendHandshake() {
        val caps = serverCapabilities()
        val seed = ByteArray(20).also { java.util.Random().nextBytes(it) }
        // 保证种子中不含 0 字节（它被当作终止符）
        for (i in seed.indices) if (seed[i] == 0.toByte()) seed[i] = 1

        val b = Packets.Buf(128)
        b.u8(10)                                  // protocol version
        b.nulStr(ServerInfo.VERSION)              // server version
        b.u32(id.toLong())                        // connection id
        b.bytes(seed.copyOfRange(0, 8))           // auth-plugin-data-part-1
        b.u8(0)                                   // filler
        b.u16(caps and 0xFFFF)                    // capability_flags_1
        b.u8(session.charset.collationId)         // character set（会话字符集，默认 utf8mb4）
        b.u16(Packets.SERVER_STATUS_AUTOCOMMIT)   // status flags
        b.u16((caps ushr 16) and 0xFFFF)          // capability_flags_2
        b.u8(21)                                  // auth plugin data len
        b.zeros(10)                               // reserved
        b.bytes(seed.copyOfRange(8, 20))          // auth-plugin-data-part-2
        b.nul()                                   // 终止
        b.nulStr("mysql_native_password")
        output.writePacket(b.toBytes())
    }

    private fun readHandshakeResponse(): Boolean {
        val payload = input.readPacket()
        if (payload.isEmpty()) return false
        if (payload[0].toInt() and 0xFF == 0xFF) return false   // 客户端直接报错

        val c = Packets.Cursor(payload)
        capabilities = c.u32().toInt()
        c.u32()                                   // max_packet_size
        // 客户端声明的初始字符集。采纳它，后续按它解码 SQL 与编码结果——
        // 忽略它、一律按 utf8mb4 处理，就是 §11.1.1 陷阱 T1 描述的乱码来源
        val clientCharset = SessionCharset.ofCollationId(c.u8() and 0xFF)
        c.skip(23)                                // reserved
        user = c.nulStr()

        when {
            capabilities and Packets.CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA != 0 -> c.lenencBytes()
            capabilities and Packets.CLIENT_SECURE_CONNECTION != 0 -> {
                val n = c.u8(); c.bytes(n)
            }
            else -> c.nulStr()
        }

        if (capabilities and Packets.CLIENT_CONNECT_WITH_DB != 0 && c.hasMore()) {
            database = c.nulStr().takeIf { it.isNotEmpty() }
        }

        session.charset = clientCharset
        // 记下客户端协商出的能力位：诊断（"这条连接处在哪种协议模式"）与
        // 测试（证明某条协议路径**真的**被走到了，而不是"没走也照样过"）都要用
        session.protocolFlags = capabilities

        // 认证：默认接受（本系统尚无用户体系，见设计文档 §16.3 的设计空白）
        if (!config.allowAnyPassword && user.isEmpty()) {
            sendError(MySqlErrors.ER_ACCESS_DENIED, "28000", "Access denied for user ''@'${socket.inetAddress.hostAddress}'")
            return false
        }

        if (database != null) session.currentDbName = database
        // 序号必须与客户端保持连续：客户端用 seq=1 发了响应，服务端这里必须用 seq=2。
        // 不同步的话包序号会错位，客户端会报协议错误（实现期间踩到的真实缺陷）。
        output.setSequence(input.sequence)
        sendOk(0, 0, "")
        return true
    }

    // ── 命令循环 ──────────────────────────────────────────────────

    private suspend fun commandLoop() {
        while (!socket.isClosed) {
            val payload = try {
                input.readPacket()
            } catch (_: Exception) {
                return
            }
            // ★ **每个命令的应答都必须接着客户端的包序号往下走。**
            //
            // 协议是「一条命令一条应答」共用一个序号序列：客户端用 seq=0 发 COM_QUERY，
            // 服务端的第一个应答包必须是 seq=1。曾经这里（以及各个命令分支里）
            // 把写侧序号**清零**，于是应答从 seq=0 开始。
            //
            // 为什么一直没被发现：Connector/J 对这种错位是宽容的，而我们全部
            // 端到端测试走的都是 JDBC。但 **libmysqlclient 严格校验**——
            // `mysql` 命令行客户端、`mysqladmin`、`mysqldump`、Python 的
            // MySQLdb 都是它，它们看到序号错位会直接掐掉连接，报
            // `ERROR 2013 Lost connection to MySQL server during query`。
            // 也就是说：这不是"少支持一个客户端"，而是**标准的 MySQL 命令行工具全灭**。
            //
            // 握手路径早就这么做了（见 `readHandshakeResponse` 末尾），
            // 命令循环漏了——同一件事只做对了一半。
            output.setSequence(input.sequence)
            if (payload.isEmpty()) continue

            val cmd = payload[0].toInt() and 0xFF
            val args = payload.copyOfRange(1, payload.size)
            try {
                when (cmd) {
                    Packets.COM_QUIT -> return
                    Packets.COM_PING -> { sendOk(0, 0) }
                    Packets.COM_INIT_DB -> {
                        val dbName = session.charset.decode(args)
                        if (engine.catalog.findDatabase(dbName) == null) {
                            sendError(MySqlErrors.ER_BAD_DB_ERROR, "42000", "Unknown database '$dbName'")
                        } else {
                            session.currentDbName = dbName
                            sendOk(0, 0)
                        }
                    }
                    Packets.COM_RESET_CONNECTION -> {
                        session.endTransaction()
                        prepared.clear()
                        sendOk(0, 0)
                    }
                    Packets.COM_STATISTICS -> {
                        val s = engine.stats()
                        output.writePacket("Uptime: 0  Shards: ${s.openShards}  Cache hit: %.2f".format(s.cacheHitRate).toByteArray())
                    }
                    // ★ 按**当时的**会话字符集解码。不是在连接建立时算一次：
                    // `SET NAMES` 之后发来的 SQL 必须按新字符集解（陷阱 T1/T2）
                    Packets.COM_QUERY -> handleQuery(session.charset.decode(args))
                    Packets.COM_FIELD_LIST -> handleFieldList(args)
                    Packets.COM_STMT_PREPARE -> handlePrepare(session.charset.decode(args))
                    Packets.COM_STMT_EXECUTE -> handleStmtExecute(args)
                    Packets.COM_STMT_FETCH -> handleStmtFetch(args)
                    Packets.COM_STMT_CLOSE -> {
                        if (args.size >= 4) prepared.remove(readU32(args, 0))
                        // COM_STMT_CLOSE 没有响应
                    }
                    Packets.COM_STMT_RESET -> {
                        sendOk(0, 0)
                    }
                    Packets.COM_SET_OPTION -> { sendResultTerminator() }
                    else -> {
                        sendError(MySqlErrors.ER_NOT_SUPPORTED_YET, "42000", "不支持的命令: 0x%02x".format(cmd))
                    }
                }
            } catch (e: SqlException) {
                sendError(e.errorCode, e.sqlState, e.message ?: "未知错误")
            } catch (e: Packets.ProtocolException) {
                // 客户端发来的包不符合协议：这是**客户端**的错误，回一个干净的
                // 协议错误（1156 ER_NET_PACKETS_OUT_OF_ORDER 语义接近），
                // 不要把内部细节发出去
                sendError(1156, "08S01", "协议错误：${e.message}")
                return
            } catch (e: Throwable) {
                // 非 SQL 异常意味着**我们自己的**缺陷（NPE、类型转换…），
                // 不是用户写错了 SQL。只把 message 发给客户端等于把栈丢掉，
                // 现场只剩一句 "NullPointerException: null"——这里打到 stderr。
                System.err.println("[yb] 语句执行抛出未预期异常: ${e::class.qualifiedName}: ${e.message}")
                e.printStackTrace()
                // ⚠️ 回给客户端的文案**不能带 message**：`ObjectNotFoundException` 的
                // message 里有对象键（`shard/3/0/manifest/CURRENT`），S3 的异常里
                // 有桶名与 endpoint——那些都是不该外泄的内部信息。
                // 客户端只需要知道"服务端内部错误 + 一个可以拿去查日志的短标识"
                sendError(
                    1105, "HY000",
                    "Internal error (${e::class.simpleName}). " +
                        "详情见服务端日志；对象键、桶名等内部信息不会发给客户端",
                )
            }
        }
    }

    private suspend fun handleQuery(sql: String) {
        val stmts = Parser.parseAll(sql)
        // ★ 多语句只有在客户端**协商过** `CLIENT_MULTI_STATEMENTS` 时才执行。
        // 无条件执行是危险的：`CLIENT_MULTI_STATEMENTS` 本来是一个让客户端
        // 明确声明「我知道这条 SQL 里可能有多个语句」的开关，SQL 注入防护
        // 依赖客户端不开启它。服务端自己把它当默认行为，等于替客户端关掉了那道闸。
        // MySQL 在未协商时把 `a; b` 判成语法错误，这里照做。
        if (stmts.size > 1 && capabilities and Packets.CLIENT_MULTI_STATEMENTS == 0) {
            sendError(
                MySqlErrors.ER_PARSE_ERROR, "42000",
                "一条 COM_QUERY 里有 ${stmts.size} 条语句，但连接没有协商 CLIENT_MULTI_STATEMENTS。" +
                    "请在连接串上开启多语句（Connector/J 是 allowMultiQueries=true）。",
            )
            return
        }
        for ((idx, stmt) in stmts.withIndex()) {
            // ⚠️ **每一条**结果集的收尾包（OK 或 EOF）都要带
            // SERVER_MORE_RESULTS_EXISTS，除了最后一条。漏掉 OK 那一半的话，
            // 一条 `INSERT; SELECT` 会让客户端在 OK 之后就停下，
            // 后续结果集留在连接里——下一个查询读到的是上一个查询的残留。
            val more = idx < stmts.size - 1
            when (val r = executeObserved(stmt, sql)) {
                is QueryResult -> sendResultSet(r.resultSet, moreResults = more)
                is OkResult -> sendOk(r.affectedRows, r.lastInsertId, r.info, moreResults = more)
            }
        }
    }

    /**
     * 执行一条语句，并在**有观测者时**通知它前后各一次。
     *
     * 没有观测者时（默认）直接调执行器：多出来的只是一次空判断。
     */
    private suspend fun executeObserved(stmt: Statement, sql: String): ExecResult {
        val obs = observer ?: return executor.execute(session, stmt)
        val token = obs.queryStarted(sql, stmt, session)
        try {
            val r = executor.execute(session, stmt)
            val rows = when (r) {
                is QueryResult -> r.resultSet.rows.size.toLong()
                is OkResult -> r.affectedRows
            }
            obs.queryFinished(token, sql, stmt, session, rows, null)
            return r
        } catch (t: Throwable) {
            obs.queryFinished(token, sql, stmt, session, -1, t)
            throw t
        }
    }

    private suspend fun handleFieldList(args: ByteArray) {
        // COM_FIELD_LIST 已被 MySQL 弃用，但部分老客户端仍会发
        val c = Packets.Cursor(args)
        val tableName = c.nulStr()
        try {
            val db = session.requireDb()
            val t = engine.catalog.requireTable(db.databaseId, tableName)
            for (col in t.columns) {
                output.writePacket(
                    columnDefinition(
                        "def", t, col.name, col.name, col.type, 0,
                        fieldListDefault = col.defaultValue, includeDefault = true,
                    ),
                )
            }
            // ★ **收尾包必须无条件发**，不能走 `sendColumnSectionEnd()`。
            //
            // 那个函数在协商了 `CLIENT_DEPRECATE_EOF` 时**什么都不发**——因为
            // "结果集里列定义段之后的 EOF"确实被废弃了。但 `COM_FIELD_LIST` 的应答
            // **不是结果集**：它就是"若干个列定义 + 一个收尾包"，收尾包并没有被废弃。
            //
            // 漏掉它的后果不是"少一个包"：客户端会一直等那个包，然后判定连接坏掉。
            // 触发者是**标准 `mysql` 客户端的自动补全**——交互式会话里它会对每张表
            // 发 `COM_FIELD_LIST`（`use db` 之后尤其明显），于是
            // `show tables` 之后连接就没了，报的还是 `ERROR 2013 Lost connection`。
            // Connector/J 根本不发这个命令，所以 JDBC 测试全绿（真事，2026-09-25）。
            sendResultTerminator()
        } catch (e: SqlException) {
            sendError(e.errorCode, e.sqlState, e.message ?: "")
        }
    }

    // ── 预编译语句 ────────────────────────────────────────────────

    private fun handlePrepare(sql: String) {
        val paramCount = countPlaceholders(sql)
        val stmtId = stmtCounter.getAndIncrement()
        prepared[stmtId] = PreparedStatement(stmtId, sql, paramCount)

        val b = Packets.Buf(16)
        b.u8(0x00)
        b.u32(stmtId.toLong())
        b.u16(0)                    // num_columns（无法在 prepare 阶段确定，置 0）
        b.u16(paramCount)           // num_params
        b.u8(0)                     // filler
        b.u16(0)                    // warning_count
        output.writePacket(b.toBytes())

        // 参数定义 + EOF
        for (i in 0 until paramCount) {
            output.writePacket(paramDefinition(i))
        }
        if (paramCount > 0) sendColumnSectionEnd()
    }

    private suspend fun handleStmtExecute(args: ByteArray) {
        val c = Packets.Cursor(args)
        val stmtId = c.u32().toInt()
        val flags = c.u8()
        c.u32()                     // iteration count
        val stmt = prepared[stmtId]
        if (stmt == null) {
            sendError(1243, "HY000", "Unknown prepared statement handler ($stmtId) given to mysqld_stmt_execute")
            return
        }
        // 上一条游标若没取完就丢掉：协议是一条语句一个游标位，
        // 留着它会让后续 FETCH 读到上一次的结果
        stmt.cursor = null
        // 参数绑定：Connector/J 默认走客户端预编译，因此这条路径较少被触发。
        // 完整实现需要解析 null bitmap 与参数值 —— 见设计文档 §11.1.1 的陷阱 T3。
        try {
            val sql = bindPlaceholders(stmt, c)
            val parsed = Parser.parse(sql)
            when (val r = executeObserved(parsed, sql)) {
                is QueryResult -> {
                    if (flags and Packets.CURSOR_TYPE_READ_ONLY != 0) {
                        // 只读游标：**先建游标、不发数据**。
                        // 客户端接着用 COM_STMT_FETCH 一批一批取（T6）
                        openCursor(stmt, r.resultSet)
                    } else {
                        sendBinaryResultSet(r.resultSet)
                    }
                }
                is OkResult -> sendOk(r.affectedRows, r.lastInsertId, r.info)
            }
        } catch (e: SqlException) {
            sendError(e.errorCode, e.sqlState, e.message ?: "")
        }
    }

    /**
     * 建立一个只读游标：把结果集的**列定义**先发出去，行留着等 `COM_STMT_FETCH` 取
     * （设计文档 §11.1.1 陷阱 T6）。
     *
     * 列定义必须先发：客户端要凭它决定怎么解二进制行，而二进制行里
     * **没有类型信息**（陷阱 T3）。先发列、后取行，正好对上这个顺序。
     */
    private fun openCursor(stmt: PreparedStatement, rs: ResultSet) {
        val n = rs.columns.size
        val types = (0 until n).map { inferType(rs, it) }
        val b = Packets.Buf(16)
        b.lenenc(n.toLong())
        output.writePacket(b.toBytes())
        for ((i, name) in rs.columns.withIndex()) {
            output.writePacket(columnDefinition("def", null, "", name, types[i], 0))
        }
        if (n > 0) sendColumnSectionEnd()
        stmt.cursor = CursorState(rs, types)
    }

    /**
     * `COM_STMT_FETCH`：从游标取 [count] 行，按二进制协议发回。
     *
     * 取完（或游标不存在）时回一个**空的 EOF**——这是 MySQL 的语义：
     * 游标耗尽表现为「这一批一行都没有」，而不是一个错误。
     * 客户端据此停止取数并复用这个 statement。
     */
    private fun handleStmtFetch(args: ByteArray) {
        val c = Packets.Cursor(args)
        val stmtId = c.u32().toInt()
        val count = c.u32()
        val stmt = prepared[stmtId]
        val cur = stmt?.cursor
        if (stmt == null || cur == null) {
            // MySQL 在没有游标时回一个空结果（不是错误）：客户端可能在
            // 一个已经取完的 statement 上多调一次 FETCH
            sendResultTerminator()
            return
        }
        val n = cur.resultSet.columns.size
        // 按 Long 比：count 是客户端给的 u32，取很大值时 Int 相加会溢出成负数，
        // 那时 `until` 会变成一段空区间——查询悄悄返回 0 行
        val want = cur.pos.toLong() + count
        val end = if (want >= cur.resultSet.rows.size) cur.resultSet.rows.size else want.toInt()
        for (i in cur.pos until end) {
            output.writePacket(binaryRow(cur.resultSet.rows[i], n))
        }
        cur.pos = end
        // moreResults=false 是刻意的：游标耗尽不是「还有下一个结果集」
        sendResultTerminator()
    }

    /** 一行的二进制编码。独立出来是为了能单独测（陷阱 T3 的应对）。 */
    private fun binaryRow(row: List<Datum>, n: Int): ByteArray {
        val b = Packets.Buf(64)
        b.u8(0x00)   // 二进制行头
        val bitmapLen = (n + 9) / 8
        val bitmap = ByteArray(bitmapLen)
        for (i in 0 until n) {
            if (row.getOrNull(i) is Datum.Null) {
                val bit = i + 2
                bitmap[bit / 8] = (bitmap[bit / 8].toInt() or (1 shl (bit % 8))).toByte()
            }
        }
        b.bytes(bitmap)
        for (i in 0 until n) {
            val v = row.getOrNull(i) ?: Datum.Null
            if (v is Datum.Null) continue
            writeBinaryValue(b, v)
        }
        return b.toBytes()
    }

    private fun bindPlaceholders(stmt: PreparedStatement, c: Packets.Cursor): String {
        if (stmt.paramCount == 0) return stmt.sql

        val nullBitmapLen = (stmt.paramCount + 7) / 8
        if (c.remaining < nullBitmapLen + 1) {
            throw SqlException(MySqlErrors.ER_PARSE_ERROR, "HY000", "COM_STMT_EXECUTE 参数不足")
        }
        val nullBitmap = c.bytes(nullBitmapLen)
        val newParamsBound = c.u8()
        val isNull = { i: Int -> (nullBitmap[i / 8].toInt() and (1 shl (i % 8))) != 0 }
        if (newParamsBound != 0) {
            // 每个参数两个字节的类型描述，实际值紧随其后
            val types = IntArray(stmt.paramCount) { c.u16() }
            val values = (0 until stmt.paramCount).map { i ->
                if (isNull(i)) "NULL" else readBinaryParam(c, types[i])
            }
            return substitute(stmt.sql, values)
        }
        // 未重新绑定类型：参数仍在服务器的上次绑定中（本实现不支持，保守报错）
        throw SqlException(
            MySqlErrors.ER_NOT_SUPPORTED_YET, "HY000",
            "本实现要求每次 COM_STMT_EXECUTE 都重新绑定参数类型（设计文档 §11.1.1 陷阱 T2）"
        )
    }

    private fun substitute(sql: String, values: List<String>): String {
        val sb = StringBuilder(sql.length + values.sumOf { it.length })
        var vi = 0
        var i = 0
        var inStr = false
        var quote = ' '
        while (i < sql.length) {
            val ch = sql[i]
            if (inStr) {
                sb.append(ch)
                if (ch == quote && (i == 0 || sql[i - 1] != '\\')) inStr = false
                i++
                continue
            }
            when {
                ch == '\'' || ch == '"' -> { inStr = true; quote = ch; sb.append(ch) }
                ch == '?' -> { sb.append(values.getOrElse(vi++) { "NULL" }) }
                else -> sb.append(ch)
            }
            i++
        }
        return sb.toString()
    }

    /**
     * 把一个二进制协议参数渲染成 SQL 字面量。
     *
     * ## ⚠️ 两个曾经的缺陷，都属于"静默给错结果"
     *
     * 1. **有符号读**：参数是按**有符号**整数传的（`ps.setInt(1, -5)` 的线上是补码）。
     *    曾经用 `u8/u16/u32` 无符号读，`-5` 被渲染成 `4294967291`
     *    —— `WHERE int_col > ?` 于是返回**另一批行**（或空集），且不报错。
     * 2. **转义必须连反斜杠一起处理**：词法层认 `\'` 是转义的单引号，
     *    所以只把 `'` 变成 `''` 是不够的——参数 `\' OR 1=1 -- ` 渲染出来会让
     *    引号提前闭合，**条件退化成恒真**（SQL 注入）。
     */
    private fun readBinaryParam(c: Packets.Cursor, type: Int): String = when (type) {
        1 -> c.u8().toByte().toString()               // TINY（有符号）
        2, 13 -> c.u16().toShort().toString()         // SHORT, YEAR
        3, 9 -> c.u32().toInt().toString()            // LONG, INT24
        8 -> c.u64().toString()                       // LONGLONG（已经是补码语义）
        4 -> java.lang.Float.intBitsToFloat(c.u32().toInt()).toString()
        5 -> java.lang.Double.longBitsToDouble(c.u64()).toString()
        0xFB -> "NULL"
        else -> {
            val b = c.lenencBytes() ?: return "NULL"
            // 会话字符集解码（不是硬编码 UTF-8）：`SET NAMES latin1` 之后
            // 客户端按 latin1 发参数，按 UTF-8 解会得到一串 U+FFFD
            val text = session.charset.decode(b)
            "'" + escapeSqlString(text) + "'"
        }
    }

    /**
     * 把字符串渲染进单引号字面量。
     *
     * **必须同时转义反斜杠与单引号**：词法层把 `\x` 当转义序列（`\'` 是字面单引号），
     * 所以先转义 `\` 再转义 `'`，顺序反了会把新加的反斜杠再转义一次。
     */
    private fun escapeSqlString(text: String): String =
        text.replace("\\", "\\\\").replace("'", "''")

    // ── 结果集编码 ────────────────────────────────────────────────

    private fun sendResultSet(rs: ResultSet, moreResults: Boolean = false) {
        val n = rs.columns.size
        output.writePacket(Packets.Buf(16).lenenc(n.toLong()).toBytes())

        val declared = rs.declaredTypes
        for ((i, name) in rs.columns.withIndex()) {
            output.writePacket(columnDefinition("def", null, "", name, columnKind(rs, i, declared), 0))
        }
        if (n > 0) sendColumnSectionEnd()

        for (row in rs.rows) {
            val b = Packets.Buf(64)
            for (i in 0 until n) {
                val v = row.getOrNull(i) ?: Datum.Null
                if (v is Datum.Null) b.u8(0xFB)
                // 二进制列发**原始字节**：它不是文本，经过字符集编码会被破坏
                // （而且 `Datum.Bin.asString()` 给的是 `0x…` 的十六进制表示，
                //  客户端拿到的就完全不是它存进去的东西）
                else if (v is Datum.Bin) b.lenencBytes(v.v)
                // 其余按会话字符集编码。latin1/binary 会话下遇到中文会**报错**，
                // 而不是替换成 '?' 之后让客户端拿到一个内容已经不对的字符串
                else b.lenencBytes(session.charset.encode(textValue(v, declared.getOrNull(i))))
            }
            output.writePacket(b.toBytes())
        }
        sendResultTerminator(moreResults)
    }

    private fun sendBinaryResultSet(rs: ResultSet) {
        val n = rs.columns.size
        output.writePacket(Packets.Buf(16).lenenc(n.toLong()).toBytes())
        val declared = rs.declaredTypes
        for ((i, name) in rs.columns.withIndex()) {
            output.writePacket(columnDefinition("def", null, "", name, columnKind(rs, i, declared), 0))
        }
        if (n > 0) sendColumnSectionEnd()

        for (row in rs.rows) {
            // 与游标取数共用同一段编码：两处各写一份迟早会漂移，
            // 而二进制行的 NULL 位图与长度一旦算错，客户端读到的就是错位的列
            output.writePacket(binaryRow(row, n))
        }
        sendResultTerminator()
    }

    private fun writeBinaryValue(b: Packets.Buf, v: Datum) {
        when (v) {
            is Datum.I64 -> b.u64(v.v)
            is Datum.Bool -> b.u8(if (v.v) 1 else 0)
            is Datum.F64 -> b.u64(java.lang.Double.doubleToLongBits(v.v))
            is Datum.Ts -> writeBinaryDatetime(b, v.millis)
            is Datum.Dec -> b.lenencStr(v.toString())
            is Datum.Str -> b.lenencBytes(session.charset.encode(v.v))
            is Datum.Bin -> b.lenencBytes(v.v)
            Datum.Null -> {}
        }
    }

    private fun writeBinaryDatetime(b: Packets.Buf, millis: Long) {
        // ⚠️ 必须是 UTC：时间字面量的解析（`Datum.parseDateTime`）按 UTC，
        // 这里若按服务器时区渲染，写进去 10:00 会读出来 18:00（UTC+8 的机器上）
        // ——**往返漂移**，不报错。见 `DateTimeRoundTripTest`。
        val dt = java.time.Instant.ofEpochMilli(millis).atZone(java.time.ZoneOffset.UTC)
        val micros = (millis % 1000) * 1000
        if (micros == 0L) {
            b.u8(7); b.u16(dt.year); b.u8(dt.monthValue); b.u8(dt.dayOfMonth)
            b.u8(dt.hour); b.u8(dt.minute); b.u8(dt.second)
        } else {
            b.u8(11); b.u16(dt.year); b.u8(dt.monthValue); b.u8(dt.dayOfMonth)
            b.u8(dt.hour); b.u8(dt.minute); b.u8(dt.second)
            b.u32(micros)
        }
    }

    /**
     * 结果列的 MySQL 类型：**声明类型优先**，推断兜底。
     *
     * 声明类型来自执行器（`ResultSet.declaredTypes`，例如"这一列就是 DATE 列"）。
     * 没有它时退回按值推断——那条路对 `DATE` 与 `DATETIME` 分辨不出来
     * （两者都是 `Datum.Ts`），于是 DATE 会被声明成 DATETIME、
     * 渲染出多余的时刻部分。
     */
    private fun columnKind(rs: ResultSet, col: Int, declared: List<ColumnType?>): ColumnType {
        val d = declared.getOrNull(col)
        // ⚠️ 只对**时间族**采用声明类型，其余仍按值推断。
        //
        // 理由：JDBC 驱动是按"声明的列类型"决定返回哪个 Java 类的——
        // 一个 INT 列会返回 Integer、BIGINT 列返回 Long。按值推断时所有整数列
        // 一律报 BIGINT（读出来都是 Long），换成真实宽度会让一堆既有代码
        // （和测试断言）拿到 Integer。那是**行为变更**，不是修缺陷。
        //
        // 而时间族必须用声明类型：DATE 与 DATETIME 在内存里都是 `Datum.Ts`，
        // 按值推断会把 DATE 也报成 DATETIME，于是 `SELECT d FROM t`
        // 返回 `2026-02-01 00:00:00` 而不是 `2026-02-01`。
        return if (d == ColumnType.DATE || d == ColumnType.DATETIME || d == ColumnType.TIMESTAMP) d
        else inferType(rs, col)
    }

    /** 从一个非空样本值推断结果列的 MySQL 类型。全为 NULL 时退化为 VAR_STRING。 */
    private fun inferType(rs: ResultSet, col: Int): ColumnType {
        probe@ for (row in rs.rows) {
            val v = row.getOrNull(col) ?: continue@probe
            return when (v) {
                is Datum.Null -> continue@probe
                is Datum.I64 -> ColumnType.BIGINT
                is Datum.Bool -> ColumnType.BOOLEAN
                is Datum.F64 -> ColumnType.DOUBLE
                is Datum.Ts -> ColumnType.DATETIME
                is Datum.Dec -> ColumnType.DECIMAL
                is Datum.Bin -> ColumnType.BLOB
                is Datum.Str -> ColumnType.VARCHAR
            }
        }
        return ColumnType.VARCHAR
    }

    /**
     * 文本协议的取值渲染。
     *
     * [declared] 是这一列的声明类型（可能为 null）。`DATE` 列只渲染日期部分——
     * 少了这一步，`SELECT d FROM t`（d 是 DATE）会返回 `2026-02-01 00:00:00`，
     * 而客户端那边 `getString` 拿到的就是这一串多余的零时刻。
     */
    private fun textValue(v: Datum, declared: ColumnType? = null): String = when (v) {
        is Datum.Null -> ""
        is Datum.Bool -> if (v.v) "1" else "0"
        // 与 `writeBinaryDatetime` 同一条规则：**UTC**。两者的口径必须一致，
        // 否则同一个查询走文本协议与二进制协议会得到不同的时刻
        is Datum.Ts -> if (declared == ColumnType.DATE) Datum.formatDate(v.millis) else Datum.formatDateTime(v.millis)
        is Datum.Dec -> v.toString()
        else -> v.asString()
    }

    private fun columnDefinition(
        catalog: String,
        table: TableSchema?,
        tableName: String,
        name: String,
        type: ColumnType,
        flags: Int,
        /**
         * `COM_FIELD_LIST` 专用：包尾要**再跟一个"列的默认值"**（长度编码字符串，
         * `0xFB` 表示 NULL）。结果集的列定义里**没有**这一段。
         *
         * 这是实测出来的（对照真实 MySQL 8.0 的字节）：`COM_FIELD_LIST` 的应答
         * 与结果集的列定义不是同一种包——少了这段，客户端读默认值时越界，
         * 报 `ERROR 2027 Malformed packet`，然后掐连接。
         */
        fieldListDefault: String? = null,
        includeDefault: Boolean = false,
    ): ByteArray {
        val b = Packets.Buf(128)
        b.lenencStr(catalog)
        b.lenencStr(if (table != null) "" else tableName)
        b.lenencStr(if (table != null) "" else tableName)
        b.lenencStr(if (table != null) "" else tableName)
        b.lenencStr(name)
        b.lenencStr(name)
        b.u8(0x0c)                       // 固定填充
        b.u16(session.charset.collationId)   // 字符集（随会话，默认 utf8mb4）
        b.u32(1024)                      // column length
        b.u8(mysqlType(type))            // type
        b.u16(flags)                     // flags
        b.u8(0)                          // decimals
        b.u16(0)                         // filler
        if (includeDefault) {
            if (fieldListDefault == null) b.u8(0xFB) else b.lenencStr(fieldListDefault)
        }
        return b.toBytes()
    }

    private fun paramDefinition(i: Int): ByteArray = columnDefinition("def", null, "", "?", ColumnType.VARCHAR, 0)

    private fun mysqlType(t: ColumnType): Int = when (t) {
        ColumnType.TINYINT -> 1
        ColumnType.SMALLINT -> 2
        ColumnType.INT -> 3
        ColumnType.BIGINT -> 8
        ColumnType.FLOAT -> 4
        ColumnType.DOUBLE -> 5
        ColumnType.DECIMAL -> 246
        ColumnType.BOOLEAN -> 1
        ColumnType.VARCHAR -> 253
        ColumnType.TEXT -> 252
        ColumnType.BLOB -> 252
        ColumnType.DATE -> 10
        ColumnType.DATETIME, ColumnType.TIMESTAMP -> 12
        // 下面这几个是 MySQL 的编号，写错不会报错——客户端只会按错的类型去解列值
        ColumnType.YEAR -> 13
        ColumnType.ENUM -> 247
        ColumnType.SET -> 248
        ColumnType.JSON -> 245
    }

    // ── 响应包 ────────────────────────────────────────────────────

    private fun statusFlags(moreResults: Boolean = false): Int {
        var f = 0
        if (session.inTransaction) f = f or Packets.SERVER_STATUS_IN_TRANS
        if (session.autocommit) f = f or Packets.SERVER_STATUS_AUTOCOMMIT
        if (moreResults) f = f or Packets.SERVER_MORE_RESULTS_EXISTS
        return f
    }

    /**
     * OK 包。
     *
     * ⚠️ **`info` 字段不发**。按协议它有两种编码：协商了 `CLIENT_SESSION_TRACK`
     * 时是长度编码字符串，否则是「到包尾」的裸字符串——而不同客户端在这上面的
     * 判断不尽一致。实测 Connector/J 会把它当长度编码读，于是 "Database ... created"
     * 的首字节 'D'(0x44) 被当成 68 字节的长度，直接报
     * "Buffer length is less than expected payload length"（实现期间踩到的真实缺陷）。
     *
     * 该字段纯属提示信息（"N rows affected"），删掉不影响任何客户端的正确性，
     * 却消除了一个跨客户端的协议歧义。需要提示时走 SELECT 结果集即可。
     */
    private fun sendOk(
        affected: Long,
        lastInsertId: Long,
        @Suppress("UNUSED_PARAMETER") info: String = "",
        moreResults: Boolean = false,
    ) {
        val b = Packets.Buf(32)
        b.u8(0x00)
        b.lenenc(affected)
        b.lenenc(lastInsertId)
        b.u16(statusFlags(moreResults))
        b.u16(0)                          // warnings
        output.writePacket(b.toBytes())
    }

    /**
     * **列定义段**的收尾。
     *
     * ⚠️ 协商了 `CLIENT_DEPRECATE_EOF` 时这里**什么都不发**——这是这份协议规范里
     * 最反直觉的一处：客户端已经知道有几个列（列数包说了），列定义读满就结束了，
     * 那个"分段用的 EOF"正是 DEPRECATE_EOF 要删掉的东西。
     *
     * 多发一个 OK 包在这里的后果不是报错，而是**结果集变成空的**：
     * 客户端把紧跟列定义的那个 OK 当成"结果集到此为止"（OK 的含义本来就是
     * "没有结果集，只有状态"），于是 `rs.next()` 直接返回 false。
     * 这个坑是靠 `ProtocolCompatTest` 里那条"协商 DEPRECATE_EOF 的连接能正常取数"
     * 抓出来的——不协商的连接走的是老路径，完全看不到它。
     */
    private fun sendColumnSectionEnd() {
        if (capabilities and Packets.CLIENT_DEPRECATE_EOF != 0) return
        sendResultTerminator()
    }

    /**
     * 结果集的**收尾包**。
     *
     * 协商了 `CLIENT_DEPRECATE_EOF` 时它是「0xFE 开头的 OK 包」，否则是传统的 EOF 包。
     * 两者的区别不只是形状：OK 包能把 `affected_rows` / `last_insert_id` /
     * `SERVER_MORE_RESULTS_EXISTS` 一并带回来，**每个结果集少一个包的往返**。
     *
     * ⚠️ 两个包都必须是**短包**（< 9 字节）：文本协议的行也可能以 0xFE 开头
     * （长度编码的 8 字节前缀），客户端靠"长度是否小于 9"来区分收尾包与行。
     * 我们这里固定 7 字节，安全。
     */
    private fun sendResultTerminator(moreResults: Boolean = false) {
        val b = Packets.Buf(8)
        b.u8(0xFE)
        if (capabilities and Packets.CLIENT_DEPRECATE_EOF != 0) {
            // OK 包的形状：0xFE + affected_rows + last_insert_id + status + warnings
            b.lenenc(0)
            b.lenenc(0)
        } else {
            b.u16(0)                      // warnings（EOF 只有 warnings + status）
            b.u16(statusFlags(moreResults))
            output.writePacket(b.toBytes())
            return
        }
        b.u16(statusFlags(moreResults))
        b.u16(0)                          // warnings
        output.writePacket(b.toBytes())
    }

    private fun sendError(code: Int, sqlState: String, message: String) {
        val b = Packets.Buf(64)
        b.u8(0xFF)
        b.u16(code)
        if (capabilities and Packets.CLIENT_PROTOCOL_41 != 0) {
            b.u8('#'.code)
            b.bytes(sqlState.padEnd(5).take(5).toByteArray(Charsets.US_ASCII))
        }
        b.bytes(message.toByteArray(Charsets.UTF_8))
        output.writePacket(b.toBytes())
    }

    private fun readU32(b: ByteArray, off: Int): Int =
        (b[off].toInt() and 0xFF) or ((b[off + 1].toInt() and 0xFF) shl 8) or
            ((b[off + 2].toInt() and 0xFF) shl 16) or ((b[off + 3].toInt() and 0xFF) shl 24)

    /** 统计 `?` 占位符个数（跳过字符串字面量内的 `?`）。 */
    private fun countPlaceholders(sql: String): Int {
        var n = 0
        var inStr = false
        var quote = ' '
        var i = 0
        while (i < sql.length) {
            val ch = sql[i]
            if (inStr) {
                if (ch == '\\') { i += 2; continue }
                if (ch == quote) inStr = false
            } else {
                when (ch) {
                    '\'', '"', '`' -> { inStr = true; quote = ch }
                    '?' -> n++
                    '-' -> if (i + 1 < sql.length && sql[i + 1] == '-') {
                        while (i < sql.length && sql[i] != '\n') i++
                    }
                    '#' -> while (i < sql.length && sql[i] != '\n') i++
                }
            }
            i++
        }
        return n
    }
}
