package com.cloudea.yangbase.ms

import com.cloudea.yangbase.common.SqlException
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json
import java.io.DataInputStream
import java.io.DataOutputStream
import java.net.InetSocketAddress
import java.net.ServerSocket
import java.net.Socket
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.concurrent.thread

/**
 * CN ↔ MS 的 RPC 协议（设计文档 §4）。
 *
 * ## 为什么是手写的长度前缀 JSON，而不是 gRPC
 *
 * 设计文档 §15 的技术栈表写的是「gRPC + Protobuf」。这里改用 Netty 之外的最小实现，
 * 理由是四条，且都是这个代码库自己的先例：
 *
 * 1. **协议面很小**：CN→MS 只有 7 个方法（授时 / 租约三件 / 锁三件 / catalog 版本），
 *    MS↔MS 只有 3 个（RequestVote / AppendEntries / InstallSnapshot）。
 *    gRPC 的流式、多路复用、拦截器在这里一个都用不上。
 * 2. **这个代码库一贯手写协议**：MySQL 线协议（`yb-mysql`）、SST 格式、SQL 解析器
 *    都是手写的，理由都是「格式要精确可控、错误信息要能自己写」。引入 protoc
 *    代码生成会给构建加上一份与源码平行的"生成物"，而它的调试成本远高于这 200 行。
 * 3. **依赖风险**：gRPC 需要 protobuf 插件 + protoc 二进制 + grpc-kotlin 两套运行库。
 *    本机 Gradle 缓存里已有 netty 与 protobuf，但**构建期的代码生成**是这个项目
 *    目前完全没有的基础设施。
 * 4. **可测试性**：协议是一个纯函数（请求对象 → 响应对象），进程内传输直接调用它，
 *    于是"3 个 MS + 2 个 CN"的故障切换测试可以在一个 JVM 里确定性地跑（见 [MetaTransport]）。
 *
 * 代价是**不与 gRPC 客户端互通**。MS 是纯内部服务、没有外部消费者，所以这个代价
 * 目前为零。若日后要开放 MS 协议，替换的边界就是 [MetaTransport] 这一个接口。
 *
 * ## 传输层只有三个实现
 *
 * - [InProcessMetaTransport]：进程内直连。单机部署与**全部测试**用它。
 * - [TcpMetaTransport]：多进程部署。长度前缀 + JSON，**一次请求一条连接**。
 *   请求频率很低（授时 64 秒一次 / 租约每 TTL 一次 / 锁只在 `FOR UPDATE` 时），
 *   所以连接池的复杂度现在不值得付；这是"低频定位"带来的红利。
 * - 测试用的故障注入包装见 [FlakyTransport]。
 */

/** CN → MS 的请求。 */
@Serializable
sealed interface MetaRequest {
    /**
     * 预留一段**时间戳区间**（§4.2）。
     *
     * 注意它预留的是区间而不是"若干个号"：段的跨度按毫秒算，见
     * `yb-txn` 里 [com.cloudea.yangbase.txn.TimestampSource] 的说明。
     */
    @Serializable
    data class ReserveTimestamp(val atLeast: Long, val span: Long) : MetaRequest

    /** 申请分片写者租约（§4.3）。 */
    @Serializable
    data class AcquireLease(val shard: String, val holder: String, val ttlMillis: Long) : MetaRequest

    /** 续期。epoch 不变——续期是同一个写者继续持有。 */
    @Serializable
    data class RenewLease(val shard: String, val holder: String, val epoch: Long, val ttlMillis: Long) : MetaRequest

    @Serializable
    data class ReleaseLease(val shard: String, val holder: String, val epoch: Long) : MetaRequest

    /** 加锁（行锁 / 表锁，§4.4）。 */
    @Serializable
    data class Lock(
        val tableId: Long,
        val pkHash: Long?,
        val mode: String,
        val owner: String,
        val ttlMillis: Long,
        val waitMillis: Long,
    ) : MetaRequest

    @Serializable
    data class Unlock(val token: String) : MetaRequest

    /** 读 catalog 版本（§4.5 的失效判据）。 */
    @Serializable
    data object CatalogVersion : MetaRequest

    /**
     * 读当前时间戳水位（**不推进**）。
     *
     * CN 每次启动都要问一次：不读的话，重启后的 CN 会从自己的局部状态继续发号，
     * 而那个值可能**低于**崩溃前（或别的 CN）已经发出去的——时间戳回退是静默的
     * MVCC 事故（见 `yb-txn` 的 `WindowTso.recover`）。
     */
    @Serializable
    data object ReadWatermark : MetaRequest

    /** DDL 提交后推进版本，让所有 CN 的 schema 缓存失效。 */
    @Serializable
    data object BumpCatalogVersion : MetaRequest
}

/** MS → CN 的响应。**每个响应都带 [term]**，客户端据此发现"换了领导"。 */
@Serializable
sealed interface MetaResponse {
    /** 领导者的任期。客户端凡是拿到更大的 term，就必须认为旧的锁与租约**可能已经失效**。 */
    val term: Long

    /** 我不是领导者，去 [leaderHint]（`host:port`，未知时为空）找它。 */
    @Serializable
    data class NotLeader(override val term: Long, val leaderHint: String = "") : MetaResponse

    @Serializable
    data class TimestampReserved(override val term: Long, val base: Long, val through: Long) : MetaResponse

    @Serializable
    data class LeaseGranted(override val term: Long, val epoch: Long, val expiresAtMillis: Long) : MetaResponse

    @Serializable
    data class LeaseRenewed(override val term: Long, val epoch: Long, val expiresAtMillis: Long) : MetaResponse

    @Serializable
    data class LeaseReleased(override val term: Long) : MetaResponse

    @Serializable
    data class LockGranted(override val term: Long, val token: String, val expiresAtMillis: Long) : MetaResponse

    @Serializable
    data class LockReleased(override val term: Long) : MetaResponse

    @Serializable
    data class CatalogVersionValue(override val term: Long, val version: Long) : MetaResponse

    @Serializable
    data class WatermarkValue(override val term: Long, val watermark: Long) : MetaResponse

    /**
     * 业务错误（等待超时、死锁、锁不属于你……）。
     *
     * 错误**不**通过传输层的异常传递：MS 的"拒绝"是一种正常的响应，
     * 而把 MySQL 错误码原样带回来是让上层能用同一套语义处理（1205 / 1213 都是
     * 客户端要重试或回滚的信号）。
     */
    @Serializable
    data class Failure(override val term: Long, val code: Int, val message: String) : MetaResponse

    /** MS 内部故障（日志写不进去等）。与 [Failure] 分开：这个应当重试，那个不该。 */
    @Serializable
    data class Error(override val term: Long, val message: String) : MetaResponse
}

/** MS↔MS 的 Raft 消息（§4.6）。 */
@Serializable
sealed interface RaftMessage {
    @Serializable
    data class RequestVote(
        val term: Long,
        val candidateId: String,
        val lastLogIndex: Long,
        val lastLogTerm: Long,
    ) : RaftMessage

    @Serializable
    data class VoteResponse(val term: Long, val granted: Boolean) : RaftMessage

    @Serializable
    data class AppendEntries(
        val term: Long,
        val leaderId: String,
        val prevLogIndex: Long,
        val prevLogTerm: Long,
        val entries: List<LogEntry>,
        val leaderCommit: Long,
    ) : RaftMessage

    @Serializable
    data class AppendResponse(val term: Long, val success: Boolean, val matchIndex: Long) : RaftMessage

    /**
     * 把整个快照发给落在后面的跟随者。
     *
     * 这里不做分块安装：MS 的状态只有「时间戳水位 + 租约表 + catalog 版本」，
     * 快照是**几 KB**，一次发完比实现分块流简单得多，也更不容易出错。
     * 分块安装是给"状态远大于一次 RPC"的系统准备的。
     */
    @Serializable
    data class InstallSnapshot(
        val term: Long,
        val leaderId: String,
        val lastIncludedIndex: Long,
        val lastIncludedTerm: Long,
        val snapshot: ByteArray,
    ) : RaftMessage {
        override fun equals(other: Any?) = other is InstallSnapshot && other.term == term &&
            other.lastIncludedIndex == lastIncludedIndex && other.lastIncludedTerm == lastIncludedTerm &&
            other.leaderId == leaderId && snapshot.contentEquals(other.snapshot)

        override fun hashCode() = listOf(term, lastIncludedIndex, lastIncludedTerm).hashCode()
    }

    @Serializable
    data class InstallSnapshotResponse(val term: Long, val success: Boolean) : RaftMessage
}

/** 一条 Raft 日志项。 */
@Serializable
data class LogEntry(
    val index: Long,
    val term: Long,
    val request: MetaRequest? = null,
    /**
     * 领导者生成这条日志时的物理毫秒。
     *
     * **必须随日志项一起复制**，不能由各副本在应用时各自读时钟：租约的到期时刻
     * 由它算出，而"这个分片有没有写者"必须是一个**全系统唯一**的判断。
     * 各节点各自读时钟会让租约表慢慢漂移——同一个租约在一台上已过期、在另一台上没有。
     */
    val issuedAt: Long = 0,
    /** 仅供诊断：这条日志是为什么产生的。 */
    val note: String = "",
)

/**
 * MS 的传输接口。
 *
 * 它是**唯一**需要替换就能换掉 RPC 实现的边界（见文件头的说明）。
 */
interface MetaTransport {
    /** 发送一个客户端请求。失败时抛异常（调用方负责降级）。 */
    suspend fun call(target: String, request: MetaRequest): MetaResponse

    /** 发送一个 Raft 消息。 */
    suspend fun raft(target: String, message: RaftMessage): RaftMessage
}

/**
 * 进程内传输：把请求**直接**交给对应地址的 MS 节点。
 *
 * 单机部署与全部测试都用它。它让"3 个 MS 节点"可以活在一个 JVM 里，
 * 于是故障切换测试是**确定性**的——不需要等 TCP 超时，也不需要真起三个进程。
 */
class InProcessMetaTransport : MetaTransport {
    private val servers = java.util.concurrent.ConcurrentHashMap<String, MetaRpcHandler>()

    fun register(address: String, handler: MetaRpcHandler) {
        servers[address] = handler
    }

    fun unregister(address: String) {
        servers.remove(address)
    }

    override suspend fun call(target: String, request: MetaRequest): MetaResponse {
        val h = servers[target] ?: throw MetaUnreachableException(target)
        return h.handle(request)
    }

    override suspend fun raft(target: String, message: RaftMessage): RaftMessage {
        val h = servers[target] ?: throw MetaUnreachableException(target)
        return h.handleRaft(message)
    }
}

/** MS 收到请求后要做的事。由 `MetaServiceNode` 实现。 */
interface MetaRpcHandler {
    suspend fun handle(request: MetaRequest): MetaResponse

    suspend fun handleRaft(message: RaftMessage): RaftMessage
}

/** 连不上目标 MS。**这是可重试的**：调用方应当降级或改用别的节点。 */
class MetaUnreachableException(val target: String, cause: Throwable? = null) :
    Exception("无法连接元数据服务节点 $target", cause)

/**
 * 集群**没有领导者**（选举中或多数派不可用）。
 *
 * 与 [MetaUnreachableException] 分开是因为两者的处置完全不同：这个等一会儿就好，
 * 那个要去看进程与网络。把它们混成一句"无法连接"会让排查方向错得很远。
 */
class MetaNoLeaderException(val targets: String, message: String) : Exception(message)

/** 传输层的 JSON（放这里而不是各文件各写一份：线上格式只能有一处定义）。 */
internal val MS_JSON = Json {
    ignoreUnknownKeys = true
    encodeDefaults = true
    classDiscriminator = "type"
}

/**
 * TCP 传输：长度前缀 + JSON，一次请求一条连接。
 *
 * 请求频率很低（见文件头），所以连接池现在不值得付。连接超时短（默认 2s）是刻意的：
 * MS 不可用时，**快速失败**比长时间挂起更有价值——上层要么降级（只读继续走），
 * 要么明确报错，而不是让客户端看起来"卡住了"。
 */
class TcpMetaTransport(
    private val connectTimeoutMillis: Int = 2_000,
    private val readTimeoutMillis: Int = 5_000,
) : MetaTransport {

    override suspend fun call(target: String, request: MetaRequest): MetaResponse =
        roundTrip(target, MS_JSON.encodeToString(MetaRequest.serializer(), request)) { text ->
            MS_JSON.decodeFromString(MetaResponse.serializer(), text)
        }

    override suspend fun raft(target: String, message: RaftMessage): RaftMessage =
        roundTrip(target, MS_JSON.encodeToString(RaftMessage.serializer(), message)) { text ->
            MS_JSON.decodeFromString(RaftMessage.serializer(), text)
        }

    private suspend fun <T> roundTrip(target: String, payload: String, decode: (String) -> T): T =
        withContext(Dispatchers.IO) {
            val (host, port) = parseAddress(target)
            try {
                Socket().use { socket ->
                    socket.connect(InetSocketAddress(host, port), connectTimeoutMillis)
                    socket.soTimeout = readTimeoutMillis
                    val out = DataOutputStream(socket.getOutputStream())
                    val bytes = payload.toByteArray(Charsets.UTF_8)
                    out.writeInt(bytes.size)
                    out.write(bytes)
                    out.flush()
                    val inp = DataInputStream(socket.getInputStream())
                    val len = inp.readInt()
                    val buf = ByteArray(len)
                    inp.readFully(buf)
                    decode(String(buf, Charsets.UTF_8))
                }
            } catch (e: Exception) {
                throw MetaUnreachableException(target, e)
            }
        }

    companion object {
        fun parseAddress(address: String): Pair<String, Int> {
            val i = address.lastIndexOf(':')
            require(i > 0) { "MS 地址格式应为 host:port，实际是 '$address'" }
            return address.substring(0, i) to address.substring(i + 1).toInt()
        }
    }
}

/**
 * TCP 服务端：接收 [TcpMetaTransport] 发来的请求。
 *
 * 阻塞 IO 放在 [Dispatchers.IO] 上（每个连接一个协程），**不在 Netty EventLoop 上**——
 * MS 不是数据面，它不需要为万级并发连接优化；而把阻塞 socket 放进协程池是这个代码库
 * 已经在缓存层用过的隔离方式。
 */
class TcpMetaServer(
    private val bindHost: String,
    private val bindPort: Int,
    private val handler: MetaRpcHandler,
) {
    private var serverSocket: ServerSocket? = null
    private val running = AtomicBoolean(false)

    val port: Int get() = serverSocket?.localPort ?: bindPort

    fun start() {
        val ss = ServerSocket()
        ss.reuseAddress = true
        ss.bind(InetSocketAddress(bindHost, bindPort))
        serverSocket = ss
        running.set(true)
        thread(name = "yb-ms-accept", isDaemon = true) {
            while (running.get()) {
                val socket = try {
                    ss.accept()
                } catch (_: Exception) {
                    break            // close() 会让 accept 抛异常，这是正常的退出路径
                }
                thread(name = "yb-ms-conn", isDaemon = true) { serve(socket) }
            }
        }
    }

    private fun serve(socket: Socket) {
        socket.use {
            try {
                val inp = DataInputStream(it.getInputStream())
                val out = DataOutputStream(it.getOutputStream())
                val len = inp.readInt()
                val buf = ByteArray(len)
                inp.readFully(buf)
                val text = String(buf, Charsets.UTF_8)
                val response: String = kotlinx.coroutines.runBlocking {
                    dispatch(text)
                }
                val bytes = response.toByteArray(Charsets.UTF_8)
                out.writeInt(bytes.size)
                out.write(bytes)
                out.flush()
            } catch (_: Exception) {
                // 单个连接的失败不影响服务：调用方会收到 MetaUnreachable 并重试
            }
        }
    }

    private suspend fun dispatch(text: String): String {
        // 先按 Raft 消息试，再按客户端请求试。两者都是 sealed + type 判别字段，
        // 所以"试错"是安全的：解析失败只会抛异常，不会解析成另一个类型
        return try {
            val raft = MS_JSON.decodeFromString(RaftMessage.serializer(), text)
            MS_JSON.encodeToString(RaftMessage.serializer(), handler.handleRaft(raft))
        } catch (_: Exception) {
            val req = MS_JSON.decodeFromString(MetaRequest.serializer(), text)
            MS_JSON.encodeToString(MetaResponse.serializer(), handler.handle(req))
        }
    }

    fun stop() {
        running.set(false)
        runCatching { serverSocket?.close() }
    }
}

/**
 * 故障注入包装：测试"MS 不可用时 CN 怎么表现"（§4.2 的降级语义）。
 *
 * 与 `yb-objectstore` 里的注入器同一个思路：**故障必须是可编程的**，
 * 否则"降级路径"永远只有上线后才会第一次被执行。
 */
class FlakyTransport(
    private val delegate: MetaTransport,
    /** 目标地址 → 是否断链。 */
    private val down: MutableSet<String> = java.util.Collections.synchronizedSet(mutableSetOf()),
) : MetaTransport {

    fun breakLink(target: String) = down.add(target)

    fun restoreLink(target: String) = down.remove(target)

    override suspend fun call(target: String, request: MetaRequest): MetaResponse {
        if (target in down) throw MetaUnreachableException(target)
        return delegate.call(target, request)
    }

    override suspend fun raft(target: String, message: RaftMessage): RaftMessage {
        if (target in down) throw MetaUnreachableException(target)
        return delegate.raft(target, message)
    }
}

/** 把 MS 返回的 [MetaResponse.Failure] 变成异常（上层只认 MySQL 错误码）。 */
fun MetaResponse.Failure.toException(): SqlException = SqlException(code, "HY000", message)
