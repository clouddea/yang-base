package com.cloudea.yangbase.objectstore

import com.cloudea.yangbase.common.Checksums
import com.cloudea.yangbase.common.ConcurrentModificationException
import com.cloudea.yangbase.common.ObjectNotFoundException
import com.cloudea.yangbase.common.ObjectStoreException
import com.cloudea.yangbase.common.PreconditionFailedException
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardCopyOption
import java.nio.file.StandardOpenOption
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

/**
 * 本地文件系统后端。
 *
 * **唯一目的是让开发与单元测试不需要真实对象存储。生产环境必须禁止使用**——
 * 因为「本地 FS 通过测试但生产依赖对象存储语义」是最危险的一类缺陷来源（设计文档 §3.3）。
 *
 * 把关**不在这里**，而在进程的启动路径上（`yb-server` 的
 * `--i-know-this-is-not-durable`，见 `YangBaseServer.requireDurableStorageUnlessAllowed`）：
 * 后端自己无法回答「现在是不是生产环境」，把守卫放在这里只会变成一个所有测试都得
 * 传一遍的布尔参数——而每个测试都传 `true` 之后，它就再也不会拦住任何人了。
 * 放在启动路径上，守卫拦的是**运维动作**，那才是它该拦的东西。
 *
 * 已知与真实对象存储的差异（有意为之，不试图模拟）：
 *  - 条件写的原子性只在**单进程内**成立（用进程内锁实现），跨进程不保证
 *  - 没有 multipart、没有服务端拷贝、没有事件通知
 */
class LocalFileObjectStore(
    private val root: Path,
) : ObjectStore {

    override val caps = ObjectStoreCapabilities(
        conditionalCreate = true,
        conditionalUpdate = true,
        strongReadAfterWrite = true,
        strongListConsistency = true,
        lowLatencyTier = true,
        lowLatencyTierSupportsConditionalWrite = true,
        conditionalWriteVerified = true,
        requestsAreBilled = false,
        serverSideCopy = true,
        eventNotification = false,
    )

    private val locks = ConcurrentHashMap<String, ReentrantLock>()

    init {
        Files.createDirectories(root)
    }

    private fun pathOf(key: String): Path {
        require(!key.contains("..")) { "对象键不得包含 '..': $key" }
        require(!key.startsWith("/")) { "对象键不得以 '/' 开头: $key" }
        return root.resolve(key)
    }

    /**
     * 把 IO 异常翻译成**可重试**的 [ObjectStoreException]（设计文档 §3.3.1）。
     *
     * 本地文件系统上的 `IOException` 与对象存储上的网络中断是同一类东西：调用方
     * **没有得到任何业务结论**（既不是条件不成立，也不是对象不存在），重发一次
     * 可能就好了。把它标成可重试还有一个不那么明显的好处：**重试路径本身变得
     * 可以用这个后端测**——磁盘满、文件被换成了目录，都能稳定复现"瞬时故障"，
     * 而不需要去戳真实的云。
     *
     * 反过来说，条件写的两种确定结论（[PreconditionFailedException] /
     * [ConcurrentModificationException]）与 [ObjectNotFoundException] 都很容易被
     * 误当成 IO 异常，所以它们必须在**这里之外**产生，不能落进这个 catch。
     */
    private suspend fun <T> guardIo(block: suspend () -> T): T = try {
        block()
    } catch (e: java.io.IOException) {
        throw ObjectStoreException("本地文件系统 IO 失败: ${e.message}", e, retryable = true)
    }

    /**
     * 已写出的内容的 ETag 缓存。
     *
     * 见 [etagOf] 的说明：本后端假设**单进程访问**（它本来就用进程内锁实现条件写的
     * 原子性），所以缓存不会因外部进程的写入而失效。
     */
    private val etagCache = ConcurrentHashMap<String, ETag>()

    /**
     * 计算 ETag。
     *
     * ⚠️ **不能用 `size-mtime` 这种方案**。最初就是那么写的，看起来合理，但实测
     * 5 次等长写入只产生了 3 个不同的 ETag——相同大小 + 落在同一个文件系统时间戳
     * 量子内的两次写入会碰撞。后果是**陈旧 ETag 也能通过 CAS 检查，fencing 静默失效**。
     *
     * 这个 bug 是被 `CapabilityProbe` 抓出来的（它用真实的冲突写请求实测，
     * 而不是相信实现者的假设）。修复是让 ETag 由**内容**决定。
     */
    /**
     * 算 ETag。
     *
     * ⚠️ **流式读**，不把整个对象读进内存。曾经这里是 `Files.readAllBytes(p)`：
     * 一个 `head()`（本意是"只读元信息"的廉价操作）会把 64MB 的 SST 整个读进堆——
     * 既违背 P3（元数据操作要廉价），也会在并发 HEAD 时可观地放大内存。
     * 流式读的代价仍与对象大小成正比（本地文件没有免费的哈希），
     * 但至少不再产生一份完整的内存副本。
     */
    private fun etagOf(p: Path): ETag {
        val key = p.toString()
        val cached = etagCache[key]
        if (cached != null) return cached
        // 未由本实例写过（例如重启后）：从内容算，保证正确
        var size = 0L
        val crc = java.util.zip.CRC32C()
        Files.newInputStream(p).use { input ->
            val buf = ByteArray(64 * 1024)
            while (true) {
                val n = input.read(buf)
                if (n < 0) break
                crc.update(buf, 0, n)
                size += n
            }
        }
        val etag = ETag("c-$size-${java.lang.Long.toHexString(crc.value).padStart(8, '0')}")
        etagCache[key] = etag
        return etag
    }

    override suspend fun get(key: String, range: ByteRange?): ByteArray = guardIo {
        withContext(Dispatchers.IO) {
            val p = pathOf(key)
            if (!Files.exists(p)) throw ObjectNotFoundException(key)
            if (range == null) {
                Files.readAllBytes(p)
            } else {
                val size = Files.size(p)
                if (range.start >= size) return@withContext ByteArray(0)
                val end = minOf(range.endInclusive, size - 1)
                val len = (end - range.start + 1).toInt()
                val buf = ByteArray(len)
                java.io.RandomAccessFile(p.toFile(), "r").use { raf ->
                    raf.seek(range.start)
                    raf.readFully(buf)
                }
                buf
            }
        }
    }

    override suspend fun put(key: String, data: ByteArray, precondition: Precondition?): ETag =
        guardIo {
            withContext(Dispatchers.IO) {
                val p = pathOf(key)
                val lock = locks.computeIfAbsent(key) { ReentrantLock() }
                lock.withLock {
                    Files.createDirectories(p.parent)
                    when (precondition) {
                        null -> {}
                        Precondition.MustNotExist -> {
                            if (Files.exists(p)) throw PreconditionFailedException("对象已存在: $key")
                        }
                        is Precondition.MustMatch -> {
                            if (!Files.exists(p)) throw ObjectNotFoundException(key)
                            val cur = etagOf(p)
                            if (cur != precondition.etag) {
                                throw ConcurrentModificationException(
                                    "etag 不匹配: $key 期望=${precondition.etag} 实际=$cur"
                                )
                            }
                        }
                    }
                    // 先写临时文件再原子改名，保证读者永远看不到半个对象
                    val tmp = Files.createTempFile(p.parent, ".tmp-", ".part")
                    try {
                        Files.write(tmp, data, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING)
                        try {
                            Files.move(tmp, p, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING)
                        } catch (_: java.nio.file.AtomicMoveNotSupportedException) {
                            Files.move(tmp, p, StandardCopyOption.REPLACE_EXISTING)
                        }
                    } finally {
                        Files.deleteIfExists(tmp)
                    }
                    // ETag 由内容决定：等长内容的连续写入必须产生不同的 ETag，否则 CAS 会失效
                    val etag = ETag("c-${data.size}-${Checksums.crc32cHex(data)}")
                    etagCache[p.toString()] = etag
                    etag
                }
            }
        }

    override suspend fun delete(key: String): Boolean = guardIo {
        withContext(Dispatchers.IO) {
            etagCache.remove(pathOf(key).toString())
            Files.deleteIfExists(pathOf(key))
        }
    }

    override suspend fun list(prefix: String, pageToken: String?, limit: Int): ListPage =
        guardIo {
            withContext(Dispatchers.IO) {
                val all = ArrayList<String>()
                // 遍历时顺手取大小：本地文件系统上这是一次 stat，不需要额外请求
                val sizes = HashMap<String, Long>()
                if (Files.exists(root)) {
                    Files.walk(root).use { stream ->
                        stream.filter { Files.isRegularFile(it) }
                            .forEach { p ->
                                val key = root.relativize(p).toString()
                                if (key.startsWith(prefix) && !isTemp(key)) {
                                    all.add(key)
                                    runCatching { sizes[key] = Files.size(p) }
                                }
                            }
                    }
                }
                all.sort()
                val filtered = if (pageToken == null) all else all.filter { it > pageToken }
                val page = filtered.take(limit)
                val next = if (filtered.size > limit) page.last() else null
                ListPage(page, next, page.associateWith { sizes[it] ?: 0L })
            }
        }

    private fun isTemp(key: String) = key.contains("/.tmp-") || key.startsWith(".tmp-")

    override suspend fun head(key: String): ObjectMeta = guardIo {
        withContext(Dispatchers.IO) {
            val p = pathOf(key)
            if (!Files.exists(p)) throw ObjectNotFoundException(key)
            ObjectMeta(key, Files.size(p), etagOf(p), Files.getLastModifiedTime(p).toMillis())
        }
    }

    fun rootPath(): Path = root
}
