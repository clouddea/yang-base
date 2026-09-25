package com.cloudea.yangbase.storage

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import java.nio.file.Files
import java.nio.file.Path
import java.security.MessageDigest
import java.util.concurrent.atomic.AtomicLong

/**
 * 二级缓存（本地 NVMe，设计文档 §6.7）。
 *
 * ## 它为什么是安全的
 *
 * 因为它**什么都不是**——不是状态，只是缓存。删掉整个目录，系统照常工作，
 * 只是下一次查询要回对象存储取一遍（原则 P1：本地磁盘的一切内容必须可丢弃）。
 *
 * 这是它敢存在的前提。一旦有任何「本地有、对象存储没有」的内容，它就从缓存
 * 变成了第二个真相源，整个架构的前提随之崩塌。
 *
 * ## 它解决什么
 *
 * 对象存储的请求有 RTT 成本，而 `BlockCache` 是**内存**缓存：CN 一重启就全空，
 * 于是重建之后的一段时间里所有读都打回对象存储。本地 NVMe 把这段「重新变热」
 * 的过程从「几百次网络往返」变成「几百次本地读」。**它省的是请求数，不是延迟**
 * ——延迟只是顺带。
 *
 * ## 内容带校验和
 *
 * 磁盘上的字节可能因为断电、坏道、人为编辑而损坏。如果没有校验，
 * 一个损坏的 `.idx` 块会被当成正常内容交给解码器，然后在**很远的地方**
 * 变成一个莫名其妙的解析异常——排查成本极高。每个块前面放 4 字节 CRC32C，
 * 读的时候验一次：不匹配就当作未命中，回对象存储重取。
 * **缓存可以慢，但不能撒谎。**
 *
 * ## 只缓存不可变对象
 *
 * 键就是对象键，而 `manifest/CURRENT` 这类对象**内容会变**——按 key 缓存它
 * 会读到陈旧内容，那是静默的错读。因此调用方必须显式说明「这个键的内容不会再变」
 * （见 [BlockCache.getOrLoad] 的 `immutableKey` 参数）。
 * SST 的 `.idx` 与 `.dat` 块天然满足：文件名里带文件 id，永不复用。
 */
class DiskCache(
    private val dir: Path,
    /** 容量上限。超出时按「最久未访问」淘汰——访问时间用文件 mtime 近似。 */
    private val maxBytes: Long = 10L * 1024 * 1024 * 1024,
) {

    private val hits = AtomicLong()
    private val misses = AtomicLong()
    private val writes = AtomicLong()
    private val evictions = AtomicLong()
    private var used = 0L

    init {
        Files.createDirectories(dir)
        // ★ 启动时把磁盘上已有的内容算进 `used`。
        //
        // `used` 只在 `put` 时累加，而进程重启时它从 0 开始、磁盘上的旧文件还在：
        // 于是"淘汰"要等到**再写满一个上限**才会触发，实际占用可达上限的 2 倍
        // （容器频繁重启的部署上尤其明显）。启动扫一遍两级子目录即可，
        // 一次性成本与缓存对象数成正比
        used = runCatching {
            Files.walk(dir, 2).use { paths ->
                paths.filter { Files.isRegularFile(it) }.mapToLong { runCatching { Files.size(it) }.getOrDefault(0L) }.sum()
            }
        }.getOrDefault(0L)
    }

    /**
     * 读一个已缓存的块，未命中返回 null。
     *
     * 命中时会 `touch` 文件的访问时间——那是 LRU 的依据。
     */
    suspend fun get(key: String): ByteArray? = withContext(Dispatchers.IO) {
        val f = pathOf(key)
        try {
            if (!Files.isRegularFile(f)) {
                misses.incrementAndGet()
                return@withContext null
            }
            val raw = Files.readAllBytes(f)
            if (raw.size < 4) {
                runCatching { Files.deleteIfExists(f) }
                misses.incrementAndGet()
                return@withContext null
            }
            val expected = com.cloudea.yangbase.common.BigEndian.readInt(raw, 0)
            val body = raw.copyOfRange(4, raw.size)
            // 校验和不匹配 → 当作未命中。**绝不能把可能损坏的内容交出去**：
            // 那会在解码器里变成一个与缓存毫无关系的异常，极难排查
            if (com.cloudea.yangbase.common.Checksums.crc32c(body) != expected) {
                runCatching { Files.deleteIfExists(f) }
                misses.incrementAndGet()
                return@withContext null
            }
            // 更新 mtime 以反映「最近用过」。失败不算错误——淘汰策略退化成 FIFO 而已
            runCatching { Files.setLastModifiedTime(f, java.nio.file.attribute.FileTime.fromMillis(System.currentTimeMillis())) }
            hits.incrementAndGet()
            body
        } catch (_: Exception) {
            // 缓存文件损坏或读不动 → 当作未命中。**绝不能让它变成查询失败**：
            // 一个缓存不该有能力让系统不可用
            runCatching { Files.deleteIfExists(f) }
            misses.incrementAndGet()
            null
        }
    }

    /** 写入缓存。失败静默忽略——理由同上。 */
    suspend fun put(key: String, data: ByteArray) = withContext(Dispatchers.IO) {
        try {
            val f = pathOf(key)
            if (Files.isRegularFile(f) && Files.size(f) == data.size + 4L) return@withContext
            // 两级子目录必须显式创建。忘了这一步的后果不是报错而是**静默不缓存**——
            // 异常被下面的 catch 吞掉（那是有意的：缓存不该让查询失败），
            // 于是「配了二级缓存但一次都没命中」这种状态没有任何提示。
            Files.createDirectories(f.parent)
            // 先写临时文件再原子改名：避免读到一个写了一半的块
            val tmp = f.resolveSibling(f.fileName.toString() + ".tmp")
            val framed = java.io.ByteArrayOutputStream(data.size + 4).also {
                com.cloudea.yangbase.common.BigEndian.writeInt(it, com.cloudea.yangbase.common.Checksums.crc32c(data))
                it.write(data)
            }.toByteArray()
            Files.write(tmp, framed)
            Files.move(tmp, f, java.nio.file.StandardCopyOption.REPLACE_EXISTING, java.nio.file.StandardCopyOption.ATOMIC_MOVE)
            writes.incrementAndGet()
            used += framed.size
            if (used > maxBytes) evict()
        } catch (_: Exception) {
            // 磁盘满、权限不足、被并发删除——都不该影响查询
        }
    }

    /**
     * 淘汰到容量的一半。
     *
     * 一次性淘汰到一半而不是每次只删一个：让淘汰变成**摊销**的，
     * 而不是每条写入都触发一次全目录扫描。扫描本身是 O(文件数)，
     * 在每次写入时做它是不可接受的。
     */
    private fun evict() {
        // ⚠️ 必须**递归**扫描。文件放在两级子目录里（`dir/ab/<hash>`），
        // 只扫顶层目录会得到「一个文件都没有」——于是淘汰永远不触发，
        // 而缓存会一直涨到磁盘满。这个错误不会报任何异常，只会静默地失效。
        val entries = try {
            Files.walk(dir, 2).use { s ->
                s.filter { Files.isRegularFile(it) && !it.fileName.toString().endsWith(".tmp") }
                    .map { it to Files.getLastModifiedTime(it).toMillis() }
                    .toList()
            }
        } catch (_: Exception) {
            used = 0
            return
        }
        var total = entries.sumOf { Files.size(it.first) }
        val target = maxBytes / 2
        for ((f, _) in entries.sortedBy { it.second }) {
            if (total <= target) break
            val sz = runCatching { Files.size(f) }.getOrDefault(0L)
            if (runCatching { Files.deleteIfExists(f) }.getOrDefault(false)) {
                total -= sz
                used = total
                evictions.incrementAndGet()
            }
        }
        used = total
    }

    suspend fun clear() = withContext(Dispatchers.IO) {
        runCatching {
            Files.walk(dir, 2).use { s ->
                s.sorted(Comparator.reverseOrder()).forEach { runCatching { Files.deleteIfExists(it) } }
            }
        }
        used = 0
        hits.set(0); misses.set(0); writes.set(0); evictions.set(0)
    }

    /**
     * 只读内存计数，**不碰磁盘**。
     *
     * 与 [stats] 的区别有两点，且都不是风格问题：它不是挂起函数（指标抓取
     * 发生在协议线程上，不能在那里阻塞），也不扫描目录（`/metrics` 的代价
     * 必须与缓存里有多少文件无关）。因此 [usedBytes] 是计数器维护的近似值，
     * 而 [stats] 的 `bytes` 是遍历出来的精确值——前者给监控，后者给诊断。
     */
    fun counters(): DiskCacheCounters =
        DiskCacheCounters(hits.get(), misses.get(), writes.get(), evictions.get(), used)

    suspend fun stats(): DiskCacheStats = withContext(Dispatchers.IO) {
        val (files, bytes) = try {
            Files.walk(dir, 2).use { s ->
                val fs = s.filter { Files.isRegularFile(it) }.toList()
                fs.size to fs.sumOf { runCatching { Files.size(it) }.getOrDefault(0L) }
            }
        } catch (_: Exception) {
            0 to 0L
        }
        DiskCacheStats(files, bytes, hits.get(), misses.get(), writes.get(), evictions.get())
    }

    fun hitRate(): Double {
        val h = hits.get(); val m = misses.get()
        return if (h + m == 0L) 0.0 else h.toDouble() / (h + m)
    }

    /**
     * 对象键 → 文件名。
     *
     * 用**内容的哈希**而不是把键转义成路径：对象键里有 `/` 和可能的各种字符，
     * 直接映射会形成目录树（创建成本高）或需要转义（易错、有长度上限）。
     * 哈希的唯一代价是失去可读性——而这是一个**缓存**，不需要人去看。
     */
    private fun pathOf(key: String): Path {
        val d = MessageDigest.getInstance("SHA-256").digest(key.toByteArray(Charsets.UTF_8))
        val hex = d.joinToString("") { "%02x".format(it) }
        // 两级目录：单目录下文件数过多会让文件系统本身的查找变慢
        return dir.resolve(hex.substring(0, 2)).resolve(hex)
    }
}

/** [DiskCache.counters] 的结果：纯内存计数，抓取它不产生任何 IO。 */
data class DiskCacheCounters(
    val hits: Long,
    val misses: Long,
    val writes: Long,
    val evictions: Long,
    /** 近似占用字节（由维护逻辑累加，不遍历目录）。 */
    val usedBytes: Long,
)

data class DiskCacheStats(
    val files: Int,
    val bytes: Long,
    val hits: Long,
    val misses: Long,
    val writes: Long,
    val evictions: Long,
) {
    override fun toString() =
        "DiskCache(文件 $files, ${bytes / 1024 / 1024}MB, 命中 $hits, 未命中 $misses, 写入 $writes, 淘汰 $evictions)"
}
