package com.cloudea.yangbase.index.fulltext

import com.cloudea.yangbase.common.BigEndian
import com.cloudea.yangbase.common.Checksums
import java.io.ByteArrayOutputStream

/**
 * 词典：**最小无环确定自动机**（设计文档 §8.6.1 的 `terms.fst`）。
 *
 * 只做一件事：`词条 → 输出值` 的精确查找，外加按字典序枚举。
 * 相比 `HashMap<String, Long>` 的价值是**体积**：后缀共享把数百万词条压到几 MB，
 * 因而可以**整体常驻内存**（P3）——这正是词典能参与 top-k 检索而不用打请求的前提。
 *
 * 结构（三个平行数组，节点弧段连续存放）：
 * ```
 *   nodeStart[n] .. nodeStart[n]+nodeArcs[n]   节点 n 的出弧（按无符号字节升序）
 *   labels[a] / outputs[a] / targets[a]        第 a 条弧的标签 / 输出 / 目标节点
 * ```
 * 终结符是标签 `0x00`（词条里不可能出现 NUL），**输出挂在终结弧上**——
 * 所以内部节点无输出，节点等价性判定只看弧结构，最小化是严格正确的。
 *
 * ⚠️ 输出上限 `Int.MAX_VALUE`（内部按 Int 存）。本模块用它存 postings 序号，
 * 远够；要存更大的值请改数组宽度而不是在这里加分支。
 */
class Fst internal constructor(
    private val labels: ByteArray,
    private val outputs: IntArray,
    private val targets: IntArray,
    private val nodeStart: IntArray,
    private val nodeArcs: IntArray,
    val root: Int,
    /** 最长词条的字节数（用于枚举时的路径缓冲）。 */
    val maxKeyLen: Int,
    /** 词条个数。 */
    val size: Int,
) {

    val nodeCount: Int get() = nodeStart.size
    val arcCount: Int get() = labels.size

    // ── 查找 ────────────────────────────────────────────────────────

    /** 精确查找。返回 null 表示词条不存在（**注意与输出值为 0 区分**）。 */
    fun get(term: String): Int? {
        var node = root
        val bytes = term.toByteArray(Charsets.UTF_8)
        for (b in bytes) {
            val a = findArc(node, b)
            if (a < 0) return null
            node = targets[a]
        }
        val a = findArc(node, TERMINATOR)
        return if (a < 0) null else outputs[a]
    }

    fun contains(term: String): Boolean = get(term) != null

    /**
     * 按字典序遍历以 [prefix] 开头的词条（含 [prefix] 自身）。
     * [consumer] 返回 false 时中止。返回是否遍历完整。
     */
    fun forEachTerm(prefix: String = "", consumer: (term: String, output: Int) -> Boolean): Boolean {
        val prefixBytes = prefix.toByteArray(Charsets.UTF_8)
        if (prefixBytes.size > maxKeyLen) return true
        var node = root
        for (b in prefixBytes) {
            val a = findArc(node, b)
            if (a < 0) return true
            node = targets[a]
        }
        val buf = ByteArray(maxKeyLen)
        prefixBytes.copyInto(buf)
        val stack = ArrayList<Frame>(16)
        stack.add(Frame(node, nodeStart[node], prefixBytes.size))
        while (stack.isNotEmpty()) {
            val f = stack[stack.size - 1]
            if (f.nextArc >= nodeStart[f.node] + nodeArcs[f.node]) {
                stack.removeAt(stack.size - 1)
                continue
            }
            val a = f.nextArc++
            val label = labels[a]
            if (label == TERMINATOR) {
                val term = String(buf, 0, f.depth, Charsets.UTF_8)
                if (!consumer(term, outputs[a])) return false
            } else {
                buf[f.depth] = label
                val child = targets[a]
                stack.add(Frame(child, nodeStart[child], f.depth + 1))
            }
        }
        return true
    }

    /** 词条数（同 [size]，供 `Map` 式调用点阅读）。 */
    fun terms(): List<String> {
        val out = ArrayList<String>(size)
        forEachTerm("") { t, _ -> out.add(t); true }
        return out
    }

    /** 二分查找某节点的出弧（无符号字节序）。返回全局弧下标，找不到返回 -1。 */
    private fun findArc(node: Int, label: Byte): Int {
        var lo = nodeStart[node]
        var hi = lo + nodeArcs[node] - 1
        val want = label.toInt() and 0xFF
        while (lo <= hi) {
            val mid = (lo + hi) ushr 1
            val cmp = (labels[mid].toInt() and 0xFF) - want
            if (cmp == 0) return mid
            if (cmp < 0) lo = mid + 1 else hi = mid - 1
        }
        return -1
    }

    private class Frame(var node: Int, var nextArc: Int, val depth: Int)

    // ── 序列化（手写二进制：热路径，格式要精确可控）──────────────────

    fun toBytes(): ByteArray {
        val out = ByteArrayOutputStream(HEADER + labels.size + arcCount * 8 + nodeCount * 8)
        out.write(MAGIC)
        BigEndian.writeInt(out, FORMAT_VERSION)
        BigEndian.writeInt(out, root)
        BigEndian.writeInt(out, nodeCount)
        BigEndian.writeInt(out, arcCount)
        BigEndian.writeInt(out, maxKeyLen)
        BigEndian.writeInt(out, size)
        out.write(labels)
        for (i in 0 until arcCount) BigEndian.writeInt(out, outputs[i])
        for (i in 0 until arcCount) BigEndian.writeInt(out, targets[i])
        for (i in 0 until nodeCount) BigEndian.writeInt(out, nodeStart[i])
        for (i in 0 until nodeCount) BigEndian.writeInt(out, nodeArcs[i])
        val body = out.toByteArray()
        val full = ByteArray(body.size + 4)
        body.copyInto(full)
        val crc = Checksums.crc32c(body)
        for (k in 0 until 4) {
            full[body.size + k] = ((crc ushr ((3 - k) * 8)) and 0xFF).toByte()
        }
        return full
    }

    /** 序列化后的字节数（估算体积时不必真的编码一遍）。 */
    val byteSize: Long get() = (HEADER + labels.size + arcCount * 8 + nodeCount * 8 + 4).toLong()

    override fun toString(): String = "Fst(词条=$size 节点=$nodeCount 弧=$arcCount 字节≈$byteSize)"

    companion object {
        private val MAGIC = byteArrayOf('Y'.code.toByte(), 'B'.code.toByte(), 'F'.code.toByte(), '1'.code.toByte())
        private const val FORMAT_VERSION = 1
        /** 魔数 4B + 6 个 Int 字段（version/root/nodeCount/arcCount/maxKeyLen/size）。 */
        private const val HEADER = 4 + 6 * 4
        private const val TERMINATOR: Byte = 0

        fun fromBytes(bytes: ByteArray): Fst {
            require(bytes.size >= HEADER + 4) { "FST 对象太小: ${bytes.size} 字节" }
            for (i in MAGIC.indices) {
                require(bytes[i] == MAGIC[i]) { "FST 魔数不匹配" }
            }
            val expectCrc = BigEndian.readInt(bytes, bytes.size - 4)
            val actualCrc = Checksums.crc32c(bytes, 0, bytes.size - 4)
            require(expectCrc == actualCrc) { "FST 校验和不匹配: 期望=${"%08x".format(expectCrc)} 实际=${"%08x".format(actualCrc)}" }

            var p = 4
            val version = BigEndian.readInt(bytes, p); p += 4
            require(version == FORMAT_VERSION) { "FST 格式版本不支持: $version" }
            val root = BigEndian.readInt(bytes, p); p += 4
            val nodeCount = BigEndian.readInt(bytes, p); p += 4
            val arcCount = BigEndian.readInt(bytes, p); p += 4
            val maxKeyLen = BigEndian.readInt(bytes, p); p += 4
            val size = BigEndian.readInt(bytes, p); p += 4
            require(nodeCount >= 0 && arcCount >= 0) { "FST 头部损坏" }
            val need = HEADER + arcCount * 9L + nodeCount * 8L + 4
            require(need == bytes.size.toLong()) {
                "FST 长度与头部不符: 头部声称 $need 字节，实际 ${bytes.size}"
            }
            require(root in 0 until nodeCount) { "FST root 越界: $root / $nodeCount" }

            val labels = bytes.copyOfRange(p, p + arcCount); p += arcCount
            val outputs = IntArray(arcCount)
            for (i in 0 until arcCount) { outputs[i] = BigEndian.readInt(bytes, p); p += 4 }
            val targets = IntArray(arcCount)
            for (i in 0 until arcCount) {
                targets[i] = BigEndian.readInt(bytes, p); p += 4
                require(targets[i] in 0 until nodeCount) { "FST 目标节点越界: ${targets[i]}" }
            }
            val nodeStart = IntArray(nodeCount)
            for (i in 0 until nodeCount) { nodeStart[i] = BigEndian.readInt(bytes, p); p += 4 }
            val nodeArcs = IntArray(nodeCount)
            for (i in 0 until nodeCount) {
                nodeArcs[i] = BigEndian.readInt(bytes, p); p += 4
                require(nodeArcs[i] >= 0 && nodeStart[i] >= 0 && nodeStart[i] + nodeArcs[i] <= arcCount) {
                    "FST 节点 $i 的弧区间越界"
                }
            }
            return Fst(labels, outputs, targets, nodeStart, nodeArcs, root, maxKeyLen, size)
        }
    }
}

/**
 * 增量式最小 FST 构造器（Daciuk 的「先冻结、再注册」算法）。
 *
 * 用法：**按字典序、无重复、非空**地依次 [add]，最后 [build]。
 * 违反顺序不报错是灾难（会静默构造出查不到词条的自动机），所以每一步都校验。
 *
 * 复杂度：O(总字节数)；最小化在构造过程中顺带完成，不需要额外的全局重建。
 *
 * 内存换实现的取舍：节点去重用的哈希表按 `IntArray` 内容比较（每个节点一份 key），
 * 对百万级词条会有百 MB 量级的临时占用。构建是**离线**动作（不占查询链路），
 * 且构建完即释放——这是刻意的取舍，换成紧凑哈希表只是常数优化。
 */
class FstBuilder {

    private class Node {
        /** 出弧，按标签升序追加（调用顺序保证有序，见 [ensureSorted]）。 */
        val labels = ArrayList<Byte>(4)
        val outputs = ArrayList<Int>(4)
        val targets = ArrayList<Node?>(4)
        var id: Int = -1
        var frozen: Boolean = false
        val arcCount: Int get() = labels.size

        fun addArc(label: Byte, output: Int, target: Node?) {
            labels.add(label); outputs.add(output); targets.add(target)
        }
    }

    private val registry = HashMap<NodeKey, Int>(1 shl 16)
    private val outLabels = ByteArrayOutputStream(1 shl 16)
    private val outOutputs = ArrayList<Int>(1 shl 14)
    private val outTargets = ArrayList<Int>(1 shl 14)
    private val nodeStart = ArrayList<Int>(1 shl 14)
    private val nodeArcs = ArrayList<Int>(1 shl 14)

    /** 根到当前节点的路径；[path][d] 是深度 d 的节点。 */
    private val path = ArrayList<Node>(32)
    private var lastKey: ByteArray = ByteArray(0)
    private var lastDepth = -1
    private var count = 0
    private var maxKeyLen = 0
    private var built: Fst? = null

    init {
        path.add(Node())
        lastDepth = 0
    }

    /** 词条个数。 */
    val size: Int get() = count

    /**
     * 加入一个词条。[output] 是查询时要取回的值（这里通常是 postings 序号）。
     *
     * 必须严格按 UTF-8 字节序递增调用。
     */
    fun add(key: String, output: Int) {
        check(built == null) { "FstBuilder 已经 build() 过，不能继续 add" }
        val bytes = key.toByteArray(Charsets.UTF_8)
        require(bytes.isNotEmpty()) { "词条不能为空" }
        require(bytes.none { it == TERMINATOR }) { "词条不能含 NUL 字节: $key" }
        require(output >= 0) { "FST 输出必须非负: $output (词条=$key)" }

        val cp = commonPrefix(lastKey, lastDepth, bytes)
        if (count > 0) {
            require(compare(lastKey, lastDepth, bytes) < 0) {
                "词条必须严格递增：'$key' 未大于上一个词条（重复或逆序）"
            }
            check(cp < bytes.size) {
                "内部错误：'$key' 的公共前缀吃满了整个词条（排序校验被绕过）"
            }
        }

        freezeFrom(cp + 1)
        // 路径截断到深度 cp（节点），即保留根到深度 cp 的节点
        while (path.size > cp + 1) path.removeAt(path.size - 1)

        var node = path[path.size - 1]
        for (i in cp until bytes.size) {
            val child = Node()
            node.addArc(bytes[i], 0, child)
            ensureSorted(node, key)
            path.add(child)
            node = child
        }
        val leaf = Node()
        node.addArc(TERMINATOR, output, leaf)
        ensureSorted(node, key)
        path.add(leaf)

        lastKey = bytes
        lastDepth = bytes.size
        count++
        if (bytes.size > maxKeyLen) maxKeyLen = bytes.size
    }

    fun build(): Fst {
        built?.let { return it }
        check(count > 0 || path.size == 1) { "空 FST 状态异常" }
        freezeFrom(1)
        // 根节点也要落到数组里（它是查找入口）。它在所有子节点之后登记，
        // 因此它的 id 是最后分配的——序列化时由 header 里的 root 字段指出。
        val rootId = nodeId(path[0])
        path[0].frozen = true
        val fst = Fst(
            labels = outLabels.toByteArray(),
            outputs = outOutputs.toIntArray(),
            targets = outTargets.toIntArray(),
            nodeStart = nodeStart.toIntArray(),
            nodeArcs = nodeArcs.toIntArray(),
            root = rootId,
            maxKeyLen = maxKeyLen,
            size = count,
        )
        built = fst
        registry.clear()
        return fst
    }

    // ── 内部 ────────────────────────────────────────────────────────

    /** 冻结路径上深度 >= [from] 的节点（自底向上，保证子节点先拿到 id）。 */
    private fun freezeFrom(from: Int) {
        var d = path.size - 1
        while (d >= from) {
            val n = path[d]
            if (!n.frozen) {
                n.id = nodeId(n)
                n.frozen = true
            }
            d--
        }
    }

    /** 注册（去重）并登记一个节点，返回它的 id。 */
    private fun nodeId(n: Node): Int {
        if (n.id >= 0) return n.id
        // 先给所有子节点定 id（此时子节点必已冻结）
        for (i in 0 until n.arcCount) {
            val t = n.targets[i] ?: error("内部错误：未填 target")
            if (t.id < 0) t.id = nodeId(t)
        }
        val key = NodeKey(n)
        val exist = registry[key]
        if (exist != null) {
            n.id = exist
            return exist
        }
        val id = nodeStart.size
        nodeStart.add(outLabels.size())
        nodeArcs.add(n.arcCount)
        for (i in 0 until n.arcCount) {
            outLabels.write(n.labels[i].toInt() and 0xFF)
            outOutputs.add(n.outputs[i])
            outTargets.add(n.targets[i]!!.id)
        }
        registry[key] = id
        n.id = id
        return id
    }

    private fun ensureSorted(n: Node, key: String) {
        val k = n.arcCount
        if (k < 2) return
        val a = n.labels[k - 2].toInt() and 0xFF
        val b = n.labels[k - 1].toInt() and 0xFF
        check(a < b) {
            "内部错误：节点出弧未按字节升序（词条 '$key'，$a >= $b）——" +
                "调用方没有按字典序 add？"
        }
    }

    private fun commonPrefix(a: ByteArray, aLen: Int, b: ByteArray): Int {
        val n = minOf(aLen, b.size)
        var i = 0
        while (i < n && a[i] == b[i]) i++
        return i
    }

    /** UTF-8 字节序比较（与 `String.compareTo` 一致，但避免为比较而分配 String）。 */
    private fun compare(a: ByteArray, aLen: Int, b: ByteArray): Int {
        val n = minOf(aLen, b.size)
        var i = 0
        while (i < n) {
            val x = a[i].toInt() and 0xFF
            val y = b[i].toInt() and 0xFF
            if (x != y) return x - y
            i++
        }
        return aLen - b.size
    }

    /** 节点去重键：弧的 (标签, 输出, 目标 id) 三元组序列。 */
    private class NodeKey(private val data: IntArray) {
        constructor(n: Node) : this(encodeArcs(n))

        override fun hashCode(): Int = data.contentHashCode()

        override fun equals(other: Any?): Boolean =
            other is NodeKey && data.contentEquals(other.data)

        companion object {
            private fun encodeArcs(n: Node): IntArray {
                val arr = IntArray(n.arcCount * 3)
                for (i in 0 until n.arcCount) {
                    arr[i * 3] = n.labels[i].toInt() and 0xFF
                    arr[i * 3 + 1] = n.outputs[i]
                    arr[i * 3 + 2] = n.targets[i]!!.id
                }
                return arr
            }
        }
    }

    companion object {
        private const val TERMINATOR: Byte = 0
    }
}
