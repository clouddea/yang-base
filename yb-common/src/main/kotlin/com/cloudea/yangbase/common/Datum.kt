package com.cloudea.yangbase.common

import java.io.ByteArrayOutputStream

/**
 * 一个列值。
 *
 * 字符串比较采用**二进制序**（等价 `utf8mb4_bin`）。
 * MySQL 默认的 `utf8mb4_0900_ai_ci`（大小写不敏感）**尚未实现**——
 * 这是一个已知限制，会让 `WHERE name = 'ABC'` 匹配不到 `'abc'`。
 * 见设计文档 §11.3 的限制清单。
 */
sealed class Datum : Comparable<Datum> {

    object Null : Datum() {
        override fun toString() = "NULL"
    }

    data class I64(val v: Long) : Datum() {
        override fun toString() = v.toString()
    }

    data class F64(val v: Double) : Datum() {
        override fun toString() = v.toString()
    }

    data class Str(val v: String) : Datum() {
        override fun toString() = v
    }

    data class Bool(val v: Boolean) : Datum() {
        override fun toString() = if (v) "1" else "0"
    }

    /**
     * 时间戳，毫秒精度（对齐 MySQL 的 DATETIME(3) / TIMESTAMP(3)）。
     *
     * `DATE` / `DATETIME` / `TIMESTAMP` 三种列**共用这一个表示**——
     * 它们只是列类型上的区别，值都是"某个时刻"。
     */
    data class Ts(val millis: Long) : Datum() {
        // 与 [parseDateTime] 对称的**唯一**渲染点。字符串化的口径必须与解析一致
        // （都按 UTC），否则 `CONCAT(dt)` 与 `WHERE dt = '…'` 会对不上，
        // 而且协议层的往返会漂移——见 [formatDateTime]。
        override fun toString(): String = formatDateTime(millis)
    }

    /** 定点数，用未缩放整数 + 小数位保存，避免二进制浮点误差。 */
    data class Dec(val unscaled: Long, val scale: Int) : Datum() {
        override fun toString(): String {
            if (scale == 0) return unscaled.toString()
            val neg = unscaled < 0
            val abs = kotlin.math.abs(unscaled).toString().padStart(scale + 1, '0')
            val s = abs.substring(0, abs.length - scale) + "." + abs.substring(abs.length - scale)
            return if (neg) "-$s" else s
        }
    }

    data class Bin(val v: ByteArray) : Datum() {
        override fun toString() = "0x" + v.joinToString("") { "%02x".format(it) }
        override fun equals(other: Any?) = other is Bin && v.contentEquals(other.v)
        override fun hashCode() = v.contentHashCode()
    }

    val isNull: Boolean get() = this is Null

    /** 转成 Long（整数族）。 */
    fun asLong(): Long = when (this) {
        is I64 -> v
        is Bool -> if (v) 1L else 0L
        is Ts -> millis
        is Dec -> if (scale == 0) unscaled else unscaled / pow10Long(scale)
        is F64 -> v.toLong()
        is Str -> v.toLongOrNull() ?: throw SqlException(
            MySqlErrors.ER_TRUNCATED_WRONG_VALUE, "22007", "无法把 '$v' 转为整数"
        )
        Null -> throw SqlException(MySqlErrors.ER_TRUNCATED_WRONG_VALUE, "22007", "NULL 无法转为整数")
        is Bin -> throw SqlException(MySqlErrors.ER_TRUNCATED_WRONG_VALUE, "22007", "二进制无法转为整数")
    }

    fun asDouble(): Double = when (this) {
        is F64 -> v
        is I64 -> v.toDouble()
        is Bool -> if (v) 1.0 else 0.0
        is Ts -> millis.toDouble()
        is Dec -> unscaled.toDouble() / pow10(scale)
        is Str -> v.toDoubleOrNull() ?: throw SqlException(
            MySqlErrors.ER_TRUNCATED_WRONG_VALUE, "22007", "无法把 '$v' 转为浮点数"
        )
        Null -> throw SqlException(MySqlErrors.ER_TRUNCATED_WRONG_VALUE, "22007", "NULL 无法转为浮点数")
        is Bin -> throw SqlException(MySqlErrors.ER_TRUNCATED_WRONG_VALUE, "22007", "二进制无法转为浮点数")
    }

    fun asString(): String = when (this) {
        Null -> ""
        is Str -> v
        else -> toString()
    }

    fun asBoolean(): Boolean = when (this) {
        is Bool -> v
        Null -> false
        else -> asLong() != 0L
    }

    /** 数值族之间的统一比较；非数值返回 null。 */
    private fun numericOrNull(): Double? = when (this) {
        is I64 -> v.toDouble()
        is F64 -> v
        is Bool -> if (v) 1.0 else 0.0
        is Ts -> millis.toDouble()
        is Dec -> unscaled.toDouble() / pow10(scale)
        else -> null
    }

    /**
     * SQL 全序：NULL 最小，其次数值，再字符串，最后二进制。
     * 与 MySQL 的 NULL 排序一致（NULL 排在最前）。
     */
    override fun compareTo(other: Datum): Int {
        if (this is Null && other is Null) return 0
        if (this is Null) return -1
        if (other is Null) return 1

        val a = numericOrNull()
        val b = other.numericOrNull()
        if (a != null && b != null) return a.compareTo(b)
        if (a != null) return -1
        if (b != null) return 1

        if (this is Str && other is Str) return v.compareTo(other.v)
        if (this is Bin && other is Bin) return ByteArrayComparator.compare(v, other.v)
        // 异类之间：先用类型序，再按字符串
        return typeRank().compareTo(other.typeRank())
    }

    private fun typeRank(): Int = when (this) {
        Null -> 0; is I64 -> 1; is F64 -> 1; is Bool -> 1; is Ts -> 1; is Dec -> 1
        is Str -> 2; is Bin -> 3
    }

    /**
     * 按**列类型**解释这个值。
     *
     * 存在的理由：SQL 里 `WHERE dt = '2026-01-01 10:00:00'` 是最常见的写法，
     * 而字面量在词法上只是一个字符串。若不转换，`Datum.compareTo` 会走到
     * 「异类之间按类型序」的分支——时间戳与字符串永不相等，查询静默返回空集。
     * 那不是一个显式的报错，而是**看起来正常但没有结果**，最难排查的一类缺陷。
     */
    fun coerceTo(type: ColumnType): Datum {
        if (this is Null) return this
        return when (type) {
            ColumnType.DATETIME, ColumnType.TIMESTAMP, ColumnType.DATE -> when (this) {
                is Ts -> this
                is Str -> parseDateTime(v)?.let { Ts(it) } ?: this
                is I64 -> Ts(v)
                else -> this
            }
            // YEAR 就是个整数（1901–2155 与 0），与别的整数列同一套比较语义
            ColumnType.YEAR,
            ColumnType.TINYINT, ColumnType.SMALLINT, ColumnType.INT, ColumnType.BIGINT -> when (this) {
                is Str -> v.trim().toLongOrNull()?.let { I64(it) } ?: this
                is F64 -> I64(v.toLong())
                is Bool -> I64(if (v) 1L else 0L)
                else -> this
            }
            ColumnType.DOUBLE, ColumnType.FLOAT -> when (this) {
                is Str -> v.trim().toDoubleOrNull()?.let { F64(it) } ?: this
                else -> this
            }
            ColumnType.DECIMAL -> when (this) {
                is Str -> {
                    val t = v.trim()
                    val dot = t.indexOf('.')
                    val scale = if (dot < 0) 0 else t.length - dot - 1
                    t.replace(".", "").toLongOrNull()?.let { Dec(it, scale) } ?: this
                }
                else -> this
            }
            // BLOB：字面量在词法上只是字符串，而 BLOB 列的值是 `Datum.Bin`。
            // 少了这个分支，`WHERE blob_col = 'abc'` 会走"异类比较"（Str vs Bin）
            // → **永远匹配不到**，不报错、只是查不到
            ColumnType.BLOB -> when (this) {
                is Bin -> this
                is Str -> Bin(v.toByteArray(Charsets.UTF_8))
                else -> this
            }
            // ENUM / SET / JSON 都按字符串比较（见 ColumnType 里记的差异）
            ColumnType.VARCHAR, ColumnType.TEXT, ColumnType.ENUM, ColumnType.SET, ColumnType.JSON -> when (this) {
                is Str -> this
                is Ts -> Str(toString())
                else -> Str(asString())
            }
            else -> this
        }
    }

    companion object {
        /**
         * `yyyy-MM-dd[ HH:mm:ss[.SSS]]` → 毫秒，按 **UTC** 解释。
         *
         * **全系统只有这一个时间字面量解析器**——插入、谓词转换、分区边界求值
         * 全部走它。曾经插入用 `systemDefault` 而分区边界用 UTC，
         * 同一个字符串解出两个不同的时刻，`WHERE dt = '...'` 因此静默返回空集。
         */
        fun parseDateTime(s: String): Long? = try {
            val d = s.replace('T', ' ').trim()
            val datePart = d.substringBefore(' ')
            val (y, mo, da) = datePart.split('-').let { Triple(it[0].toInt(), it[1].toInt(), it[2].toInt()) }
            val (h, mi, se) = if (d.contains(' ')) {
                val t = d.substringAfter(' ')
                val parts = t.split(':')
                Triple(
                    parts[0].toInt(),
                    parts.getOrElse(1) { "0" }.toInt(),
                    parts.getOrElse(2) { "0" }.substringBefore('.').toInt(),
                )
            } else Triple(0, 0, 0)
            java.time.LocalDateTime.of(y, mo, da, h, mi, se).toInstant(java.time.ZoneOffset.UTC).toEpochMilli()
        } catch (_: Exception) {
            null
        }

        /**
         * `毫秒 → "yyyy-MM-dd HH:mm:ss[.SSS]"`，按 **UTC**。
         *
         * **全系统只有这一个时间渲染器**，与 [parseDateTime] 是一对：解析按 UTC、
         * 渲染也按 UTC，往返才不会漂移（这是 `DateTimeRoundTripTest` 钉住的性质）。
         *
         * 曾经渲染走 `ZoneId.systemDefault()`，于是「服务器时区不是 UTC」的部署上
         * `INSERT '2026-02-01 10:00:00'` 之后 `SELECT` 出来是 `18:00:00`
         * ——**不报错，只是数值不同**，而且开发机若是 UTC 就永远看不到。
         *
         * 秒以下为 0 时不输出小数部分（与 MySQL 的 DATETIME(0) 显示一致）；
         * 非 0 时输出 3 位毫秒（本系统的精度上限）。
         */
        fun formatDateTime(millis: Long): String {
            val dt = java.time.Instant.ofEpochMilli(millis).atZone(java.time.ZoneOffset.UTC)
            val base = "%04d-%02d-%02d %02d:%02d:%02d".format(
                dt.year, dt.monthValue, dt.dayOfMonth, dt.hour, dt.minute, dt.second,
            )
            val ms = ((millis % 1000) + 1000) % 1000
            return if (ms == 0L) base else "$base.%03d".format(ms)
        }

        /** `毫秒 → "yyyy-MM-dd"`（UTC），`DATE` 列的显示口径。 */
        fun formatDate(millis: Long): String =
            java.time.Instant.ofEpochMilli(millis).atZone(java.time.ZoneOffset.UTC)
                .let { "%04d-%02d-%02d".format(it.year, it.monthValue, it.dayOfMonth) }

        private val POW10 = LongArray(19) { i -> var r = 1L; repeat(i) { r *= 10 }; r }
        fun pow10(n: Int): Double = POW10[n].toDouble()
        fun pow10Long(n: Int): Long = POW10[n]

        /** 从任意 Kotlin 值构造，供测试与字面量使用。 */
        fun of(v: Any?): Datum = when (v) {
            null -> Null
            is Datum -> v
            is Long -> I64(v)
            is Int -> I64(v.toLong())
            is Short -> I64(v.toLong())
            is Byte -> I64(v.toLong())
            is Double -> F64(v)
            is Float -> F64(v.toDouble())
            is Boolean -> Bool(v)
            is String -> Str(v)
            is ByteArray -> Bin(v)
            else -> throw IllegalArgumentException("无法转为 Datum: ${v::class}")
        }
    }
}

/**
 * 行的编解码。
 *
 * 格式（手写二进制，热路径，见 CLAUDE.md 编码约定）：
 * ```
 * nullBitmap: ceil(ncols/8) 字节，bit=1 表示该列为 NULL
 * 非空列按 schema 顺序依次编码：
 *   整数/布尔/时间/定点 → zigzag varint
 *   浮点               → 8 字节 IEEE754 大端
 *   字符串/二进制       → varint 长度 + 原始字节
 * ```
 */
object RowCodec {

    /**
     * 行格式版本。
     *
     * v1 = `[null bitmap][values]`，列数隐含在 schema 里；
     * v2 = `[varint 列数][null bitmap][values]`，**行自带列数**。
     *
     * v2 存在的唯一理由是**让 `ALTER TABLE ADD COLUMN` 成为 O(1) 的元数据操作**：
     * 旧行按写入时的列数解码，尾部缺的列补默认值；不需要为每个数据文件
     * 记录 schema 版本、也不需要读时去查历史 schema。
     * 代价是每行多一个字节——相对于「加一列就要重写整张表」，
     * 这个交换在任何规模上都划算。
     *
     * ⚠️ 格式变了就不能静默读旧数据：`SstIndex.rowFormat` 与
     * `WalCodec.FORMAT_VERSION` 会拦住 v1 数据并**明确报错**，
     * 而不是解码出一堆错位的值。
     */
    const val ROW_FORMAT_VERSION = 2

    fun encode(row: List<Datum>): ByteArray {
        val out = ByteArrayOutputStream(64)
        val n = row.size
        // 列数写在最前：null bitmap 的长度取决于它，所以它必须先被读到
        Varint.writeUnsigned(out, n)
        val bitmapLen = (n + 7) / 8
        val bitmap = ByteArray(bitmapLen)
        for (i in 0 until n) {
            if (row[i] is Datum.Null) bitmap[i / 8] = (bitmap[i / 8].toInt() or (1 shl (i % 8))).toByte()
        }
        out.write(bitmap)
        for (i in 0 until n) {
            when (val d = row[i]) {
                is Datum.Null -> {}
                is Datum.I64 -> writeZigZag(out, d.v)
                is Datum.Bool -> writeZigZag(out, if (d.v) 1L else 0L)
                is Datum.Ts -> writeZigZag(out, d.millis)
                // DECIMAL 自带小数位数：若只依赖 schema，ALTER TABLE 改精度后
                // 旧行会解码出错误的数值。1 字节换掉一整类静默错误是划算的。
                is Datum.Dec -> {
                    Varint.writeUnsigned(out, d.scale)
                    writeZigZag(out, d.unscaled)
                }
                is Datum.F64 -> {
                    val bits = java.lang.Double.doubleToLongBits(d.v)
                    for (k in 7 downTo 0) out.write(((bits ushr (k * 8)) and 0xFF).toInt())
                }
                is Datum.Str -> {
                    val b = d.v.toByteArray(Charsets.UTF_8)
                    Varint.writeUnsigned(out, b.size)
                    out.write(b)
                }
                is Datum.Bin -> {
                    Varint.writeUnsigned(out, d.v.size)
                    out.write(d.v)
                }
            }
        }
        return out.toByteArray()
    }

    /**
     * 解码。行自带列数（格式 v2），因此**旧行在 schema 演化之后仍然读得出来**。
     *
     * 三种情形：
     *  - `m == schema.columns.size`：常规
     *  - `m <  schema.columns.size`：之后**加过列** → 尾部补默认值（没默认值就是 NULL）
     *  - `m >  schema.columns.size`：之后**删过尾部的列** → 只解出前 n 列，多余的字节直接忽略
     *
     * 后两种都只在**无重写**的前提下正确：加列只能加在末尾、删列只能删末尾。
     * 在中间插入或删除会让列的位置整体错位，那种情况必须重写数据——
     * `ALTER TABLE` 会明确拒绝，而不是给出错位的值。
     */
    fun decode(bytes: ByteArray, schema: TableSchema): List<Datum> {
        if (bytes.isEmpty()) throw CorruptDataException("行数据为空")
        val (m, p0) = Varint.readUnsigned(bytes, 0)
        if (m <= 0 || m > MAX_COLUMNS) {
            throw CorruptDataException(
                "行数据的列数 $m 不合理——可能是 v1 格式的旧数据（本版本要求格式 v$ROW_FORMAT_VERSION）"
            )
        }
        val n = schema.columns.size
        // 只解出 schema 里有的列。行比 schema 宽 = 尾部删过列，多余的字节不读即可
        // （不需要知道被删列的宽度——不去读它就不需要跳过它）。
        val decodeCount = minOf(m, n)
        val bitmapLen = (m + 7) / 8
        if (bytes.size < p0 + bitmapLen) throw CorruptDataException("行数据短于 null bitmap")
        var p = p0 + bitmapLen
        val out = ArrayList<Datum>(n)
        for (i in 0 until decodeCount) {
            val isNull = (bytes[p0 + i / 8].toInt() and (1 shl (i % 8))) != 0
            if (isNull) { out.add(Datum.Null); continue }
            val (d, consumed) = decodeOne(bytes, p, schema.columns[i])
            out.add(d)
            p += consumed
        }
        // 尾部新增的列：补默认值。默认值以字面量形式存在 schema 里，
        // 求值失败就当 NULL——**绝不能因为一个默认值写法怪就让整行读不出来**。
        for (i in decodeCount until n) {
            val col = schema.columns[i]
            val dflt = col.defaultValue
            out.add(if (dflt == null) Datum.Null else runCatching { coerceLiteral(dflt, col) }.getOrElse { Datum.Null })
        }
        return out
    }

    /** schema 列数的上界。超过它几乎一定是把非 v2 数据当成了 v2。 */
    private const val MAX_COLUMNS = 4096

    /** 把 DDL 里写的默认值字面量转成 Datum。 */
    private fun coerceLiteral(text: String, col: ColumnDef): Datum {
        val t = text.trim()
        val raw = if (t.length >= 2 && (t.first() == '\'' || t.first() == '"') && t.last() == t.first())
            t.substring(1, t.length - 1) else t
        return when {
            raw.equals("NULL", ignoreCase = true) -> Datum.Null
            raw.equals("CURRENT_TIMESTAMP", ignoreCase = true) -> Datum.Ts(System.currentTimeMillis())
            col.type.isInteger || col.type == ColumnType.BOOLEAN ->
                Datum.I64(raw.toLongOrNull() ?: return Datum.Null)
            col.type == ColumnType.DOUBLE || col.type == ColumnType.FLOAT ->
                Datum.F64(raw.toDoubleOrNull() ?: return Datum.Null)
            // DECIMAL 的默认值也要是**定点数**：写成 Str 会让"加列后旧行补默认值"
            // 那一路读出与列类型不符的值（后续比较、聚合全会走错分支）
            col.type == ColumnType.DECIMAL -> {
                val text = raw.trim()
                val dot = text.indexOf('.')
                val scale = if (dot < 0) 0 else text.length - dot - 1
                val unscaled = text.replace(".", "").toLongOrNull() ?: return Datum.Null
                Datum.Dec(unscaled, scale)
            }
            col.type.isTemporal -> Datum.Ts(Datum.parseDateTime(raw) ?: return Datum.Null)
            else -> Datum.Str(raw)
        }
    }

    private fun decodeOne(bytes: ByteArray, offset: Int, col: ColumnDef): Pair<Datum, Int> {
        return when (col.type) {
            ColumnType.YEAR,
            ColumnType.TINYINT, ColumnType.SMALLINT, ColumnType.INT, ColumnType.BIGINT -> {
                val (v, n) = readZigZag(bytes, offset); Datum.I64(v) to n
            }
            ColumnType.BOOLEAN -> {
                val (v, n) = readZigZag(bytes, offset); Datum.Bool(v != 0L) to n
            }
            ColumnType.DATE, ColumnType.DATETIME, ColumnType.TIMESTAMP -> {
                val (v, n) = readZigZag(bytes, offset); Datum.Ts(v) to n
            }
            ColumnType.DECIMAL -> {
                val (scale, ns) = Varint.readUnsigned(bytes, offset)
                val (v, n) = readZigZag(bytes, offset + ns)
                Datum.Dec(v, scale) to (ns + n)
            }
            ColumnType.FLOAT, ColumnType.DOUBLE -> {
                if (offset + 8 > bytes.size) throw CorruptDataException("浮点字段截断")
                var bits = 0L
                for (k in 0 until 8) bits = (bits shl 8) or (bytes[offset + k].toLong() and 0xFF)
                Datum.F64(java.lang.Double.longBitsToDouble(bits)) to 8
            }
            ColumnType.VARCHAR, ColumnType.TEXT, ColumnType.BLOB,
            ColumnType.ENUM, ColumnType.SET, ColumnType.JSON -> {
                val (len, ln) = Varint.readUnsigned(bytes, offset)
                val start = offset + ln
                if (start + len > bytes.size) throw CorruptDataException("变长字段截断")
                val slice = bytes.copyOfRange(start, start + len)
                if (col.type == ColumnType.BLOB) Datum.Bin(slice) to (ln + len)
                else Datum.Str(String(slice, Charsets.UTF_8)) to (ln + len)
            }
        }
    }

    fun writeZigZag(out: ByteArrayOutputStream, v: Long) {
        Varint.writeUnsigned(out, (v shl 1) xor (v shr 63))
    }

    fun readZigZag(bytes: ByteArray, offset: Int): Pair<Long, Int> {
        val (u, n) = Varint.readUnsignedLong(bytes, offset)
        return ((u ushr 1) xor -(u and 1L)) to n
    }
}
