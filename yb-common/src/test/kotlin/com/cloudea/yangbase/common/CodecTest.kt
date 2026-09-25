package com.cloudea.yangbase.common

import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * 保序编码的测试。
 *
 * 这些测试守护的是一条**静默失败**的不变式：如果编码不保序，
 * 范围扫描会漏行而不报任何错误。所以这里用穷举 + 随机对拍来覆盖。
 */
class OrderPreservingCodecTest {

    @Test
    fun `long 编码保序`() {
        val values = listOf(Long.MIN_VALUE, -1_000_000L, -1L, 0L, 1L, 255L, 256L, Long.MAX_VALUE)
        for (i in 0 until values.size - 1) {
            val a = OrderPreservingCodec.encodeLong(values[i])
            val b = OrderPreservingCodec.encodeLong(values[i + 1])
            assertTrue(
                ByteArrayComparator.compare(a, b) < 0,
                "编码后应保持序: ${values[i]} < ${values[i + 1]}",
            )
        }
    }

    @Test
    fun `long 编解码往返`() {
        for (v in listOf(Long.MIN_VALUE, -1L, 0L, 42L, Long.MAX_VALUE)) {
            assertEquals(v, OrderPreservingCodec.decodeLong(OrderPreservingCodec.encodeLong(v)))
        }
    }

    @Test
    fun `int 与 short 往返`() {
        for (v in listOf(Int.MIN_VALUE, -1, 0, 7, Int.MAX_VALUE)) {
            assertEquals(v, OrderPreservingCodec.decodeInt(OrderPreservingCodec.encodeInt(v)))
        }
        for (v in listOf(Short.MIN_VALUE, -1, 0, 7, Short.MAX_VALUE)) {
            assertEquals(v, OrderPreservingCodec.decodeShort(OrderPreservingCodec.encodeShort(v)))
        }
    }

    @Test
    fun `字节串转义可往返且保序`() {
        val cases = listOf(
            ByteArray(0),                                   // 空串
            byteArrayOf(0),                                 // 单个 0x00——最刁钻的输入
            byteArrayOf(0, 0),                              // 连续 0x00
            "a".toByteArray(),
            "a\u0000b".toByteArray(),                       // 内嵌 0x00
            byteArrayOf(0x61, 0x62),
            byteArrayOf(0xFF.toByte()),
            byteArrayOf(1, 2, 3),
        )
        for (c in cases) {
            val enc = OrderPreservingCodec.encodeBytes(c)
            val (dec, consumed) = OrderPreservingCodec.decodeBytes(enc)
            assertContentEquals(c, dec, "往返失败: ${c.toList()}")
            assertEquals(enc.size, consumed)
        }
    }

    @Test
    fun `短串排在以其为前缀的长串之前`() {
        // 这是转义方案最关键的边界：前缀关系必须保持
        val pairs = listOf(
            "a" to "ab",
            "a" to "a\u0000b",
            "" to "a",
            "" to "\u0000",
            "ab" to "ab\u0000",
        )
        for ((short, long) in pairs) {
            val a = OrderPreservingCodec.encodeString(short)
            val b = OrderPreservingCodec.encodeString(long)
            assertTrue(
                ByteArrayComparator.compare(a, b) < 0,
                "'$short' 应排在 '$long' 之前",
            )
        }
    }

    @Test
    fun `复合键保序 - 与元组比较一致`() {
        // 模拟 (分区键, 主键) 的复合键，验证逐分量拼接后仍然保序
        val rows = listOf(
            -1L to "z", 1L to "a", 1L to "b",
            2L to "a", 2L to "b", 10L to "a",
        )
        val encoded = rows.map { (k, s) ->
            OrderPreservingCodec.encodeLong(k) + OrderPreservingCodec.encodeString(s)
        }
        for (i in 0 until encoded.size - 1) {
            assertTrue(
                ByteArrayComparator.compare(encoded[i], encoded[i + 1]) < 0,
                "复合键应保持元组序: ${rows[i]} < ${rows[i + 1]}",
            )
        }
    }

    @Test
    fun `随机对拍 - 编码序等于数值序`() {
        val rnd = java.util.Random(42)
        val values = (0 until 2000).map { rnd.nextLong() }
        val sorted = values.sorted()
        val encoded = sorted.map { OrderPreservingCodec.encodeLong(it) }
        for (i in 0 until encoded.size - 1) {
            assertTrue(ByteArrayComparator.compare(encoded[i], encoded[i + 1]) <= 0)
        }
    }
}

class VarintTest {

    @Test
    fun `无符号 varint 往返`() {
        for (v in listOf(0, 1, 127, 128, 300, 16383, 16384, Int.MAX_VALUE)) {
            val out = java.io.ByteArrayOutputStream()
            Varint.writeUnsigned(out, v)
            val bytes = out.toByteArray()
            assertEquals(Varint.sizeOf(v), bytes.size, "sizeOf 与实际长度不一致: $v")
            val (decoded, consumed) = Varint.readUnsigned(bytes, 0)
            assertEquals(v, decoded)
            assertEquals(bytes.size, consumed)
        }
    }

    @Test
    fun `varint 截断时抛损坏异常`() {
        val truncated = byteArrayOf(0x80.toByte(), 0x80.toByte())
        var threw = false
        try { Varint.readUnsigned(truncated, 0) } catch (_: CorruptDataException) { threw = true }
        assertTrue(threw, "截断的 varint 必须抛 CorruptDataException，而不是静默返回错值")
    }
}

class DatumTest {

    private fun schema(vararg cols: Pair<String, ColumnType>) = TableSchema(
        tableId = 1, databaseId = 1, name = "t", version = 1,
        columns = cols.map { ColumnDef(it.first, it.second) },
        primaryKey = listOf(cols[0].first),
    )

    @Test
    fun `行编解码往返 - 全部类型`() {
        val s = schema(
            "a" to ColumnType.BIGINT,
            "b" to ColumnType.VARCHAR,
            "c" to ColumnType.DOUBLE,
            "d" to ColumnType.BOOLEAN,
            "e" to ColumnType.TIMESTAMP,
            "f" to ColumnType.DECIMAL,
        )
        val row = listOf(
            Datum.I64(-42), Datum.Str("hello 世界"), Datum.F64(3.14159),
            Datum.Bool(true), Datum.Ts(1_700_000_000_000L), Datum.Dec(12345, 2),
        )
        val decoded = RowCodec.decode(RowCodec.encode(row), s)
        assertEquals(row, decoded)
    }

    @Test
    fun `行编解码往返 - 含 NULL`() {
        val s = schema("a" to ColumnType.BIGINT, "b" to ColumnType.VARCHAR, "c" to ColumnType.INT)
        val row = listOf(Datum.I64(1), Datum.Null, Datum.Null)
        val decoded = RowCodec.decode(RowCodec.encode(row), s)
        assertEquals(row, decoded)
    }

    @Test
    fun `NULL 排在最前`() {
        assertTrue(Datum.Null < Datum.I64(Long.MIN_VALUE))
        assertTrue(Datum.Null < Datum.Str(""))
    }

    @Test
    fun `数值族之间按数值比较`() {
        assertTrue(Datum.I64(1) < Datum.F64(1.5))
        assertTrue(Datum.I64(2) > Datum.F64(1.5))
        assertTrue(Datum.Dec(150, 2) < Datum.I64(2))
    }

    @Test
    fun `double 保序编码处理负零与符号`() {
        val values = listOf(Double.NEGATIVE_INFINITY, -1e10, -1.0, -0.0, 0.0, 1.0, 1e10, Double.POSITIVE_INFINITY)
        val enc = values.map { KeyEncoder.encodeDoubleOrdered(it) }
        for (i in 0 until enc.size - 1) {
            assertTrue(
                ByteArrayComparator.compare(enc[i], enc[i + 1]) < 0,
                "浮点保序失败: ${values[i]} < ${values[i + 1]}",
            )
        }
    }

    @Test
    fun `主键编码保序 - 负数与字符串混合`() {
        val s = schema("id" to ColumnType.BIGINT, "name" to ColumnType.VARCHAR)
        val rows = listOf(
            listOf(Datum.I64(-5), Datum.Str("z")),
            listOf(Datum.I64(-5), Datum.Str("a")),
            listOf(Datum.I64(0), Datum.Str("a")),
            listOf(Datum.I64(100), Datum.Str("a")),
        )
        val keys = rows.map { KeyEncoder.encodeKey(it, s, listOf(0, 1)) }
        assertTrue(ByteArrayComparator.compare(keys[1], keys[0]) < 0, "-5/a 应排在 -5/z 前")
        assertTrue(ByteArrayComparator.compare(keys[0], keys[2]) < 0, "-5/z 应排在 0/a 前")
        assertTrue(ByteArrayComparator.compare(keys[2], keys[3]) < 0, "0/a 应排在 100/a 前")
    }

    @Test
    fun `主键解码往返`() {
        val s = schema("id" to ColumnType.BIGINT, "name" to ColumnType.VARCHAR)
        val row = listOf(Datum.I64(12345), Datum.Str("测试"))
        val key = KeyEncoder.encodeKey(row, s, listOf(0, 1))
        val decoded = KeyEncoder.decodeComponents(key, s, listOf(0, 1))
        assertEquals(row, decoded)
    }
}

class IntKeyTest {

    @Test
    fun `内部键编解码往返`() {
        val uk = "user-42".toByteArray()
        for (ts in listOf(1L, 1000L, Long.MAX_VALUE - 1)) {
            val ik = InternalKeyTestHelper.encode(uk, ts)
            val (decodedKey, decodedTs) = InternalKeyTestHelper.decode(ik)
            assertContentEquals(uk, decodedKey)
            assertEquals(ts, decodedTs)
        }
    }

    @Test
    fun `同一 key 内 commitTs 降序排列`() {
        val uk = "k".toByteArray()
        val k1 = InternalKeyTestHelper.encode(uk, 100)
        val k2 = InternalKeyTestHelper.encode(uk, 200)
        val k3 = InternalKeyTestHelper.encode(uk, 50)
        // 降序：200 应排在 100 之前，100 应排在 50 之前
        assertTrue(ByteArrayComparator.compare(k2, k1) < 0, "ts=200 应排在 ts=100 之前")
        assertTrue(ByteArrayComparator.compare(k1, k3) < 0, "ts=100 应排在 ts=50 之前")
    }

    @Test
    fun `不同 key 之间按 key 升序`() {
        val a = InternalKeyTestHelper.encode("a".toByteArray(), 999)
        val b = InternalKeyTestHelper.encode("b".toByteArray(), 1)
        assertTrue(ByteArrayComparator.compare(a, b) < 0, "key 'a' 应排在 'b' 之前，与 ts 无关")
    }
}

/** 测试用的轻量代理，避免测试依赖 yb-storage（后者依赖 yb-common）。 */
object InternalKeyTestHelper {
    fun encode(userKey: ByteArray, ts: Long): ByteArray {
        val uk = OrderPreservingCodec.encodeBytes(userKey)
        val out = ByteArray(uk.size + 8)
        System.arraycopy(uk, 0, out, 0, uk.size)
        val inv = ts.inv()
        for (i in 0 until 8) out[uk.size + i] = ((inv ushr (56 - i * 8)) and 0xFF).toByte()
        return out
    }

    fun decode(ik: ByteArray): Pair<ByteArray, Long> {
        val (uk, n) = OrderPreservingCodec.decodeBytes(ik, 0)
        var inv = 0L
        for (i in 0 until 8) inv = (inv shl 8) or (ik[n + i].toLong() and 0xFF)
        return uk to inv.inv()
    }
}
