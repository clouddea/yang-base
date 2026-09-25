package com.cloudea.yangbase.common

import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

/**
 * 值类型与编解码的边界与错误路径。
 *
 * 这些用例覆盖的多是「不该发生但必须处理」的分支：损坏的数据、类型不匹配、
 * 非法输入。它们不产生正确结果，但必须产生**明确的错误**而不是静默的垃圾。
 */
class DatumConversionTest {

    @Test
    fun `整数族的转换`() {
        assertEquals(42L, Datum.I64(42).asLong())
        assertEquals(1L, Datum.Bool(true).asLong())
        assertEquals(0L, Datum.Bool(false).asLong())
        assertEquals(1_700_000_000_000L, Datum.Ts(1_700_000_000_000L).asLong())
        assertEquals(123L, Datum.Dec(12345, 2).asLong(), "123.45 截断为 123")
        assertEquals(123L, Datum.Dec(123, 0).asLong())
    }

    @Test
    fun `浮点转换`() {
        assertEquals(42.0, Datum.I64(42).asDouble())
        assertEquals(1.5, Datum.F64(1.5).asDouble())
        assertEquals(1.5, Datum.Dec(150, 2).asDouble())
        assertEquals(1.5, Datum.Str("1.5").asDouble())
    }

    @Test
    fun `字符串形式的数字可被解析`() {
        assertEquals(42L, Datum.Str("42").asLong())
        assertEquals(3.14, Datum.Str("3.14").asDouble())
    }

    @Test
    fun `非数字字符串转数值报明确的错误`() {
        val e = assertFailsWith<SqlException> { Datum.Str("abc").asLong() }
        assertEquals(MySqlErrors.ER_TRUNCATED_WRONG_VALUE, e.errorCode)
    }

    @Test
    fun `NULL 转数值报错而不是当成 0`() {
        // 把 NULL 当 0 会让聚合结果静默出错，必须显式失败
        assertFailsWith<SqlException> { Datum.Null.asLong() }
        assertFailsWith<SqlException> { Datum.Null.asDouble() }
    }

    @Test
    fun `布尔转换`() {
        assertTrue(Datum.Bool(true).asBoolean())
        assertTrue(!Datum.Bool(false).asBoolean())
        assertTrue(!Datum.Null.asBoolean(), "SQL 三值逻辑里 NULL 在布尔上下文中为假")
        assertTrue(Datum.I64(1).asBoolean())
        assertTrue(!Datum.I64(0).asBoolean())
    }

    @Test
    fun `字符串渲染`() {
        assertEquals("42", Datum.I64(42).asString())
        assertEquals("", Datum.Null.asString())
        assertEquals("1", Datum.Bool(true).asString())
        assertEquals("123.45", Datum.Dec(12345, 2).asString())
        assertEquals("-123.45", Datum.Dec(-12345, 2).asString())
        assertEquals("123", Datum.Dec(123, 0).asString())
        assertEquals("0.05", Datum.Dec(5, 2).asString())
        assertEquals("1", Datum.Bool(true).toString())
    }

    @Test
    fun `of 从 Kotlin 原生类型构造`() {
        assertEquals(Datum.I64(1), Datum.of(1))
        assertEquals(Datum.I64(1), Datum.of(1L))
        assertEquals(Datum.I64(1), Datum.of(1.toShort()))
        assertEquals(Datum.I64(1), Datum.of(1.toByte()))
        assertEquals(Datum.F64(1.0), Datum.of(1.0f))
        assertEquals(Datum.Str("x"), Datum.of("x"))
        assertEquals(Datum.Bool(true), Datum.of(true))
        assertEquals(Datum.Null, Datum.of(null))
        @Suppress("UNCHECKED_CAST")
        assertEquals(Datum.I64(5), Datum.of(Datum.I64(5)) as Datum)
        assertFailsWith<IllegalArgumentException> { Datum.of(listOf(1)) }
    }

    @Test
    fun `排序 - 跨类型有确定的全序`() {
        val sorted = listOf(
            Datum.Str("a"), Datum.I64(99), Datum.Null, Datum.F64(1.0), Datum.Bin(byteArrayOf(1)),
        ).sorted()
        // NULL 最小（与 MySQL 一致）；数值族之间按**数值**比较而不是按类型；
        // 字符串排在数值之后；二进制最后。
        assertEquals(Datum.Null, sorted[0], "NULL 最小")
        assertEquals(Datum.F64(1.0), sorted[1], "数值按数值序：1.0 < 99")
        assertEquals(Datum.I64(99), sorted[2])
        assertEquals(Datum.Str("a"), sorted[3])
        assertEquals(Datum.Bin(byteArrayOf(1)), sorted[4])
    }

    @Test
    fun `二进制值的相等性按内容比较`() {
        assertEquals(Datum.Bin(byteArrayOf(1, 2)), Datum.Bin(byteArrayOf(1, 2)))
        assertTrue(Datum.Bin(byteArrayOf(1, 2)) != Datum.Bin(byteArrayOf(1, 3)))
    }
}

class RowCodecEdgeCaseTest {

    private fun schema(vararg cols: Pair<String, ColumnType>) = TableSchema(
        tableId = 1, databaseId = 1, name = "t", version = 1,
        columns = cols.map { ColumnDef(it.first, it.second, scale = 2) },
        primaryKey = listOf(cols[0].first),
    )

    @Test
    fun `DECIMAL 的小数位数随值编码 - ALTER 改精度后旧行仍能正确解码`() {
        // 若把 scale 只放在 schema 里，改精度会让所有旧行解码出错误的数值
        val s = TableSchema(
            tableId = 1, databaseId = 1, name = "t", version = 2,
            columns = listOf(ColumnDef("d", ColumnType.DECIMAL, scale = 4)),  // schema 说是 4 位
            primaryKey = listOf("d"),
        )
        val encoded = RowCodec.encode(listOf(Datum.Dec(12345, 2)))   // 值自带 2 位
        val decoded = RowCodec.decode(encoded, s)
        assertEquals(Datum.Dec(12345, 2), decoded[0], "必须按值自带的 scale 解码，而不是 schema 的")
    }

    @Test
    fun `全 NULL 行`() {
        val s = schema("a" to ColumnType.BIGINT, "b" to ColumnType.VARCHAR)
        val allNull = listOf(Datum.Null, Datum.Null)
        assertEquals(allNull, RowCodec.decode(RowCodec.encode(allNull), s))
    }

    @Test
    fun `行比 schema 窄时按演化规则补默认值 - 这是 ADD COLUMN 的基础`() {
        val s = schema("a" to ColumnType.BIGINT, "b" to ColumnType.VARCHAR)
        // 行自带列数（格式 v2）：1 列的行按 2 列的 schema 解 = 之后加过一列。
        // 这是**合法**的，尾部补默认值（没有默认值就是 NULL）。
        // 在 v1 格式里这一条会抛异常，那是 ADD COLUMN 无法做成 O(1) 的原因。
        val row = RowCodec.decode(RowCodec.encode(listOf(Datum.I64(1))), s)
        assertEquals(2, row.size)
        assertEquals(1L, (row[0] as Datum.I64).v)
        assertEquals(Datum.Null, row[1])
    }

    @Test
    fun `行比 schema 宽时只解出 schema 的列 - 这是 DROP COLUMN 的基础`() {
        val wide = schema("a" to ColumnType.BIGINT, "b" to ColumnType.VARCHAR, "c" to ColumnType.INT)
        val narrow = schema("a" to ColumnType.BIGINT, "b" to ColumnType.VARCHAR)
        // 3 列的行按 2 列的 schema 解 = 之后删过末尾的列。多余的字节直接不读，
        // 不需要知道被删列的宽度——不去读它就不需要跳过它
        val row = RowCodec.decode(RowCodec.encode(listOf(Datum.I64(1), Datum.Str("x"), Datum.I64(9))), narrow)
        assertEquals(2, row.size)
        assertEquals(1L, (row[0] as Datum.I64).v)
        assertEquals("x", (row[1] as Datum.Str).v)
    }

    @Test
    fun `列数荒谬时判定为损坏 - 而不是把非 v2 数据当 v2 解`() {
        val s = schema("a" to ColumnType.BIGINT)
        // 一串随机字节被当成 v2 行来解时，列数几乎必然不合理。
        // 必须报错——静默按荒谬的列数解下去会给出错位的值
        assertFailsWith<CorruptDataException> {
            RowCodec.decode(ByteArray(8) { 0xFF.toByte() }, s)
        }
    }

    @Test
    fun `超长的字符串值`() {
        val s = schema("a" to ColumnType.BIGINT, "b" to ColumnType.TEXT)
        val big = "x".repeat(100_000)
        val row = listOf(Datum.I64(1), Datum.Str(big))
        assertEquals(row, RowCodec.decode(RowCodec.encode(row), s))
    }

    @Test
    fun `UTF-8 多字节字符`() {
        val s = schema("a" to ColumnType.BIGINT, "b" to ColumnType.TEXT)
        for (text in listOf("中文", "emoji 🎉", "ümlaut", "日本語", "\u0000内嵌空字节")) {
            val row = listOf(Datum.I64(1), Datum.Str(text))
            assertEquals(row, RowCodec.decode(RowCodec.encode(row), s), "往返失败: $text")
        }
    }

    @Test
    fun `极端数值边界`() {
        val s = schema(
            "a" to ColumnType.BIGINT, "b" to ColumnType.INT,
            "c" to ColumnType.DOUBLE, "d" to ColumnType.DECIMAL,
        )
        val row = listOf(
            Datum.I64(Long.MIN_VALUE), Datum.I64(Int.MIN_VALUE.toLong()),
            Datum.F64(Double.NEGATIVE_INFINITY), Datum.Dec(Long.MIN_VALUE, 2),
        )
        assertEquals(row, RowCodec.decode(RowCodec.encode(row), s))
        val row2 = listOf(
            Datum.I64(Long.MAX_VALUE), Datum.I64(Int.MAX_VALUE.toLong()),
            Datum.F64(Double.POSITIVE_INFINITY), Datum.Dec(Long.MAX_VALUE, 2),
        )
        assertEquals(row2, RowCodec.decode(RowCodec.encode(row2), s))
    }

    @Test
    fun `NaN 往返`() {
        val s = schema("a" to ColumnType.BIGINT, "b" to ColumnType.DOUBLE)
        val enc = RowCodec.encode(listOf(Datum.I64(1), Datum.F64(Double.NaN)))
        val decoded = RowCodec.decode(enc, s)
        assertTrue((decoded[1] as Datum.F64).v.isNaN())
    }

    @Test
    fun `截断的行数据抛损坏异常`() {
        val s = schema("a" to ColumnType.BIGINT, "b" to ColumnType.VARCHAR)
        val good = RowCodec.encode(listOf(Datum.I64(1), Datum.Str("hello")))
        assertFailsWith<CorruptDataException> {
            RowCodec.decode(good.copyOf(good.size - 3), s)
        }
    }

    @Test
    fun `过短的行数据抛损坏异常`() {
        val s = schema("a" to ColumnType.BIGINT)
        assertFailsWith<CorruptDataException> { RowCodec.decode(ByteArray(0), s) }
    }
}

class OrderPreservingEdgeCaseTest {

    @Test
    fun `解码非法的转义序列时抛损坏异常`() {
        // 0x00 后面只允许跟 0xFF（转义）或 0x00（终止）
        assertFailsWith<CorruptDataException> {
            OrderPreservingCodec.decodeBytes(byteArrayOf(0x00, 0x42, 0x00, 0x00))
        }
    }

    @Test
    fun `解码缺少终结符的字节串抛损坏异常`() {
        assertFailsWith<CorruptDataException> {
            OrderPreservingCodec.decodeBytes(byteArrayOf(0x61, 0x62))   // "ab" 无终结符
        }
    }

    @Test
    fun `转义在终结符前截断时抛损坏异常`() {
        assertFailsWith<CorruptDataException> {
            OrderPreservingCodec.decodeBytes(byteArrayOf(0x61, 0x00))
        }
    }

    @Test
    fun `长度编码的边界值`() {
        for (v in listOf(0, 1, 127, 128, 16383, 16384, Int.MAX_VALUE)) {
            val out = java.io.ByteArrayOutputStream()
            Varint.writeUnsigned(out, v.toLong())
            val bytes = out.toByteArray()
            val (decoded, consumed) = Varint.readUnsignedLong(bytes, 0)
            assertEquals(v.toLong(), decoded)
            assertEquals(bytes.size, consumed)
        }
    }

    @Test
    fun `负数不能被编码为无符号 varint`() {
        assertFailsWith<IllegalArgumentException> {
            Varint.writeUnsigned(java.io.ByteArrayOutputStream(), -1)
        }
    }

    @Test
    fun `过长的 varint 抛损坏异常`() {
        val tooLong = ByteArray(12) { 0x80.toByte() }
        assertFailsWith<CorruptDataException> { Varint.readUnsigned(tooLong, 0) }
    }

    @Test
    fun `short 往返与边界`() {
        for (v in listOf(Short.MIN_VALUE, -1, 0, 1, Short.MAX_VALUE)) {
            assertEquals(v, OrderPreservingCodec.decodeShort(OrderPreservingCodec.encodeShort(v)))
        }
        assertTrue(
            ByteArrayComparator.compare(
                OrderPreservingCodec.encodeShort(Short.MIN_VALUE),
                OrderPreservingCodec.encodeShort(Short.MAX_VALUE),
            ) < 0
        )
    }

    @Test
    fun `Bytes 包装的相等性与比较`() {
        val a = Bytes(byteArrayOf(1, 2))
        val b = Bytes(byteArrayOf(1, 2))
        val c = Bytes(byteArrayOf(1, 3))
        assertEquals(a, b)
        assertTrue(a != c)
        assertTrue(a < c)
        assertEquals(a.hashCode(), b.hashCode())
        assertEquals(2, a.size())
    }

    @Test
    fun `CRC32C 对相同输入稳定`() {
        val data = "hello".toByteArray()
        assertEquals(Checksums.crc32c(data), Checksums.crc32c(data))
        assertTrue(Checksums.crc32c(data) != Checksums.crc32c("world".toByteArray()))
    }

    @Test
    fun `大端读写往返`() {
        val out = java.io.ByteArrayOutputStream()
        BigEndian.writeLong(out, 0x0123456789ABCDEFL)
        BigEndian.writeInt(out, 0x11223344)
        BigEndian.writeShort(out, 0xAABB)
        val b = out.toByteArray()
        assertEquals(14, b.size)
        assertEquals(0x0123456789ABCDEFL, BigEndian.readLong(b, 0))
        assertEquals(0x11223344, BigEndian.readInt(b, 8))
        assertEquals(0xAABB, BigEndian.readShort(b, 12))
    }
}

class KeyEncoderEdgeCaseTest {

    private val schema = TableSchema(
        tableId = 1, databaseId = 1, name = "t", version = 1,
        columns = listOf(
            ColumnDef("i", ColumnType.BIGINT), ColumnDef("s", ColumnType.VARCHAR),
            ColumnDef("d", ColumnType.DOUBLE), ColumnDef("b", ColumnType.BOOLEAN),
        ),
        primaryKey = listOf("i"),
    )

    @Test
    fun `NULL 排在所有非 NULL 之前`() {
        val nullKey = KeyEncoder.encodeDatum(Datum.Null, ColumnType.BIGINT)
        val minKey = KeyEncoder.encodeDatum(Datum.I64(Long.MIN_VALUE), ColumnType.BIGINT)
        assertTrue(ByteArrayComparator.compare(nullKey, minKey) < 0)
    }

    @Test
    fun `浮点保序包含极值`() {
        val values = listOf(
            Double.NEGATIVE_INFINITY, -1e308, -1.0, -Double.MIN_VALUE, -0.0,
            0.0, Double.MIN_VALUE, 1.0, 1e308, Double.POSITIVE_INFINITY,
        )
        val enc = values.map { KeyEncoder.encodeDoubleOrdered(it) }
        for (i in 0 until enc.size - 1) {
            assertTrue(
                ByteArrayComparator.compare(enc[i], enc[i + 1]) < 0,
                "浮点保序失败: ${values[i]} 应小于 ${values[i + 1]}",
            )
        }
        for (v in values) {
            assertEquals(v, KeyEncoder.decodeDoubleOrdered(KeyEncoder.encodeDoubleOrdered(v)))
        }
    }

    @Test
    fun `双精度往返 - 随机对拍`() {
        val rnd = java.util.Random(7)
        repeat(500) {
            val v = java.lang.Double.longBitsToDouble(rnd.nextLong())
            if (v.isNaN()) return@repeat
            assertEquals(v, KeyEncoder.decodeDoubleOrdered(KeyEncoder.encodeDoubleOrdered(v)))
        }
    }

    @Test
    fun `复合键的部分前缀比较`() {
        val rows = listOf(
            listOf(Datum.I64(1), Datum.Str("a")),
            listOf(Datum.I64(1), Datum.Str("b")),
            listOf(Datum.I64(2), Datum.Str("a")),
        )
        val keys = rows.map { KeyEncoder.encodeKey(it, schema, listOf(0, 1)) }
        assertTrue(ByteArrayComparator.compare(keys[0], keys[1]) < 0)
        assertTrue(ByteArrayComparator.compare(keys[1], keys[2]) < 0)

        val prefix = KeyEncoder.encodeKey(listOf(Datum.I64(1), Datum.Null), schema, listOf(0))
        assertTrue(KeyEncoder.hasPrefix(keys[0], prefix))
        assertTrue(KeyEncoder.hasPrefix(keys[1], prefix))
        assertTrue(!KeyEncoder.hasPrefix(keys[2], prefix))
    }

    @Test
    fun `主键分量解码`() {
        val row = listOf(Datum.I64(-7), Datum.Str("x"), Datum.F64(1.5), Datum.Bool(true))
        val key = KeyEncoder.encodeKey(row, schema, listOf(0, 1, 2, 3))
        assertEquals(row, KeyEncoder.decodeComponents(key, schema, listOf(0, 1, 2, 3)))
    }

    @Test
    fun `布尔列按整数保序`() {
        val f = KeyEncoder.encodeDatum(Datum.Bool(false), ColumnType.BOOLEAN)
        val t = KeyEncoder.encodeDatum(Datum.Bool(true), ColumnType.BOOLEAN)
        assertTrue(ByteArrayComparator.compare(f, t) < 0, "false 应排在 true 之前")
    }
}

class SchemaTest {

    private val schema = TableSchema(
        tableId = 1, databaseId = 1, name = "t", version = 1,
        columns = listOf(
            ColumnDef("id", ColumnType.BIGINT, nullable = false),
            ColumnDef("Name", ColumnType.VARCHAR, length = 64),
            ColumnDef("age", ColumnType.INT),
        ),
        primaryKey = listOf("id"),
    )

    @Test
    fun `列名查找大小写不敏感`() {
        // MySQL 的列名比较是大小写不敏感的
        assertEquals(1, schema.indexOf("name"))
        assertEquals(1, schema.indexOf("NAME"))
        assertEquals(1, schema.indexOf("Name"))
    }

    @Test
    fun `不存在的列抛带错误码的异常`() {
        val e = assertFailsWith<SqlException> { schema.indexOf("nope") }
        assertEquals(MySqlErrors.ER_BAD_FIELD_ERROR, e.errorCode)
        assertEquals("42S22", e.sqlState)
    }

    @Test
    fun `indexOfOrNull 对不存在的列返回 null`() {
        assertEquals(0, schema.indexOfOrNull("id"))
        assertEquals(null, schema.indexOfOrNull("nope"))
    }

    @Test
    fun `主键下标`() {
        assertEquals(listOf(0), schema.primaryKeyIndexes)
    }

    @Test
    fun `列类型分类`() {
        assertTrue(ColumnType.BIGINT.isInteger)
        assertTrue(ColumnType.BIGINT.isNumeric)
        assertTrue(ColumnType.DOUBLE.isNumeric)
        assertTrue(!ColumnType.DOUBLE.isInteger)
        assertTrue(ColumnType.VARCHAR.isString)
        assertTrue(ColumnType.TEXT.isString)
        assertTrue(ColumnType.DATETIME.isTemporal)
        assertTrue(!ColumnType.BIGINT.isString)
    }
}

class ErrorsTest {

    @Test
    fun `异常层次结构`() {
        val cases: List<YangBaseException> = listOf(
            PreconditionFailedException("a"),
            ConcurrentModificationException("b"),
            CapabilityNotSupportedException("c"),
            ObjectStoreException("d"),
            LeaseLostException(
                ShardId(TableId(1), PartitionId(0)), Epoch(1)
            ),
            ObjectNotFoundException("k"),
            CorruptDataException("e"),
            SqlException(MySqlErrors.ER_PARSE_ERROR, "42000", "f"),
        )
        for (e in cases) {
            assertTrue(e is RuntimeException, "${e::class.simpleName} 应可被抛出")
            assertTrue(e.message!!.isNotEmpty(), "${e::class.simpleName} 应带描述性消息")
        }
    }

    @Test
    fun `SqlException 携带错误码与 SQLSTATE`() {
        val e = SqlException(MySqlErrors.ER_NO_SUCH_TABLE, "42S02", "no table")
        assertEquals(1146, e.errorCode)
        assertEquals("42S02", e.sqlState)
    }

    @Test
    fun `MySQL 错误码常量与官方一致`() {
        assertEquals(1146, MySqlErrors.ER_NO_SUCH_TABLE)
        assertEquals(1049, MySqlErrors.ER_BAD_DB_ERROR)
        assertEquals(1062, MySqlErrors.ER_DUP_ENTRY)
        assertEquals(1064, MySqlErrors.ER_PARSE_ERROR)
        assertEquals(1045, MySqlErrors.ER_ACCESS_DENIED)
    }

    @Test
    fun `异常可以携带原因链`() {
        val cause = IllegalStateException("root")
        val e = ObjectStoreException("wrap", cause)
        assertEquals(cause, e.cause)
    }
}
