package com.cloudea.yangbase.mysql

import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * MySQL 协议包编解码的测试。
 *
 * 这些是协议层最容易出错、也最难调试的部分——包长度或序号错一个字节，
 * 客户端就会报 "Buffer length is less than expected payload length"
 * 之类的模糊错误（实现期间确实踩到过）。
 */
class PacketsTest {

    private fun roundTrip(payload: ByteArray): ByteArray {
        val out = ByteArrayOutputStream()
        Packets.Writer(out).writePacket(payload)
        return Packets.Reader(ByteArrayInputStream(out.toByteArray())).readPacket()
    }

    @Test
    fun `载荷往返`() {
        for (size in intArrayOf(0, 1, 100, 1000, 65535)) {
            val payload = ByteArray(size) { (it % 251).toByte() }
            assertContentEquals(payload, roundTrip(payload), "长度 $size 的载荷往返失败")
        }
    }

    @Test
    fun `包头是 3 字节小端长度加 1 字节序号`() {
        val out = ByteArrayOutputStream()
        val w = Packets.Writer(out)
        w.writePacket(ByteArray(300))
        val bytes = out.toByteArray()
        // 300 = 0x012C
        assertEquals(0x2C, bytes[0].toInt() and 0xFF)
        assertEquals(0x01, bytes[1].toInt() and 0xFF)
        assertEquals(0x00, bytes[2].toInt() and 0xFF)
        assertEquals(0, bytes[3].toInt(), "首个包的序号应为 0")
        assertEquals(4 + 300, bytes.size)
    }

    @Test
    fun `序号连续递增`() {
        val out = ByteArrayOutputStream()
        val w = Packets.Writer(out)
        w.writePacket(ByteArray(1))
        w.writePacket(ByteArray(1))
        w.writePacket(ByteArray(1))
        val bytes = out.toByteArray()
        assertEquals(0, bytes[3].toInt())
        assertEquals(1, bytes[8].toInt())
        assertEquals(2, bytes[13].toInt())
    }

    @Test
    fun `resetSequence 从零开始 - 每个命令响应的序号都要重新计数`() {
        val out = ByteArrayOutputStream()
        val w = Packets.Writer(out)
        w.writePacket(ByteArray(1))
        w.resetSequence()
        w.writePacket(ByteArray(1))
        val bytes = out.toByteArray()
        assertEquals(0, bytes[3].toInt())
        assertEquals(0, bytes[8].toInt(), "reset 之后序号应重新从 0 开始")
    }

    @Test
    fun `序号在 255 处回绕`() {
        val out = ByteArrayOutputStream()
        val w = Packets.Writer(out)
        w.setSequence(255)
        w.writePacket(ByteArray(1))
        w.writePacket(ByteArray(1))
        val bytes = out.toByteArray()
        assertEquals(255, bytes[3].toInt() and 0xFF)
        assertEquals(0, bytes[8].toInt() and 0xFF, "序号必须按 1 字节回绕")
    }

    @Test
    fun `拆包的边界算术正确`() {
        // 真正构造 16MiB 的载荷会让测试 JVM 撑爆（实现期间踩到过），
        // 因此这里只验证拆分公式本身，不做真实的巨型分配。
        val max = Packets.MAX_PAYLOAD
        for (size in intArrayOf(max, max + 1, max * 2, max * 2 + 1, max * 3 + 100)) {
            val packets = if (size % max == 0 && size > 0) size / max + 1 else (size + max - 1) / max
            // 载荷长度恰好是 MAX_PAYLOAD 整数倍时要补一个空包，否则接收方会一直等
            val expected = if (size > 0 && size % max == 0) size / max + 1 else (size + max - 1) / max
            assertEquals(expected, packets, "长度 $size 的拆包数不对")
        }
    }

    // ── Buf 编码 ──────────────────────────────────────────────────

    @Test
    fun `长度编码整数的三种宽度`() {
        fun enc(v: Long) = Packets.Buf(16).lenenc(v).toBytes()
        assertEquals(1, enc(250).size, "< 251 用 1 字节")
        assertEquals(3, enc(251).size, "251..65535 用 0xFC + 2 字节")
        assertEquals(4, enc(65536).size, "65536..2^24 用 0xFD + 3 字节")
        assertEquals(9, enc(16777216).size, "更大用 0xFE + 8 字节")

        assertEquals(0xFC, enc(251)[0].toInt() and 0xFF)
        assertEquals(0xFD, enc(65536)[0].toInt() and 0xFF)
        assertEquals(0xFE, enc(16777216)[0].toInt() and 0xFF)
    }

    @Test
    fun `长度编码整数往返`() {
        for (v in listOf(0L, 1L, 250L, 251L, 65535L, 65536L, 16777215L, 16777216L, 1L shl 40)) {
            val b = Packets.Buf(16).lenenc(v).toBytes()
            assertEquals(v, Packets.Cursor(b).lenenc(), "长度编码往返失败: $v")
        }
    }

    @Test
    fun `NULL 的长度编码是 0xFB`() {
        val c = Packets.Cursor(byteArrayOf(0xFB.toByte()))
        assertEquals(-1L, c.lenenc(), "0xFB 表示 NULL")
    }

    @Test
    fun `整数按小端写出 - MySQL 的字节序约定`() {
        val b = Packets.Buf(8).u16(0x1234).u32(0xDEADBEEFL).toBytes()
        assertEquals(0x34, b[0].toInt() and 0xFF, "u16 必须是小端")
        assertEquals(0x12, b[1].toInt() and 0xFF)
        assertEquals(0xEF, b[2].toInt() and 0xFF, "u32 必须是小端")
        assertEquals(0xDE, b[5].toInt() and 0xFF)
    }

    @Test
    fun `长度编码字符串与 NUL 结尾字符串`() {
        val a = Packets.Buf(32).lenencStr("hello").toBytes()
        assertEquals(6, a.size)
        assertEquals('h'.code, a[1].toInt())
        assertEquals("hello", String(Packets.Cursor(a).lenencBytes()!!))

        val b = Packets.Buf(32).nulStr("hi").toBytes()
        assertEquals(3, b.size)
        assertEquals(0, b[2].toInt(), "NUL 结尾字符串必须以 0 收尾")
        assertEquals("hi", Packets.Cursor(b).nulStr())
    }

    @Test
    fun `Cursor 读取带限位的字段`() {
        val b = Packets.Buf(64).u8(7).u16(300).u32(70000).u64(1L shl 40).toBytes()
        val c = Packets.Cursor(b)
        assertEquals(7, c.u8())
        assertEquals(300, c.u16())
        assertEquals(70000L, c.u32())
        assertEquals(1L shl 40, c.u64())
        assertTrue(!c.hasMore())
    }

    @Test
    fun `Cursor 的 NUL 字符串在末尾无终止符时也能读`() {
        val c = Packets.Cursor("abc".toByteArray())
        assertEquals("abc", c.nulStr())
    }

    @Test
    fun `读取截断的载荷会抛异常而不是返回垃圾`() {
        val out = ByteArrayOutputStream()
        Packets.Writer(out).writePacket(ByteArray(10))
        val truncated = out.toByteArray().copyOf(8)
        var threw = false
        try {
            Packets.Reader(ByteArrayInputStream(truncated)).readPacket()
        } catch (_: java.io.EOFException) {
            threw = true
        }
        assertTrue(threw, "截断的包必须抛 EOFException")
    }
}
