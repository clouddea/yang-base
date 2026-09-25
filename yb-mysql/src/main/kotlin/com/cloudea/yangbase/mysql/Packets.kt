package com.cloudea.yangbase.mysql

import java.io.EOFException
import java.io.InputStream
import java.io.OutputStream

/**
 * MySQL 协议的包格式。
 *
 * 每个包 = 3 字节小端长度 + 1 字节序号 + 载荷。
 * 载荷 ≥ 16MiB 时要拆成多个包，本实现里结果集是按批次写的，因此只在
 * 极端情况下才会触发拆包；拆包逻辑仍然实现了，因为静默截断是不可接受的。
 */
object Packets {

    const val MAX_PAYLOAD = 0xFFFFFF

    class Reader(private val input: InputStream) {
        var sequence: Int = 0
            private set

        fun resetSequence() { sequence = 0 }

        /**
         * 读一个**完整**的逻辑包。
         *
         * ⚠️ 协议规定：载荷达到 `0xFFFFFF` 时表示"还有后续包"，要一直读到
         * 载荷小于该值为止。写侧（[Writer.writePacket]）早就实现了拆分，
         * 读侧曾经只读一个包——后果不是"包不完整"这么温和：
         * **剩下的字节会被当成下一条命令**（首字节被解释成命令码），
         * 于是客户端发的一条 20 MiB 的 INSERT 会被静默截断成 16 MiB 执行，
         * 尾部还被当成一条随机命令。所以这里必须与写侧对称地重组。
         */
        fun readPacket(): ByteArray {
            val first = readOnePacket()
            if (first.size < MAX_PAYLOAD) return first
            val out = java.io.ByteArrayOutputStream(first.size * 2)
            out.write(first)
            while (true) {
                val next = readOnePacket()
                out.write(next)
                if (next.size < MAX_PAYLOAD) break
            }
            return out.toByteArray()
        }

        private fun readOnePacket(): ByteArray {
            val len = readFully(3)
            val payloadLen = (len[0].toInt() and 0xFF) or
                ((len[1].toInt() and 0xFF) shl 8) or
                ((len[2].toInt() and 0xFF) shl 16)
            val seq = readFully(1)[0].toInt() and 0xFF
            sequence = seq + 1
            if (payloadLen == 0) return ByteArray(0)
            return readFully(payloadLen)
        }

        private fun readFully(n: Int): ByteArray {
            val buf = ByteArray(n)
            var read = 0
            while (read < n) {
                val r = input.read(buf, read, n - read)
                if (r < 0) throw EOFException("连接已关闭（已读 $read/$n 字节）")
                read += r
            }
            return buf
        }
    }

    class Writer(private val out: OutputStream) {
        var sequence: Int = 0
            private set

        fun resetSequence() { sequence = 0 }

        fun writePacket(payload: ByteArray) {
            var offset = 0
            do {
                val remaining = payload.size - offset
                val chunk = minOf(remaining, MAX_PAYLOAD)
                writeHeader(chunk, sequence)
                out.write(payload, offset, chunk)
                offset += chunk
                sequence = (sequence + 1) and 0xFF
            } while (remaining > MAX_PAYLOAD)
            // 载荷长度恰好是 MAX_PAYLOAD 的整数倍时，需要补一个空包
            if (payload.size > 0 && payload.size % MAX_PAYLOAD == 0) {
                writeHeader(0, sequence)
                sequence = (sequence + 1) and 0xFF
            }
            out.flush()
        }

        private fun writeHeader(len: Int, seq: Int) {
            out.write(len and 0xFF)
            out.write((len ushr 8) and 0xFF)
            out.write((len ushr 16) and 0xFF)
            out.write(seq and 0xFF)
        }

        /** 直接写一个已编码好的载荷（调用方已算好序号）。 */
        fun bumpSequence() { sequence = (sequence + 1) and 0xFF }

        fun setSequence(s: Int) { sequence = s }
    }

    /** 增量式载荷构造器。所有整数按 MySQL 的约定编码。 */
    class Buf(initial: Int = 64) {
        private var buf = ByteArray(initial)
        private var len = 0

        private fun ensure(extra: Int) {
            if (len + extra > buf.size) {
                var n = buf.size * 2
                while (n < len + extra) n *= 2
                buf = buf.copyOf(n)
            }
        }

        fun u8(v: Int): Buf { ensure(1); buf[len++] = (v and 0xFF).toByte(); return this }
        fun u16(v: Int): Buf { ensure(2); buf[len++] = (v and 0xFF).toByte(); buf[len++] = ((v ushr 8) and 0xFF).toByte(); return this }
        fun u24(v: Int): Buf { ensure(3); buf[len++] = (v and 0xFF).toByte(); buf[len++] = ((v ushr 8) and 0xFF).toByte(); buf[len++] = ((v ushr 16) and 0xFF).toByte(); return this }
        fun u32(v: Long): Buf {
            ensure(4)
            buf[len++] = (v and 0xFF).toByte()
            buf[len++] = ((v ushr 8) and 0xFF).toByte()
            buf[len++] = ((v ushr 16) and 0xFF).toByte()
            buf[len++] = ((v ushr 24) and 0xFF).toByte()
            return this
        }

        fun u48(v: Long): Buf { u32(v); u16((v ushr 32).toInt()); return this }
        fun u64(v: Long): Buf { ensure(8); for (i in 0 until 8) buf[len++] = ((v ushr (i * 8)) and 0xFF).toByte(); return this }

        fun bytes(b: ByteArray): Buf { ensure(b.size); System.arraycopy(b, 0, buf, len, b.size); len += b.size; return this }

        fun nul(): Buf { ensure(1); buf[len++] = 0; return this }

        fun zeros(n: Int): Buf { ensure(n); java.util.Arrays.fill(buf, len, len + n, 0); len += n; return this }

        /** 长度编码整数：<251 用 1 字节，<65536 用 0xFC+2 字节，其余用 0xFD+3 或 0xFE+8。 */
        fun lenenc(v: Long): Buf = when {
            v < 251L -> u8(v.toInt())
            v < 65536L -> { u8(0xFC); u16(v.toInt()) }
            v < 16777216L -> { u8(0xFD); u24(v.toInt()) }
            else -> { u8(0xFE); u64(v) }
        }

        fun lenencBytes(b: ByteArray): Buf { lenenc(b.size.toLong()); return bytes(b) }

        fun lenencStr(s: String): Buf = lenencBytes(s.toByteArray(Charsets.UTF_8))

        fun lenencNulStr(s: String): Buf = nulStr(s)

        fun nulStr(s: String): Buf { bytes(s.toByteArray(Charsets.UTF_8)); return nul() }

        val size: Int get() = len

        fun toBytes(): ByteArray = buf.copyOf(len)
    }

    /** 载荷读取器。 */
    class Cursor(private val data: ByteArray, private var pos: Int = 0) {
        val remaining: Int get() = data.size - pos
        fun hasMore(): Boolean = pos < data.size

        fun u8(): Int = data[pos++].toInt() and 0xFF
        fun u16(): Int = u8() or (u8() shl 8)
        fun u24(): Int = u8() or (u8() shl 8) or (u8() shl 16)
        fun u32(): Long = u16().toLong() or (u16().toLong() shl 16)

        fun u64(): Long {
            var v = 0L
            for (i in 0 until 8) v = v or ((data[pos++].toLong() and 0xFF) shl (i * 8))
            return v
        }

        fun lenenc(): Long {
            val first = u8()
            return when {
                first < 0xFB -> first.toLong()
                first == 0xFB -> -1L          // NULL
                first == 0xFC -> u16().toLong()
                first == 0xFD -> u24().toLong()
                first == 0xFE -> u64()
                else -> -1L
            }
        }

        /**
         * 读一个长度编码字符串。长度超过剩余字节时**必须报协议错**。
         *
         * 曾经直接 `copyOfRange`（先算后取、无校验）：畸形包会抛
         * `IndexOutOfBoundsException`，最终被兜成 `1105` + **把内部缓冲区偏移
         * 回给客户端**（`fromIndex: 20, toIndex: 2147483647`）。而且握手路径
         * 在认证之前就会读它——一条畸形包就能把内部数字泄漏出去。
         */
        fun lenencBytes(): ByteArray? {
            val n = lenenc()
            if (n < 0) return null
            if (n > remaining()) {
                throw ProtocolException("长度编码字符串声称 $n 字节，但包里只剩 ${remaining()} 字节")
            }
            val b = data.copyOfRange(pos, pos + n.toInt())
            pos += n.toInt()
            return b
        }

        /** 剩余可读字节数。`skip`/`bytes`/`u16` 等都要用它做边界校验。 */
        fun remaining(): Int = data.size - pos

        fun nulStr(): String {
            val start = pos
            while (pos < data.size && data[pos] != 0.toByte()) pos++
            val s = String(data, start, pos - start, Charsets.UTF_8)
            if (pos < data.size) pos++   // 跳过 '\0'
            return s
        }

        fun restBytes(): ByteArray = data.copyOfRange(pos, data.size)

        fun skip(n: Int) {
            require(n >= 0) { "skip 不接受负数（$n）" }
            if (n > remaining()) throw ProtocolException("试图跳过 $n 字节，但包里只剩 ${remaining()} 字节")
            pos += n
        }

        fun bytes(n: Int): ByteArray {
            require(n >= 0) { "bytes 不接受负数（$n）" }
            if (n > remaining()) throw ProtocolException("试图读 $n 字节，但包里只剩 ${remaining()} 字节")
            val b = data.copyOfRange(pos, pos + n); pos += n; return b
        }
    }

    /**
     * 客户端发来的包不符合协议。
     *
     * 单独一个类型是为了让上层把它**当作客户端错误**（回一个干净的协议错误包）
     * 而不是"服务端内部异常"——后者会把缓冲区偏移、对象键这类内部信息
     * 原样发给客户端。
     */
    class ProtocolException(message: String) : Exception(message)

    // ── 能力标志 ──────────────────────────────────────────────────

    const val CLIENT_LONG_PASSWORD = 0x00000001
    const val CLIENT_FOUND_ROWS = 0x00000002
    const val CLIENT_LONG_FLAG = 0x00000004
    const val CLIENT_CONNECT_WITH_DB = 0x00000008
    const val CLIENT_NO_SCHEMA = 0x00000010
    const val CLIENT_COMPRESS = 0x00000020
    const val CLIENT_ODBC = 0x00000040
    const val CLIENT_LOCAL_FILES = 0x00000080
    const val CLIENT_IGNORE_SPACE = 0x00000100
    const val CLIENT_PROTOCOL_41 = 0x00000200
    const val CLIENT_INTERACTIVE = 0x00000400
    const val CLIENT_SSL = 0x00000800
    const val CLIENT_TRANSACTIONS = 0x00002000
    const val CLIENT_SECURE_CONNECTION = 0x00008000
    const val CLIENT_MULTI_STATEMENTS = 0x00010000
    const val CLIENT_MULTI_RESULTS = 0x00020000
    const val CLIENT_PS_MULTI_RESULTS = 0x00040000
    const val CLIENT_PLUGIN_AUTH = 0x00080000
    const val CLIENT_CONNECT_ATTRS = 0x00100000
    const val CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA = 0x00200000
    const val CLIENT_SESSION_TRACK = 0x00800000
    const val CLIENT_DEPRECATE_EOF = 0x01000000

    // ── 命令 ──────────────────────────────────────────────────────

    const val COM_QUIT = 0x01
    const val COM_INIT_DB = 0x02
    const val COM_QUERY = 0x03
    const val COM_FIELD_LIST = 0x04
    const val COM_STATISTICS = 0x09
    const val COM_PING = 0x0e
    const val COM_STMT_PREPARE = 0x16
    const val COM_STMT_EXECUTE = 0x17
    /** 服务端游标取数（设计文档 §11.1.1 陷阱 T6）。 */
    const val COM_STMT_FETCH = 0x1c

    /** `COM_STMT_EXECUTE` 的 flags：以只读游标方式执行，结果留给 `COM_STMT_FETCH` 分批取。 */
    const val CURSOR_TYPE_READ_ONLY = 0x01

    /** `COM_STMT_EXECUTE` 的 flags：打开一个游标但不取数。 */
    const val CURSOR_TYPE_NO_CURSOR = 0x00
    const val COM_STMT_CLOSE = 0x19
    const val COM_STMT_RESET = 0x1a
    const val COM_SET_OPTION = 0x1b
    const val COM_RESET_CONNECTION = 0x1f

    // ── 状态标志 ──────────────────────────────────────────────────

    const val SERVER_STATUS_IN_TRANS = 0x0001
    const val SERVER_STATUS_AUTOCOMMIT = 0x0002
    const val SERVER_MORE_RESULTS_EXISTS = 0x0008
    const val SERVER_STATUS_NO_GOOD_INDEX_USED = 0x0010
    const val SERVER_STATUS_NO_INDEX_USED = 0x0020
}
