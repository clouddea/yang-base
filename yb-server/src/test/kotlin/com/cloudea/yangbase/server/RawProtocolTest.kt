package com.cloudea.yangbase.server

import java.io.DataInputStream
import java.io.DataOutputStream
import java.net.Socket
import java.sql.DriverManager
import java.nio.file.Files
import kotlin.test.AfterTest
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * **字节级**的协议断言。
 *
 * 绝大多数协议行为用 JDBC 驱动验证就够了，但有几处是驱动**看不到**的：
 * 收尾包的形状（EOF vs `CLIENT_DEPRECATE_EOF` 之后的 OK 包）、状态字段的置位、
 * 包的边界。这些地方出错时驱动往往只是"结果集是空的"或者"连接卡住"，
 * 排查成本极高——所以这里直接开一个 socket 看字节。
 *
 * 这也是设计文档 §11.1.1 陷阱 T3 的应对：**把值编码/包编码做成可单独观察的东西**。
 */
class RawProtocolTest {

    private lateinit var dataDir: java.nio.file.Path
    private lateinit var server: YangBaseServer
    private var port: Int = 0

    @BeforeTest
    fun setUp() {
        dataDir = Files.createTempDirectory("yangbase-raw")
        server = YangBaseServer(
            ServerConfig(host = "127.0.0.1", port = 0, nodeId = "raw", dataDir = dataDir.toString(), allowNonDurableStorage = true)
        )
        server.start()
        port = server.boundPort
    }

    @AfterTest
    fun tearDown() {
        runCatching { server.stop() }
        runCatching { dataDir.toFile().deleteRecursively() }
    }

    /** 一个极简的 MySQL 客户端：只做握手 + 一条 COM_QUERY，把包按原始字节收下来。 */
    private inner class RawClient(private val deprecateEof: Boolean) : AutoCloseable {
        val socket = Socket("127.0.0.1", port)
        val input = DataInputStream(socket.getInputStream())
        val output = DataOutputStream(socket.getOutputStream())
        var serverCaps: Int = 0

        /** 读到的包序号（与 [readPacket] 一一对应），用于断言"应答接着客户端的序号"。 */
        val readSequences = ArrayList<Int>()

        /** 读一个包，返回负载（去掉 4 字节包头）。 */
        fun readPacket(): ByteArray {
            val len = input.readUnsignedByte() or (input.readUnsignedByte() shl 8) or
                (input.readUnsignedByte() shl 16)
            readSequences.add(input.readByte().toInt() and 0xFF)
            val buf = ByteArray(len)
            input.readFully(buf)
            return buf
        }

        fun writePacket(payload: ByteArray, seq: Int) {
            output.writeByte(payload.size and 0xFF)
            output.writeByte((payload.size ushr 8) and 0xFF)
            output.writeByte((payload.size ushr 16) and 0xFF)
            output.writeByte(seq)
            output.write(payload)
            output.flush()
        }

        fun handshake() {
            val hello = readPacket()
            // 跳过 protocol version(1) + server version(nul 结尾) + connection id(4) + auth(8) + filler(1)
            var i = 1
            while (hello[i].toInt() != 0) i++
            i += 1 + 4 + 8 + 1
            // 握手包里**不是**连续的 4 字节：capability 分低/高两半，
            // 中间夹着 character_set(1) 与 status_flags(2)
            serverCaps = (hello[i].toInt() and 0xFF) or ((hello[i + 1].toInt() and 0xFF) shl 8) or
                ((hello[i + 5].toInt() and 0xFF) shl 16) or ((hello[i + 6].toInt() and 0xFF) shl 24)

            var caps = 0x0000_0001 or 0x0000_0004 or 0x0000_0200 or 0x0000_8000 or 0x0000_0800
            if (deprecateEof) caps = caps or CLIENT_DEPRECATE_EOF
            val b = java.io.ByteArrayOutputStream()
            fun u32(v: Int) { for (k in 0 until 4) b.write((v ushr (k * 8)) and 0xFF) }
            fun u16(v: Int) { b.write(v and 0xFF); b.write((v ushr 8) and 0xFF) }
            u32(caps)
            u32(16 * 1024 * 1024)
            b.write(45)                            // utf8mb4_general_ci
            repeat(23) { b.write(0) }
            b.write("root".toByteArray(Charsets.UTF_8)); b.write(0)
            b.write(0)                             // 空密码（native password 长度 0）
            writePacket(b.toByteArray(), 1)
            readPacket()                           // OK
        }

        /** 发一条命令并读**恰好一个**包——给 OK/ERR 类应答（`USE`、`CREATE`…）用。 */
        fun command(payload: ByteArray): ByteArray {
            writePacket(payload, 0)
            return readPacket()
        }

        /** 发一条 COM_QUERY，把后续包原样收回来（直到收到收尾包）。 */
        fun query(sql: String): List<ByteArray> {
            val payload = ByteArray(sql.length + 1)
            payload[0] = 0x03                       // COM_QUERY
            sql.toByteArray(Charsets.UTF_8).copyInto(payload, 1)
            writePacket(payload, 0)
            val out = ArrayList<ByteArray>()
            val first = readPacket()
            out.add(first)
            // 结果集：列数 + 列定义*n + 收尾 + 行… + 收尾
            val n = if (first[0].toInt() and 0xFF == 0xFF) 0 else first[0].toInt()
            if (first[0].toInt() and 0xFF == 0xFF) return out
            var k = 0
            while (k < n) { out.add(readPacket()); k++ }
            out.add(readPacket())                   // 列定义段的收尾
            while (true) {
                val p = readPacket()
                out.add(p)
                val h = p[0].toInt() and 0xFF
                if (h == 0xFE && p.size < 9) break   // 收尾包（EOF 或 0xFE 开头的 OK）
                if (h == 0x00 && p.size < 9) break
            }
            return out
        }

        override fun close() { runCatching { socket.close() } }
    }

    private fun hex(b: ByteArray) = b.joinToString(" ") { "%02x".format(it) }

    /**
     * ★ **应答包的序号必须接着客户端的序号往下走。**
     *
     * 协议是"一条命令一条应答"共用一个序号序列：客户端用 seq=0 发 COM_QUERY，
     * 服务端的第一个应答包必须是 seq=1。命令循环曾经把写侧序号**清零**，
     * 于是应答从 seq=0 开始。
     *
     * 为什么这条断言必须存在：Connector/J 对这种错位是**宽容**的，我们全部端到端
     * 测试走的都是 JDBC；而 **libmysqlclient 严格校验**——`mysql` 命令行客户端、
     * `mysqladmin`、`mysqldump`、Python 的 MySQLdb 都是它，它们看到序号错位会直接
     * 掐掉连接，报 `ERROR 2013 Lost connection to MySQL server during query`。
     * 也就是说：这个缺陷的表现形式是"**标准 MySQL 工具全都用不了**"，
     * 而 JDBC 测试一条都不会红。
     */
    @Test
    fun `应答包的序号接在客户端之后`() {
        RawClient(deprecateEof = true).use { c ->
            c.handshake()
            c.readSequences.clear()
            c.query("SELECT 1")
            // 客户端用 seq=0 发命令，所以服务端的包必须是 1,2,3,…
            assertEquals(
                (1..c.readSequences.size).toList(), c.readSequences,
                "应答包序号必须从 1 开始且连续（真实 MySQL 8.0 实测如此）",
            )
        }
    }

    /**
     * ★ **`COM_FIELD_LIST` 的应答必须有收尾包。**
     *
     * 它不是结果集：应答就是"若干个列定义 + 一个收尾包"，而 `CLIENT_DEPRECATE_EOF`
     * 只废弃"结果集里列定义段之后的那个 EOF"，**不废弃这一个**。
     * 少了它，客户端会一直等 → 判定连接坏掉 → `ERROR 2013 Lost connection`。
     *
     * 触发者是**标准 `mysql` 客户端的自动补全**：交互式会话里它会对每张表发这个命令
     * （`use db` 之后尤其明显），Connector/J 则根本不发——所以这条只能在这一层守。
     */
    @Test
    fun `COM_FIELD_LIST 必须有收尾包`() {
        RawClient(deprecateEof = true).use { c ->
            c.handshake()
            // 建表走 JDBC（`query()` 是给结果集用的，OK 类应答只有一个包，不能用它）
            DriverManager.getConnection(
                "jdbc:mysql://127.0.0.1:$port/raw?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC",
                "root", "",
            ).use { jc ->
                jc.createStatement().use { st ->
                    st.execute("CREATE DATABASE IF NOT EXISTS raw")
                    st.execute("USE raw")
                    st.execute("DROP TABLE IF EXISTS fl")
                    st.execute("CREATE TABLE fl (id BIGINT PRIMARY KEY, v VARCHAR(8))")
                }
            }
            // 让这个原始连接也切到 raw（`USE` 的应答是一个 OK 包）
            c.command(byteArrayOf(0x02) + "raw".toByteArray())

            // COM_FIELD_LIST：0x04 + 表名 + NUL
            val payload = byteArrayOf(0x04) + "fl".toByteArray() + byteArrayOf(0)
            c.writePacket(payload, 0)
            val packets = ArrayList<ByteArray>()
            while (true) {
                val p = c.readPacket()
                packets.add(p)
                val h = p[0].toInt() and 0xFF
                // 收尾包：0xFE 开头的短包（EOF 或 OK-as-EOF）
                if (h == 0xFE && p.size < 9) break
                if (h == 0xFF) break            // 错误也算"有应答"，但下面的断言会拦下
            }
            assertTrue(packets.size >= 3, "两张列定义 + 一个收尾包，实际 ${packets.size} 个包")
            assertEquals(0xFE, packets.last()[0].toInt() and 0xFF, "最后一个包必须是收尾包")
            // 列定义包里有列名（偏移随 catalog/schema/table 的长度而变，这里不做手工算术）
            assertTrue(
                packets[0].decodeToString().contains("id"),
                "第一个列定义应当是 id：${packets[0].decodeToString()}",
            )
            // ★ 而且**包尾还要有一个"列的默认值"**（0xFB = NULL）——
            // COM_FIELD_LIST 的列定义与结果集的列定义**不是同一种包**（实测对照真实
            // MySQL 8.0 的字节）。少了这段，客户端读默认值时越界 → `ERROR 2027
            // Malformed packet` → 掐连接
            assertEquals(
                0xFB, packets[0].last().toInt() and 0xFF,
                "COM_FIELD_LIST 的列定义必须带默认值（无默认时是 0xFB）",
            )
        }
    }

    @Test
    fun `不协商时收尾包是传统 EOF`() {
        RawClient(deprecateEof = false).use { c ->
            c.handshake()
            assertTrue(c.serverCaps and CLIENT_DEPRECATE_EOF != 0, "服务端应当声明支持 DEPRECATE_EOF")
            val packets = c.query("SELECT 1")
            // 列数、列定义、段收尾、行、结果收尾
            val terminator = packets.last()
            assertEquals(0xFE, terminator[0].toInt() and 0xFF)
            assertEquals(5, terminator.size, "EOF 包是 0xFE + warnings(2) + status(2) = 5 字节，实际 ${hex(terminator)}")
        }
    }

    /**
     * ★ 协商 `CLIENT_DEPRECATE_EOF` 之后，收尾包必须变成 **0xFE 开头的 OK 包**
     * 并且**长度仍然小于 9**——客户端就是靠这个长度区分"收尾包"和"行"的
     * （文本协议的行也可能以 0xFE 开头）。写错的症状是"结果集是空的"。
     */
    @Test
    fun `协商后收尾包是 0xFE 开头的 OK 包`() {
        RawClient(deprecateEof = true).use { c ->
            c.handshake()
            val packets = c.query("SELECT 1")
            val terminator = packets.last()
            assertEquals(0xFE, terminator[0].toInt() and 0xFF, "收尾包应当是 0xFE 开头")
            val row = packets[packets.size - 2]
            assertEquals(0x01, row[0].toInt() and 0xFF, "'1' 的长度编码是 01")
            assertEquals(0x31, row[1].toInt() and 0xFF, "'1' 的字节是 0x31")
            assertEquals(
                7, terminator.size,
                "OK 形式的收尾包 = 0xFE + affected(1) + insert_id(1) + status(2) + warnings(2) = 7 字节，" +
                    "实际 ${hex(terminator)}",
            )
            assertTrue(terminator.size < 9, "收尾包必须短于 9 字节，否则客户端会把它当成一行")
            // 行必须还在，而且**内容对**：'1' 的长度编码是 01、字节是 0x31。
            //
            // 客户端区分「收尾包」与「行」靠的是**长度 ≥ 9**：文本协议的行确实可能
            // 以 0xFE 开头（那是 8 字节长度前缀的形式，只有 ≥ 16MB 的值才会用到），
            // 那种行必然远大于 9 字节，所以这条规则是安全的。这里造不出 16MB 的值，
            // 但规则本身由收尾包的 7 字节长度守着。
        }
    }

    /** 未协商多语句时，多条语句要被拒（错误包 0xFF 开头）。 */
    @Test
    fun `未协商多语句时返回错误包`() {
        RawClient(deprecateEof = false).use { c ->
            c.handshake()
            val packets = c.query("SELECT 1; SELECT 2")
            assertEquals(0xFF, packets.first()[0].toInt() and 0xFF, "应当是错误包")
        }
    }

    private companion object {
        const val CLIENT_DEPRECATE_EOF = 0x0100_0000
    }
}
