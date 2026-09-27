package com.cloudea.yangbase.server

import com.cloudea.yangbase.objectstore.CountingObjectStore
import kotlinx.coroutines.runBlocking
import java.sql.DriverManager
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

@org.junit.jupiter.api.Tag("integration")
class HashPartitionIntegrationTest {
    @Test fun `HASH 声明列路由在恢复后生效 - 裁剪结果与全扫一致`() {
        IntegrationStore("hash-pruning").use { backend ->
            val store = CountingObjectStore(backend.store, recordKeys = true)
            fun start() = YangBaseServer.forTesting(store, port = 0, cacheBytes = 0,
                allowNonAtomicPointerSwitch = backend.oss).also { it.start() }
            var server = start()
            fun connect() = DriverManager.getConnection(
                "jdbc:mysql://127.0.0.1:${server.boundPort}/?useSSL=false&allowPublicKeyRetrieval=true", "root", "")
            try {
                connect().use { c -> c.createStatement().use { s ->
                    s.execute("CREATE DATABASE h")
                    s.execute("USE h")
                    s.execute("CREATE TABLE t (id BIGINT, tenant BIGINT, payload VARCHAR(256), PRIMARY KEY(id,tenant)) PARTITION BY HASH(tenant) PARTITIONS 4")
                    for (from in 1..4000 step 500) s.execute("INSERT INTO t VALUES " +
                        (from until from + 500).joinToString(",") { "($it,${it % 100},'${"x".repeat(160)}')" })
                } }
                runBlocking { server.engine.flushAll() }
                server.stop(); server = start()
                connect().use { c -> c.createStatement().use { s ->
                    s.execute("USE h")
                    fun query(where: String): List<Long> = s.executeQuery("SELECT id FROM t WHERE $where ORDER BY id").use { rs ->
                        buildList { while (rs.next()) add(rs.getLong(1)) }
                    }
                    s.executeQuery("EXPLAIN SELECT id FROM t WHERE tenant=3").use { rs ->
                        assertTrue(rs.next())
                        val partitions = rs.getString("partitions")
                        assertTrue(partitions != null && partitions.startsWith("p") && !partitions.contains(','))
                    }
                    query("tenant+0=3"); query("tenant=3")
                    store.reset()
                    val scan = query("tenant+0=3")
                    val bytes = store.bytesOut("get_range")
                    store.reset()
                    assertEquals(scan, query("tenant=3"))
                    assertEquals(40, scan.size)
                    assertTrue(store.bytesOut("get_range") < bytes * 0.6)
                    assertEquals(1, store.keysFor("get_range").filter { it.first.endsWith(".dat") }
                        .map { it.first.substringBefore("/sst/") }.toSet().size)
                    for (predicate in listOf("= '3'", "IN (3,7)", "> 90", "<=> 3", "= -1")) {
                        assertEquals(query("tenant+0 $predicate"), query("tenant $predicate"), predicate)
                    }
                    assertEquals(query("tenant+0=3 OR id=1"), query("tenant=3 OR id=1"))
                    assertEquals(query("tenant+0=3 OR tenant+0=7"), query("tenant=3 OR tenant=7"))
                    assertEquals(query("tenant+0=3 AND tenant+0='3'"), query("tenant=3 AND tenant='3'"))
                    assertEquals(listOf(3L), query("id=3 AND tenant=3"))
                    s.execute("UPDATE t SET tenant=7 WHERE id=3 AND tenant=3")
                    assertEquals(emptyList(), query("id=3 AND tenant=3"))
                    assertEquals(listOf(3L), query("id=3 AND tenant=7"))
                    s.executeQuery("SELECT COUNT(*) FROM t WHERE tenant=3").use { rs ->
                        assertTrue(rs.next()); assertEquals(39L, rs.getLong(1))
                    }
                } }
            } finally { server.stop() }
        }
    }
}
