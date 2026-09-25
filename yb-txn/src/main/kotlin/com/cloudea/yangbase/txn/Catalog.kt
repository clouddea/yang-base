package com.cloudea.yangbase.txn

import com.cloudea.yangbase.common.ColumnDef
import com.cloudea.yangbase.common.ColumnType
import com.cloudea.yangbase.common.DatabaseSchema
import com.cloudea.yangbase.common.IndexDef
import com.cloudea.yangbase.common.IndexState
import com.cloudea.yangbase.common.IndexKind
import com.cloudea.yangbase.common.MySqlErrors
import com.cloudea.yangbase.common.ObjectNotFoundException
import com.cloudea.yangbase.common.PartitionDef
import com.cloudea.yangbase.common.PartitionKind
import com.cloudea.yangbase.common.SqlException
import com.cloudea.yangbase.common.TableSchema
import com.cloudea.yangbase.objectstore.ObjectKeys
import com.cloudea.yangbase.objectstore.ObjectStore
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.serialization.json.Json
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

/**
 * 库表元数据目录（设计文档 §4.5）。
 *
 * 真相源是对象存储上的 `catalog/` 目录，采用与数据面相同的
 * **「不可变版本 + 指针」**模式：
 * ```
 * catalog/
 *   db/{db_id}.json                      库定义
 *   table/{table_id}/schema.v{n}.json    不可变的 schema 版本
 *   table/{table_id}/CURRENT             指针 → 当前版本
 *   byname/{db_id}/{name}.json           名字 → id 的映射
 * ```
 * 全部内容小且热，因此**整体常驻内存**，只有 DDL 才写对象存储。
 */
class Catalog(
    private val store: ObjectStore,
    /**
     * 协调实现，**只用来推进 catalog 版本号**（§4.5 的缓存失效判据）。
     *
     * 为什么放在这里而不是每个 DDL 的调用点：Catalog 是**唯一**写 schema 的地方，
     * 在调用点逐个加一遍，早晚会有人新加一个 DDL 却忘了加——而漏掉的后果是静默的
     * （别的 CN 一直按旧 schema 解码新行，多出来的列被丢掉）。
     */
    private val coordination: CoordinationService? = null,
) {

    private val json = Json { prettyPrint = true; encodeDefaults = true; ignoreUnknownKeys = true }

    private val dbsByName = ConcurrentHashMap<String, DatabaseSchema>()
    private val tablesByKey = ConcurrentHashMap<String, TableSchema>()  // "$dbId/$tableName"
    private val nextDbId = AtomicLong(100)
    private val nextTableId = AtomicLong(1000)
    private val nextIndexId = AtomicLong(10000)
    private val ddlMutex = Mutex()

    // ── 打开与加载 ────────────────────────────────────────────────

    /** 把整个 catalog 加载进内存。它很小，一次性加载即可。 */
    suspend fun load() {
        // ★ 这是**整表替换**，不是"合并"。
        //
        // 少了这两行 `clear()`，`load()` 只能往里加、不能往外减——于是
        // 「CN1 `DROP TABLE t`（写墓碑 + 推版本）→ CN2 轮询到版本变化 → 重载」之后，
        // `t` **仍然留在 CN2 的内存目录里**，继续可读、继续可写。
        // 使用者以为删掉的表在另一个 CN 上照常接受写入，直到那个 CN 重启。
        // （墓碑检查只能阻止"重新加入"，挡不住"从来没走"。）
        dbsByName.clear()
        tablesByKey.clear()

        // 库
        val dbKeys = store.listAll(ObjectKeys.databasePrefix())
        var maxDbId = 100L
        for (k in dbKeys) {
            val db = readJson(k) { s -> json.decodeFromString(DatabaseSchema.serializer(), s) }
            dbsByName[db.name.lowercase()] = db
            if (db.databaseId > maxDbId) maxDbId = db.databaseId
        }
        nextDbId.set(maxDbId + 1)

        // 表
        //
        // ★ **必须跳过有墓碑的表**（`DROP TABLE` 之后那些 schema 与 CURRENT 指针是
        // 故意保留的，为了 30 天内能 O(1) 恢复）。不检查墓碑的后果是：
        // **删掉的表会在重启（或任何一次目录重载）之后回来**——数据一行不少地回来了，
        // 而使用者以为自己删掉了。这是最不该静默发生的一类事。
        //
        // 墓碑一次性列举出来（1 次 LIST），而不是每张表 HEAD 一次：
        // 启动路径上的请求数也是成本（§14.2），而表可能有几十张。
        val droppedTables = store.listAll(ObjectKeys.tablePrefixAll())
            .filter { it.endsWith("/DELETED.json") }
            .mapNotNull { k -> k.removePrefix(ObjectKeys.tablePrefixAll()).substringBefore('/').toLongOrNull() }
            .toHashSet()
        val tableKeys = store.listAll(ObjectKeys.tablePrefixAll())
        var maxTableId = 1000L
        var maxIndexId = 10000L
        for (k in tableKeys) {
            if (!k.endsWith("/CURRENT")) continue
            val tid = k.removePrefix(ObjectKeys.tablePrefixAll()).substringBefore('/').toLongOrNull()
            if (tid != null && tid in droppedTables) continue
            val ptr = readJsonOrNull(k) { s -> json.decodeFromString(TablePointer.serializer(), s) } ?: continue
            val schema = readJsonOrNull(ptr.schemaPath) { s -> json.decodeFromString(TableSchema.serializer(), s) }
                ?: continue
            val db = dbsByName.values.firstOrNull { it.databaseId == schema.databaseId }
            if (db != null) tablesByKey[key(db.databaseId, schema.name)] = schema
            if (schema.tableId > maxTableId) maxTableId = schema.tableId
            for (idx in schema.indexes) if (idx.id > maxIndexId) maxIndexId = idx.id
        }
        nextTableId.set(maxTableId + 1)
        nextIndexId.set(maxIndexId + 1)
    }

    private suspend fun <T> readJson(key: String, parse: (String) -> T): T =
        parse(String(store.get(key), Charsets.UTF_8))

    private suspend fun <T> readJsonOrNull(key: String, parse: (String) -> T): T? = try {
        parse(String(store.get(key), Charsets.UTF_8))
    } catch (_: ObjectNotFoundException) {
        null
    }

    // ── 库 ────────────────────────────────────────────────────────

    suspend fun createDatabase(name: String, ifNotExists: Boolean = false): DatabaseSchema =
        ddlMutex.withLock {
            dbsByName[name.lowercase()]?.let {
                if (ifNotExists) return@withLock it
                throw SqlException(MySqlErrors.ER_DB_CREATE_EXISTS, "42S01", "Can't create database '$name'; database exists")
            }
            val db = DatabaseSchema(nextDbId.getAndIncrement(), name)
            store.put(ObjectKeys.database(db.databaseId), json.encodeToString(DatabaseSchema.serializer(), db).toByteArray())
            dbsByName[name.lowercase()] = db
            afterDdl()
            db
        }

    suspend fun dropDatabase(name: String): Boolean = ddlMutex.withLock {
        val db = dbsByName.remove(name.lowercase()) ?: return@withLock false
        store.delete(ObjectKeys.database(db.databaseId))
        val toDrop = tablesByKey.values.filter { it.databaseId == db.databaseId }
        for (t in toDrop) dropTableInternal(t)
        afterDdl()
        true
    }

    fun findDatabase(name: String): DatabaseSchema? = dbsByName[name.lowercase()]

    fun requireDatabase(name: String): DatabaseSchema = findDatabase(name)
        ?: throw SqlException(MySqlErrors.ER_BAD_DB_ERROR, "42000", "Unknown database '$name'")

    fun listDatabases(): List<DatabaseSchema> = dbsByName.values.sortedBy { it.name }

    // ── 表 ────────────────────────────────────────────────────────

    fun findTable(dbId: Long, tableName: String): TableSchema? = tablesByKey[key(dbId, tableName)]

    fun requireTable(dbId: Long, tableName: String): TableSchema = findTable(dbId, tableName)
        ?: throw SqlException(MySqlErrors.ER_NO_SUCH_TABLE, "42S02", "Table '$tableName' doesn't exist")

    /** 全部表。用于由分片反查表（提交事务缓冲时需要）。 */
    fun allTables(): List<TableSchema> = tablesByKey.values.toList()

    fun listTables(dbId: Long): List<TableSchema> =
        tablesByKey.values.filter { it.databaseId == dbId }.sortedBy { it.name }

    /**
     * 建表。写 schema 对象 + 切换 CURRENT 指针——顺序不能反。
     */
    suspend fun createTable(
        dbId: Long,
        name: String,
        columns: List<ColumnDef>,
        primaryKey: List<String>,
        indexes: List<IndexDef> = emptyList(),
        partitionKind: PartitionKind = PartitionKind.NONE,
        partitionCount: Int = 1,
        partitions: List<PartitionDef> = emptyList(),
        partitionColumn: String? = null,
        partitionExpr: String? = null,
        exactCount: Boolean = true,
        ifNotExists: Boolean = false,
        /** 非 null 表示建的是**视图**，值是它的 SELECT 原文（见 [TableSchema.viewSql]）。 */
        viewSql: String? = null,
    ): TableSchema = ddlMutex.withLock {
        findTable(dbId, name)?.let {
            if (ifNotExists) return@withLock it
            throw SqlException(MySqlErrors.ER_TABLE_EXISTS_ERROR, "42S01", "Table '$name' already exists")
        }
        // 视图没有主键、也没有数据分片，`validateTable` 的主键校验对它不适用
        if (viewSql == null) validateTable(name, columns, primaryKey)

        val tableId = nextTableId.getAndIncrement()
        val withIds = indexes.map { it.copy(id = nextIndexId.getAndIncrement()) }
        val schema = TableSchema(
            tableId = tableId,
            databaseId = dbId,
            name = name,
            version = 1,
            columns = columns,
            primaryKey = primaryKey,
            indexes = withIds,
            partitionKind = partitionKind,
            partitionCount = partitionCount,
            partitions = partitions,
            partitionColumn = partitionColumn,
            partitionExpr = partitionExpr,
            viewSql = viewSql,
            exactCount = exactCount,
        )
        // 分区定义在**建表时**校验：错误的分区边界必须在此刻暴露，
        // 而不是等第一条插入落不进去时才发现（那时用户已经在生产环境里了）。
        Partitioning.validate(schema)
        writeSchema(schema)
        tablesByKey[key(dbId, name)] = schema
        schema
    }

    /** schema 演化：生成新版本 + 切指针。读旧版本文件时按版本投影（设计文档 §4.5）。 */
    suspend fun updateTable(schema: TableSchema): TableSchema = ddlMutex.withLock {
        val next = schema.copy(version = schema.version + 1)
        writeSchema(next)
        tablesByKey[key(schema.databaseId, schema.name)] = next
        next
    }

    suspend fun dropTable(dbId: Long, tableName: String): Boolean = ddlMutex.withLock {
        val t = tablesByKey[key(dbId, tableName)] ?: return@withLock false
        dropTableInternal(t)
        true
    }

    /**
     * 删除表。
     *
     * **只写墓碑，不删对象**（设计文档 §12.3：误删表恢复，保留 30 天）。
     *
     * 在此之前这里是直接 `listAll + delete`——一次误操作永久丢表。
     * 而在这个架构里保留一张表的成本几乎为零：数据对象本来就在那里，
     * 我们只需要**停止引用它们**，真正的回收交给 GC 的延迟删除。
     *
     * 30 天之后由 GC 清掉墓碑，那时这些对象才真正可回收。
     */
    private suspend fun dropTableInternal(t: TableSchema) {
        tablesByKey.remove(key(t.databaseId, t.name))
        val tid = com.cloudea.yangbase.common.TableId(t.tableId)
        val tombstone = """
            {"tableId":${t.tableId},"databaseId":${t.databaseId},"name":"${t.name}",
             "droppedAt":${System.currentTimeMillis()},"version":${t.version}}
        """.trimIndent()
        store.put(ObjectKeys.tableTombstone(tid), tombstone.toByteArray())
        store.delete(ObjectKeys.tableByName(t.databaseId, t.name))
        // ⚠️ schema 对象与 CURRENT 指针**保留**——恢复时要靠它们找回定义。
        // 数据文件同样保留，交给 GC 的延迟删除（§6.9）。
        // 删表是一条 DDL，同样要让别的 CN 的缓存失效（它们还缓存着这张表的 schema）
        afterDdl()
    }

    /**
     * 恢复一张被删除的表（`DROP TABLE` 的 30 天内）。
     *
     * 数据从未被删过，所以这里只是把墓碑撤掉、把引用放回去——
     * **恢复是 O(1) 的**，不需要任何数据拷贝。
     */
    suspend fun restoreTable(dbId: Long, tableName: String): TableSchema = ddlMutex.withLock {
        val db = dbsByName.values.firstOrNull { it.databaseId == dbId }
            ?: throw SqlException(MySqlErrors.ER_BAD_DB_ERROR, "42000", "Unknown database")
        val existing = findTable(dbId, tableName)
        if (existing != null) return@withLock existing

        // 按名字找回表 id：从被删表的墓碑里找
        val cand = store.listAll(ObjectKeys.tablePrefixAll())
            .filter { it.endsWith("/DELETED.json") }
            .mapNotNull { k ->
                val tid = com.cloudea.yangbase.common.TableId(k.removePrefix(ObjectKeys.tablePrefixAll()).substringBefore('/').toLongOrNull() ?: return@mapNotNull null)
                val text = runCatching { String(store.get(k), Charsets.UTF_8) }.getOrNull() ?: return@mapNotNull null
                if (!text.contains("\"name\":\"$tableName\"")) return@mapNotNull null
                val droppedAt = Regex("\"droppedAt\":(\\d+)").find(text)?.groupValues?.get(1)?.toLongOrNull() ?: 0L
                Triple(tid, droppedAt, text)
            }
            .filter { it.second > 0 }
            .maxByOrNull { it.second }
            ?: throw SqlException(
                MySqlErrors.ER_NO_SUCH_TABLE, "42S02",
                "表 '$tableName' 不存在，也没有可恢复的墓碑（可能已超过 30 天保留期）",
            )

        val schemaBytes = runCatching { store.get(ObjectKeys.schemaCurrent(cand.first)) }.getOrNull()
            ?: throw SqlException(
                MySqlErrors.ER_NO_SUCH_TABLE, "42S02",
                "表 '$tableName' 的墓碑还在，但 schema 指针已丢失，无法恢复",
            )
        val ptr = json.decodeFromString(TablePointer.serializer(), String(schemaBytes, Charsets.UTF_8))
        val schema = json.decodeFromString(TableSchema.serializer(), String(store.get(ptr.schemaPath), Charsets.UTF_8))

        // 撤掉墓碑、把引用放回去。数据对象一直在原地，不需要任何拷贝
        runCatching { store.delete(ObjectKeys.tableTombstone(cand.first)) }
        store.put(
            ObjectKeys.tableByName(dbId, tableName),
            """{"tableId":${schema.tableId}}""".toByteArray(),
        )
        tablesByKey[key(dbId, tableName)] = schema
        // ★ 推进版本号，否则**别的 CN 永远发现不了这张表回来了**：
        // 它们的 catalog 轮询只在版本变化时才重载，于是会一直报
        // "表不存在"——直到下一次谁做了别的 DDL 顺带把它带过来。
        // 恢复是一次元数据变更，与 DROP 对称（后者也是 afterDdl）
        afterDdl()
        schema
    }

    /**
     * DDL 写完之后推进版本号。
     *
     * 调用点在每次成功的元数据变更**之后**：先写对象、再推进版本，顺序不能反——
     * 反过来的话，别的 CN 会在新 schema 落盘之前就刷新缓存，读到旧的。
     */
    private suspend fun afterDdl() {
        runCatching { coordination?.bumpCatalogVersion() }
            .onFailure { println("[Catalog] 推进 catalog 版本失败（其他 CN 的缓存可能短暂陈旧）：${it.message}") }
    }

    private suspend fun writeSchema(schema: TableSchema) {
        store.put(
            ObjectKeys.schema(com.cloudea.yangbase.common.TableId(schema.tableId), schema.version),
            json.encodeToString(TableSchema.serializer(), schema).toByteArray(),
        )
        val ptr = TablePointer(schema.version, ObjectKeys.schema(com.cloudea.yangbase.common.TableId(schema.tableId), schema.version))
        store.put(
            ObjectKeys.schemaCurrent(com.cloudea.yangbase.common.TableId(schema.tableId)),
            json.encodeToString(TablePointer.serializer(), ptr).toByteArray(),
        )
        store.put(
            ObjectKeys.tableByName(schema.databaseId, schema.name),
            """{"tableId":${schema.tableId}}""".toByteArray(),
        )
        // 所有**表级** DDL（建表 / 加列 / 改索引状态 / 重命名 …）都汇流到这里，
        // 所以版本推进只需要这一处
        afterDdl()
    }

    private fun validateTable(name: String, columns: List<ColumnDef>, pk: List<String>) {
        if (columns.isEmpty()) {
            throw SqlException(MySqlErrors.ER_PARSE_ERROR, "42000", "表 $name 必须至少有一列")
        }
        val names = columns.map { it.name.lowercase() }
        if (names.size != names.toSet().size) {
            throw SqlException(MySqlErrors.ER_DUP_FIELDNAME, "42S21", "表 $name 存在重名列")
        }
        for (k in pk) {
            if (k.lowercase() !in names) {
                throw SqlException(MySqlErrors.ER_BAD_FIELD_ERROR, "42S22", "主键列 '$k' 不存在")
            }
        }
    }

    private fun key(dbId: Long, tableName: String) = "$dbId/${tableName.lowercase()}"

    fun stats(): CatalogStats = CatalogStats(dbsByName.size, tablesByKey.size)

    // ── 索引维护 ──────────────────────────────────────────────────

    suspend fun addIndex(dbId: Long, tableName: String, index: IndexDef): TableSchema {
        val t = requireTable(dbId, tableName)
        if (t.indexes.any { it.name.equals(index.name, ignoreCase = true) }) {
            throw SqlException(MySqlErrors.ER_DUP_ENTRY, "42000", "索引 '${index.name}' 已存在")
        }
        for (c in index.columns) t.indexOf(c) // 校验列存在
        return updateTable(t.copy(indexes = t.indexes + index.copy(id = nextIndexId.getAndIncrement())))
    }

    /**
     * 改一个索引的状态（设计文档 §8.7）。
     *
     * 走的是和别的 schema 变更同一条路：写新的 schema 版本 + 切 `CURRENT` 指针。
     * 因此「索引对查询可见」这件事本身是**原子**的——要么是 BUILDING
     * （查询不用它），要么是 ACTIVE（用它），不存在中间态。
     */
    /**
     * 用新定义**替换**一张已存在的表/视图，保留 tableId。
     *
     * 只给 `CREATE OR REPLACE VIEW` 用：视图的定义改了但它的身份（tableId）不变，
     * 于是 `information_schema` 里的引用、以及任何按 tableId 记录的东西都不会错位。
     * 普通表不允许这样替换——那等于悄悄换掉一张有数据的表。
     */
    suspend fun replaceTable(schema: TableSchema): TableSchema {
        // ⚠️ **不要再套一层 `ddlMutex`**：`updateTable` 自己会取它，而这个 Mutex
        // 不是可重入的——重复获取会**永久挂住**（实测踩到：`CREATE OR REPLACE VIEW`
        // 在服务端死死等锁，客户端表现为查询永远不返回）
        val existing = requireTable(schema.databaseId, schema.name)
        return updateTable(schema.copy(tableId = existing.tableId, version = existing.version))
    }

    /**
     * 改一个索引的状态（设计文档 §8.7）。
     *
     * 走的是和别的 schema 变更同一条路：写新的 schema 版本 + 切 `CURRENT` 指针。
     * 因此「索引对查询可见」这件事本身是**原子**的——要么是 BUILDING
     * （查询不用它），要么是 ACTIVE（用它），不存在中间态。
     */
    suspend fun setIndexState(dbId: Long, tableName: String, indexName: String, state: IndexState): TableSchema {
        val t = requireTable(dbId, tableName)
        val idx = t.indexes.firstOrNull { it.name.equals(indexName, ignoreCase = true) }
            ?: throw SqlException(MySqlErrors.ER_CANT_DROP_FIELD_OR_KEY, "42000", "索引 '$indexName' 不存在")
        if (idx.state == state) return t
        return updateTable(t.copy(indexes = t.indexes.map { if (it === idx) it.copy(state = state) else it }))
    }
}

@kotlinx.serialization.Serializable
data class TablePointer(val version: Long, val schemaPath: String)

data class CatalogStats(val databases: Int, val tables: Int)

/** 常见列类型的便捷构造，供 DDL 与测试使用。 */
object Columns {
    fun bigint(name: String, nullable: Boolean = false, autoInc: Boolean = false) =
        ColumnDef(name, ColumnType.BIGINT, nullable, autoIncrement = autoInc)

    fun int(name: String, nullable: Boolean = true) = ColumnDef(name, ColumnType.INT, nullable)

    fun varchar(name: String, len: Int = 255, nullable: Boolean = true) =
        ColumnDef(name, ColumnType.VARCHAR, nullable, length = len)

    fun text(name: String, nullable: Boolean = true) = ColumnDef(name, ColumnType.TEXT, nullable)

    fun bool(name: String, nullable: Boolean = true) = ColumnDef(name, ColumnType.BOOLEAN, nullable)

    fun datetime(name: String, nullable: Boolean = true) = ColumnDef(name, ColumnType.DATETIME, nullable)

    fun decimal(name: String, p: Int, s: Int, nullable: Boolean = true) =
        ColumnDef(name, ColumnType.DECIMAL, nullable, precision = p, scale = s)

    fun double(name: String, nullable: Boolean = true) = ColumnDef(name, ColumnType.DOUBLE, nullable)
}
