package com.cloudea.yangbase.sql

import com.cloudea.yangbase.common.ColumnDef
import com.cloudea.yangbase.common.ColumnType
import com.cloudea.yangbase.common.Datum
import com.cloudea.yangbase.common.IndexKind
import com.cloudea.yangbase.common.MAXVALUE
import com.cloudea.yangbase.common.PartitionKind
import com.cloudea.yangbase.common.TableSchema
import com.cloudea.yangbase.txn.DatabaseEngine

/**
 * `information_schema` 等系统库的**虚拟表**（设计文档 §11.2）。
 *
 * 这些表不是真实的表：它们由内存里的 catalog 直接物化，不占对象存储、不产生请求。
 * 之所以必须有，是因为**任何数据库客户端连接后的第一件事都是查它们**——
 * DataGrip、Navicat、DBeaver、各类 ORM 的 introspection 全都如此。
 * 没有这一层，协议层面连得上，工具层面却用不了，等于没兼容 MySQL。
 *
 * 设计上刻意**只做只读物化**：这些表全部很小（正比于库表数量，不是数据量），
 * 因此一次性全部物化再交给统一的 SELECT 收尾逻辑过滤/排序，
 * 比为每张表写一个查询下推简单得多，也不会出错。
 */
object InformationSchema {

    /** 一张虚拟表：列定义 + 行物化。 */
    class VirtualTable(
        val schema: TableSchema,
        private val materialize: (Session, DatabaseEngine) -> List<List<Datum>>,
    ) {
        fun rows(session: Session, engine: DatabaseEngine): List<List<Datum>> {
            val rows = materialize(session, engine)
            // 列数与行宽必须一致。不一致时下游只会抛一个越界的
            // IndexOutOfBoundsException，排查起来完全看不出是哪张虚拟表——
            // 在这里一次性说清楚，比让使用者去猜便宜得多。
            val n = schema.columns.size
            rows.forEachIndexed { i, r ->
                require(r.size == n) {
                    "information_schema.${schema.name} 第 $i 行的列数是 ${r.size}，与定义的 $n 列不符"
                }
            }
            return rows
        }
    }

    /**
     * `information_schema` 里**有名字**的那些表（含"存在但为空"的）。
     *
     * 用途是让 `USE information_schema; SHOW TABLES` 能列出它们——真实 MySQL 里
     * `information_schema` 就是一个 schema，客户端（和手敲命令的人）都会这么用。
     */
    val TABLE_NAMES: List<String> = listOf(
        "schemata", "tables", "columns", "key_column_usage", "table_constraints",
        "partitions", "views", "engines", "character_sets", "collations", "processlist",
        "triggers", "routines", "events", "referential_constraints", "table_privileges",
        "schema_privileges", "user_privileges", "column_privileges", "check_constraints",
        "parameters", "plugins",
    )

    /**
     * 这是不是一个**系统库名**（`USE` 时用来区分"真库"与"内存里物化的系统库"）。
     *
     * ⚠️ 与 [isSystemSchema] 的区别：那个判的是**限定表名**（含点），这个判的是**库名**。
     */
    fun isSystemSchemaName(name: String): Boolean {
        val n = name.lowercase()
        return n == "information_schema" || n == "performance_schema" || n == "mysql" || n == "sys"
    }

    /** 判据是**限定表名**（含点的 `information_schema.TABLES` 形态）；只判库名用 [isSystemSchemaName]。 */
    private fun isSystemSchema(name: String): Boolean {
        val n = name.lowercase()
        return n.startsWith("information_schema.") || n.startsWith("performance_schema.") ||
            n.startsWith("mysql.") || n.startsWith("sys.")
    }

    fun tableFor(rawName: String): VirtualTable? {
        if (!isSystemSchema(rawName)) return null
        val schema = rawName.substringBefore('.').lowercase()
        val table = rawName.substringAfter('.', "").lowercase()

        // performance_schema / mysql / sys 只需要**存在且为空**：
        // 客户端查它们是为了探测特性，返回空集比报「表不存在」友好得多，
        // 也避免了工具反复重试。
        if (schema != "information_schema") {
            return if (table.isBlank()) null else emptyTable(rawName)
        }

        return when (table) {
            "schemata" -> virtual(rawName, SCHEMATA_COLS) { s, e ->
                e.catalog.listDatabases().map { db ->
                    listOf(
                        Datum.Str("def"), Datum.Str(db.name),
                        Datum.Str(db.charset), Datum.Str(db.collation), Datum.Null,
                    )
                }
            }
            "tables" -> virtual(rawName, TABLES_COLS) { _, e ->
                e.catalog.listDatabases().flatMap { db ->
                    e.catalog.listTables(db.databaseId).map { t -> tableRow(db.name, t, e) }
                }
            }
            "columns" -> virtual(rawName, COLUMNS_COLS) { _, e ->
                e.catalog.listDatabases().flatMap { db ->
                    e.catalog.listTables(db.databaseId).flatMap { t -> columnRows(db.name, t) }
                }
            }
            "statistics", "indexes" -> virtual(rawName, STATISTICS_COLS) { _, e ->
                e.catalog.listDatabases().flatMap { db ->
                    e.catalog.listTables(db.databaseId).flatMap { t -> indexRows(db.name, t) }
                }
            }
            "key_column_usage" -> virtual(rawName, KEY_COLUMN_USAGE_COLS) { _, e ->
                e.catalog.listDatabases().flatMap { db ->
                    e.catalog.listTables(db.databaseId).flatMap { t ->
                        // 顺序对齐 MySQL：CONSTRAINT_{CATALOG,SCHEMA,NAME} 然后是
                        // TABLE_{CATALOG,SCHEMA,NAME}、COLUMN_NAME、ORDINAL_POSITION、…
                        fun usage(constraint: String, col: String, pos: Int) = listOf<Datum>(
                            Datum.Str("def"), Datum.Str(db.name), Datum.Str(constraint),
                            Datum.Str("def"), Datum.Str(db.name), Datum.Str(t.name), Datum.Str(col),
                            Datum.I64((pos + 1).toLong()), Datum.Null,
                            Datum.Null, Datum.Null, Datum.Null,
                        )
                        val pk = t.primaryKey.mapIndexed { i, c -> usage("PRIMARY", c, i) }
                        // 只看 ACTIVE：构建中的索引对查询不可见，对外也应如此
                        pk + t.activeIndexes.flatMap { ix ->
                            ix.columns.mapIndexed { i, c -> usage(ix.name, c, i) }
                        }
                    }
                }
            }
            "table_constraints" -> virtual(rawName, TABLE_CONSTRAINTS_COLS) { _, e ->
                e.catalog.listDatabases().flatMap { db ->
                    e.catalog.listTables(db.databaseId).flatMap { t ->
                        buildList {
                            if (t.primaryKey.isNotEmpty()) {
                                add(constraintRow(db.name, t.name, "PRIMARY", "PRIMARY KEY"))
                            }
                            // MySQL 里 UNIQUE 索引同时也是 UNIQUE 约束
                            t.activeIndexes.filter { it.unique }.forEach {
                                add(constraintRow(db.name, t.name, it.name, "UNIQUE"))
                            }
                        }
                    }
                }
            }
            "partitions" -> virtual(rawName, PARTITIONS_COLS) { _, e ->
                e.catalog.listDatabases().flatMap { db ->
                    e.catalog.listTables(db.databaseId)
                        .filter { it.partitionKind != PartitionKind.NONE }
                        .flatMap { t -> partitionRows(db.name, t, e) }
                }
            }
            "views" -> virtual(rawName, VIEWS_COLS) { _, e ->
                e.catalog.listDatabases().flatMap { db ->
                    e.catalog.listTables(db.databaseId).filter { it.isView }
                        .map { viewRows(db.name, it) }
                }
            }
            "engines" -> virtual(rawName, ENGINES_COLS) { _, _ ->
                listOf(
                    listOf(
                        Datum.Str("YangBase"), Datum.Str("DEFAULT"), Datum.Str("Object-storage native LSM"),
                        Datum.Str("YES"), Datum.Str("YES"), Datum.Str("YES"), Datum.Str("NO"),
                    )
                )
            }
            "character_sets" -> virtual(rawName, CHARACTER_SETS_COLS) { _, _ ->
                listOf(
                    listOf(Datum.Str("utf8mb4"), Datum.Str("UTF-8 Unicode"), Datum.Str("utf8mb4_bin"), Datum.I64(4)),
                    listOf(Datum.Str("binary"), Datum.Str("Binary pseudo charset"), Datum.Str("binary"), Datum.I64(1)),
                )
            }
            "collations" -> virtual(rawName, COLLATIONS_COLS) { _, _ ->
                listOf(
                    listOf(Datum.Str("utf8mb4_bin"), Datum.Str("utf8mb4"), Datum.I64(46), Datum.Str("Yes"), Datum.Str("Yes"), Datum.I64(1)),
                    listOf(Datum.Str("utf8mb4_general_ci"), Datum.Str("utf8mb4"), Datum.I64(45), Datum.Str("Yes"), Datum.Str("Yes"), Datum.I64(1)),
                )
            }
            "processlist" -> virtual(rawName, PROCESSLIST_COLS) { session, _ ->
                listOf(
                    listOf(
                        Datum.I64(1), Datum.Str("root"), Datum.Str("localhost"), Datum.Str(session.currentDbName ?: ""),
                        Datum.Str("Query"), Datum.I64(0), Datum.Str(""), Datum.Str(""),
                    )
                )
            }
            "views", "triggers", "routines", "events",
            "referential_constraints", "table_privileges", "schema_privileges", "user_privileges",
            "column_privileges", "check_constraints", "parameters", "plugins",
            -> emptyTable(rawName)
            else -> emptyTable(rawName)   // 未知的 information_schema 表：存在但为空，好过报错
        }
    }

    // ── 行构造 ────────────────────────────────────────────────────

    /**
     * `information_schema.VIEWS`。
     *
     * `IS_UPDATABLE` 如实写 `NO`：本系统的视图是**只读**的（设计文档 §11.3 的限制清单里
     * 明说"视图本身支持只读"）。写 `YES` 会让 ORM 之类的工具尝试去写视图，
     * 然后收到一个更远的错误。
     */
    private fun viewRows(dbName: String, t: TableSchema): List<Datum> = listOf(
        Datum.Str("def"), Datum.Str(dbName), Datum.Str(t.name),
        Datum.Str(t.viewSql ?: ""),
        Datum.Str("NONE"), Datum.Str("NO"), Datum.Str("root@%"),
        Datum.Str("DEFINER"), Datum.Str("utf8mb4"), Datum.Str("utf8mb4_0900_ai_ci"),
    )

    private fun tableRow(dbName: String, t: TableSchema, e: DatabaseEngine): List<Datum> {
        // 行数只在**分片已经打开**时给出精确值，否则如实返回 NULL。
        // 为了填一个展示字段去打开所有分片，会让客户端连上来的一瞬间
        // 产生成百上千次对象存储请求（见 DatabaseEngine.peekRowCount）。
        val rows = e.peekRowCount(t)
        return listOf(
            // 视图与表在 TABLES 里**必须能区分**：客户端据此决定"能不能写它"，
            // 一律报 BASE TABLE 会让工具把视图当成可更新的表
            Datum.Str("def"), Datum.Str(dbName), Datum.Str(t.name),
            Datum.Str(if (t.isView) "VIEW" else "BASE TABLE"),
            Datum.Str("YangBase"), Datum.I64(t.version), Datum.Str("Dynamic"),
            rows?.let { Datum.I64(it) } ?: Datum.Null,                 // TABLE_ROWS
            Datum.Null, Datum.Null, Datum.Null,                        // AVG_ROW_LENGTH / DATA_LENGTH / MAX_DATA_LENGTH
            Datum.Null, Datum.Null, Datum.Null, Datum.Null,            // INDEX_LENGTH / DATA_FREE / AUTO_INCREMENT / CREATE_TIME
            Datum.Null, Datum.Null, Datum.Null, Datum.Null,            // UPDATE_TIME / CHECK_TIME / TABLE_COLLATION / CHECKSUM
            Datum.Str(""),                                             // CREATE_OPTIONS
            Datum.Str(""),                                             // TABLE_COMMENT
        )
    }

    private fun columnRows(dbName: String, t: TableSchema): List<List<Datum>> =
        t.columns.mapIndexed { i, c ->
            val isPk = t.primaryKey.any { it.equals(c.name, true) }
            val uniqueIdx = t.activeIndexes.firstOrNull { it.unique && it.columns.size == 1 && it.columns[0].equals(c.name, true) }
            listOf(
                Datum.Str("def"), Datum.Str(dbName), Datum.Str(t.name), Datum.Str(c.name),
                Datum.I64((i + 1).toLong()),                          // ORDINAL_POSITION
                c.defaultValue?.let { Datum.Str(it) } ?: Datum.Null,
                Datum.Str(if (c.nullable) "YES" else "NO"),
                Datum.Str(c.type.mysqlName),                           // DATA_TYPE
                c.length?.let { Datum.I64(it.toLong()) } ?: Datum.Null,
                c.precision?.let { Datum.I64(it.toLong()) } ?: Datum.Null,
                c.scale?.let { Datum.I64(it.toLong()) } ?: Datum.Null,
                Datum.Str(columnTypeLabel(c)),
                Datum.Str(if (isPk) "PRI" else if (uniqueIdx != null) "UNI" else if (t.activeIndexes.any { ix -> ix.columns.firstOrNull()?.equals(c.name, true) == true }) "MUL" else ""),
                Datum.Str(if (c.autoIncrement) "auto_increment" else ""),
                Datum.Str(""),                                         // PRIVILEGES
                Datum.Str(""),                                         // COLUMN_COMMENT
                Datum.Str(""),                                         // GENERATION_EXPRESSION
            )
        }

    /** 主键也是一条索引，索引名固定为 `PRIMARY`（与 MySQL 一致）。 */
    private fun indexRows(dbName: String, t: TableSchema): List<List<Datum>> {
        val out = ArrayList<List<Datum>>()
        if (t.primaryKey.isNotEmpty()) {
            t.primaryKey.forEachIndexed { i, c ->
                out.add(statRow(dbName, t.name, "PRIMARY", true, i, c, "BTREE"))
            }
        }
        for (ix in t.activeIndexes) {
            val type = if (ix.kind == IndexKind.HASH) "HASH" else "BTREE"
            ix.columns.forEachIndexed { i, c ->
                out.add(statRow(dbName, t.name, ix.name, ix.unique, i, c, type))
            }
        }
        return out
    }

    private fun statRow(
        db: String, table: String, index: String, unique: Boolean, seq: Int, column: String, type: String,
    ): List<Datum> = listOf(
        // MySQL 的顺序：TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, NON_UNIQUE, INDEX_SCHEMA,
        // INDEX_NAME, SEQ_IN_INDEX, COLUMN_NAME, COLLATION, CARDINALITY, SUB_PART, PACKED,
        // NULLABLE, INDEX_TYPE, COMMENT, INDEX_COMMENT
        Datum.Str("def"), Datum.Str(db), Datum.Str(table), Datum.I64(if (unique) 0 else 1), Datum.Str(db),
        Datum.Str(index), Datum.I64((seq + 1).toLong()), Datum.Str(column), Datum.Str("A"),
        Datum.Null,      // CARDINALITY：本系统不维护索引基数统计，如实返回 NULL
        Datum.Null, Datum.Null,
        Datum.Str(""), Datum.Str(type), Datum.Str(""), Datum.Str(""),
    )

    private fun constraintRow(db: String, table: String, name: String, type: String): List<Datum> =
        listOf(Datum.Str("def"), Datum.Str(db), Datum.Str(name), Datum.Str(db), Datum.Str(table), Datum.Str(type))

    private fun partitionRows(dbName: String, t: TableSchema, e: DatabaseEngine): List<List<Datum>> {
        val method = t.partitionKind.name
        // 行数只在分片已打开时给出（同 tableRow 的理由）
        val countPerPartition = if (e.isOpen(t)) {
            (0 until t.effectivePartitionCount).map { e.peekRowCount(t) }
        } else {
            List(t.effectivePartitionCount) { null }
        }
        return (0 until t.effectivePartitionCount).map { i ->
            val def = t.partitions.getOrNull(i)
            val bound = def?.rangeUpperBound
            val desc = when {
                def == null -> Datum.Null
                bound != null && bound.equals(MAXVALUE, ignoreCase = true) -> Datum.Str(MAXVALUE)
                bound != null -> Datum.Str(bound)
                def.listValues.isNotEmpty() -> Datum.Str(def.listValues.joinToString(","))
                else -> Datum.Null
            }
            listOf(
                Datum.Str("def"), Datum.Str(dbName), Datum.Str(t.name),
                Datum.Str(def?.name ?: "p$i"),
                Datum.I64((i + 1).toLong()),          // PARTITION_ORDINAL_POSITION
                Datum.Str(method),                     // PARTITION_METHOD
                t.partitionColumn?.let { Datum.Str(it) } ?: Datum.Null,   // PARTITION_EXPRESSION
                desc,                                  // PARTITION_DESCRIPTION
                countPerPartition.getOrNull(i)?.let { Datum.I64(it) } ?: Datum.Null,  // TABLE_ROWS
                Datum.Null, Datum.Null,                // AVG_ROW_LENGTH / DATA_LENGTH
                Datum.Str(""),                         // PARTITION_COMMENT
            )
        }
    }

    private fun columnTypeLabel(c: ColumnDef): String = when (c.type) {
        ColumnType.VARCHAR -> "varchar(${c.length ?: 255})"
        ColumnType.DECIMAL -> "decimal(${c.precision ?: 10},${c.scale ?: 0})"
        // ENUM/SET 的成员表是类型定义的一部分：不显示出来的话，
        // `mysqldump` 拿 COLUMN_TYPE 去重建表就会丢掉整个值域
        ColumnType.ENUM -> "enum(${c.enumValues.joinToString(",") { "'$it'" }})"
        ColumnType.SET -> "set(${c.enumValues.joinToString(",") { "'$it'" }})"
        else -> c.type.mysqlName
    }

    // ── 构造辅助 ──────────────────────────────────────────────────

    private fun virtual(
        name: String,
        cols: List<ColumnDef>,
        materialize: (Session, DatabaseEngine) -> List<List<Datum>>,
    ): VirtualTable = VirtualTable(syntheticSchema(name, cols), materialize)

    private fun emptyTable(name: String): VirtualTable =
        virtual(name, listOf(str("DUMMY")), { _, _ -> emptyList() })

    /**
     * 给虚拟表造一个 [TableSchema]。
     *
     * 这样做的价值是**复用**：虚拟表因而可以走与真实表完全相同的
     * 过滤 / 排序 / 投影 / LIMIT 代码，不需要为系统库再写一遍 SQL 语义。
     * tableId 用负数是为了绝不与真实表（catalog 从 1000 开始）相撞。
     */
    private fun syntheticSchema(name: String, cols: List<ColumnDef>) = TableSchema(
        tableId = -name.hashCode().toLong() - 1,
        databaseId = -1,
        name = name,
        version = 0,
        columns = cols,
        primaryKey = listOf(cols.first().name),
    )

    private fun str(n: String) = ColumnDef(n, ColumnType.VARCHAR, nullable = true, length = 64)

    // ── 列定义（顺序与 MySQL 8.0 一致，客户端按位置取值）───────────

    private val SCHEMATA_COLS = listOf(
        str("CATALOG_NAME"), str("SCHEMA_NAME"), str("DEFAULT_CHARACTER_SET_NAME"),
        str("DEFAULT_COLLATION_NAME"), str("SQL_PATH"),
    )

    private val TABLES_COLS = listOf(
        str("TABLE_CATALOG"), str("TABLE_SCHEMA"), str("TABLE_NAME"), str("TABLE_TYPE"),
        str("ENGINE"), str("VERSION"), str("ROW_FORMAT"), str("TABLE_ROWS"),
        str("AVG_ROW_LENGTH"), str("DATA_LENGTH"), str("MAX_DATA_LENGTH"), str("INDEX_LENGTH"),
        str("DATA_FREE"), str("AUTO_INCREMENT"), str("CREATE_TIME"), str("UPDATE_TIME"),
        str("CHECK_TIME"), str("TABLE_COLLATION"), str("CHECKSUM"), str("CREATE_OPTIONS"),
        str("TABLE_COMMENT"),
    )

    private val COLUMNS_COLS = listOf(
        str("TABLE_CATALOG"), str("TABLE_SCHEMA"), str("TABLE_NAME"), str("COLUMN_NAME"),
        str("ORDINAL_POSITION"), str("COLUMN_DEFAULT"), str("IS_NULLABLE"), str("DATA_TYPE"),
        str("CHARACTER_MAXIMUM_LENGTH"), str("NUMERIC_PRECISION"), str("NUMERIC_SCALE"),
        str("COLUMN_TYPE"), str("COLUMN_KEY"), str("EXTRA"), str("PRIVILEGES"),
        str("COLUMN_COMMENT"), str("GENERATION_EXPRESSION"),
    )

    private val STATISTICS_COLS = listOf(
        str("TABLE_CATALOG"), str("TABLE_SCHEMA"), str("TABLE_NAME"), str("NON_UNIQUE"),
        str("INDEX_SCHEMA"), str("INDEX_NAME"), str("SEQ_IN_INDEX"), str("COLUMN_NAME"),
        str("COLLATION"), str("CARDINALITY"), str("SUB_PART"), str("PACKED"),
        str("NULLABLE"), str("INDEX_TYPE"), str("COMMENT"), str("INDEX_COMMENT"),
    )

    private val KEY_COLUMN_USAGE_COLS = listOf(
        str("CONSTRAINT_CATALOG"), str("CONSTRAINT_SCHEMA"), str("CONSTRAINT_NAME"),
        str("TABLE_CATALOG"), str("TABLE_SCHEMA"), str("TABLE_NAME"), str("COLUMN_NAME"),
        str("ORDINAL_POSITION"), str("POSITION_IN_UNIQUE_CONSTRAINT"),
        str("REFERENCED_TABLE_SCHEMA"), str("REFERENCED_TABLE_NAME"), str("REFERENCED_COLUMN_NAME"),
    )

    private val TABLE_CONSTRAINTS_COLS = listOf(
        str("CONSTRAINT_CATALOG"), str("CONSTRAINT_SCHEMA"), str("CONSTRAINT_NAME"),
        str("TABLE_SCHEMA"), str("TABLE_NAME"), str("CONSTRAINT_TYPE"),
    )

    // 只暴露真正有值的列。MySQL 还有 SUBPARTITION_*/INDEX_LENGTH/CHECK_TIME 等，
    // 本系统不支持子分区，因此它们恒为 NULL——列出恒 NULL 的列只会误导使用者。
    private val VIEWS_COLS = listOf(
        str("TABLE_CATALOG"), str("TABLE_SCHEMA"), str("TABLE_NAME"),
        str("VIEW_DEFINITION"), str("CHECK_OPTION"), str("IS_UPDATABLE"), str("DEFINER"),
        str("SECURITY_TYPE"), str("CHARACTER_SET_CLIENT"), str("COLLATION_CONNECTION"),
    )

    private val PARTITIONS_COLS = listOf(
        str("TABLE_CATALOG"), str("TABLE_SCHEMA"), str("TABLE_NAME"), str("PARTITION_NAME"),
        str("PARTITION_ORDINAL_POSITION"), str("PARTITION_METHOD"), str("PARTITION_EXPRESSION"),
        str("PARTITION_DESCRIPTION"), str("TABLE_ROWS"), str("AVG_ROW_LENGTH"),
        str("DATA_LENGTH"), str("PARTITION_COMMENT"),
    )

    private val ENGINES_COLS = listOf(
        str("ENGINE"), str("SUPPORT"), str("COMMENT"), str("TRANSACTIONS"),
        str("XA"), str("SAVEPOINTS"), str("LOG_FILE_GROUP"),
    )

    private val CHARACTER_SETS_COLS = listOf(
        str("CHARACTER_SET_NAME"), str("DESCRIPTION"), str("DEFAULT_COLLATE_NAME"), str("MAXLEN"),
    )

    private val COLLATIONS_COLS = listOf(
        str("COLLATION_NAME"), str("CHARACTER_SET_NAME"), str("ID"), str("IS_DEFAULT"),
        str("IS_COMPILED"), str("SORTLEN"),
    )

    private val PROCESSLIST_COLS = listOf(
        str("ID"), str("USER"), str("HOST"), str("DB"), str("COMMAND"),
        str("TIME"), str("STATE"), str("INFO"),
    )
}
