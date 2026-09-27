package com.cloudea.yangbase.sql

import com.cloudea.yangbase.common.ColumnDef
import com.cloudea.yangbase.common.ColumnType
import com.cloudea.yangbase.common.TableSchema
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class LikePruningTest {
    private val table = TableSchema(1, 1, "t", 1,
        listOf(ColumnDef("id", ColumnType.BIGINT), ColumnDef("txt", ColumnType.VARCHAR)), listOf("id"))

    private fun ranges(predicate: String) = PruneRanges.extract(
        (Parser.parse("SELECT id FROM t WHERE $predicate") as SelectStmt).where, table)

    @Test fun `LIKE 只下推正向字符串列与字面量`() {
        assertEquals("%needle%", ranges("txt LIKE '%needle%'").single().likePattern)
        for (predicate in listOf("txt NOT LIKE '%needle%'", "id LIKE '%12%'", "txt LIKE txt",
            "CONCAT(txt,'') LIKE '%needle%'", "txt LIKE NULL", "txt LIKE '%needle%' OR id=1")) {
            assertTrue(ranges(predicate).isEmpty(), predicate)
        }
        assertEquals(2, ranges("txt LIKE '%needle%' AND id>5").size)
    }
}
