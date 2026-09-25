package com.cloudea.yangbase.sql

import kotlin.test.Test
import kotlin.test.assertEquals

class ForUpdateParseTest {
    @Test fun parse() {
        val stmt = Parser.parse("SELECT id FROM t WHERE id = 20 FOR UPDATE") as SelectStmt
        println("DBG|lockMode=${stmt.lockMode} where=${stmt.where}")
        assertEquals(com.cloudea.yangbase.txn.LockMode.EXCLUSIVE, stmt.lockMode)

        val s2 = Parser.parse("SELECT id FROM t WHERE id = 1 LOCK IN SHARE MODE") as SelectStmt
        println("DBG|lockMode2=${s2.lockMode}")
        assertEquals(com.cloudea.yangbase.txn.LockMode.SHARED, s2.lockMode)
    }
}
