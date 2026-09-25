package com.cloudea.yangbase.mysql

import com.cloudea.yangbase.sql.Session
import com.cloudea.yangbase.sql.Statement

/**
 * 语句执行的观测点。
 *
 * ## 为什么挂在协议层而不是执行器里
 *
 * 「一条语句花了多久、读了几个对象、返回了几行」——这三个量在协议层
 * **全部可见**：纳秒时间戳、前后对象存储计数之差、结果集大小。
 * 而把它们塞进执行器需要在每个分支上重复埋点，且会让 SQL 层
 * 依赖观测实现。
 *
 * 协议层**不改变任何协议行为**：观测点只是在调用执行器前后各通知一次，
 * 报错与返回码都与没有观测点时完全一致。
 *
 * 默认没有观测者（[MysqlServer] 的 `observer` 为 null），此时热路径上
 * 只有一次空判断。
 */
interface QueryObserver {

    /**
     * 语句开始执行。
     *
     * @return 一个不透明的标记，会原样传回 [queryFinished]（通常是 `System.nanoTime()`）
     */
    fun queryStarted(sql: String, stmt: Statement, session: Session): Long

    /**
     * 语句结束（无论成功还是抛错）。
     *
     * @param session 与 [queryStarted] 收到的是同一个对象（每条连接一个）。
     *   观测实现可以据此把「开始」时的状态取回来——**不能用线程局部变量**：
     *   执行器会在协程里挂起与恢复，恢复后可能落在另一个线程上。
     * @param rows 返回/影响的行数；失败时为 -1
     * @param error 抛出的异常；成功时为 null
     */
    fun queryFinished(
        token: Long,
        sql: String,
        stmt: Statement,
        session: Session,
        rows: Long,
        error: Throwable?,
    )
}
