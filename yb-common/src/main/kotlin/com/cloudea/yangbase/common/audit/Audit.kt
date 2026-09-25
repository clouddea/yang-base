package com.cloudea.yangbase.common.audit

import kotlinx.serialization.Serializable

/**
 * 一条审计记录（设计文档 §12.5 / §12.4 的运维面）。
 *
 * 审计与普通日志的区别在于**它是要长期保存、可被外部核查的证据**：
 * 谁在什么时候对哪个对象做了结构性变更（DDL），以及 GC 删掉了什么。
 * 因此它写进**对象存储**（`ops/audit/{date}.jsonl`）而不是 stderr——
 * CN 是可以随时销毁重建的，进程日志留不住任何东西（原则 P1）。
 *
 * ⚠️ 审计日志是**唯一被允许**在生产对象存储上产生写入的观测手段
 * （设计文档 §12.5 的指标一律不得引入请求，见原则 P4）。
 * 它的代价用「批量 + 异步」压到最低，且失败绝不影响主流程（见 `Audit.record`）。
 */
@Serializable
data class AuditEvent(
    /** 墙上时钟毫秒。用真实时间而不是 TSO：审计要能和外部的时钟对齐。 */
    val ts: Long,
    /** 动作类别，如 `ddl.create_table`、`gc.sweep_delete`。 */
    val action: String,
    /** 发起者。协议层目前没有用户体系，为空串（见设计文档 §16.3 的设计空白）。 */
    val actor: String = "",
    val database: String = "",
    /** 动作对象：表名 / 索引名 / 分片名。 */
    val target: String = "",
    /** 原始 SQL（DDL 才有）。 */
    val sql: String = "",
    /** 附加上下文，如删除对象的个数、清单键。 */
    val detail: Map<String, String> = emptyMap(),
)

/** 审计记录的落点。实现必须**非阻塞**——它会被调用在前台路径上。 */
interface AuditSink {

    fun record(event: AuditEvent)

    companion object {
        /** 默认什么都不做。没有配置审计时，埋点仍然安全（也仍然零成本）。 */
        val NOOP = object : AuditSink {
            override fun record(event: AuditEvent) {}
        }
    }
}

/**
 * 审计的全局落点。
 *
 * 与 `Metrics` 同理：DDL 的埋点在协议层（可以注入），但 GC 的埋点在
 * `GarbageCollector` 内部，而它由 `Executor` 构造——那条构造链不在本次改动的
 * 范围内。挂一个进程级的落点，是让两处埋点共用一套写法的唯一现实办法。
 *
 * [record] **吞掉所有异常**：审计失败绝不能让一次 DDL 或一次 GC 失败。
 * 沉默的失败同样不可接受，所以实现侧必须把失败记到 stderr 与
 * `yb_audit_write_failed_total` 上。
 */
object Audit {

    @Volatile
    var sink: AuditSink = AuditSink.NOOP

    private var installed: AuditSink? = null

    fun install(s: AuditSink) {
        installed = s
        sink = s
    }

    /** 恢复为「不记录」。测试与关闭路径用。 */
    fun uninstall() {
        installed = null
        sink = AuditSink.NOOP
    }

    /** 若当前安装的正是 [s] 才卸载——避免把别的测试装的落点顺手拆掉。 */
    fun uninstallIf(s: AuditSink) {
        if (installed === s) uninstall()
    }

    fun record(event: AuditEvent) {
        try {
            sink.record(event)
        } catch (_: Throwable) {
            // 审计不能影响主流程。落点自己负责把失败报出来（stderr + 指标）。
        }
    }
}

/** 审计动作名。集中定义，避免各处手写字符串拼错。 */
object AuditActions {
    const val DDL_CREATE_DATABASE = "ddl.create_database"
    const val DDL_DROP_DATABASE = "ddl.drop_database"
    const val DDL_CREATE_TABLE = "ddl.create_table"
    const val DDL_DROP_TABLE = "ddl.drop_table"
    const val DDL_CREATE_INDEX = "ddl.create_index"
    const val DDL_ALTER_TABLE = "ddl.alter_table"

    /** GC 阶段 1：标记出待回收对象。 */
    const val GC_MARK = "gc.mark"

    /** GC 阶段 2：真正删除对象。**这是审计最重要的动作**——它在删数据。 */
    const val GC_SWEEP_DELETE = "gc.sweep_delete"

    /** 推进 GC 水位：之后的时间旅行查询会被拒绝。 */
    const val GC_ADVANCE_WATERMARK = "gc.advance_watermark"
}
