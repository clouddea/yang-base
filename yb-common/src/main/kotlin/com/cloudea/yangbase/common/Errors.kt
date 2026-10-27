package com.cloudea.yangbase.common

/** YangBase 异常基类。 */
sealed class YangBaseException(message: String, cause: Throwable? = null) :
    RuntimeException(message, cause)

/**
 * 条件写的前置条件不成立，**且结论是确定的**——重发永远不会成功。
 *
 * 现在的语义是「**目标状态**不成立」：`MustNotExist` 撞上已存在的对象。
 * 而「目标状态还在，但已经被别人改过了」（`MustMatch` 的 etag 不匹配）**不是**这一类，
 * 它是 [ConcurrentModificationException]：重读当前值再试是可能成功的。
 *
 * 这条区分曾经在 S3 上丢失过（两个场景都返回 412，旧实现一律归成这一类），
 * 后果是 manifest 指针的 CAS 失败被当成"确定失败"、fencing 分支接不住。见 issue #8。
 */
class PreconditionFailedException(message: String, cause: Throwable? = null) :
    YangBaseException(message, cause)

/** 条件写并发冲突（`MustMatch` 的 etag 已变），**重读当前值后可以再试**。 */
class ConcurrentModificationException(message: String, cause: Throwable? = null) :
    YangBaseException(message, cause)

/**
 * 后端不支持某个被依赖的能力。
 *
 * 关键：当调用方传入了非空 Precondition 而后端不支持时，
 * 实现**必须抛出本异常**，而不是静默忽略前置条件。
 * GCS 的 S3 互操作层会静默忽略条件头，这正是本系统要主动防御的情形。
 */
class CapabilityNotSupportedException(message: String) :
    YangBaseException(message)

/**
 * 对象存储操作失败（网络、鉴权、5xx 等）。
 *
 * [retryable] 是**后端的判断**，不是调用方的猜测：只有最了解自己错误码的那一层
 * （`S3ObjectStore` 看 HTTP 状态、`LocalFileObjectStore` 看 IO 异常）才填得出它。
 * 让重试层去匹配 message 里的状态码是在错误的地方做判断——后端换用词时会静默失效。
 *
 * 判据见设计文档 §3.3.1：
 *  - `5xx` / `429` / `408` / 超时 / 网络中断 → `true`（瞬时故障，重发可能成功）
 *  - `412 PreconditionFailed` → **按前置条件分两种**：`MustNotExist` 失败是
 *    [PreconditionFailedException]（绝不可重试）；`MustMatch` 失败是
 *    [ConcurrentModificationException]（重读后可重试）。两者在 S3 上是同一个状态码，
 *    只有发起请求的那一方知道该算哪一类
 *  - `409 ConditionalRequestConflict` → 由 [ConcurrentModificationException] 表达，可退避重试
 *  - `404` → 由 [ObjectNotFoundException] 表达，是明确的业务语义，**不可重试**
 *
 * 默认 `false` 是刻意选的保守值：一个新的失败分支如果忘了分类，症状是
 * 「不重试」（退化成改造前的行为），而不是「无限重试」。
 */
class ObjectStoreException(
    message: String,
    cause: Throwable? = null,
    val retryable: Boolean = false,
) : YangBaseException(message, cause)

/** 租约已失效——写者已被 fence，必须立即停止写入。 */
class LeaseLostException(shard: ShardId, epoch: Epoch) :
    YangBaseException("租约已失效: shard=$shard epoch=$epoch")

/** 对象不存在。 */
class ObjectNotFoundException(key: String) :
    YangBaseException("对象不存在: $key")

/** 数据损坏（CRC 校验失败、magic 不匹配等）。 */
class CorruptDataException(message: String, cause: Throwable? = null) :
    YangBaseException(message, cause)

/** SQL 层错误，携带 MySQL 错误码。 */
class SqlException(
    val errorCode: Int,
    val sqlState: String,
    message: String,
) : YangBaseException(message)

// ── MySQL 常用错误码 ────────────────────────────────────────────────
object MySqlErrors {
    const val ER_NO_SUCH_TABLE = 1146
    const val ER_BAD_DB_ERROR = 1049
    const val ER_DB_CREATE_EXISTS = 1007
    const val ER_TABLE_EXISTS_ERROR = 1050
    const val ER_BAD_FIELD_ERROR = 1054
    const val ER_PARSE_ERROR = 1064
    const val ER_NO_PARTITION_FOR_GIVEN_VALUE = 1526
    const val ER_UNKNOWN_ERROR = 1105
    const val ER_KEY_COLUMN_DOES_NOT_EXITS = 1176
    const val ER_INVALID_USE_OF_NULL = 1138
    const val ER_CANT_DROP_FIELD_OR_KEY = 1091
    const val ER_SAME_NAME_PARTITION = 1517
    const val ER_PARTITION_MGMT_ON_NONPARTITIONED = 1505
    const val ER_DUP_ENTRY = 1062
    const val ER_NO_DEFAULT_FOR_FIELD = 1364
    const val ER_DUP_FIELDNAME = 1060
    const val ER_UNKNOWN_SYSTEM_VARIABLE = 1193
    const val ER_NOT_SUPPORTED_YET = 1235
    const val ER_ACCESS_DENIED = 1045
    const val ER_LOCK_WAIT_TIMEOUT = 1205
    const val ER_DEADLOCK = 1213
    const val ER_DATA_TOO_LONG = 1406
    const val ER_TRUNCATED_WRONG_VALUE = 1292
    /** `SET NAMES gbk` 这类不在支持矩阵里的字符集。 */
    const val ER_UNKNOWN_CHARACTER_SET = 1115

    /** 连接查询里引用的列在多张表里都有（必须写成 `表.列`）。 */
    const val ER_NON_UNIQ_ERROR = 1052

    /** JSON 路径表达式语法错。 */
    const val ER_INVALID_JSON_PATH = 3143

    /** 对象类型不对（`SHOW CREATE VIEW` 给了一张表）。 */
    const val ER_WRONG_OBJECT = 1347

    /** 标量子查询返回了多行（MySQL 1242）。 */
    const val ER_SUBQUERY_NO_1_ROW = 1242

    /** UNION 两侧的列数不一致。 */
    const val ER_WRONG_NUMBER_OF_COLUMNS_IN_SELECT = 1222

    /** 引用了不存在的表 / 表别名。 */
    const val ER_BAD_TABLE_ERROR = 1053

    /** 聚合结果超出该类型能表示的范围（本系统的 SUM 溢出会报它）。 */
    const val ER_DATA_OUT_OF_RANGE = 1690

    /** 函数的参数类型不对（如对字符串做 SUM）。 */
    const val ER_WRONG_ARGUMENTS = 1210

    /** 聚合函数用在了 WHERE 这类"分组之前"的位置。 */
    const val ER_INVALID_GROUP_FUNC_USE = 1111

    /** 有聚合但没 GROUP BY 时，选了一个非聚合列（ONLY_FULL_GROUP_BY）。 */
    const val ER_MIX_OF_GROUP_FUNC_AND_FIELDS = 1140

    /** 有 GROUP BY 时，选了一个既非分组键也非聚合的列（ONLY_FULL_GROUP_BY）。 */
    const val ER_WRONG_FIELD_WITH_GROUP = 1055

    /** 结果字符串含客户端字符集无法表示的字符（如 latin1 连接里返回中文）。 */
    const val ER_CANT_AGGREGATE_2COLLATIONS = 1267

    /** `SELECT DISTINCT` 下按结果里没有的列排序——MySQL 的 3065。 */
    const val ER_FIELD_IN_ORDER_NOT_SELECT = 3065

    /** `CAST(... AS JSON)` 收到了不合法的 JSON 文本（MySQL 3140）。 */
    const val ER_INVALID_JSON_TEXT = 3140

    /** 写入的值超出列声明的范围（整数宽度、`VARCHAR(n)` 长度、`DECIMAL(p,s)`）。 */
    const val ER_WARN_DATA_OUT_OF_RANGE = 1264
}
