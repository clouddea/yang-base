package com.cloudea.yangbase.storage

import com.cloudea.yangbase.common.Datum

/**
 * 跳数索引的写路径 SPI（设计文档 §8.5）。
 *
 * ## 为什么是 SPI 而不是直接调用
 *
 * 跳数索引要在**写 SST 的那一刻**顺带收集统计量——数据正好在手上，
 * 事后再扫一遍等于白读一次。但索引的实现住在 `yb-index`，而 `yb-index`
 * 依赖 `yb-storage`；让 `yb-storage` 反向依赖它会成环。
 *
 * 所以把「我需要你告诉我块边界和块内的行」抽象成一个接口放在这里，
 * 由 `yb-index` 实现、由装配层（`yb-server`）注入。这是依赖倒置的标准用法，
 * 也是这个项目里唯一一处跨越依赖方向的接口——值得，因为替代方案
 * （flush 之后再读一遍 SST 来建索引）会把平白多出来的 IO 加到每条写入路径上。
 */
interface SstBlockObserver {

    /** 一个新块开始。`blockIdx` 与 `.idx` 里的块号一致，是剪枝时的对齐依据。 */
    fun beginBlock(blockIdx: Int)

    /** 块内的一行（已解码）。 */
    fun observe(row: List<Datum>)

    /** 当前块结束。 */
    fun endBlock()

    /**
     * SST 落盘之后调用：把收集到的统计量构造成索引对象并写出。
     *
     * @param sstFileId 对应的 SST 文件 id——索引与它是**多对一**的关系，
     *   读取时靠这个 id 找到 `.skp`。
     * @return 写出的对象数
     */
    suspend fun persist(sstFileId: String, level: Int): Int

    /** 这一轮什么都没收集到（例如所有行在该列上都是 NULL）时可跳过写出。 */
    val isEmpty: Boolean
}

/**
 * 块级剪枝 SPI（设计文档 §8.5 的读半边）。
 *
 * 在**读块之前**回答「这个块可能含有满足谓词的行吗」。返回 false 的块
 * **一次 GET 都不产生**——这是跳数索引省下 IO 的全部机制。
 */
interface BlockSkipper {

    /**
     * @param handle 目标 SST。传句柄而不是文件 id，是因为剪枝**应该优先用
     *   `.idx` 里已有的块级列统计**——那份数据查询时本就常驻内存，
     *   用它判定是 0 次 GET、0 字节传输，永远优于去读 `.skp`。
     *   `.skp` 只补充 `.idx` 没有的东西（set / bloom / ngram）。
     * @param blockIdx 块号（与 `.idx` 的块序一致）
     */
    suspend fun mightMatchBlock(handle: SstHandle, blockIdx: Int): Boolean
}

/**
 * 一次扫描可以下推到存储层的约束。
 *
 * 表达列区间、等值和 LIKE 字面量模式。更复杂的谓词（函数、OR 组合）到不了这一层，
 * 由上层逐行过滤兜底——**裁剪只允许放宽，绝不允许收紧**。
 */
data class ColumnRange(
    /** schema 列下标。 */
    val column: Int,
    val low: Datum? = null,
    val lowInclusive: Boolean = true,
    val high: Datum? = null,
    val highInclusive: Boolean = true,
    /** 等值查询：minmax 与 bloom 都能用，且能裁得更狠。 */
    val equality: Datum? = null,
    /** LIKE 原始模式；按与 SQL 相同的 % / _ 语义交给 ngram 跳数索引。 */
    val likePattern: String? = null,
)
