package com.cloudea.yangbase.txn

import com.cloudea.yangbase.common.Datum
import com.cloudea.yangbase.common.IndexDef
import com.cloudea.yangbase.common.IndexKind
import com.cloudea.yangbase.common.IndexState
import com.cloudea.yangbase.common.RowCodec
import com.cloudea.yangbase.common.ShardId
import com.cloudea.yangbase.common.TableSchema
import com.cloudea.yangbase.common.ReadTs
import com.cloudea.yangbase.storage.WalBatch
import com.cloudea.yangbase.storage.WalOp
import java.util.Base64

/**
 * 在线索引构建（设计文档 §8.7）。
 *
 * ## 要解决的问题
 *
 * 在已有数据的表上建索引，必须把已有行也写进索引。天真做法是「扫一遍表、
 * 把行写进索引」，但它有一个**静默**的漏洞：
 *
 * ```
 *   t0  DDL 开始，取快照 read_ts
 *   t1  把索引写进 catalog        ← 这之后的写入才维护它
 *   t2  按 read_ts 扫表、回填
 *   t3  完成
 *
 *   在 [t0, t1) 之间提交的写入：回填看不到（它们晚于 read_ts），
 *   写入路径也不会维护它们（那时索引还不在 schema 里）→ **永久缺失索引项**。
 * ```
 *
 * 而查询一旦走上索引就**不再回退扫描**，于是那些行就静默地查不出来了——
 * 不报错、不降级，看起来就像它们不存在。这是本项目最不能接受的一类缺陷。
 *
 * ## 解法：快照回填 + 增量追平
 *
 * ```
 *   ① read_ts 取自**发布之前**
 *   ② 发布索引（state = BUILDING）——写入路径从此维护它
 *   ③ 按 read_ts 扫表回填
 *   ④ 追平：凡是 commit_ts > read_ts 的写入，都按「差值」补一次索引
 *   ⑤ 翻成 ACTIVE，查询才可以用它
 * ```
 *
 * 完整的推理是：一次写入要么 `commit_ts <= read_ts`（那它就在快照里，③ 已覆盖），
 * 要么 `commit_ts > read_ts`（那 ④ 覆盖）。两者互补且穷尽，**不存在缺口**，
 * 也不需要给表加锁——这正是「在线」的含义（对齐 MySQL 的 `ALGORITHM=INPLACE`）。
 *
 * ## 为什么是「重读两边的行」而不是「重放 WAL」
 *
 * WAL 里只有 `(主键, 新值)`，**没有旧值**。而一次 UPDATE 需要在索引里
 * 抹掉旧值对应的那一项；只补新值的话旧项会永远留着，
 * 于是 `WHERE col = <旧值>` 会命中一行其实已经不是那个值的记录。
 *
 * 所以 ④ 从 WAL 里只取「受影响的主键」，再分别按 `read_ts` 和「现在」读这一行，
 * 得到旧值与新值，按差值写：旧值打墓碑、新值写入。
 * **幂等**——重跑得到同样的结果，因此构建中断后可以直接重试（§8.7 的失败处理）。
 */
class OnlineIndexBuilder(private val engine: DatabaseEngine) {

    /**
     * 构建一个索引，返回写入了多少条索引项。
     *
     * [readTs] 必须是**发布索引之前**取得的时间戳，理由见类注释。
     * 「先取 readTs、再发布」这个顺序由调用方（DDL 路径）保证。
     */
    suspend fun build(table: TableSchema, index: IndexDef, readTs: ReadTs): Long {
        require(index.state == IndexState.BUILDING) { "只有 BUILDING 状态的索引需要构建" }
        // 跳数索引随 SST 一起生成、倒排索引走自己的段文件，
        // 都不是「主表回填」能补的东西，各自有各自的构建路径
        if (index.kind.name.startsWith("SKIP_") || index.kind == IndexKind.FULLTEXT) return 0

        // ③ 快照回填。唯一索引在这里自检重复值：有重复就直接失败，
        // 而不是建出一个「声称唯一、实际有重复」的索引——那会让之后每一次
        // 走索引的查询都只返回其中一行
        val snapshot = engine.scanAll(table, readTs)
        var written = engine.backfillIndex(table, index, snapshot).toLong()
        engine.flushIndexShard(table, index)

        // ④ 追平 [read_ts, 现在) 之间的写入
        written += catchUp(table, index, readTs)
        engine.flushIndexShard(table, index)
        return written
    }

    /**
     * 追平：把 `commit_ts > readTs` 的写入按差值补进索引。
     *
     * 受影响的主键来自**这张表各数据分片的 WAL**——WAL 本身就是完整、有序、
     * 不可变的变更日志，不必另建一份 delta 表（§16.1 的 Q5 决策：变更流读已有的 WAL）。
     *
     * ⚠️ 必须**逐数据分片**处理，然后把这一批主键读回**同一个分片**。
     * 不能拿着主键让引擎去路由：分区列不在主键里时，光凭主键根本算不出这一行
     * 属于哪个分区，路由到别的分片就会读到 null，于是把「行还在」当成「行没了」。
     */
    private suspend fun catchUp(table: TableSchema, index: IndexDef, readTs: ReadTs): Long {
        val indexShard = IndexCodec.shardOf(index)
        var total = 0L
        for (dataShard in engine.allShards(table)) {
            for (batch in engine.walKeysSince(dataShard, readTs.value).chunked(BATCH)) {
                total += applyDelta(table, index, indexShard, dataShard, batch, readTs)
            }
        }
        return total
    }

    /**
     * 对一批受影响的主键做差值修补。
     *
     * 每一对 (旧行, 新行) 只做必要的那几件事：
     * - 值没变（改了别的列、或改回原值）→ 只补写新值那一项，**不打墓碑**。
     *   漏掉这条会把写入路径刚写好的索引项抹掉，索引里反而少一行。
     * - 值变了 → 旧值打墓碑、新值写入。
     * - 行被删了 → 只打墓碑。
     * - 含 NULL 的值不建索引项（SQL 语义：唯一键里的 NULL 不算重复）。
     */
    private suspend fun applyDelta(
        table: TableSchema,
        index: IndexDef,
        indexShard: ShardId,
        dataShard: ShardId,
        pks: List<ByteArray>,
        readTs: ReadTs,
    ): Long {
        val lsm = engine.shardStore(dataShard, table)
        val ops = ArrayList<WalOp>()
        for (pk in pks.distinctBy { Base64.getEncoder().encodeToString(it) }) {
            val before = lsm.get(pk, readTs)?.let { RowCodec.decode(it, table) }
            val after = lsm.get(pk, ReadTs(Long.MAX_VALUE))?.let { RowCodec.decode(it, table) }
            val beforeVals = before?.let { indexValues(table, index, it) }
            val afterVals = after?.let { indexValues(table, index, it) }

            // 值确实变了才打墓碑：同值时打墓碑会把写入路径刚建好的项抹掉
            if (beforeVals != null && beforeVals != afterVals) {
                ops.add(WalOp(IndexCodec.entryKey(table, index, before), null))
            }
            if (afterVals != null) {
                ops.add(WalOp(IndexCodec.entryKey(table, index, after), RowCodec.encode(after)))
            }
        }
        if (ops.isEmpty()) return 0
        val ts = engine.allocateCommitTs()
        engine.shardStore(indexShard, table).write(
            WalBatch("idx-catchup-${index.id}-$ts", ts, ops)
        )
        return ops.size.toLong()
    }

    /**
     * 索引列的值。
     *
     * ⚠️ **含 NULL 的行也要建项**，与写入路径保持一致（写入路径不区分 NULL）。
     * 「构建时跳过 NULL、写入时不跳过」会让同一张表在构建前后对
     * `IS NULL` 这类查询给出不同答案——而构建是后台发生的，
     * 那就是一个随时间出现的静默错误。唯一性约束里 NULL 不参与，
     * 那是**判重**的语义，与「建不建项」是两件事。
     */
    private fun indexValues(table: TableSchema, index: IndexDef, row: List<Datum>): List<Datum> =
        index.columns.map { row.getOrNull(table.indexOf(it)) ?: Datum.Null }

    private companion object {
        /** 一批处理多少个主键。分批是为了不把内存压爆。 */
        const val BATCH = 4096
    }
}
