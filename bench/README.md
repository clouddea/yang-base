# bench/ — 基准基线

这个目录里放的是**机器可比的基准快照**，不是宣传材料。

| 文件 | 作用 |
|---|---|
| `baseline-ci.json` | PR 门禁的对照基线（`ci` 档） |
| `baseline-default.json` | 完整档位的结果（发布用，渲染成 `docs/*/benchmarks.md`） |
| `current-ci.json` | 本地/CI 跑出来的当前结果（不入库，`.gitignore`） |

## 为什么基线可以在本地生成、在 CI 里对比

因为**门禁只盯确定性指标**：每次操作的对象存储请求数、每操作字节数、存储足迹。
这三个量只取决于代码与数据集——同样的代码在任何机器上都得到同样的值。

延迟完全不进硬门禁。共享 CI runner 的延迟波动轻松超过 50%，把它做成硬失败的唯一
结局是有人把门禁关掉，那比没有门禁更糟。所以延迟只出现在对比表的告警行里。

## 口径（改基准前先读）

- **请求数是一等指标。** 每一步都记录 `get / get_range / put / head / list` 的明细。
- **并发写入的请求数不是确定性的**（批次大小随调度变化，第一次 CI 就撞到 0.188 → 0.212，代码没改）。
  它只告警，硬门禁换成不变式：平均每事务请求数 ≤ 1。
- **缓存命中与未命中分开报。** 命中一栏不代表对象存储便宜。
- **冷点查严格按「每次 1 次 GET」测**：每个样本新建一个 `LsmStore`（空 `BlockCache`），
  读一次就丢。用一个 store 连读 N 个键会互相预热，测出来的 0.x 次/查询毫无意义。
- **装载用批量提交（每批 500 行）**，但写入负载本身逐事务提交——装载不是被测对象。
- **数据集是确定性推导的**，不用随机数：换机器、换 JDK 都得到同一份数据。

## 怎么用

```bash
# 1. 跑一次（ci 档，几分钟）
./gradlew :yb-bench:benchPublic -PYB_BENCH_PROFILE=ci -PYB_BENCH_OUT=bench/current-ci.json

# 2. 和基线对比（确定性指标回退 >5% 会非零退出）
python3 scripts/bench-compare.py --baseline bench/baseline-ci.json \
                                 --current bench/current-ci.json --markdown bench/compare.md

# 3. 重新生成双语文档（数字由生成器写，不手抄）
python3 scripts/bench-render.py --input bench/baseline-default.json
```

打真实对象存储（MinIO / S3 / OSS）：

```bash
YB_S3_ACCESS_KEY=... YB_S3_SECRET_KEY=... \
./gradlew :yb-bench:benchPublic -PYB_BENCH_PROFILE=default \
  -PYB_S3_ENDPOINT=https://s3.example.com -PYB_S3_BUCKET=yangbase-bench
```

## 什么时候刷新基线

- **发布时**（打 tag 之前）用 `default` 档跑一次，更新 `baseline-default.json` 与文档；
- **改了基准本身**（数据集、负载、采样数）必须同时刷新 `baseline-ci.json`，
  否则对比会变成「比较两个不同的东西」——`bench-compare.py` 会检查 profile 是否一致，
  但数据集变化它看不出来，这一点靠人守。

## 已知局限（写在这里，免得被当成结论）

- 默认打**进程内 S3 兼容服务器**。它的每次请求有固定开销（`backend-raw` 那两行就是量它的），
  所以冷读延迟主要由夹具决定，不代表真实云端延迟。
- 只有单分片、单表、无并发读。多 CN、读转发、跨分片事务都还没进基准。
- 存储足迹用内存后端测（体积与后端无关），统计的是 `.dat` + `.idx`，不含 WAL 与 manifest。
