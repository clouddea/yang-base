---
---

# 索引性能验收

`./gradlew :yb-bench:benchIndexes` 是独立的性能门禁，默认使用内嵌 S3。
所有查询经 JDBC/MySQL 协议进入生产优化器和执行器；任何用例失败都返回非零退出码。
正式测试代码入库，运行报告放在被 Git 忽略的 `yb-bench/build/reports/index-bench/`。

## 覆盖范围

| 路径 | 用例与证据 |
|---|---|
| 主键 | 点查、范围查询；EXPLAIN 为 const/range；点查最多 1 GET |
| 二级 | 有序、HASH、UNIQUE、复合；EXPLAIN 为 ref；与等价全扫比较 |
| 跳数 | MINMAX、SET、BLOOM、NGRAM；后 3 种必须实际读取 `.skp` |
| 全文 | standard 英文、ngram 中文；EXPLAIN 为 FULLTEXT；实际读取 postings |
| 分区 | RANGE、LIST、HASH 从 4 个数据分片裁到 1 个；KEY 完整主键定位到 1 个分片 |
| 计数 | 全表 COUNT、低基数列等值 COUNT；EXPLAIN 为 METADATA_COUNT，0 GET |

共 18 个场景。每张表默认 6,000 行，每个场景 3 组交替顺序的查询。
行数至少 4,000 且须被 4 整除，确保跨越足够多的数据块。

## 验收方法

1. 固定种子生成带较宽 payload 的数据，通过 SQL 批量插入。
2. flush 全部表和索引，关闭写入节点，再从同一对象存储重启。
3. 禁用数据块缓存和磁盘缓存，预热恢复元数据。在同一只读快照内测量，
   避免 TSO 窗口续领随机混入索引请求预算。这里测的是**元数据已热、数据块未缓存**，
   不包含节点冷启动，也不是数据块全热的延迟。
4. 每条索引查询与等价的扫描查询核对完整排序结果，并与独立生成的期望结果比较。
   `列 + 0` / `CONCAT(列,'')` 阻止主键、二级、分区和跳数谓词下推。
   全文对照使用 LIKE；生成的文本确保指定测试词与 LIKE 的结果完全相同，
   且只有 10 个命中，不受默认 top-k 截断影响。这个等价关系不适用于任意自然语言语料。
5. **每次样本**必须：全扫至少 4 GET；索引 GET 更少、读取字节至少减少 40%；
   主键点查 ≤1 GET，计数 =0 GET。分区测试检查实际 `.dat` 请求所属分片。
   查询期间不得发生 LIST 或 PUT。

SET/BLOOM 的数据在块内交替使用 0 与 100，仅末尾含 42；这样 min/max
无法排除前面的块，且数值列不会被自动收集为值计数字典，必须使用 `.skp`。
MINMAX 可直接利用 `.idx` 的同等列统计，这是生产中的优先路径。
NGRAM 用子串谓词，避免让前缀 min/max 代替 ngram。

TSV 输出结果数、计划、GET 次数、读取字节及延迟 p50/p95。
请求数和字节列展示第一组样本，但门禁检查全部样本。
默认仅 3 次采样，延迟用于观察，**不构成延迟 SLO 或统计显著性结论**。
计数器统计成功返回的 ObjectStore 调用，不包含 SDK 内部重试与失败请求。

## 运行

需要 JDK 21 与 Python 3。使用 Python 入口会额外输出 JSON 运行配置和退出码；
密钥仅经环境传递，不写入 JVM 参数、报告或源码。

```bash
# 默认内嵌 S3，无外部账号
python3 scripts/bench-indexes.py

# 本地真实 MinIO（采用 scripts/minio.sh 的默认端口和凭证）
./scripts/minio.sh start
python3 scripts/bench-indexes.py --backend minio

# 用户明确授权的 OSS 验收；桶必须已经存在
python3 scripts/bench-indexes.py --backend oss \
  --region cn-hangzhou --bucket yangbase-local-test \
  --access-key-csv /path/to/AccessKey.csv

# 扩大数据规模与采样数量
python3 scripts/bench-indexes.py --backend minio --rows 20000 --samples 20
```

CSV 使用阿里云导出的 `AccessKey ID,AccessKey Secret` 列，且恰好一组凭证。
也可设置 `YB_S3_ACCESS_KEY`、`YB_S3_SECRET_KEY`。MinIO 支持
`MINIO_ROOT_USER`、`MINIO_ROOT_PASSWORD`；端点可用 `--endpoint` 覆盖。
OSS 使用三级域名寻址和项目已有的单写者非原子指针切换降级。

每次运行只在随机 `index-bench/<UUID>/` 前缀下读写，结束时只清理该前缀，
**不复用会清空整个桶的 IntegrationStore**。强杀进程可能留下该前缀；日志中
打印其位置，可按前缀清理。不得直接调用旧的全量外部集成测试去代替此入口。

`build integrationTest` 仍使用内嵌 S3。外部后端验收是显式 opt-in，
不挂到默认 build/check，避免 CI 意外使用真实云账号。
