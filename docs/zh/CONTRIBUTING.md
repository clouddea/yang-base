---
---

# 参与 YangBase 的开发

[English](https://github.com/clouddea/yang-base/blob/main/CONTRIBUTING.md) | [简体中文](CONTRIBUTING.md)

感谢你愿意花时间。这份文档说明我们对贡献的期望、项目怎么构建与测试、以及一个改动如何被合入。

YangBase 是一个**把全部持久状态都放在对象存储上的事务型数据库**。这一条设计决定了下面几乎所有规则：对象存储按请求计费、不能原地更新、尾延迟高。违反这三条约束的贡献往往不是"写错了"才被拒，而是"代价太高"。

---

## 1. 先做什么

| 你想… | 这样做 |
|---|---|
| 报告 bug | 提 [bug report](https://github.com/clouddea/yang-base/blob/main/.github/ISSUE_TEMPLATE/bug_report.yml)，带上版本/提交号、对象存储后端、**完整 SQL**、期望结果与实际结果 |
| 提新功能 | **先**提 [feature request](https://github.com/clouddea/yang-base/blob/main/.github/ISSUE_TEMPLATE/feature_request.yml)。没讨论过的大 PR 很难合 |
| 提问 | 用 [Discussions](https://github.com/clouddea/yang-base/discussions)，不要开 issue |
| 报告安全问题 | **不要开公开 issue**，看 [SECURITY.md](https://github.com/clouddea/yang-base/blob/main/SECURITY.md) |

想找第一个任务，挑 `good first issue` 标签的——那些是特意做成自包含的。

---

## 2. 开发环境

要求：

- **JDK 21**（构建用 `jvmToolchain(21)`）
- POSIX shell（Linux / macOS；Windows 走 WSL）
- 可选：MySQL 8 客户端工具（`mysql`、`mysqldump`）——缺了时少数兼容性用例会自己跳过
- 可选：Docker（想用 MinIO 而不是内嵌 S3 测试服务器时）

```bash
git clone https://github.com/clouddea/yang-base.git
cd yang-base
./gradlew build              # 编译 + 单元测试 + 覆盖率门禁
./gradlew integrationTest    # 集成测试（内嵌 S3，不需要 Docker）
```

**不要把机器相关的绝对路径**（JDK home、本地 S3 凭据）写进被跟踪的文件：CI 会直接失败，别人的 checkout 也会坏。

本地起一个实例（MinIO 后端）：

```bash
./scripts/minio.sh start
./gradlew :yb-server:run --args="--s3-bucket yangbase \
  --s3-endpoint http://127.0.0.1:9000 --s3-access-key yangbase --s3-secret-key yangbase123 \
  --port 13306"
```

---

## 3. 分支与 PR

完整模型见 [development-process.md](development-process.md)，速记：

- `main` 永远可发布，**禁止直接推送**。
- 干活在 `feature/<version>/<slug>` 或 `bugfix/<version>/<slug>`，从 `dev/<version>` 切出，PR **目标也是 `dev/<version>`**，用 **squash merge** 合入。
- 发布时 `dev/<version>` 用 **merge commit** 合入 `main`。
- 已发布版本的紧急修复用 `hotfix/<patch>/<slug>`，从 `main` 切出。

一个 PR 只做**一件逻辑上的事**。如果你在描述里开始写"另外还…"，请拆开。

### 提交信息

轻量 Conventional Commits + 模块 scope：

```text
fix(yb-sql): 修 DECIMAL 与整数混算丢精度
test(yb-server): mysqldump 恢复超时时打印客户端输出
docs: 增加分支模型与开发流程
```

允许的 type：`feat`、`fix`、`perf`、`refactor`、`test`、`docs`、`build`、`ci`、`chore`。
scope 用模块名（`yb-sql`、`yb-storage`、`yb-mysql`、`yb-server`…），全仓库级改动可以省略。

### PR 检查清单

- [ ] 本地 `./gradlew build` 通过
- [ ] 改动涉及存储 / 事务 / 索引 / MySQL 协议时，`./gradlew integrationTest` 通过
- [ ] 新行为有测试；修 bug 的 PR 带一个**修之前会失败**的测试
- [ ] 覆盖率不低于门禁（`yb-server` 50%；不要为了让 PR 过而降低门禁）
- [ ] 没有绝对路径、没有硬编码凭据、没有提交构建产物
- [ ] 用户可见行为变了就同步文档
- [ ] PR 描述讲清**为什么**，不只是做了什么

---

## 4. 不可谈判的项目规则

这些来自设计与生产约束。违反它们的 PR 即使代码本身写得很好，也会被要求修改。

1. **请求数是一等指标。** 对象存储按请求计费。为了回答一个小问题而去扫全表（比如 `COUNT(*)` 全扫）不可接受。
2. **所有对象键必须走 `ObjectKeys`。** 别处不许拼字符串；键的布局是落盘格式的一部分，悄悄改了会破坏恢复。
3. **对象不可变 + manifest 指针切换。** 永不原地修改已写入的对象；更新＝写新对象 + 原子切换指针。
4. **统计与元数据从 `.idx` 读，不从数据对象读。** 保持 `.idx` / `.dat` 分离。
5. **引擎不认识 SQL 方言。** 方言差异只存在于方言/解析层，执行器里不许出现 `if (mysqlMode)`。
6. **正确性优先于性能。** 更快但算错，永远是回退。
7. **新代码注释写英文**（2026-11 起）。存量中文注释按模块逐步迁移。

---

## 5. 测试

- 单元测试在 `src/test`；集成测试在 `src/integrationTest`，**不属于** `build`。
- 测试里不许出现无界阻塞：任何子进程或 IO 等待都要有超时，超时必须把已缓冲的输出一起打进断言消息。一个挂住的测试要烧掉 45 分钟 CI，而且什么也说明不了。
- 测试不许依赖文件系统遍历顺序，也不许依赖"只有快机器才成立"的时序假设。
- 断言 IO 预算时，断言**具体计数器**（`get` / `get_range` / `put`），不要断言墙钟时间。

---

## 6. 评审与合并

- 每个改动至少一位维护者批准 + CI 全绿。
- 评审看的是：崩溃/重启后的正确性、请求成本影响、新增的失败模式、以及测试是否真能抓住它声称抓住的 bug。
- 我们争取几天内回 PR。两周没动静的 PR 可能被关掉并留言——你想继续随时重开。

---

## 7. 许可

YangBase 使用 [Apache-2.0](https://github.com/clouddea/yang-base/blob/main/LICENSE)。提交 PR 即表示你同意以同样条款授权你的贡献，不需要签 CLA。
