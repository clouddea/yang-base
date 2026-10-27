---
---

# 开发流程与分支模型

> 生效日期：2026-10-26　决策人：杨圆飞
> 一句话：**任何新需求、bugfix 都必须走 PR；feature/bugfix 合入 `dev/<version>`，`dev/<version>` 统一合入 `main`。**
> 版本号语义、发布节奏、冻结规则与发布门禁在 [versioning.md](versioning.md)，本文只管分支与 PR 操作。

---

## 1. 分支命名

| 分支 | 用途 | 从哪切出 | PR 目标 |
|---|---|---|---|
| `main` | 始终可发布。**禁止直接推送** | — | — |
| `dev/<version>` | 迭代集成分支，一个目标版本一条 | `main` | `main`（发布时） |
| `feature/<version>/<slug>` | 新功能 | `dev/<version>` | `dev/<version>` |
| `bugfix/<version>/<slug>` | 修 bug（非线上紧急） | `dev/<version>` | `dev/<version>` |
| `hotfix/<patch-version>/<slug>` | 线上紧急修复 | `main`（当前线上 tag） | `main`，并**回合并**到 `dev/<下一个版本>` |

示例：

```text
dev/0.1.0
feature/0.1.0/english-readme
feature/0.1.0/backend-conformance
bugfix/0.1.0/mysqldump-test-hang
hotfix/0.1.1/shard-lease-regression
```

约定：

- `<version>` 是**目标版本号**（`MAJOR.MINOR.PATCH`），不是日期。`0.1.0` 就是当前迭代要发的版本。
- `<slug>` 用小写字母、数字和连字符，2–4 个词，能看出做了什么：`english-readme`、`count-zero-get`。
- 不使用 `iter/`、`release/`、`wip/` 这类前缀，也不使用个人名或日期前缀。

---

## 2. 日常流程（新需求 / 普通 bugfix）

```bash
# 1. 从迭代分支切出
git checkout dev/0.1.0 && git pull
git checkout -b feature/0.1.0/<slug>

# 2. 干活，提交（一个提交只做一件事）
git commit -m "add english README"

# 3. 推送并开 PR，base 选 dev/0.1.0
git push -u origin feature/0.1.0/<slug>
gh pr create --base dev/0.1.0 --fill

# 4. CI 绿 → 合并（squash）→ 删除源分支
```

**合并方式**：`feature/*` `bugfix/*` 用 **squash merge**，让 `dev` 上的历史一个 PR 一个提交；
`dev/*` → `main` 用 **merge commit**，保留迭代边界。

---

## 3. 发布流程

```bash
# 1. 门禁：CI 绿、无 severity: critical/major 的 issue、CHANGELOG 有这一段、version.txt 对齐
scripts/release-preflight.sh --version 0.1.0

# 2. release PR（feature PR 用 squash，这一层用 merge commit 保留迭代边界）
gh pr create --base main --head dev/0.1.0 --title "release: 0.1.0"

# 3. 合并后打 tag（在 main 上），tag 会触发 release.yml（校验 + 构建 + 发布 GitHub Release）
git checkout main && git pull
git tag -a v0.1.0 -m "v0.1.0" && git push origin v0.1.0

# 4. 冻结旧版本、开新窗口
echo "dev/0.1.0" >> .github/frozen-branches.txt   # 经 PR 提交到 main
git switch -c dev/0.2.0                            # 从 main 切
```

> 🔒 **打了 tag 之后，`dev/0.1.0` 只读。** 任何新 feat/fix 都开新版本，不再碰已发布的分支和 tag。
> CI 的 `policy · frozen release branches` 会让目标为冻结分支的 PR 直接失败。

---

## 4. 线上紧急修复（hotfix）

hotfix 的对象是**已经发布到 main 的版本**，所以它从 `main` 切出，直接修线上：

```bash
git checkout main && git pull
git checkout -b hotfix/0.1.1/<slug>

# 修 + 测试
git push -u origin hotfix/0.1.1/<slug>
gh pr create --base main --title "hotfix: ..."

# 合并 main 后：打 patch tag
git tag -a v0.1.1 -m "v0.1.1" && git push origin v0.1.1

# ⚠️ 必须回合并，否则下一个版本会把这修好的 bug 带回来
git checkout dev/<下一个版本> && git merge main && git push
```

> **回合并这一步最容易漏**，漏了就等于下个版本重新引入同一个 bug。
> hotfix PR 的检查清单里必须有这一条。

---

## 5. PR 的硬性要求

- **CI 必须全绿**（`build` + `integrationTest` 两个 job），不允许“先合后修”
- 描述里写清：做了什么、为什么、怎么验证的（贴输出，不要只写“测试通过”）
- 改了读路径 → 必须有**对象存储请求数断言**
- 改了 SQL 语义 → 必须同步 `docs/zh/compatibility.md`
- 新代码注释写英文（2026-11 起）
- 不把功能改动和格式化 / 重命名混在同一个提交里

模板见 `.github/PULL_REQUEST_TEMPLATE.md`。

---

## 6. 迭代节奏

- **一周一个版本**（每周五 18:00 发），对应一条 `dev/<version>` 分支；`0.x` 期间每次升 MINOR
- 迭代开始：从 `main` 切出 `dev/<version>`，把该迭代的任务拆成 `feature/*` / `bugfix/*`
- 迭代中：只往 `dev/<version>` 合并，不碰 `main`
- 迭代结束：`dev/<version>` → `main` 的 release PR + tag + 发布说明
- 与既有节奏对齐：周五周报写迭代进度，月末月报对齐 OKR 的 KR 达成率
- 这一周没有值得发的东西就**跳过**，并在下一版发布说明里写明“<日期> 未发版”——不凑数

发布门禁（**有已知重大缺陷就不发**）见 [versioning.md](versioning.md) §4；机械可查的部分由
`scripts/release-preflight.sh` 执行。

### 迭代的完成定义（DoD）

一个迭代算完成，必须同时满足：

1. `./gradlew build` 与 `./gradlew integrationTest` 本地全绿
2. CI 两个 job 全绿
3. release PR 已合入 `main`，tag 已打
4. 迭代目标里的每一项都有**可验证的证据**——测试、基准数字、文档链接，不是“做了”
5. 迭代总结写进 `docs/changelog`（或 release notes）

---

## 7. 分支保护（GitHub 设置）

`main` 分支需要开启：

- 禁止直接 push（Require a pull request before merging）
- Require status checks to pass：`build · test · coverage`、`integration tests · embedded S3`
- 禁止 force push、禁止删除分支

`dev/*` 建议同样禁止直接 push，但不强制——单人维护时允许紧急情况直推，事后补 PR 记录。
