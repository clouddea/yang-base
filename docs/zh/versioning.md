# 版本号与发布策略

[English](../en/versioning.md) | [简体中文](versioning.md)

> 生效日期：2026-10-27　决策人：杨圆飞　执行人：我（AI 研发）
> 本文是**版本号语义、发布节奏、冻结规则、发布门禁**的唯一权威记录。
> 分支与 PR 的操作细节见 [development-process.md](development-process.md)，本文只在其上加发布规则。

---

## 1. 三个数字的含义

`MAJOR.MINOR.PATCH` —— 与 [语义化版本](https://semver.org/lang/zh-CN/) 同义，但按数据库的形态做了特化：
这里的"公共接口"不只是 Java 类，还包括**盘上的数据**和**线上协议**。

| 位 | 何时升 | 例子 |
|---|---|---|
| **MAJOR** | **升级不兼容**：对象布局/恢复不兼容（新二进制必须迁移旧数据，或旧二进制读不了新数据）、删除或改变用户依赖的 SQL 语义、MySQL 协议不兼容、`ObjectStore` SPI 破坏性变更、删除或改变 CLI 参数/配置项含义 | `0.x → 1.0`、`1.x → 2.0` |
| **MINOR** | **向后兼容**的新行为：新 SQL 语法、新对象存储后端、新索引类型、性能/空间优化、新指标、新 CLI 子命令 | `0.1 → 0.2` |
| **PATCH** | **只修缺陷**：不加功能、不加配置项、除了修掉的那个缺陷之外不改变行为 | `0.2.0 → 0.2.1` |

两条最容易被搞错的推论：

- **数据格式变更就是 MAJOR，哪怕接口没动。** manifest 加字段、`.idx` 头改结构、对象键前缀改规则——
  都是 MAJOR。放进 MINOR 意味着别人的升级会读到垃圾而不报错。
- **改变可观察结果的修复只有"旧结果是错的"才算 PATCH。** 如果修复改变了文档里写明的行为，
  那是 MINOR（需要用户配合的话就是 MAJOR）。

`0.x` 期间兼容承诺天然更弱，但上面的规则照样执行——版本号的意义是"升级可预期"，不是"数字够大"。

---

## 2. 节奏

- **每周五 18:00（Asia/Shanghai）发一个版本。** `0.x` 期间每周期升 MINOR（`0.1.0`、`0.2.0`…），
  用户因此可以指着某个周五说"那是哪个版本"。
- **PATCH 不占班次**，只给已发布版本的紧急修复，不固定日期。
- **MAJOR 不按周发。** `1.0.0` 在达到生产门禁时发（年度 OKR 里 2026-12 打 tag、2027-09 达到"生产可用"）；
  之后 MAJOR 很少见，且必须提前公告。
- 这一周没有值得发的东西就**跳过，不凑数**。跳版要写进下一版的发布说明（"<日期> 未发版"）。
  为了守节奏而发一个空版本，比破一次节奏更糟。

---

## 3. 分支模型与冻结规则

```text
main  ──●──────────────●──────────────●──   只接受发版合并；每个提交都在某个 tag 之下
         \            /              /
     dev/0.1.0 \      /  dev/0.2.0   /
                \    /              /
                 ●──●──────────────●          一个版本一条迭代分支
```

- `dev/X.Y.Z` 在上一个 `vX.Y.Z` 打 tag 之后**立刻**从 `main` 切出。
- 日常改动走短命分支：`feature/X.Y.Z/<slug>`、`fix/…`、`docs/…`、`test/…`，squash 合入 `dev/X.Y.Z`。
- 发版：`dev/X.Y.Z → main` 的 PR（标题 `release: X.Y.Z`）→ merge commit →
  在 `main` 上打 annotated tag `vX.Y.Z` → `.github/workflows/release.yml` 校验、构建、发布 GitHub Release。

### 🔒 硬规则

**`vX.Y.Z` 一旦打上，版本 `X.Y.Z` 就关闭了。不允许再有任何提交。**

- `dev/X.Y.Z` **冻结**、只读。它会写进 [`.github/frozen-branches.txt`](../../.github/frozen-branches.txt)，
  CI 对**目标为冻结分支的 PR 一律失败**。分支保留 90 天供追溯，然后删除。
- tag 受保护：不可移动、不可删除（仓库 ruleset `protect-release-tags`）。
- 新功能、新修复**绝不**落在已发布的版本上。要开新版本：fix → 下一个 PATCH（`dev/X.Y.(Z+1)`），
  feat → 下一个 MINOR（`dev/X.(Y+1).0`）。
- 已发布版本上发现缺陷？**向前修，作为新版本发。**"在发布分支上再补一个提交"正是这条规则禁止的事。

两个例外都很窄，且不碰代码：

1. **发布说明写错字** —— 改 GitHub Release 描述，不改仓库。
2. **发布 workflow 本身坏了** —— 在 `main` 上走 PR 修，对**下一个**版本生效，对已发布的那个不生效。

为什么这么严：会动的 tag 等于没人能复现的 tag。`v0.3.0` 的全部价值就是一年后陌生人检出它，
拿到的源码与今天逐字节相同。

---

## 4. 发布门禁 —— 有已知重大缺陷就不发

每个候选版本必须**全部**满足：

1. **`dev/X.Y.Z` 上 CI 绿**：`build · test · coverage`、`integration tests · embedded S3`、
   后端 conformance 矩阵。红的、抖的都不算绿。
2. **没有 label 为 `severity: critical` / `severity: major` 的未关闭 issue 指向该 milestone。**
   这类 issue 必须修掉，或者**书面降级**（在 issue 里写清为什么不阻塞发布、用户影响是什么）。
   降级是署名的决定，不是偷偷改个 label。
3. **没有已知的丢数据 / 正确性 / 静默错误结果缺陷**，不分级别。这类问题永远不是"已知问题"，是阻塞项。
4. **flaky 测试等同于缺陷。** 间歇性失败的测试阻塞发布，直到修掉或带跟踪 issue 隔离。
   靠调大超时让 CI 变绿不算修复。
5. **`CHANGELOG.md` 里有 `## [X.Y.Z]` 段**，写给人类看：改了什么、什么会坏、还有什么没修。
   只要有已知问题，`Known issues` 段必填。
6. **文档同步**：用户可见的变化都要更新，`docs/en` 与 `docs/zh` 保持一致。
7. **`version.txt` 与 tag 一致**，且 `scripts/release-preflight.sh --version X.Y.Z` 通过。

预检脚本自动检查 1、2、5、7；3、4、6 需要人看。

---

## 5. CHANGELOG

`CHANGELOG.md` 遵循 [Keep a Changelog](https://keepachangelog.com/zh-CN/1.1.0/)：
`Added` / `Changed` / `Fixed` / `Deprecated` / `Removed` / `Security`，
外加本项目视为必填的 `Known issues`。

条目在**改动行为的那个 PR 里**就写进 `## [Unreleased]`，而不是发版时从 `git log` 生成——
后者产出的是提交标题清单，不是用户关心的变化清单。

发版时把 `## [Unreleased]` 改名为 `## [X.Y.Z] - YYYY-MM-DD`，再补一个新的空 `Unreleased`。
`release.yml` 抽取这一段作为 GitHub Release 正文。

---

## 6. 操作清单

```bash
# ── 日常：短命分支 + PR 合入 dev/X.Y.Z ──
git switch dev/0.2.0 && git pull
git switch -c feature/0.2.0/<slug>

# ── 周五 18:00 发版 ──
scripts/release-preflight.sh --version 0.2.0      # 门禁：CI、issue、changelog、版本号、tag
gh pr create --base main --head dev/0.2.0 --title "release: 0.2.0"
gh api -X PUT repos/clouddea/yang-base/pulls/<N>/merge -f merge_method=merge
git switch main && git pull
git tag -a v0.2.0 -m "YangBase 0.2.0" && git push origin v0.2.0
#   → release.yml：校验 tag == version.txt，跑 build + integrationTest，
#     用 CHANGELOG 那一段发布 GitHub Release，上传发行包

# ── 紧接着：冻结旧版本、开下一个窗口 ──
echo "dev/0.2.0" >> .github/frozen-branches.txt     # 经 PR 提交到 main
git switch main && git switch -c dev/0.3.0          # 下一个 MINOR 窗口
```

排期见 GitHub Milestones（`0.2.0`、`0.3.0`… 每个都落在某个周五）。
