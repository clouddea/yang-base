#!/usr/bin/env bash
# ── 发布预检 / release preflight ────────────────────────────────────────────
#
# 检查一个候选版本是否满足发布门禁（规则见 docs/en/versioning.md §4）。
# 只做**机械可查**的 4 类：版本号一致性、changelog 段落、冻结状态、CI/issue 状态。
# 数据正确性、flaky 测试、文档同步要人看，脚本查不出来，也不假装能查。
#
# 用法：
#   scripts/release-preflight.sh --version 0.2.0
#   scripts/release-preflight.sh --version 0.2.0 --branch dev/0.2.0 --no-ci
#
# 退出码：0 全部通过；1 有 FAIL；2 参数错误。

set -uo pipefail

version=""
branch=""
check_ci=1
repo_hint="clouddea/yang-base"

while [ $# -gt 0 ]; do
  case "$1" in
    --version) version="${2:-}"; shift 2 ;;
    --branch)  branch="${2:-}"; shift 2 ;;
    --no-ci)   check_ci=0; shift ;;
    -h|--help) sed -n '2,20p' "$0"; exit 0 ;;
    *) echo "未知参数：$1" >&2; exit 2 ;;
  esac
done

if ! printf '%s' "$version" | grep -Eq '^[0-9]+\.[0-9]+\.[0-9]+$'; then
  echo "用法：$0 --version MAJOR.MINOR.PATCH [--branch dev/X.Y.Z] [--no-ci]" >&2
  exit 2
fi

cd "$(git rev-parse --show-toplevel)" || exit 2
branch="${branch:-$(git rev-parse --abbrev-ref HEAD)}"
tag="v$version"

fails=0
warns=0
# 颜色只在终端上开：日志里带 ANSI 转义会让 grep / 截图都变脏
if [ -t 1 ]; then G=$'\033[32m'; R=$'\033[31m'; Y=$'\033[33m'; N=$'\033[0m'; else G=''; R=''; Y=''; N=''; fi
pass() { printf '  %s✓%s %s\n' "$G" "$N" "$*"; }
fail() { printf '  %s✗%s %s\n' "$R" "$N" "$*"; fails=$((fails + 1)); }
warn() { printf '  %s!%s %s\n' "$Y" "$N" "$*"; warns=$((warns + 1)); }

echo "发布预检：$version（分支 $branch，tag $tag）"
echo

# ── 1. 版本号单一来源 ────────────────────────────────────────────────────
declared="$(tr -d '[:space:]' < version.txt 2>/dev/null || true)"
if [ "$declared" = "$version" ]; then
  pass "version.txt = $declared"
else
  fail "version.txt 是 '$declared'，与要发的 $version 不一致（先改 version.txt 并提交）"
fi

# ── 2. 分支正确且未冻结 ──────────────────────────────────────────────────
case "$branch" in
  "dev/$version"|main) pass "分支 $branch 属于本次发布" ;;
  *) warn "分支 $branch 既不是 dev/$version 也不是 main" ;;
esac

if [ -f .github/frozen-branches.txt ] &&
   grep -vE '^[[:space:]]*(#|$)' .github/frozen-branches.txt | tr -d '[:space:]' | grep -Fxq "$branch"; then
  fail "$branch 已冻结（见 .github/frozen-branches.txt）——已发布的版本不能再提交，请开新版本"
else
  pass "$branch 未冻结"
fi

# ── 3. tag 还没被占用 ────────────────────────────────────────────────────
if git rev-parse -q --verify "refs/tags/$tag" >/dev/null; then
  fail "本地已存在 tag $tag"
elif git ls-remote --tags --exit-code origin "$tag" >/dev/null 2>&1; then
  fail "远端已存在 tag $tag（发布过的版本号不可复用）"
else
  pass "tag $tag 未占用"
fi

# ── 4. CHANGELOG 有这一段，且写了 Known issues 标题 ──────────────────────
if grep -Eq "^## \[$version\]" CHANGELOG.md 2>/dev/null; then
  pass "CHANGELOG.md 有 [Unreleased] 之外的 $version 段"
  awk -v v="$version" '
    $0 ~ "^## \\[" v "\\]" {f=1; next}
    f && /^## \[/ {exit}
    f {print}
  ' CHANGELOG.md | grep -Eq '^### Known issues' \
    && pass "该段包含 Known issues" \
    || warn "该段没有 Known issues——如果确实没有已知问题，写一句「无」也行，不要省掉这一节"
else
  fail "CHANGELOG.md 没有 '## [$version]' 段（把 [Unreleased] 改名并补日期）"
fi

# ── 5. 工作区干净（打 tag 的提交必须就是被测过的提交）───────────────────
if [ -z "$(git status --porcelain)" ]; then
  pass "工作区干净"
else
  fail "工作区有未提交改动：$(git status --porcelain | wc -l) 个文件"
fi

# ── 6. 没有 severity: critical / major 的未关闭 issue ────────────────────
if ! command -v gh >/dev/null 2>&1; then
  warn "没有 gh，跳过 issue 门禁（手工确认 severity: critical / major 为空）"
else
  blockers="$(gh issue list --repo "$repo_hint" --state open --limit 200 \
      --json number,title,labels \
      --jq '[.[] | select([.labels[].name] | any(. == "severity: critical" or . == "severity: major")) | "#\(.number) \(.title)"] | join("; ")' 2>/dev/null)"
  gh_rc=$?
  if [ "$gh_rc" -ne 0 ]; then
    warn "无法读取 issue 列表（网络或鉴权），门禁未验证"
  elif [ -z "$blockers" ]; then
    pass "没有 severity: critical / major 的未关闭 issue"
  else
    fail "阻塞发布的 issue：$blockers"
  fi
fi

# ── 7. 目标提交上 CI 全绿 ────────────────────────────────────────────────
if [ "$check_ci" -eq 0 ]; then
  warn "--no-ci：跳过 CI 检查"
elif ! command -v gh >/dev/null 2>&1; then
  warn "没有 gh，跳过 CI 检查"
else
  sha="$(git rev-parse HEAD)"
  runs="$(gh api "repos/$repo_hint/commits/$sha/check-runs" \
      --jq '[.check_runs[] | "\(.name)=\(.conclusion // .status)"] | join(", ")' 2>/dev/null)"
  rc=$?
  bad="$(gh api "repos/$repo_hint/commits/$sha/check-runs" \
      --jq '[.check_runs[] | select(.conclusion != "success" and .conclusion != "skipped" and .conclusion != "neutral") | .name] | join(", ")' 2>/dev/null)"
  if [ "$rc" -ne 0 ] || [ -z "$runs" ]; then
    warn "读不到 $sha 的 check-runs（可能还没推送或未配置 CI）；发布前请手工确认 GitHub 上全绿"
  elif [ -z "$bad" ]; then
    pass "CI 全绿：$runs"
  else
    fail "CI 未全绿：$bad（$runs）"
  fi
fi

echo
if [ "$fails" -gt 0 ]; then
  printf '%s发布门禁未通过：%d 项失败，%d 项警告%s\n' "$R" "$fails" "$warns" "$N"
  exit 1
fi
printf '%s发布门禁通过（%d 项警告）%s —— 可以开 release PR 并打 %s\n' "$G" "$warns" "$N" "$tag"
