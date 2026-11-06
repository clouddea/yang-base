#!/usr/bin/env bash
# 从 CHANGELOG.md 抽取某个版本的段落（不含标题行），用作 GitHub Release 正文。
# 用法：scripts/changelog-section.sh 0.2.0
# 找不到该版本时以非零退出——发布流程不允许"没有发布说明的版本"。

set -euo pipefail

version="${1:?用法: $0 MAJOR.MINOR.PATCH}"
root="$(git rev-parse --show-toplevel)"
cd "$root"

body="$(awk -v v="$version" '
  $0 ~ "^## \\[" v "\\]" { f = 1; next }
  f && /^## \[/ { exit }
  f { print }
' CHANGELOG.md)"

# 去掉段首尾空行
body="$(printf '%s\n' "$body" | sed -e '/./,$!d' | tac | sed -e '/./,$!d' | tac)"

if [ -z "$body" ]; then
  echo "CHANGELOG.md 里没有 ## [$version] 段" >&2
  exit 1
fi

printf '%s\n' "$body"
