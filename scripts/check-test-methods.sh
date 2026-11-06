#!/usr/bin/env bash
#
# 找出**永远不会执行、但永远显示绿色**的测试方法。
#
# 背景（踩了两次：2026-10-26 扫出 3 条，2026-10-27 新写的测试里又复现 1 条）：
#
#   fun `某个用例`() = runBlocking { … }
#
# 这是**表达式体**：`runBlocking` 的返回值就是方法的返回值。只要花括号里最后一个
# 表达式有值（比如 `assertFailsWith { … }` 返回异常对象），Kotlin 生成的签名就是
# 返回 `Object` 而不是 `void`。JUnit 5 按签名找测试方法，**返回非 void 的一律跳过**，
# 而且不报错、不警告——测试报告里连一条记录都没有。
#
# 判据：测试类里「public、无参、返回非 void、非 static」的方法。
# 构造函数（`public com.x.Foo();` 只有两个 token）、编译器生成的成员
# （`toString` / `hashCode` / `componentN` / `getX` / `isX` / `$…`）按 token 数与白名单排掉；
# `@TestFactory`（返回 `List<DynamicTest>` 这类集合）是合法工厂，也排掉。
#
# 修法：写成 `= runBlocking<Unit> { … }`，或改用块体 `{ runBlocking { … } }`。
#
# 用法：scripts/check-test-methods.sh
#       需要先跑过一次 `./gradlew testClasses`（CI 在 build 步骤之后天然满足）

set -uo pipefail

JAVAP="javap"
if [ -n "${JAVA_HOME:-}" ] && [ -x "$JAVA_HOME/bin/javap" ]; then
  JAVAP="$JAVA_HOME/bin/javap"
fi

WHITELIST='^(toString|hashCode|clone|finalize|values|valueOf|ordinal|name|copy|component[0-9]+|access\$.*|.*\$annotations|.*\$default|get[A-Z_].*|is[A-Z_].*)$'

fail=0
checked=0
checked_methods=0
bad_list=""

while IFS= read -r -d '' classfile; do
  base=$(basename "$classfile" .class)
  case "$base" in
    *Test|*Tests) ;;
    *) continue ;;
  esac
  case "$base" in
    *'$'*) continue ;;   # 内部类/lambda 类单独成文件，不当作测试类
  esac

  checked=$((checked + 1))

  # 只认「同一行、公开、无参、非 static」。javap 会把带注解/泛型的方法折行，
  # 折行后的续行 token 数不对，自然被下面的判据挡掉（宁可漏报也不误报）
  while IFS= read -r name; do
    [ -z "$name" ] && continue
    checked_methods=$((checked_methods + 1))
    fail=1
    bad_list="$bad_list\n  ${classfile#./}  →  ${name}()  （返回非 void，JUnit 会静默跳过）"
  done < <(
    "$JAVAP" -p "$classfile" 2>/dev/null | awk '
      {
        line = $0
        sub(/^[ \t]+/, "", line)
        sub(/;[ \t]*$/, "", line)
        n = split(line, a, " ")
        if (n < 3 || n > 4) next                  # 2 个 token = 构造函数，>4 = 折行/异常
        if (a[1] != "public") next
        for (i = 2; i < n; i++) if (a[i] == "static") next
        if (a[n - 1] == "void") next              # 正常的 JUnit 用例
        # `@TestFactory` 返回 List<DynamicTest> / Collection / Stream——那是合法工厂，不是用例
        if (a[n - 1] ~ /(List|Collection|Set|Stream)/) next
        nm = a[n]
        if (nm !~ /\(\)$/) next                   # 必须是无参方法
        sub(/\(\)$/, "", nm)
        if (nm ~ /\$/) next
        print nm
      }
    ' | grep -vE "$WHITELIST" || true
  )
done < <(find . -path '*/build/classes/kotlin/test/*' -name '*.class' -print0 2>/dev/null)

if [ "$checked" -eq 0 ]; then
  echo "⚠️ 没有找到任何测试类产物——先跑 ./gradlew testClasses（本检查不猜，没东西可查就明说）"
  exit 0
fi

if [ "$fail" -ne 0 ]; then
  echo "❌ 有测试方法永远不会执行（JUnit 5 对返回非 void 的方法静默跳过）："
  printf '%b\n' "$bad_list"
  echo
  echo "修法：把 \`= runBlocking { … }\` 改成 \`= runBlocking<Unit> { … }\`，"
  echo "     或者用块体 \`{ runBlocking { … } }\`。见本脚本开头的说明。"
  exit 1
fi

echo "✔ 扫过 $checked 个测试类 / $checked_methods 个可疑签名：没有「返回非 void」的用例"
