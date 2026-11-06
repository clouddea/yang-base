#!/usr/bin/env python3
"""对比两次公开基准结果（bench/current.json 的两个版本），判定是否回退。

设计口径（与本项目「请求数是一等指标」的原则一致）：

* **确定性指标 = 硬门禁。** `requestsPerOp`（每次操作的对象存储请求数）、
  `bytesPerOp`、以及存储足迹里的 `indexRatio` / `bytesPerRow` 只取决于代码与
  数据集，与机器快慢无关。回退 >5% 就是真回退，直接非零退出。
* **延迟 = 只告警。** `p50/p95/p99` 在共享 CI runner 上噪声可以轻松到几十个百分点。
  把它做成硬失败，结局一定是有人把门禁关掉——那比没有门禁更糟。
  另外设了绝对噪声地板（默认 200µs）：小基数上的百分比波动没有意义。

用法：
    scripts/bench-compare.py --baseline bench/baseline-ci.json --current bench/current-ci.json
                             [--threshold 5] [--noise-floor-us 200]
                             [--markdown bench/compare.md]

退出码：0 = 无硬回退（可能有告警）；1 = 有硬回退；2 = 输入有问题。
"""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass
from typing import Any

# 这些键是确定性的：允许的例外必须写进这里，而不是整体放宽阈值
HARD_KEYS = ("requestsPerOp", "bytesPerOp")

# 并发负载的请求数**不是确定性的**：组提交的批次大小取决于线程调度，
# 换台机器（或换一次运行）就会不同。第一次 CI 就抓到了：0.188 → 0.212（+13%），
# 代码一行没改。所以这类指标只告警，改用一个**不变式**来硬门禁：
# 并发写入的摊薄效果不该差于「一个事务一次 PUT」（即 requestsPerOp ≤ 1）。
CONCURRENT_PREFIX = "concurrent-"
INVARIANTS = {
    # (workload, 指标) -> 上限；超过就是机制失效，不是噪声
    ("write", "requestsPerOp"): 1.0,
}
HARD_EXTRAS = ("indexRatio", "bytesPerRow", "dataBytesPerRow")
LATENCY_KEYS = ("p50", "p95", "p99")


@dataclass
class Row:
    workload: str
    cache: str
    name: str
    baseline: float
    current: float
    delta_pct: float
    verdict: str  # ok | warn | REGRESSION


def load(path: str) -> dict[str, Any]:
    with open(path, encoding="utf-8") as f:
        data = json.load(f)
    if data.get("schema") != "yangbase-bench/1":
        sys.exit(f"不认识的基准格式: {data.get('schema')!r}（期望 yangbase-bench/1）")
    return data


def index(report: dict[str, Any]) -> dict[tuple[str, str], dict[str, Any]]:
    return {(m["workload"], m["cache"]): m for m in report["metrics"]}


def pct_delta(base: float, cur: float) -> float:
    if base == 0:
        return 0.0 if cur == 0 else float("inf")
    return (cur - base) / base * 100.0


def compare(baseline: dict[str, Any], current: dict[str, Any], threshold: float,
            noise_floor_us: float) -> list[Row]:
    b = index(baseline)
    c = index(current)
    rows: list[Row] = []

    for key in sorted(set(b) & set(c)):
        mb, mc = b[key], c[key]
        noisy = mb["cache"].startswith(CONCURRENT_PREFIX)
        for name in HARD_KEYS:
            rows.append(judge(key, name, mb[name], mc[name], threshold, hard=not noisy))
        for name in LATENCY_KEYS:
            vb, vc = mb["latencyUs"][name], mc["latencyUs"][name]
            verdict_floor = noise_floor_us
            rows.append(judge(key, f"latencyUs.{name}", vb, vc, threshold,
                              hard=False, noise_floor=verdict_floor))
        for name in HARD_EXTRAS:
            if name in mb.get("extra", {}) and name in mc.get("extra", {}):
                rows.append(judge(key, name, mb["extra"][name], mc["extra"][name],
                                  threshold, hard=True))

    # 不变式：并发写入的平均请求数不得超过 1 次/事务（组提交失效的硬信号）
    for key, m in sorted(c.items()):
        limit = INVARIANTS.get(key) if m["cache"].startswith(CONCURRENT_PREFIX) else None
        if limit is None:
            continue
        value = m["requestsPerOp"]
        rows.append(Row(key[0], key[1], f"invariant {key[1]}<= {limit}", limit, value,
                        0.0 if value <= limit else 100.0 * (value / limit - 1),
                        "ok" if value <= limit else "REGRESSION"))

    # 只在一边出现的负载是结构变化，必须说出来（否则「删掉一个负载」= 静默变绿）
    for key in sorted(set(b) - set(c)):
        rows.append(Row(key[0], key[1], "(workload)", 1.0, 0.0, -100.0, "warn: 本次未测"))
    for key in sorted(set(c) - set(b)):
        rows.append(Row(key[0], key[1], "(workload)", 0.0, 1.0, 0.0, "warn: 基线未测"))
    return rows


def judge(key: tuple[str, str], name: str, base: float, cur: float, threshold: float,
          hard: bool, noise_floor: float = 0.0) -> Row:
    delta = pct_delta(base, cur)
    worse = cur - base
    # 噪声地板只用来忽略**绝对值很小**的抖动；但相对变化极大的情况（例如 12µs → 40µs）
    # 即使绝对值不到地板也必须说出来——「小基数上翻三倍」通常是真的退化了。
    # 硬门禁不受地板影响（它比对的是确定性指标，没有噪声可言）。
    floor_ok = abs(worse) >= noise_floor or delta >= 2 * threshold
    exceeded = delta > threshold and floor_ok and abs(worse) > 1e-9
    if exceeded:
        verdict = "REGRESSION" if hard else "warn"
    else:
        verdict = "ok"
    return Row(key[0], key[1], name, base, cur, delta, verdict)


def markdown(rows: list[Row], baseline: dict[str, Any], current: dict[str, Any]) -> str:
    hard = [r for r in rows if r.verdict == "REGRESSION"]
    warn = [r for r in rows if r.verdict == "warn"]
    out = ["### YangBase 基准对比", ""]
    out.append(f"- 基线：`{baseline['label']}` · {baseline['commit']} · profile `{baseline['profile']}`")
    out.append(f"- 本次：`{current['label']}` · {current['commit']} · profile `{current['profile']}`")
    out.append("")
    if hard:
        out.append(f"**❌ 确定性指标回退 {len(hard)} 项（硬门禁）**")
    elif warn:
        out.append(f"**⚠️ 无硬回退；{len(warn)} 项告警（延迟受机器噪声影响，仅供参考）**")
    else:
        out.append("**✅ 无回退**")
    out.append("")
    out.append("| 负载 | 缓存 | 指标 | 基线 | 本次 | 变化 | 判定 |")
    out.append("|---|---|---|---:|---:|---:|---|")
    for r in rows:
        if r.verdict == "ok":
            continue
        flag = {"REGRESSION": "❌ 回退", "warn": "⚠️"}.get(r.verdict, r.verdict)
        out.append(f"| {r.workload} | {r.cache} | {r.name} | {fmt(r.baseline)} | "
                   f"{fmt(r.current)} | {r.delta_pct:+.1f}% | {flag} |")
    if not hard and not warn:
        out.append("| — | — | — | — | — | — | — |")
    out.append("")
    return "\n".join(out)


def fmt(v: float) -> str:
    if v == int(v) and abs(v) >= 10:
        return f"{int(v)}"
    return f"{v:.3g}"


def main() -> int:
    ap = argparse.ArgumentParser(description="对比两份 YangBase 基准 JSON")
    ap.add_argument("--baseline", required=True)
    ap.add_argument("--current", required=True)
    ap.add_argument("--threshold", type=float, default=5.0, help="回退阈值百分比，默认 5")
    ap.add_argument("--noise-floor-us", type=float, default=200.0,
                    help="延迟告警的绝对噪声地板（微秒），默认 200")
    ap.add_argument("--markdown", help="把对比表写进这个文件（CI 直接用）")
    ap.add_argument("--fail-on-regression", action="store_true", default=True,
                    help="确定性指标回退时非零退出（默认开启；用 --no-fail 关掉）")
    ap.add_argument("--no-fail", dest="fail_on_regression", action="store_false")
    args = ap.parse_args()

    baseline = load(args.baseline)
    current = load(args.current)
    if baseline["profile"] != current["profile"]:
        print(f"⚠️ profile 不同：{baseline['profile']} vs {current['profile']}，对比无意义",
              file=sys.stderr)
        return 2

    rows = compare(baseline, current, args.threshold, args.noise_floor_us)
    md = markdown(rows, baseline, current)
    print(md)
    if args.markdown:
        with open(args.markdown, "w", encoding="utf-8") as f:
            f.write(md + "\n")

    hard = [r for r in rows if r.verdict == "REGRESSION"]
    warn = [r for r in rows if r.verdict == "warn"]
    if hard:
        print(f"\n❌ {len(hard)} 项确定性指标回退超过 {args.threshold}%：", file=sys.stderr)
        for r in hard:
            print(f"  - {r.workload}/{r.cache} {r.name}: {fmt(r.baseline)} → "
                  f"{fmt(r.current)} ({r.delta_pct:+.1f}%)", file=sys.stderr)
        return 1 if args.fail_on_regression else 0
    if warn:
        print(f"\n⚠️ {len(warn)} 项告警（不阻塞合并，请人工确认）", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
