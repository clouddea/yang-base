#!/usr/bin/env bash
# ── 镜像冒烟：把一个镜像真的跑起来，确认它能接 MySQL 握手并回答一条查询 ──────
#
# 用法：scripts/docker-image-smoke.sh <image-ref> [host-port]
#
# 为什么是握手而不是「端口 listen」：
#   宿主机端口在 docker-proxy 起来时就通了，而引擎还要 3-4 秒才 bind。
#   那一刻容器里 `exec 3<>/dev/tcp/127.0.0.1/3306` 仍然是 Connection refused
#   （2026-10-28 实测）。用端口判断就绪，会在真失败时偶尔变绿、在慢机器上无故变红。
#
# 为什么单拉一个脚本：
#   同一段逻辑被两处用——`ci.yml`（对本机构建的镜像）和 `docker.yml`（对推上去的镜像）。
#   复制两份的结局是「CI 验的那份」和「发布时跑的那份」慢慢不是同一份。
set -uo pipefail

image="${1:?用法: $0 <image-ref> [host-port]}"
port="${2:-3307}"
name="yb-smoke-$$"

cleanup() { docker rm -f "$name" >/dev/null 2>&1 || true; }
trap cleanup EXIT

echo "冒烟镜像: $image（宿主机端口 $port）"
docker run -d --name "$name" -p "${port}:3306" "$image" >/dev/null || {
  echo "::error::docker run 失败" >&2
  exit 1
}

ok=""
for i in $(seq 1 60); do
  if mysqladmin ping -h127.0.0.1 -P"$port" -uroot --protocol=tcp >/dev/null 2>&1; then
    echo "MySQL 握手成功（${i}s）"
    ok=1
    break
  fi
  # 容器已经退出就没必要再等 60 秒
  if [ -z "$(docker ps -q --filter "name=^${name}$")" ]; then
    echo "::error::容器在握手成功前已退出" >&2
    break
  fi
  sleep 1
done

if [ -z "$ok" ]; then
  echo "::error::60 秒内没有完成 MySQL 握手" >&2
  docker logs --tail=50 "$name" >&2 || true
  exit 1
fi

# 握手成功还不够：真的发一条查询，证明协议层是通的而不只是 TCP 能连
mysql -h127.0.0.1 -P"$port" -uroot --protocol=tcp -e "SELECT 1 AS ok;"
echo "镜像可用 ✔"
