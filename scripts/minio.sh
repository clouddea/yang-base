#!/usr/bin/env bash
#
# 启动/停止一个本地 MinIO，用于把集成测试跑在**真实对象存储**上。
#
# 为什么不用 Docker：本机当前用户不在 docker 组（需要 `sudo usermod -aG docker $USER`
# 并重新登录），而 MinIO 的单个静态二进制不需要任何权限就能跑。两者效果一样。
#
# ⚠️ 下载 MinIO 的坑：`dl.min.io/aistor/...` 给的是 **AIStor 商业版**，无 license 时
# 拒绝所有 S3 操作（"All S3 operations are denied"）。开源版在 GitHub Releases：
#   https://github.com/minio/minio/releases/download/RELEASE.2025-09-07T16-13-09Z/minio.linux-amd64.RELEASE.2025-09-07T16-13-09Z
# 判别：启动日志里出现 `License: GNU AGPLv3` 才是开源版。
#
# 两种运行模式：
#   二进制（默认）——单个静态二进制，零依赖，不需要任何权限
#   容器（docker）——用 scripts/minio.Dockerfile 离线构建的镜像（见该文件的注释：
#                     本机 Docker 被配了走代理而代理不可用，所以不拉官方镜像）
#
# 用法：
#   ./scripts/minio.sh start          启动（二进制，后台）
#   ./scripts/minio.sh start docker   启动（容器）
#   ./scripts/minio.sh stop           停止
#   ./scripts/minio.sh status         查看状态
#   ./scripts/minio.sh clean          清空数据目录（数据损坏或权限错乱时用）
#   ./scripts/minio.sh test           启动 + 把集成测试指向它并运行（:yb-server 与 :yb-ms）
#   ./scripts/minio.sh bench          启动 + 跑基准测试（真实对象存储的数字）
#
set -euo pipefail

MINIO_BIN="${MINIO_BIN:-$HOME/app/dl/minio}"
# 二进制与容器**必须用不同的数据目录**：scratch 镜像以 root 运行，
# 它写的文件归 root 所有，共用目录会让另一种模式连读都读不了（踩过的坑）。
BINARY_DATA="${MINIO_DATA:-$HOME/.yangbase/minio-data}"
DOCKER_DATA="${MINIO_DOCKER_DATA:-$HOME/.yangbase/minio-docker-data}"
DATA_DIR="$BINARY_DATA"
CONSOLE_DIR="${MINIO_CONSOLE:-$HOME/.yangbase/minio-console}"
LOG_FILE="${MINIO_LOG:-$HOME/.yangbase/minio.log}"
PID_FILE="${MINIO_PID:-$HOME/.yangbase/minio.pid}"

API_PORT="${MINIO_API_PORT:-9000}"
CONSOLE_PORT="${MINIO_CONSOLE_PORT:-9001}"
ROOT_USER="${MINIO_ROOT_USER:-yangbase}"
ROOT_PASS="${MINIO_ROOT_PASSWORD:-yangbase123}"
BUCKET="${MINIO_BUCKET:-yangbase}"

export MINIO_ROOT_USER="$ROOT_USER"
export MINIO_ROOT_PASSWORD="$ROOT_PASS"

MODE="binary"
IMAGE="yangbase/minio:local"
CONTAINER="yangbase-minio"

die() { echo "错误: $*" >&2; exit 1; }

# 当前 shell 若未激活 docker 组，用 sg 临时激活（usermod 只对新登录会话生效）
docker_do() {
  if docker info >/dev/null 2>&1; then docker "$@"; else sg docker -c "docker $(printf '%q ' "$@")"; fi
}

# 用静态二进制离线构建镜像。不拉任何基础镜像。
build_image() {
  if docker_do image inspect "$IMAGE" >/dev/null 2>&1; then return 0; fi
  [ -x "$MINIO_BIN" ] || die "构建镜像需要 MinIO 二进制：$MINIO_BIN"
  local ctx
  ctx="$(mktemp -d)"
  cp "$MINIO_BIN" "$ctx/minio"
  cp "$(dirname "$0")/minio.Dockerfile" "$ctx/Dockerfile"
  echo "离线构建镜像 $IMAGE（FROM scratch，不拉取任何基础镜像）…"
  docker_do build -t "$IMAGE" "$ctx" >/dev/null
  rm -rf "$ctx"
}

cmd_start_docker() {
  build_image
  if docker_do ps --filter "name=^${CONTAINER}$" --format '{{.Names}}' | grep -q "$CONTAINER"; then
    echo "容器 $CONTAINER 已在运行"; return 0
  fi
  mkdir -p "$DOCKER_DATA"
  echo "  数据目录: $DOCKER_DATA"
  docker_do rm -f "$CONTAINER" >/dev/null 2>&1 || true
  # --user 让容器以当前用户运行：scratch 镜像默认是 root，
  # 它写出来的数据文件会归 root 所有，宿主机的另一种模式连读都读不了。
  docker_do run -d --name "$CONTAINER" \
    --user "$(id -u):$(id -g)" \
    -p "$API_PORT:9000" -p "$CONSOLE_PORT:9001" \
    -e MINIO_ROOT_USER="$ROOT_USER" -e MINIO_ROOT_PASSWORD="$ROOT_PASS" \
    -v "$DOCKER_DATA:/data" "$IMAGE" >/dev/null
  for _ in $(seq 1 40); do
    curl -sf -o /dev/null "http://127.0.0.1:$API_PORT/minio/health/live" && { echo "容器已就绪"; return 0; }
    sleep 0.5
  done
  die "容器启动超时。日志: docker logs $CONTAINER"
}

cmd_stop_docker() {
  if docker_do ps -a --filter "name=^${CONTAINER}$" --format '{{.Names}}' | grep -q "$CONTAINER"; then
    docker_do rm -f "$CONTAINER" >/dev/null && echo "容器已移除"
  else
    echo "容器未在运行"
  fi
}

cmd_start() {
  if [ "$MODE" = "docker" ]; then cmd_start_docker; return; fi
  [ -x "$MINIO_BIN" ] || die "找不到可执行的 MinIO: $MINIO_BIN（先下载，见 README）"
  if is_running; then echo "MinIO 已在运行 (pid $(cat "$PID_FILE"))"; return 0; fi

  mkdir -p "$DATA_DIR" "$CONSOLE_DIR"
  echo "启动 MinIO…"
  echo "  数据目录: $DATA_DIR"
  echo "  API:      http://127.0.0.1:$API_PORT"
  echo "  控制台:   http://127.0.0.1:$CONSOLE_PORT   ($ROOT_USER / $ROOT_PASS)"
  nohup "$MINIO_BIN" server "$DATA_DIR" \
    --address ":$API_PORT" \
    --console-address ":$CONSOLE_PORT" \
    >"$LOG_FILE" 2>&1 &
  echo $! > "$PID_FILE"

  # 等待就绪：MinIO 起来需要一两秒
  for _ in $(seq 1 40); do
    if curl -sf -o /dev/null "http://127.0.0.1:$API_PORT/minio/health/live"; then
      echo "MinIO 已就绪"
      return 0
    fi
    sleep 0.5
  done
  die "MinIO 启动超时，日志见 $LOG_FILE"
}

is_running() {
  [ -f "$PID_FILE" ] && kill -0 "$(cat "$PID_FILE")" 2>/dev/null
}

cmd_stop() {
  if [ "$MODE" = "docker" ] || docker_do ps --filter "name=^${CONTAINER}$" --format '{{.Names}}' 2>/dev/null | grep -q "$CONTAINER"; then
    cmd_stop_docker
    return
  fi
  if is_running; then
    kill "$(cat "$PID_FILE")" && rm -f "$PID_FILE"
    echo "MinIO 已停止"
  else
    echo "MinIO 未在运行"
  fi
}

# 清空数据目录。用于数据损坏、或两种模式的目录权限错乱（容器曾以 root 运行过）。
cmd_clean() {
  cmd_stop >/dev/null 2>&1 || true
  rm -rf "$BINARY_DATA" 2>/dev/null || true
  if [ -d "$BINARY_DATA" ]; then
    # 属 root 的残骸（容器早期版本以 root 写过）需要借一个带 shell 的镜像删除
    if docker_do image inspect redis:latest >/dev/null 2>&1; then
      docker_do run --rm --entrypoint sh -v "$(dirname "$BINARY_DATA"):/w" redis:latest \
        -c "rm -rf /w/$(basename "$BINARY_DATA")" >/dev/null 2>&1 || true
    else
      echo "提示: $BINARY_DATA 有 root 所有的文件，需要 sudo rm -rf 或装个带 shell 的镜像"
    fi
  fi
  rm -rf "$DOCKER_DATA" 2>/dev/null || true
  echo "数据目录已清空"
}

cmd_status() {
  # 两种模式都要检查：原先只看二进制的 PID 文件，
  # 导致容器在跑时也报「未运行」（踩过的坑）。
  local mode="" detail=""
  if docker_do ps --filter "name=^${CONTAINER}$" --format '{{.Names}}' 2>/dev/null | grep -q "$CONTAINER"; then
    mode="容器"
    detail="$(docker_do ps --filter "name=^${CONTAINER}$" --format '{{.Status}}' 2>/dev/null)"
  elif is_running; then
    mode="二进制"
    detail="pid $(cat "$PID_FILE")"
  fi

  if [ -z "$mode" ]; then
    echo "未运行"
    return
  fi
  echo "运行中（$mode）  $detail"
  if curl -sf -o /dev/null "http://127.0.0.1:$API_PORT/minio/health/live"; then
    echo "健康检查: OK   http://127.0.0.1:$API_PORT"
    echo "控制台:   http://127.0.0.1:$CONSOLE_PORT  ($ROOT_USER / $ROOT_PASS)"
  else
    echo "健康检查: 失败 —— 端口 $API_PORT 没有响应"
  fi
}

# 建桶由测试自己通过 SDK 完成（S3ObjectStore.ensureBucket），这里只做提示。
# 不用 curl 裸 PUT：MinIO 要求 SigV4 签名，未签名请求会被拒为 AccessDenied。
cmd_bucket() {
  echo "bucket '$BUCKET' 会由集成测试在 setUp 阶段通过 SDK 自动创建（ensureBucket）。"
  echo "手动查看：浏览器打开 http://127.0.0.1:$CONSOLE_PORT （$ROOT_USER / $ROOT_PASS）"
}

cmd_test() {
  cmd_start
  echo
  # ⚠️ 必须用 -P 属性而不是环境变量：Gradle 守护进程长期存活，
  # shell 里 export 的变量传导不到它启动的测试 JVM（踩过的坑）。
  echo "把集成测试指向 MinIO 并运行…"
  echo
  # --no-build-cache 是必需的：外部对象存储不是 Gradle 的受跟踪输入，
  # 缓存命中会让「打真实 MinIO 的测试」被上一次的结果顶替掉（踩过的坑）。
  # :yb-ms:integrationTest 也要跑（S3BackedMsTest）——它是「MS 的日志真的落在
  # 对象存储上」的那一个：内存 store 的 PUT 是纳秒级的，共识的活性问题只在
  # 毫秒级的真实后端上才出现（见 PROJECT_GUIDE 的测试基建一节）。
  "$(dirname "$0")/../gradlew" \
    :yb-server:cleanIntegrationTest :yb-server:integrationTest \
    :yb-ms:integrationTest --console=plain --no-build-cache \
    -PYB_S3_ENDPOINT="http://127.0.0.1:$API_PORT" \
    -PYB_S3_BUCKET="$BUCKET" \
    -PYB_S3_ACCESS_KEY="$ROOT_USER" \
    -PYB_S3_SECRET_KEY="$ROOT_PASS"
}

if [ "${2:-}" = "docker" ]; then MODE="docker"; fi

case "${1:-}" in
  start)  cmd_start ;;
  stop)   cmd_stop ;;
  status) cmd_status ;;
  clean)  cmd_clean ;;
  bucket) cmd_bucket ;;
  test)   cmd_test ;;
  bench)
    cmd_start
    echo "跑基准测试（真实对象存储）…"
    "$(dirname "$0")/../gradlew" :yb-bench:bench --console=plain -q --no-build-cache \
      -PYB_S3_ENDPOINT="http://127.0.0.1:$API_PORT" \
      -PYB_S3_BUCKET="yangbase-bench" \
      -PYB_S3_ACCESS_KEY="$ROOT_USER" \
      -PYB_S3_SECRET_KEY="$ROOT_PASS"
    ;;
  *) echo "用法: $0 {start|stop|status|clean|test|bench} [docker]"; exit 1 ;;
esac
