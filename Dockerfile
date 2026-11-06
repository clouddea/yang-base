# syntax=docker/dockerfile:1.7
#
# YangBase 的运行镜像。
#
# 两个阶段：JDK 里构建 → JRE 里运行。运行镜像里**没有**构建工具、没有源码、
# 没有 root 权限，只有 `installDist` 产出的启动脚本与 jar。
#
# 用法：
#   docker build -t yangbase:dev .
#   docker run --rm -p 3306:3306 yangbase:dev                      # 本地文件后端，仅演示
#   docker run --rm -p 3306:3306 -e ... yangbase:dev --s3-bucket ...  # 生产形态
#
# ⚠️ 默认 CMD 走的是**本地文件后端**（带 `--i-know-this-is-not-durable`）。
# 那条路只适合「60 秒看看它能不能跑」：本地盘没有多副本、没有跨进程条件写，
# 而写者选举 / manifest 指针切换 / fencing 都依赖对象存储语义。
# 真要用请按 docker-compose.yml 那样接对象存储。

ARG JDK_IMAGE=eclipse-temurin:21-jdk-jammy
ARG JRE_IMAGE=eclipse-temurin:21-jre-jammy

# ── 构建阶段 ────────────────────────────────────────────────────────────────
FROM ${JDK_IMAGE} AS build
WORKDIR /src

# 先只拷构建描述与各模块的 build 脚本：这一层的缓存能跨源码改动存活，
# 依赖解析不必每次重来。
COPY gradle ./gradle
COPY gradlew settings.gradle.kts build.gradle.kts gradle.properties version.txt ./
COPY yb-common/build.gradle.kts ./yb-common/
COPY yb-objectstore/build.gradle.kts ./yb-objectstore/
COPY yb-storage/build.gradle.kts ./yb-storage/
COPY yb-txn/build.gradle.kts ./yb-txn/
COPY yb-ms/build.gradle.kts ./yb-ms/
COPY yb-index/build.gradle.kts ./yb-index/
COPY yb-sql/build.gradle.kts ./yb-sql/
COPY yb-mysql/build.gradle.kts ./yb-mysql/
COPY yb-server/build.gradle.kts ./yb-server/
COPY yb-bench/build.gradle.kts ./yb-bench/
COPY yb-cli/build.gradle.kts ./yb-cli/

# 下载依赖（空跑一次解析，把依赖钉进这一层缓存里）
#
# 刻意**不用** BuildKit 的 cache mount（`--mount=type=cache,target=/root/.gradle`）：
# 实测在容器里会撞上 Gradle 的 journal 锁竞争（"Timeout waiting to lock journal cache"，
# `--no-daemon` 仍然会起一个单次 daemon 进程，两个 JVM 抢同一个 journal 锁），
# 而镜像层本身已经提供了跨构建的缓存。
RUN ./gradlew --no-daemon :yb-server:dependencies :yb-cli:dependencies > /dev/null

COPY . .

# `-x test`：测试在 CI 的 build job 里跑（覆盖率门禁也在那里），
# 镜像构建不是第二套测试入口——它只负责打包。
RUN ./gradlew --no-daemon -x test :yb-server:installDist :yb-cli:installDist

# ── 运行阶段 ────────────────────────────────────────────────────────────────
FROM ${JRE_IMAGE}

ARG YB_VERSION=0.0.0-dev
LABEL org.opencontainers.image.title="YangBase" \
      org.opencontainers.image.description="Transactional database built entirely on object storage (MySQL protocol)" \
      org.opencontainers.image.version="${YB_VERSION}" \
      org.opencontainers.image.licenses="Apache-2.0" \
      org.opencontainers.image.source="https://github.com/clouddea/yang-base"

# 非 root 运行。UID/GID 固定成 10001：这样挂载的数据卷权限在宿主机上也可预期，
# 而不是每次重建镜像都换一个 uid。
RUN groupadd -g 10001 yangbase \
 && useradd -u 10001 -g yangbase -m -s /usr/sbin/nologin yangbase \
 && mkdir -p /var/lib/yangbase /var/cache/yangbase \
 && chown -R yangbase:yangbase /var/lib/yangbase /var/cache/yangbase

COPY --from=build --chown=10001:10001 /src/yb-server/build/install/yangbase-server /opt/yangbase
COPY --from=build --chown=10001:10001 /src/yb-cli/build/install/yb-cli /opt/yb-cli

ENV PATH="/opt/yangbase/bin:/opt/yb-cli/bin:${PATH}" \
    JAVA_OPTS="-XX:MaxRAMPercentage=75 -XX:+ExitOnOutOfMemoryError"

USER 10001:10001
WORKDIR /var/lib/yangbase

EXPOSE 3306
# 观测端点默认关闭；需要 /metrics 时加 --metrics-port 9104 并 EXPOSE 它
VOLUME ["/var/lib/yangbase", "/var/cache/yangbase"]

# 就绪判据是**能接 MySQL 连接**（3306 可连通），不是「进程还活着」：
# 进程活着但握手失败，对客户端来说等于没起来。
HEALTHCHECK --interval=10s --timeout=3s --start-period=20s --retries=12 \
  CMD bash -c 'exec 3<>/dev/tcp/127.0.0.1/${YB_HEALTH_PORT:-3306}' || exit 1

ENTRYPOINT ["/opt/yangbase/bin/yangbase-server"]
CMD ["--port", "3306", "--data-dir", "/var/lib/yangbase", "--i-know-this-is-not-durable"]
