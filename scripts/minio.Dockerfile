# MinIO 的容器镜像，**完全离线构建**。
#
# 为什么不用官方的 `minio/minio` 镜像：
#   1. 本机 Docker daemon 被配置为走本地代理（见 /lib/systemd/system/docker.service
#      里的 HTTP_PROXY=http://127.0.0.1:7890），而该代理当前不可用，
#      导致 `docker pull` 报 EOF——registry 本身是可达的（直连返回 401）。
#   2. 就算代理正常，MinIO 官方镜像也在 docker.io 上，拉取要走网络。
#
# 而 MinIO 是**静态链接的 Go 二进制**（`file` 显示 statically linked），
# 因此 `FROM scratch` + COPY 就能得到一个完整可用的镜像，不需要任何基础镜像。
#
# 构建：
#   docker build -f scripts/minio.Dockerfile -t yangbase/minio:local <含 minio 二进制的目录>
# 或直接用：./scripts/minio.sh start docker
FROM scratch

COPY minio /usr/bin/minio

EXPOSE 9000 9001
VOLUME /data

ENTRYPOINT ["/usr/bin/minio"]
CMD ["server", "/data", "--address", ":9000", "--console-address", ":9001"]
