# syntax=docker/dockerfile:1
FROM golang:1.25-bookworm AS builder

WORKDIR /src

# 代理配置
ARG HTTP_PROXY
ARG HTTPS_PROXY
ARG NO_PROXY
ENV GOPROXY=https://goproxy.cn,direct
ENV GOCACHE=/root/.cache/go-build

# 安装编译依赖
RUN apt-get update && apt-get install -y --no-install-recommends \
    bash ca-certificates tzdata \
    && rm -rf /var/lib/apt/lists/*

# 复制依赖文件
COPY go.mod go.sum ./
RUN go mod download

# 复制全部源代码
COPY . .

# 编译参数
ARG SERVICE_NAME="article"
ARG API_BUILD_PATH=""
ARG RPC_BUILD_PATH=""
ARG BUILD_API="1"
ARG BUILD_RPC="1"

# 编译 API
RUN set -eux; \
    api_build_path="${API_BUILD_PATH:-}"; \
    api_build_path="${api_build_path#./}"; \
    api_build_path="${api_build_path%/}"; \
    if [ "${BUILD_API}" = "1" ] && [ -n "${api_build_path}" ] && [ "${api_build_path}" != "\"\"" ]; then \
        api_dir="/src/${api_build_path}"; \
        echo "Building API from: ${api_dir}"; \
        if [ ! -d "${api_dir}" ]; then \
            echo "API build path not found: ${api_dir} (API_BUILD_PATH=${API_BUILD_PATH})" >&2; \
            ls -la /src >&2; \
            exit 1; \
        fi; \
        mkdir -p /out/bin /out/etc/api; \
        cd "${api_dir}" && \
        go build -trimpath -ldflags='-s -w' -o /out/bin/api . && \
        cd /src && \
        if [ -d "${api_dir}/etc" ]; then cp -r "${api_dir}/etc/"* /out/etc/api/ 2>/dev/null || true; fi; \
    else \
        echo "API build skipped (path is empty or BUILD_API != 1)"; \
    fi

# 编译 RPC
RUN set -eux; \
    rpc_build_path="${RPC_BUILD_PATH:-}"; \
    rpc_build_path="${rpc_build_path#./}"; \
    rpc_build_path="${rpc_build_path%/}"; \
    if [ "${BUILD_RPC}" = "1" ] && [ -n "${rpc_build_path}" ] && [ "${rpc_build_path}" != "\"\"" ]; then \
        rpc_dir="/src/${rpc_build_path}"; \
        echo "Building RPC from: ${rpc_dir}"; \
        if [ ! -d "${rpc_dir}" ]; then \
            echo "RPC build path not found: ${rpc_dir} (RPC_BUILD_PATH=${RPC_BUILD_PATH})" >&2; \
            ls -la /src >&2; \
            exit 1; \
        fi; \
        mkdir -p /out/bin /out/etc/rpc; \
        cd "${rpc_dir}" && \
        go build -trimpath -ldflags='-s -w' -o /out/bin/rpc . && \
        cd /src && \
        if [ -d "${rpc_dir}/etc" ]; then cp -r "${rpc_dir}/etc/"* /out/etc/rpc/ 2>/dev/null || true; fi; \
    else \
        echo "RPC build skipped (path is empty or BUILD_RPC != 1)"; \
    fi

# 运行阶段
FROM debian:bookworm-slim

WORKDIR /app

# 必须重新声明 ARG，以便在第二阶段使用
ARG SERVICE_NAME

ENV DEBIAN_FRONTEND=noninteractive
ENV TZ=Asia/Shanghai
ENV SERVICE_NAME=${SERVICE_NAME}

# 安装运行时依赖
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates tzdata bash \
    && rm -rf /var/lib/apt/lists/*

# 复制编译产物和配置文件
COPY --from=builder /out/bin /app/bin
COPY --from=builder /out/etc /app/etc

# 复制入口脚本
COPY docker-entrypoint.sh /app/
RUN chmod +x /app/bin/api /app/bin/rpc /app/docker-entrypoint.sh 2>/dev/null || true

# 创建日志目录
RUN mkdir -p /app/log

ENTRYPOINT ["/app/docker-entrypoint.sh"]
