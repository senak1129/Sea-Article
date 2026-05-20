#!/usr/bin/env bash
set -euo pipefail

SERVICE_NAME="${SERVICE_NAME:-unknown}"
API_ENABLED="${API_ENABLED:-0}"
RPC_ENABLED="${RPC_ENABLED:-0}"
API_BIN="${API_BIN:-/app/bin/api}"
RPC_BIN="${RPC_BIN:-/app/bin/rpc}"

API_CONFIG_SRC="/app/etc/api/${SERVICE_NAME}-api.yaml"
RPC_CONFIG_SRC="/app/etc/rpc/${SERVICE_NAME}.yaml"

if [ "$SERVICE_NAME" = "user" ]; then
    API_CONFIG_SRC="/app/etc/api/usercenter.yaml"
    RPC_CONFIG_SRC="/app/etc/rpc/user.yaml"
elif [ "$SERVICE_NAME" = "admin" ]; then
    API_CONFIG_SRC="/app/etc/api/admincenter.yaml"
    RPC_CONFIG_SRC="/app/etc/rpc/admin.yaml"
elif [ "$SERVICE_NAME" = "message" ]; then
    API_CONFIG_SRC="/app/etc/api/messagecenter.yaml"
    RPC_CONFIG_SRC="/app/etc/rpc/message.yaml"
elif [ "$SERVICE_NAME" = "article" ]; then
    API_CONFIG_SRC="/app/etc/api/article-api.yaml"
    RPC_CONFIG_SRC="/app/etc/rpc/article.yaml"
elif [ "$SERVICE_NAME" = "security" ]; then
    RPC_CONFIG_SRC="/app/etc/rpc/security.yaml"
fi

ETCD_ADDR="${ETCD_ADDR:-127.0.0.1:32379}"
REDIS_ADDR="${REDIS_ADDR:-127.0.0.1:36379}"
POSTGRES_ADDR="${POSTGRES_ADDR:-127.0.0.1}"
POSTGRES_PORT="${POSTGRES_PORT:-35432}"
KAFKA_ADDR="${KAFKA_ADDR:-127.0.0.1:49092}"
MINIO_ADDR="${MINIO_ADDR:-127.0.0.1:39000}"
OTEL_ADDR="${OTEL_ADDR:-127.0.0.1:34317}"

mkdir -p /tmp/configs
mkdir -p /app/log

# --------------------------
# 安全替换，不破坏 YAML 格式
# --------------------------
patch_config() {
    local src="$1"
    local dst="$2"
    cp "$src" "$dst"

    # 只替换地址，不修改任何 YAML 结构
    sed -i "s|127.0.0.1:32379|${ETCD_ADDR}|g" "$dst"
    sed -i "s|127.0.0.1:36379|${REDIS_ADDR}|g" "$dst"
    sed -i "s|127.0.0.1:49092|${KAFKA_ADDR}|g" "$dst"
    sed -i "s|127.0.0.1:39000|${MINIO_ADDR}|g" "$dst"
    sed -i "s|localhost:34317|${OTEL_ADDR}|g" "$dst"
    sed -i "s|127.0.0.1:34317|${OTEL_ADDR}|g" "$dst"

    # --------------------------
    # 只替换 PostgreSQL 连接信息，不碰 Host/Port 行
    # --------------------------
    sed -i "s|host=127.0.0.1|host=${POSTGRES_ADDR}|g" "$dst"
    sed -i "s|port=35432|port=${POSTGRES_PORT}|g" "$dst"
    sed -i "s|127.0.0.1:35432|${POSTGRES_ADDR}:${POSTGRES_PORT}|g" "$dst"
}

# 启动前清理残留进程，解决端口占用
pkill -f "$API_BIN" 2>/dev/null || true
pkill -f "$RPC_BIN" 2>/dev/null || true
sleep 1

echo "Starting service: ${SERVICE_NAME}"
echo "API_ENABLED: ${API_ENABLED}, RPC_ENABLED: ${RPC_ENABLED}"

api_pid=""
rpc_pid=""

stop_all() {
    [ -n "$api_pid" ] && kill "$api_pid" 2>/dev/null
    [ -n "$rpc_pid" ] && kill "$rpc_pid" 2>/dev/null
    wait
    exit 0
}

trap stop_all SIGTERM SIGINT

# 启动 RPC
if [ "${RPC_ENABLED}" = "1" ]; then
    target="/tmp/configs/${SERVICE_NAME}-rpc.yaml"
    patch_config "$RPC_CONFIG_SRC" "$target"
    echo "Starting RPC: ${RPC_BIN} -f $target"
    $RPC_BIN -f "$target" &
    rpc_pid=$!
fi

# 启动 API
if [ "${API_ENABLED}" = "1" ]; then
    target="/tmp/configs/${SERVICE_NAME}-api.yaml"
    patch_config "$API_CONFIG_SRC" "$target"
    echo "Starting API: ${API_BIN} -f $target"
    $API_BIN -f "$target" &
    api_pid=$!
fi

wait