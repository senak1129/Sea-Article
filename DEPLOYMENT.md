# Sea-TryGo 部署指南

本文档详细说明如何将 Sea-TryGo 微服务项目部署到生产服务器。

## 目录

- [架构概览](#架构概览)
- [环境要求](#环境要求)
- [服务器初始化](#服务器初始化)
- [GitHub Secrets 配置](#github-secrets-配置)
- [CI/CD 流程说明](#cicd-流程说明)
- [手动部署](#手动部署)
- [服务端口一览](#服务端口一览)
- [配置说明](#配置说明)
- [运维操作](#运维操作)
- [常见问题](#常见问题)

---

## 架构概览

```
GitHub Push (main)
      │
      ▼
GitHub Actions ──► Docker Hub (latest + <git-sha>)
      │
      ▼
SSH Deploy ──► 服务器按 <git-sha> pull + up
      │
      ▼
┌─────────────────────────────────────────────────────┐
│  微服务层                                            │
│  article / user / admin / message / security         │
├─────────────────────────────────────────────────────┤
│  基础设施层                                          │
│  etcd · postgres · redis+sentinel · kafka · minio    │
├─────────────────────────────────────────────────────┤
│  可观测层                                            │
│  prometheus · grafana · jaeger                       │
└─────────────────────────────────────────────────────┘
```

### 微服务列表

| 服务 | API 端口 | RPC 端口 | 说明 |
|------|--------|-------|------|
| article | 7777   | 6666  | 文章服务 |
| user | 7776   | 6665  | 用户中心 |
| admin | 7779   | 6669  | 管理后台 |
| message | 7778   | 6667  | 消息服务 |
| security | 无 API  | 6668      | 安全/鉴权（仅 RPC） |

---

## 环境要求

### 服务器

| 项目 | 最低要求 | 推荐 |
|------|---------|------|
| 操作系统 | Ubuntu 20.04 / Debian 11+ | Ubuntu 22.04 LTS |
| CPU | 2 核 | 4 核+ |
| 内存 | 4 GB | 8 GB+ |
| 磁盘 | 40 GB SSD | 100 GB+ SSD |
| Docker | 24.0+ | 最新稳定版 |
| Docker Compose | v2（`docker compose` 命令） | 最新稳定版 |

### 开发环境

- Go 1.25.4+
- Git
- GitHub 账号（用于 CI/CD）

---

## 服务器初始化

### 1. 安装 Docker

```bash
# 使用官方脚本安装
curl -fsSL https://get.docker.com | bash

# 验证安装
docker --version
docker compose version

# 将当前用户加入 docker 组（可选，避免每次 sudo）
sudo usermod -aG docker $USER
newgrp docker
```

### 2. 创建部署目录

```bash
mkdir -p /root/SeaTest
cd /root/SeaTest
```

### 3. 创建环境变量文件

```bash
cat > .env << 'EOF'
# Kafka 集群节点 IP（根据实际服务器 IP 填写）
KAFKA_BROKER1_IP=192.168.1.10
KAFKA_BROKER2_IP=192.168.1.11
KAFKA_BROKER3_IP=192.168.1.12

# 阿里云 DashScope API Key
DASHSCOPE_API_KEY=sk-xxxxxxxxxxxxxxxxxxxxxxxx
EOF
```

### 4. 准备 Redis 配置

项目使用 Redis 一主二从 + Sentinel 架构，需要在 `/root/SeaTest/conf/` 下放置配置文件。

#### conf/redis.conf（Master 节点）

```conf
bind 0.0.0.0
port 6379
requirepass 123456
masterauth 123456

# 持久化
appendonly yes
appendfsync everysec
save 900 1
save 300 10
save 60 10000

# 内存限制
maxmemory 2gb
maxmemory-policy allkeys-lru
```

#### conf/sentinel.conf

```conf
port 26379
sentinel monitor mymaster <master-ip> 6379 2
sentinel auth-pass mymaster 123456
sentinel down-after-milliseconds mymaster 5000
sentinel failover-timeout mymaster 10000
sentinel parallel-syncs mymaster 1
```

> **注意：** 将 `<master-ip>` 替换为 Redis Master 节点的实际 IP。从节点地址由 Sentinel 自动发现，无需手动配置。

#### conf/redis-slave.conf（每个 Slave 节点）

```conf
bind 0.0.0.0
port 6379
requirepass 123456
masterauth 123456
replicaof <master-ip> 6379

appendonly yes
appendfsync everysec
```

### 5. 准备 Prometheus 配置

```bash
mkdir -p /root/SeaTest/prometheus
```

将项目中的 `prometheus/settings.yml` 复制到服务器，或手动创建：

```yaml
global:
  scrape_interval: 15s
  evaluation_interval: 15s

scrape_configs:
  - job_name: "prometheus"
    static_configs:
      - targets: ["prometheus:9090"]

  - job_name: "etcd"
    metrics_path: /metrics
    static_configs:
      - targets: ["etcd:2379"]

  - job_name: "postgres"
    static_configs:
      - targets: ["postgres-exporter:9187"]

  - job_name: "redis"
    static_configs:
      - targets: ["redis-exporter:9121"]

  - job_name: "kafka"
    static_configs:
      - targets: ["kafka-exporter:9308"]

  - job_name: "minio-cluster"
    metrics_path: /minio/v2/metrics/cluster
    static_configs:
      - targets: ["minio:9000"]

  - job_name: "minio-node"
    metrics_path: /minio/v2/metrics/node
    static_configs:
      - targets: ["minio:9000"]

  - job_name: "article-rpc"
    static_configs:
      - targets: ["host.docker.internal:9092"]

  - job_name: "user-rpc"
    static_configs:
      - targets: ["host.docker.internal:9094"]
```

### 6. 创建日志目录

```bash
mkdir -p /root/SeaTest/logs/{article,user,admin,message,security}
```

### 7. 最终目录结构

```
/root/SeaTest/
├── .env                          # 环境变量
├── docker-compose.yaml           # 由 CI/CD 自动覆盖
├── conf/
│   ├── redis.conf                # Redis 主节点配置
│   ├── redis-slave.conf          # Redis 从节点配置
│   └── sentinel.conf             # Sentinel 配置
├── prometheus/
│   └── settings.yml              # Prometheus 抓取配置
└── logs/
    ├── article/
    ├── user/
    ├── admin/
    ├── message/
    └── security/
```

---

## GitHub Secrets 配置

在仓库的 **Settings → Secrets and variables → Actions → New repository secret** 中添加以下 Secrets：

| Secret 名称 | 说明 | 获取方式 |
|---|---|---|
| `DOCKERHUB_USERNAME` | Docker Hub 用户名 | 你的 Docker Hub 账号，如 `senak1129` |
| `DOCKERHUB_TOKEN` | Docker Hub Access Token | Docker Hub → Account Settings → Security → Access Tokens → New Access Token（权限选 Read, Write, Delete） |
| `SERVER_HOST` | 部署服务器 IP 或域名 | 如 `123.45.67.89` |
| `SERVER_USER` | SSH 登录用户名 | 如 `root` |
| `SERVER_SSH_KEY` | SSH 私钥（完整内容） | 在本地生成：`ssh-keygen -t ed25519 -C "deploy"`，然后 `cat ~/.ssh/id_ed25519` 复制完整内容 |

### SSH 密钥生成与配置

```bash
# 1. 在本地生成密钥对
ssh-keygen -t ed25519 -C "github-deploy"

# 2. 将公钥添加到服务器
ssh-copy-id -i ~/.ssh/id_ed25519.pub root@<SERVER_HOST>

# 3. 测试免密登录
ssh -i ~/.ssh/id_ed25519 root@<SERVER_HOST> "echo ok"

# 4. 复制私钥内容，粘贴到 GitHub Secret `SERVER_SSH_KEY`
cat ~/.ssh/id_ed25519
```

> **安全提示：** 专钥专用，建议为 GitHub Actions 单独生成密钥对，不要复用个人密钥。

---

## CI/CD 流程说明

### 触发条件

- 每次 `push` 到 `main` 分支自动触发
- 通过 `concurrency` 保证同一时间只有一个部署任务在执行，避免冲突

### 流程

```
push to main
    │
    ├── Job 1: build-and-push (并行构建 5 个镜像)
    │   ├── article  → latest + <git-sha>
    │   ├── message  → latest + <git-sha>
    │   ├── security → latest + <git-sha>
    │   ├── user     → latest + <git-sha>
    │   └── admin    → latest + <git-sha>
    │
    └── Job 2: deploy (依赖 Job 1 全部完成)
        ├── scp docker-compose.yaml → 服务器 /root/SeaTest/
        └── ssh 执行：
            cd /root/SeaTest
            export *_IMAGE_TAG=<git-sha>
            docker compose pull (失败自动重试)
            docker compose up -d --force-recreate --remove-orphans
            docker image prune -f
```

### Dockerfile 构建参数

每个微服务通过 `build-args` 控制构建行为：

| 参数 | 说明 |
|------|------|
| `SERVICE_NAME` | 服务名（article/message/security/user/admin） |
| `BUILD_API` | 是否编译 API（`1` 或 `0`） |
| `BUILD_RPC` | 是否编译 RPC（`1` 或 `0`） |
| `API_BUILD_PATH` | API 源码路径 |
| `RPC_BUILD_PATH` | RPC 源码路径 |

---

## 手动部署

如果不使用 CI/CD，可以手动构建和部署。

### 1. 克隆代码

```bash
git clone https://github.com/<your-username>/Sea-Article.git
cd Sea-Article
```

### 2. 构建镜像

以 article 服务为例：

```bash
TAG=$(git rev-parse HEAD)
docker build \
  --build-arg SERVICE_NAME=article \
  --build-arg BUILD_API=1 \
  --build-arg BUILD_RPC=1 \
  --build-arg API_BUILD_PATH=./service/article/api/ \
  --build-arg RPC_BUILD_PATH=./service/article/rpc/ \
  -t senak1129/sea-article:latest \
  -t senak1129/sea-article:${TAG} .
```

对其他服务重复此步骤，修改对应的 `SERVICE_NAME` 和路径。

### 3. 推送镜像（可选）

```bash
docker login
docker push senak1129/sea-article:latest
docker push senak1129/sea-article:${TAG}
# ... 其他镜像
```

### 4. 部署

```bash
cp docker-compose.yaml /root/SeaTest/
cd /root/SeaTest
export ARTICLE_IMAGE_TAG=${TAG}
docker compose pull sea-article
docker compose up -d --force-recreate sea-article
```

---

## 服务端口一览

### 基础设施端口（宿主机映射）

| 服务 | 宿主机端口 | 容器端口 | 说明 |
|------|-----------|---------|------|
| etcd | 32379 | 2379 | 服务发现 & 配置中心 |
| PostgreSQL | 35432 | 5432 | 关系型数据库 |
| Redis | 36379 | 6379 | 缓存（主节点） |
| Redis Sentinel | 26379 | 26379 | 哨兵 |
| Redis Insight | 35540 | 5540 | Redis 可视化管理 |
| Kafka | 49092 (host 模式) | 49092 | 消息队列（外部访问） |
| Kafka UI | 38080 | 8080 | Kafka 可视化管理 |
| MinIO API | 39000 | 9000 | 对象存储 API |
| MinIO Console | 39001 | 9001 | 对象存储管理后台 |
| Prometheus | 39090 | 9090 | 监控指标 |
| Grafana | 33000 | 3000 | 监控面板 |
| Jaeger UI | 16686 | 16686 | 链路追踪 |

### 微服务端口（host 网络模式）

| 服务 | API 端口 | RPC 端口 | Prometheus 指标端口 |
|------|---------|---------|-------------------|
| article | 7777 | 6666 | 9091 (API) / 9092 (RPC) |
| user | 7776 | 6665 | 9094 (RPC) / 9095 (API) |
| admin | 7779 | 6669 | 9096 (API) / 9097 (RPC) |
| message | 7778 | 6667 | 16066 (API) / 16067 (RPC) |
| security | 无 API | 6668 | 9093 (RPC) |

---

## 配置说明

### 环境变量注入机制

微服务容器启动时，`docker-entrypoint.sh` 会根据 `SERVICE_NAME` 选择对应的配置文件模板，然后通过 `sed` 将环境变量中的地址注入到配置文件中。

支持的环境变量：

| 环境变量 | 默认值 | 说明 |
|---------|-------|------|
| `SERVICE_NAME` | `unknown` | 服务名 |
| `API_ENABLED` | `0` | 是否启动 API |
| `RPC_ENABLED` | `0` | 是否启动 RPC |
| `ETCD_ADDR` | `127.0.0.1:32379` | etcd 地址 |
| `REDIS_ADDR` | `127.0.0.1:36379` | Redis 地址 |
| `POSTGRES_ADDR` | `127.0.0.1` | PostgreSQL 地址 |
| `POSTGRES_PORT` | `35432` | PostgreSQL 端口 |
| `KAFKA_ADDR` | `127.0.0.1:49092` | Kafka 地址 |
| `MINIO_ADDR` | `127.0.0.1:39000` | MinIO 地址 |
| `OTEL_ADDR` | `127.0.0.1:34317` | OpenTelemetry Collector 地址 |

### 为什么 Kafka 需要 IP 而 Redis 不需要？

| | Kafka (KRaft) | Redis (Sentinel) |
|---|---|---|
| 节点发现 | **静态配置**，`KAFKA_CONTROLLER_QUORUM_VOTERS` 必须写死所有节点 IP | **动态发现**，Sentinel 通过 Pub/Sub 自动感知主从拓扑 |
| 配置位置 | `.env` 文件中的 `KAFKA_BROKER*_IP` | `conf/sentinel.conf` 中的 `sentinel monitor` |

---

## 运维操作

### 查看服务状态

```bash
cd /root/SeaTest
docker compose ps
```

### 查看日志

```bash
# 查看某个服务的日志
docker compose logs -f sea-article

# 查看宿主机上的日志文件
tail -f /root/SeaTest/logs/article/*.log
```

### 重启单个服务

```bash
docker compose restart sea-article
```

### 更新服务（仅拉取新镜像）

```bash
export ARTICLE_IMAGE_TAG=<git-sha>
docker compose pull sea-article
docker compose up -d --force-recreate sea-article
```

### 清理悬空镜像

```bash
docker image prune -f
```

### 进入容器调试

```bash
docker exec -it sea-article bash
```

---

## 常见问题

### Q: GitHub Actions 构建失败，提示 Docker Hub 登录失败

**A:** 检查 `DOCKERHUB_USERNAME` 和 `DOCKERHUB_TOKEN` 是否正确。Token 需要在 Docker Hub 的 Access Tokens 中生成，不是账号密码。

### Q: 部署后服务无法连接 Kafka

**A:** 确认 `.env` 文件中的 `KAFKA_BROKER*_IP` 是服务器的**内网 IP**（不是 `127.0.0.1`）。Kafka 使用 host 网络模式，其他容器通过宿主机 IP 访问。

### Q: Redis Sentinel 无法发现 Master

**A:** 检查 `conf/sentinel.conf` 中的 `sentinel monitor mymaster <master-ip> 6379 2` 是否指向正确的 Master IP。确认 Redis 主节点已启动且密码正确。

### Q: 容器启动后立即退出

**A:** 查看容器日志排查原因：

```bash
docker compose logs sea-article
```

常见原因：
- 配置文件中的地址无法连通
- 依赖服务（etcd/postgres/redis）未就绪
- 端口冲突

### Q: 如何回滚到上一个版本？

**A:** 现在优先使用 `git sha` 或 digest 回滚，而不是依赖 `latest`。

```bash
export ARTICLE_IMAGE_TAG=<previous-git-sha>
docker compose pull sea-article
docker compose up -d --force-recreate sea-article
```

如果需要更强的不可变保证，也可以直接使用镜像 digest 进行回滚。

### Q: 如何添加新的微服务？

**A:**

1. 在 `service/` 下创建服务代码
2. 在 `.github/workflows/deploy.yml` 的 `matrix.include` 中添加新服务条目
3. 在 `docker-compose.yaml` 中添加新服务的容器定义
4. 提交到 `main` 分支，CI/CD 会自动构建和部署
