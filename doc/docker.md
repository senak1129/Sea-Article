# Sea-Article Docker Compose 组件说明

## 可视化面板入口

| 面板 | 地址 | 账号/密码 | 用途 |
|---|---|---|---|
| **Grafana** | http://localhost:33000 | `admin / admin` | 指标可视化大盘 |
| **Prometheus** | http://localhost:39090 | 无 | 指标查询与告警排查 |
| **Jaeger UI** | http://localhost:16686 | 无 | 分布式链路追踪 |
| **Kafka UI** | http://localhost:38080 | 无 | Kafka 集群管理、Topic/消息查看 |
| **RedisInsight** | http://localhost:35540 | 无（首次引导创建） | Redis 可视化管理、键浏览、慢查询 |
| **MinIO Console** | http://localhost:39001 | `minioadmin / minioadmin` | 对象存储管理 |

---

## 组件清单

### 1) etcd（服务发现 / 配置中心）
- **作用**：强一致分布式 KV 存储，用于服务发现与配置管理
- **端口**：`32379 -> 2379`

### 2) PostgreSQL（关系型数据库）
- **作用**：文章数据等结构化业务存储
- **端口**：`35432 -> 5432`
- **账号**：`admin` / `Sea-TryGo` / 数据库 `first_db`

### 3) Redis（缓存 / 键值存储）
- **作用**：缓存、分布式锁、计数器、会话管理
- **端口**：`36379 -> 6379`
- **配置**：AOF 持久化（`--appendonly yes`）

### 4) Redis Sentinel（Redis 哨兵）
- **作用**：Redis 高可用，自动故障转移与主从切换
- **端口**：`26379 -> 26379`

### 5) Kafka（消息队列）
- **作用**：事件总线、异步解耦、消息缓冲
- **端口**：`19092`（PLAINTEXT）、`49092`（EXTERNAL）
- **模式**：KRaft（`broker,controller`），host 网络模式

### 6) MinIO（对象存储）
- **作用**：S3 兼容对象存储，保存图片、文件等
- **端口**：`39000 -> 9000`（S3 API），`39001 -> 9001`（Console）
- **账号**：`minioadmin / minioadmin`

---

## 可视化管理组件

### 7) Kafka UI（Kafka 可视化管理）
- **作用**：查看 Topic、消息、Consumer Group、Offset 等
- **端口**：`38080 -> 8080`

### 8) RedisInsight（Redis 可视化管理）
- **作用**：Redis 官方 GUI，键空间浏览、命令执行、慢查询、监控
- **端口**：`35540 -> 5540`

---

## 可观测性组件

### 9) Prometheus（指标采集）
- **作用**：抓取各组件 exporter 指标，提供 PromQL 查询
- **端口**：`39090 -> 9090`

### 10) Grafana（指标可视化）
- **作用**：基于 Prometheus 构建监控大盘
- **端口**：`33000 -> 3000`
- **账号**：`admin / admin`

### 11) Jaeger（分布式追踪）
- **作用**：采集与查询 trace/span，服务依赖图分析
- **端口**：
  - `16686 -> 16686`：Jaeger UI
  - `34317 -> 4317`：OTLP gRPC
  - `34318 -> 4318`：OTLP HTTP
  - `14268 -> 14268`：Collector HTTP
  - `14250 -> 14250`：Collector gRPC
  - `36831 -> 6831/udp`：Agent thrift compact
  - `36832 -> 6832/udp`：Agent thrift binary

---

## Exporters（Prometheus 指标导出）

以下 exporter 不对外暴露端口，由 Prometheus 在 Docker 网络内直接抓取。

| Exporter | 作用 | 内部端口 |
|---|---|---|
| postgres-exporter | PostgreSQL 指标导出 | 9187 |
| redis-exporter | Redis 指标导出 | 9121 |
| kafka-exporter | Kafka 指标导出 | 9308 |

---

## 端口速查表

| 宿主机端口 | 组件 | 用途 |
|---:|---|---|
| 14250 | jaeger | Collector gRPC |
| 14268 | jaeger | Collector HTTP |
| 16686 | jaeger | Jaeger UI |
| 19092 | kafka | Kafka PLAINTEXT |
| 26379 | redis-sentinel | Sentinel 端口 |
| 32379 | etcd | Client API |
| 33000 | grafana | Grafana Web UI |
| 34317 | jaeger | OTLP gRPC |
| 34318 | jaeger | OTLP HTTP |
| 35432 | postgres | PostgreSQL 连接 |
| 35540 | redisinsight | RedisInsight Web UI |
| 36379 | redis | Redis 连接 |
| 36831/udp | jaeger | Agent thrift compact |
| 36832/udp | jaeger | Agent thrift binary |
| 38080 | kafka-ui | Kafka UI Web |
| 39000 | minio | S3 API |
| 39001 | minio | MinIO Console |
| 39090 | prometheus | Prometheus Web UI |
| 49092 | kafka | Kafka EXTERNAL |

---

## 网络与数据卷

- **网络**：`Sea-TryGo`，所有服务加入同一 Docker 网络，容器间可用服务名互通
- **数据卷**：
  - `redis-data`：Redis 持久化数据
  - `prometheus_data`：Prometheus 时序数据
  - `grafana_data`：Grafana 配置与仪表盘
  - `redisinsight_data`：RedisInsight 本地数据
