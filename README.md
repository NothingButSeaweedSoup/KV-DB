# KV-DB

基于 LSM 树的高性能键值存储数据库，Java 21 开发，支持单机模式与主从集群模式。

## 核心能力

**存储引擎**
- LSM 树架构：MemTable (ConcurrentSkipListMap) -> SSTable 多级持久化
- 双缓冲 MemTable：active/immutable CAS 无锁切换，异步 flush 不阻塞写入
- WAL 持久化：CRC32 校验、分段管理、可配置 fsync 策略 (SYNC/BATCH/ASYNC)
- Leveled Compaction：后台异步执行，按 key 范围拆分，原子元数据替换
- 布隆过滤器：SSTable 查询加速，SSTableCache 支持 LRU 淘汰与 mmap 读取

**可靠性**
- 数据完整性校验：启动时多级验证 (索引加载/文件结构/数据块抽样)
- 快照与增量备份：全量快照复制、基于时间戳的增量备份
- 配置热加载：ConfigWatcher 轮询 db-config.json，变更自动通知

**集群**
- 主从异步/同步复制：ReplicationMessage 自定义二进制序列化，ACK 确认
- 心跳检测：HeartbeatManager 周期心跳，连接池复用，超时标记离线
- 复制序列号持久化：崩溃恢复后断点续传

**网络协议**
- 二进制帧协议：Magic(2B) + Version + Cmd + ReqId + KeyLen + Key + ValLen + Value
- NIO 服务端：Selector Reactor 模型，Business 线程池，支持 GET/PUT/DELETE/PING/STATS
- 行文本协议服务端：兼容旧版 CLI/Socket 客户端

**可观测性**
- Metrics 模块：Counter/Histogram/Gauge 指标类型
- Prometheus 格式：HTTP /metrics 端点，可对接 Grafana
- 覆盖 QPS、延迟、MemTable 大小、SSTable 数量、Compaction/WAL 统计

**序列化**
- 可插拔 Serializer 接口：BytesSerializer / StringSerializer / JsonSerializer / JavaSerializer
- 集群层自定义二进制序列化：ReplicationCodec 替代 ObjectOutputStream

## 项目结构

```
src/main/java/
  api/           StorageEngine 接口
  benchmark/     JMH 性能基准测试
  cli/           命令行工具
  client/        BinaryClient (二进制协议) / DBClient (行文本协议)
  cluster/       MasterNode / SlaveNode / ClusterManager / HeartbeatManager
  config/        Config (Builder 模式 + Defaults 常量)
  core/          存储引擎核心
    LSMStorageEngine   引擎入口，双缓冲 + 异步 flush
    MemTable / MemTableState   活跃/不可变 MemTable 管理
    SSTable / SSTableCache / VersionSet   持久化存储与元数据管理
    Compaction         异步 Leveled Compaction
    WALManager         WAL 日志，CRC32 + 分段 + fsync 策略
    SnapshotManager    快照与增量备份
    DataIntegrityChecker   启动时数据完整性校验
  metrics/       MetricRegistry / PrometheusExporter / MetricsHttpServer
  protocol/      二进制帧协议定义
  serializer/    可插拔序列化器
  server/        NioServer (NIO) / DBServer (阻塞IO) / ClusterStarter
  util/          BloomFilter / ByteUtil / ConfigLoader / ConfigWatcher

src/test/java/
  core/          存储引擎测试 (11 个测试类)
  cluster/       集群模块测试
  protocol/      协议编解码 + 端到端测试
  metrics/       Metrics 模块测试
  serializer/    序列化器测试
  util/          工具类测试
```

## 快速开始

### 编译

```bash
mvn clean compile
```

### 运行测试

```bash
mvn test
```

### 单机模式

```bash
# 启动 CLI
./dbcli.sh

# 或直接运行
mvn exec:java -Dexec.mainClass="cli.CommandLineInterface"
```

CLI 命令：

```
put <key> <value>    写入键值对
get <key>            查询键值
delete <key>         删除键值
flush                刷新数据到磁盘
exit                 退出
```

### 集群模式

编辑 `cluster-config.json` 配置节点信息，然后启动：

```bash
# 启动集群（根据配置自动启动主从节点）
java -cp target/classes server.ClusterStarter
```

### NIO 二进制协议服务端

```bash
# 以 NIO 模式启动服务端（默认监听 19876，Metrics 端口 19877）
java -cp target/KV-DB-1.0-SNAPSHOT.jar server.NioServer
```

客户端连接：

```java
try (BinaryClient client = new BinaryClient("localhost", 19876)) {
    client.put("key".getBytes(), "value".getBytes());
    byte[] result = client.get("key".getBytes());
    System.out.println(client.ping());   // true
    System.out.println(client.stats());  // put_total=1, get_total=1, ...
}
```

### 打包

```bash
mvn package -Pcli          # CLI fat-jar
mvn package -Pserver       # 服务端 fat-jar
mvn package -Pnio-server   # NIO 服务端 fat-jar
mvn package -Pclient       # 客户端 jar（不 shade 依赖）
mvn package -Pall          # 全部打包
```

## 配置

编辑 `db-config.json`：

```json
{
  "walSegmentSize": 10485760,
  "memTableThreshold": 4194304,
  "fsyncStrategy": "BATCH",
  "sstTargetFileSize": 8388608,
  "level0FileNumCompactionTrigger": 4
}
```

| 参数 | 说明 | 默认值 |
|------|------|--------|
| walSegmentSize | WAL 段大小 (字节) | 1MB |
| memTableThreshold | MemTable 触发 flush 的阈值 (字节) | 4MB |
| fsyncStrategy | WAL 刷盘策略：SYNC / BATCH / ASYNC | BATCH |
| sstTargetFileSize | SSTable 目标文件大小 (字节) | 8MB |
| level0FileNumCompactionTrigger | Level 0 触发 compaction 的文件数 | 4 |

支持配置热加载，修改 `db-config.json` 后 5 秒内自动生效。

## 技术栈

- Java 21
- LSM 树存储引擎
- NIO Selector Reactor 网络模型
- JUnit 5 + AssertJ + Mockito 测试框架
- JMH 性能基准测试
- SLF4J + Logback 日志
- Jackson JSON 配置解析
- JaCoCo 测试覆盖率
- Maven 多 Profile 打包
- GitHub Actions CI
