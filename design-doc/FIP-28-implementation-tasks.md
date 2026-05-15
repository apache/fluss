# FIP-28 实现任务拆分

基于设计文档 [FIP: Support Write and Lookup for Expired Partitions in DataLake-Enabled Tables](./FIP:%20Support%20Write%20and%20Lookup%20for%20Expired%20Partitions%20in%20DataLake-Enabled%20Tables.md) 拆分的实现任务，每个 task 对应一个 PR。

## 依赖关系图

```
Layer 0 (无依赖):    PR 1
Layer 1 (依赖 PR1):  PR 2, PR 3, PR 4, PR 5  (可并行)
Layer 2:             PR 6  (← PR 2)
                     PR 7a (← PR 2)
Layer 3:             PR 7b (← PR 7a, PR 5)
Layer 4:             PR 7c (← PR 7b, PR 3)
                     PR 9  (← PR 7b)
Layer 5:             PR 8  (← PR 7c)
Layer 6:             PR 10 (← all)
```

**关键路径**:
- PR 1 → PR 2 → PR 7a → PR 7b → PR 7c → PR 8 → PR 10

---

## PR 1: 基础设施层 — RPC 协议扩展 + `__historical__` 分区常量与生命周期

**目标**: 为整个 feature 打下协议和元数据基础。

**对应设计文档章节**: Public Interfaces (RPC Extensions), A.1 Historical Partition

**前置依赖**: 无

### 改动范围

| 模块 | 文件 | 改动内容 |
|---|---|---|
| fluss-rpc | `fluss-rpc/src/main/proto/FlussApi.proto` | `PbLookupReqForBucket` 和 `PbPutKvReqForBucket` 添加 `optional string partition_name` 字段 |
| fluss-client | `fluss-client/src/main/java/org/apache/fluss/client/utils/ClientRpcMessageUtils.java` | 序列化/反序列化 `partition_name` |
| fluss-server | `fluss-server/src/main/java/org/apache/fluss/server/utils/ServerRpcMessageUtils.java` | 服务端解析 `partition_name` |
| fluss-common | 常量定义（已有常量文件或新增） | 定义 `__historical__` 分区名常量（如 `HISTORICAL_PARTITION_NAME = "__historical__"`） |
| fluss-server | `fluss-server/src/main/java/org/apache/fluss/server/coordinator/AutoPartitionManager.java` | 识别系统分区，TTL 过期检查时跳过 `__historical__` |
| fluss-server | `fluss-server/src/main/java/org/apache/fluss/server/coordinator/MetadataManager.java` | 分区创建校验：拒绝用户创建 `__historical__` 同名分区 |
| fluss-server | `fluss-server/src/main/java/org/apache/fluss/server/coordinator/CoordinatorService.java` | 允许系统分区创建绕过 `dynamicPartitionEnabled` 检查 |

### 注意事项

- protobuf 修改后需要 regenerate：`./mvnw clean install -DskipTests -pl fluss-protogen,fluss-rpc`

### 测试要求

- AutoPartitionManager 跳过 `__historical__` 分区的 TTL 检查
- 用户通过 DDL / 动态分区创建 `__historical__` 同名分区时被拒绝
- protobuf `partition_name` 字段序列化反序列化正确性
- `__historical__` 创建不受 `dynamicPartitionEnabled = false` 阻断

### 验收标准

- `partition_name` 字段在 RPC 中可选传递，旧协议兼容（字段缺失时为 null）
- `__historical__` 名称被系统保留，用户无法创建
- `AutoPartitionManager` 永不过期 `__historical__`

---

## PR 2: 客户端基础 — 过期分区判定谓词 + `__historical__` 动态创建

**目标**: 客户端能准确判断一个不存在的分区是否"已过期"，并在需要时创建 `__historical__`。

**对应设计文档章节**: A.1 (Expired partition predicate, Creation)

**前置依赖**: PR 1

### 改动范围

| 模块 | 文件 | 改动内容 |
|---|---|---|
| fluss-client | 新增 `ExpiredPartitionPredicate.java`，放在 write/lookup 共享的公共包（如 `fluss-client/.../client/partition/`） | 实现 A.1 的四条件判定逻辑 |
| fluss-client | `fluss-client/src/main/java/org/apache/fluss/client/write/DynamicPartitionCreator.java` 或提取到公共位置 | 支持 `__historical__` 的系统分区创建：绕过 `dynamicPartitionEnabled`；处理 `PartitionAlreadyExistException` 幂等性 |
| fluss-client | `fluss-client/src/main/java/org/apache/fluss/client/write/WriterClient.java` 或 `AbstractTableWriter.java` | 在分区不存在时调用过期谓词，决定是 redirect 还是抛异常 |

### 过期分区判定谓词（四条件）

```
partition 被判定为"已过期"，当且仅当：
1. 表是 auto-partitioned + lake tiering enabled（eligible table）
2. 分区名匹配 auto-partition spec 的命名规则（如合法日期格式）
3. 分区名在 TTL 过期边界之前（比 retention window 更老）
4. 分区在当前 metadata 中不存在
```

条件 4 为真但 1-3 任一为假时，保持原始 `PartitionNotExistException`。

### 测试要求

- 合法过期分区 → 谓词返回 true
- 非法分区名（不匹配 spec 格式）→ 谓词返回 false → 抛 `PartitionNotExistException`
- 未来分区（在 retention window 内）→ 谓词返回 false
- non-eligible table（无 lake tiering）→ 谓词返回 false
- 首次创建 `__historical__` 成功
- 多 client 并发创建 `__historical__` 幂等性（`PartitionAlreadyExistException` 被静默处理）

### 验收标准

- 只有真正过期的分区才会触发 redirect，其他情况保持原错误行为
- `__historical__` 创建不依赖 `dynamicPartitionEnabled` 配置
- 过期谓词和 `__historical__` 创建逻辑放在 write/lookup 共享位置，可被 PR 8（lookup 路径）直接复用，无需重复实现

---

## PR 3: LakeStorage SPI 扩展 + PaimonLakeTableLookuper 实现

**目标**: 提供 lake 侧的点查能力，为 PK 写入的 old-value fallback 和历史 lookup 做准备。

**对应设计文档章节**: Public Interfaces (New SPI Interface), B.3

**前置依赖**: PR 1（可与 PR 2 并行开发）

### 改动范围

| 模块 | 文件 | 改动内容 |
|---|---|---|
| fluss-common | `fluss-common/src/main/java/org/apache/fluss/lake/lakestorage/LakeStorage.java` | 新增 `createLakeTableLookuper(TablePath)` 默认方法 |
| fluss-common | 新增 `LakeTableLookuper.java` + `LookupContext.java` | 定义 lake 点查 SPI 接口 |
| fluss-lake-paimon | 新增 `PaimonLakeTableLookuper.java` | 基于 Paimon `LocalTableQuery` 的点查实现 |
| fluss-lake-paimon | `fluss-lake-paimon/src/main/java/org/apache/fluss/lake/paimon/PaimonLakeStorage.java` | 实现 `createLakeTableLookuper()` |

### PaimonLakeTableLookuper 关键实现

1. **懒初始化**: Catalog、FileStoreTable、LocalTableQuery 等资源在首次 lookup 时创建
2. **Snapshot-based file refresh 失效策略**:
   - 维护 `Map<Tuple2<String, Integer>, Long> refreshedBuckets`（(partitionName, bucketId) → snapshot ID）
   - 每次 lookup 比较当前最新 snapshot ID 与缓存的 snapshot ID，有新 snapshot 时 refresh files
3. **Lookup 流程**:
   - key bytes 已由客户端 `PaimonKeyEncoder` 编码为 BinaryRow 格式，直接 wrap
   - 调用 `tableQuery.lookup(partition, bucketId, keyRow)`
   - 结果通过 `PaimonRowAsFlussRow` 适配后编码返回

### 测试要求

- 点查存在的 key → 返回正确值
- 点查不存在的 key → 返回 null
- 新数据 tiered 后（新 snapshot），重新 refresh 能查到最新值
- snapshot 无变化时跳过 refresh

### 验收标准

- `LakeTableLookuper` SPI 接口可被多种 lake backend 实现（默认抛 `UnsupportedOperationException`）
- Paimon 实现能正确利用 `LocalTableQuery` 进行高效点查

---

## PR 4: 服务端线程隔离基础设施 — ioExecutor + per-bucket serial executor + flow control

**目标**: 建立历史操作与实时操作的性能隔离机制，确保历史路径的 lake I/O 不阻塞实时路径。

**对应设计文档章节**: C.2, C.3

**前置依赖**: PR 1（可与 PR 2, PR 3 并行开发）

### 改动范围

| 模块 | 文件 | 改动内容 |
|---|---|---|
| fluss-server | `fluss-server/src/main/java/org/apache/fluss/server/replica/ReplicaManager.java` | 创建共享 `ioExecutor`（bounded thread pool）；`putRecordsToKv()` 和 `lookups()` 增加 `__historical__` 分区判断和异步分发 |
| fluss-server | 新增 `HistoricalPartitionExecutor.java` 或类似类 | 封装 per-bucket keyed serial executor |
| fluss-common | 配置类 | 新增 ioExecutor 线程数、队列容量配置项 |
| fluss-rpc | 错误码定义 | 新增 `HISTORICAL_PARTITION_THROTTLED` 错误码 |
| fluss-client | 现有重试逻辑 | 确认 `HISTORICAL_PARTITION_THROTTLED` 走 backoff retry |

### 注意事项

- `putRecordsToKv()` 和 `lookups()` **已经使用 callback 模式**（`Consumer<List<PutKvResultForBucket>> responseCallback` / `Consumer<Map<TableBucket, LookupResultForBucket>> responseCallback`），ioExecutor 集成只需将处理逻辑提交到 executor，完成后调用 callback 即可，**无需修改方法签名**。

### Per-bucket serial executor 语义

```
同一 __historical__ bucket 的写入 → FIFO 串行执行（防止 lake lookup 延迟差异导致乱序）
不同 bucket 的写入 → 可并发
lookup → 可并发（不需要 ordering）
```

### 测试要求

- ioExecutor 队列满 → 返回 `HISTORICAL_PARTITION_THROTTLED` → 客户端 backoff retry
- 同一 bucket 的两个 historical write → 严格按提交顺序完成
- 不同 bucket 的 historical write → 可并发
- 实时路径写入和 lookup 完全不经过 ioExecutor，延迟不受影响

### 验收标准

- 实时路径保持完全同步，零性能影响
- 历史路径通过 ioExecutor 异步处理，RPC 线程立即释放
- 同 bucket 写入严格有序

---

## PR 5: Historical RocksDB 基础设施 — 非持久化 RocksDB + Composite Key 编解码

**目标**: 为 `__historical__` bucket 提供 KV 状态存储能力。

**对应设计文档章节**: A.3.1 (server-side step 3-4), A.3.2, A.3.3 (reference-counted handle)

**前置依赖**: PR 1（可与 PR 2, PR 3, PR 4 并行开发）

### 改动范围

| 模块 | 文件 | 改动内容 |
|---|---|---|
| fluss-server | 新增 `HistoricalKvManager.java` 或类似类 | 管理 `__historical__` bucket 的非持久化 RocksDB 实例：创建、引用计数、关闭/删除 |
| fluss-server | 新增 `CompositeKeyEncoder.java` | composite key 编解码 |
| fluss-server | `fluss-server/src/main/java/org/apache/fluss/server/kv/rocksdb/RocksDBKvBuilder.java` | 支持构建非持久化 RocksDB 实例（无 snapshot/checkpoint 配置） |

### 注意事项

- 需要确认现有 `RocksDBKvBuilder` 是否支持不配置 snapshot/checkpoint 的构建模式；若不支持，需要评估改动量（例如添加 builder 选项来跳过 snapshot/checkpoint 相关配置）。

### Composite Key 格式

```
composite key = partitionName length (4 bytes, big-endian)
              + partitionName bytes (UTF-8)
              + original key bytes
```

### Reference-counted RocksDB Handle

```
lookup 读取前 → acquire reference
lookup 完成后 → release reference
cleanup 时 → 等待所有 reference 释放 → close + delete directory
cleanup 完成后 → 后续 lookup 看到 null handle → fall through 到 lake
```

### 测试要求

- composite key 编解码正确性（各种 partition 名长度、特殊字符、空 key）
- 非持久化 RocksDB 创建/写入/读取/删除工作正常
- reference-counted handle 生命周期：正常读取、cleanup 等待、cleanup 后 lookup fall through

### 验收标准

- composite key 编解码对称、无数据丢失
- RocksDB 实例不生成 snapshot/checkpoint 文件
- reference counting 正确防止 cleanup 与并发 lookup 的竞态

---

## PR 6: Log 表历史写入路径

**目标**: Log 表对过期分区的写入能正确 redirect 到 `__historical__` 并被下游消费。

**对应设计文档章节**: A.2, C.1 (constraint 1), C.4.1

**前置依赖**: PR 2

> **说明**: 服务端对 `__historical__` 的 Log append 是正常的 replication/ACK 流程，不涉及 lake I/O，不经过 ioExecutor，因此不依赖 PR 4。

### 改动范围

| 模块 | 文件 | 改动内容 |
|---|---|---|
| fluss-client | `fluss-client/src/main/java/org/apache/fluss/client/table/writer/AppendWriterImpl.java` | 写入时检测过期分区（调用 PR 2 的谓词），redirect 到 `__historical__` |
| fluss-client | `fluss-client/src/main/java/org/apache/fluss/client/write/RecordAccumulator.java` | 实时和 `__historical__` 分离 batch |
| fluss-client | `fluss-client/src/main/java/org/apache/fluss/client/write/Sender.java` | 确保历史 batch 作为独立 request 发送 |
| fluss-client | bucket assigner 相关类 | `__historical__` 分区的 bucket 计算复用 sticky / bucket-key 策略 |

### RecordAccumulator 改动边界

此 PR 对 `RecordAccumulator` 的改动仅限于 **real-time / historical batch 分离**：

- 当前 `RecordAccumulator` 使用 `ConcurrentMap<PhysicalTablePath, BucketAndWriteBatches>` 管理 batch
- Log 表历史写入不需要 per-original-partition 拆分（Log 没有 key-only delete 的约束）
- 只需确保 `__historical__` 作为独立的 `PhysicalTablePath` 与实时分区的 batch 分开即可

> PR 7a 会在此基础上增加 PK 历史写入所需的 per-original-partition batch 拆分。

### 关键行为

- 服务端对 `__historical__` 的 Log append 无需额外改动，正常 replication/ACK
- Row payload 保留原始分区列，consumer 从 row 中提取原始分区身份
- 实时 batch 和历史 batch 完全独立发送

### 测试要求

- Log 表写入过期分区 → redirect 到 `__historical__` → 写入成功
- 下游 consumer 从 `__historical__` 消费数据 → 能从 row 中还原原始分区名
- 同时写入实时分区和过期分区 → batch 分离 → 互不影响
- 非过期分区不存在 → 仍抛 `PartitionNotExistException`

### 验收标准

- Log 表过期分区写入完全兼容现有 producer/consumer 协议
- 实时写入路径不受任何影响
- `flush()` 等待所有 batch（包括历史）完成

---

## PR 7a: 客户端 PK 历史写入路径

**目标**: 客户端侧 PK 表对过期分区的写入能正确 redirect 到 `__historical__`，并按 `(bucket, original partition name)` 拆分 batch。

**对应设计文档章节**: A.3.1 (client-side), C.1 (constraint 2)

**前置依赖**: PR 2

### 改动范围

| 模块 | 文件 | 改动内容 |
|---|---|---|
| fluss-client | `fluss-client/src/main/java/org/apache/fluss/client/table/writer/UpsertWriterImpl.java` | 过期分区检测 + redirect 到 `__historical__`；携带 `partition_name` |
| fluss-client | `fluss-client/src/main/java/org/apache/fluss/client/write/KvWriteBatch.java` | 设置 `partition_name` 到 batch 元数据 |
| fluss-client | `fluss-client/src/main/java/org/apache/fluss/client/write/RecordAccumulator.java` | Historical PK batch 按 `(__historical__ bucket, original partition name)` 拆分 |
| fluss-client | `fluss-client/src/main/java/org/apache/fluss/client/write/Sender.java` | 发送历史 PK batch 时设置 request-level `partition_name` |

### RecordAccumulator 改动（在 PR 6 基础上扩展）

当前 `RecordAccumulator` 使用 `ConcurrentMap<PhysicalTablePath, BucketAndWriteBatches>` 管理 batch，内部按 `bucketId` 拆分为 `Map<Integer, Deque<WriteBatch>>`。

PK 历史写入需要额外的 per-original-partition 维度：

```
原因：PbPutKvReqForBucket.partition_name 是 request 级别字段，
      key-only delete (row == null) 依赖这个字段提取 original partition，
      同一个 request 不能混不同 original partition 的 delete。
```

实现方式：在 `BucketAndWriteBatches` 内部为 historical PK 增加以 `originalPartitionName` 为维度的 batch 管理，或通过将 `(__historical__ partition, originalPartitionName)` 编码为独立的 `PhysicalTablePath` 来复用现有结构。

### 测试要求

- PK 表写入过期分区 → batch 携带正确的 `partition_name`
- 不同 original partition 的 PK write → 拆分为独立 batch
- key-only delete → `partition_name` 正确设置
- 发送的 request 中 `partition_name` 字段正确

### 验收标准

- 客户端正确 redirect PK 历史写入到 `__historical__` 分区
- 每个发送到服务端的 PK historical request 只包含一个 original partition 的数据
- 实时 PK 写入路径不受任何影响

---

## PR 7b: 服务端 composite key encoding for historical PK writes

**目标**: 服务端读取客户端发送的 `partition_name`，使用 `CompositeKeyEncoder` 对 key 加上分区前缀，实现不同过期分区在同一 `__historical__` RocksDB 中的 key 隔离。

**对应设计文档章节**: A.3.1 (server-side step 3-4), A.3.2

**前置依赖**: PR 7a（客户端发送 `partition_name`）, PR 5（`CompositeKeyEncoder`）

### 改动范围

| 模块 | 文件 | 改动内容 |
|---|---|---|
| fluss-server | `ServerRpcMessageUtils.java` | 新增 `getPartitionNames()` 从 `PutKvRequest` 提取 `partition_name` |
| fluss-server | `TabletService.java` | 调用 `getPartitionNames()` 并传递给 `ReplicaManager` |
| fluss-server | `ReplicaManager.java` | `putRecordsToKv()` / `putToLocalKv()` 透传 `partitionNames` |
| fluss-server | `Replica.java` | `putRecordsToLeader()` 增加 `@Nullable String partitionName` 参数 |
| fluss-server | `KvTablet.java` | 新增 `isHistoricalPartition` 字段；`putAsLeader()` / `processKvRecords()` 中基于该字段应用 `CompositeKeyEncoder` |
| fluss-client | `HistoricalPartitionTableITCase.java` | 新增多分区隔离测试 |

### 核心逻辑

KvTablet 构造时通过 `PartitionUtils.isHistoricalPartitionName()` 判断自身是否属于历史分区，存储为 `isHistoricalPartition` 字段。在 `processKvRecords()` 中：

```java
byte[] keyBytes =
        isHistoricalPartition
                ? CompositeKeyEncoder.encode(partitionName, rawKeyBytes)
                : rawKeyBytes;
```

composite key 用于 pre-write buffer 和 RocksDB 的所有操作（lookup、insert、update、delete），下游方法无需改动。

### 不在本 PR 范围

- lake fallback old-value lookup（PR 7c）

### 测试要求

- 不同 original partition 的 PK write 在 `__historical__` 中 key 不碰撞
- PK insert 到过期分区（无旧值）→ changelog `INSERT` 正确
- 同一 PK 写入同一过期分区 → 正常 upsert（`UPDATE_BEFORE + UPDATE_AFTER`）
- 实时路径完全不受影响

### 验收标准

- composite key 隔离不同 original partition 的 key space
- `isHistoricalPartition` 判断基于 tablet 自身身份，语义明确
- 方法签名变更对实时路径透明（传 null）

---

## PR 7c: 服务端 lake fallback old-value lookup for historical PK writes

**目标**: 历史 PK 写入时，当 prewrite buffer 和 RocksDB 中都没有 old-value 时，fallback 到 lake（`LakeTableLookuper`）获取旧值，生成正确的 changelog。

**对应设计文档章节**: A.3.1 (server-side old-value resolution), B.3

**前置依赖**: PR 7b（composite key encoding）, PR 3（`LakeTableLookuper` SPI + Paimon 实现）

### 改动范围

| 模块 | 文件 | 改动内容 |
|---|---|---|
| fluss-server | `KvTablet.java` | Old-value resolution chain 扩展：prewrite buffer → RocksDB → lake fallback |
| fluss-server | `Replica.java` 或 `ReplicaManager.java` | 初始化和管理 `LakeTableLookuper` 实例 |

### Old-value resolution chain

```
1. prewrite buffer (composite key lookup)  → 命中则使用
2. historical RocksDB (composite key lookup) → 命中则使用
3. lake fallback (LakeTableLookuper: original partition + original key) → 命中则使用，否则视为新 INSERT
```

lake fallback 时需要将 composite key 解码回 original partition name + original key，用于 `LakeTableLookuper` 查询。

### 测试要求

- PK upsert 到过期分区 → old-value 从 lake 获取 → changelog `UPDATE_BEFORE + UPDATE_AFTER` 正确
- PK insert 到过期分区（lake 无旧值）→ changelog `INSERT` 正确
- key-only delete 到过期分区 → 通过 `partition_name` 路由正确

### 验收标准

- PK 历史写入生成的 changelog 与正常分区行为一致
- lake fallback 仅在 local（buffer + RocksDB）miss 时触发
- `LakeTableLookuper` 异常不影响实时路径

---

## PR 8: 历史分区点查路径

**目标**: 对过期分区的 point lookup 能正确路由到 `__historical__` 并返回最新值（含 local-first 语义），且不影响实时分区的 lookup 延迟。

**对应设计文档章节**: B.1, B.2, C.4.2

**前置依赖**: PR 7c（需要 lake fallback old-value lookup 才能完整测试 local-first lookup 语义）

### 改动范围

| 模块 | 文件 | 改动内容 |
|---|---|---|
| fluss-client | `fluss-client/src/main/java/org/apache/fluss/client/lookup/PrimaryKeyLookuper.java` | 过期分区检测 → 路由到 `__historical__` bucket leader；携带 `partition_name`；调用 PR 2 提供的过期谓词 + `__historical__` 创建逻辑 |
| fluss-client | `fluss-client/src/main/java/org/apache/fluss/client/lookup/LookupSender.java` | batch 按 `(__historical__ bucket, original partition name)` 拆分；拆分 inflight 信号量（实时/历史独立） |
| fluss-server | `fluss-server/src/main/java/org/apache/fluss/server/replica/ReplicaManager.java` | `__historical__` lookup 分发到 ioExecutor（并发，无需 ordering） |
| fluss-server | `fluss-server/src/main/java/org/apache/fluss/server/kv/KvTablet.java` 或新增 | local-first lookup 逻辑 |
| fluss-common | 配置类 | 新增 `historicalLookupPermits` 配置项 |

### LookupSender 客户端隔离：拆分 inflight 信号量

当前 `LookupSender` 使用一个共享 `Semaphore(128)` 控制所有 lookup 的 inflight 并发。历史 lookup response 慢（lake fallback），可能占满所有 permit，导致实时 lookup 在 `acquire()` 处阻塞。

解决方案（对应设计文档 C.4.2）：

```
当前:
  maxInFlightRequestsSemaphore = new Semaphore(128)  // 实时 + 历史共用

改为:
  realtimeLookupSemaphore    = new Semaphore(realtimeLookupPermits)    // 如 96
  historicalLookupSemaphore  = new Semaphore(historicalLookupPermits)  // 如 32

发送前判断 lookup 类型:
  实时 lookup → realtimeLookupSemaphore.acquire()
  历史 lookup → historicalLookupSemaphore.acquire()
```

效果：历史 lookup 占满 32 个 permit 时，只阻塞后续历史 lookup，实时 lookup 的 96 个 permit 不受影响。

### Server-side lookup chain（与写入的 old-value resolution 一致）

```
1. prewrite buffer (composite key)  → 命中则直接返回
2. historical RocksDB (composite key) → 命中则直接返回
3. lake fallback (LakeTableLookuper)  → 命中则返回，否则返回 null
```

### Batching 约束

```
Historical lookup batch key = (__historical__ bucket, original partition name)

原因：与写入路径相同，PbLookupReqForBucket.partition_name 是 bucket-request 级别字段，
      同一个 request 中的所有 key 必须属于同一个 original partition。
```

### 测试要求

- lookup 过期分区（数据只在 lake）→ 返回正确值
- 历史写入后立即 lookup（数据在 local，尚未 tier）→ 返回最新值（local-first）
- lookup 不存在的 key → 返回 null
- lookup batch 包含不同 original partition 的 key → 正确拆分为多个 request
- lookup 不受 per-bucket write ordering 约束，可并发
- **隔离性**: 大量历史 lookup 占满历史 permit 时，实时 lookup 延迟不受影响

### 验收标准

- local-first 语义保证：写入后立即可查到最新值，不依赖 tiering 延迟
- lookup 通过 ioExecutor 异步执行，不阻塞 RPC 线程
- 正常分区 lookup 完全不受影响
- 历史 lookup 的慢 response 不占用实时 lookup 的 inflight permit

---

## PR 9: Recovery + Cleanup

**目标**: `__historical__` 重启后能正确恢复状态；tiering 完成后能安全清理 RocksDB。

**对应设计文档章节**: A.3.3, A.3.4

**前置依赖**: PR 7b（cleanup 使用 PR 4 的 per-bucket serial executor，recovery 使用 PR 5 的 historical RocksDB）

### 改动范围

| 模块 | 文件 | 改动内容 |
|---|---|---|
| fluss-server | `fluss-server/src/main/java/org/apache/fluss/server/kv/KvRecoverHelper.java` | `__historical__` 恢复逻辑 |
| fluss-server | `KvTablet.java` / `HistoricalKvManager.java` | cleanup 逻辑 |
| fluss-server | `ReplicaManager.java` 或 tiering 回调 | cleanup 触发时机 |

### Recovery 流程

```
1. 删除旧 historical RocksDB 目录（如果存在）
2. 创建全新空 RocksDB 实例
3. 获取 __historical__ 分区的 tiered offset（exclusive next-offset 语义）
4. 从 tiered offset 开始 replay WAL 到 log end：
   - 提取 original partition from row partition columns
   - 编码 composite key
   - apply upsert/delete
5. 到达 high watermark 后，后续 record 写入 prewrite buffer（而非直接写 RocksDB）
```

### Cleanup 流程

```
条件：tieredOffset >= logEndOffset（均为 exclusive next-offset 语义）

1. 提交 cleanup 任务到 per-bucket serial executor
2. 串行执行器内 re-check tieredOffset >= logEndOffset
   - 如果新写入已到达 → 条件不满足 → 跳过
   - 如果条件成立 → 继续
3. 等待所有 outstanding lookup reference 释放（reference-counted handle）
4. close + delete 整个 historical RocksDB 目录
5. 后续 lookup 看到 null handle → fall through 到 lake
6. 后续 write 到达时 lazily 重新创建 RocksDB
```

### 测试要求

- 写入 → 重启 → recovery（WAL replay）→ lookup 数据正确
- 写入 → tiering 完成 → cleanup → RocksDB 被清理 → 后续 lookup fall through 到 lake 返回正确值
- cleanup 过程中有并发 lookup → reference counting 正确协调
- cleanup check 后有新写入到达 → re-check 失败 → cleanup 跳过（不误删）
- recovery 数据量 = log end - tiered offset（有界的）

### 验收标准

- recovery 不依赖 snapshot，始终从 clean RocksDB + WAL replay 恢复
- cleanup 与写入通过 serial executor 串行化，无竞态
- cleanup 与 lookup 通过 reference counting 协调，无读关闭竞态

---

## PR 10: 端到端集成测试

**目标**: 覆盖完整用户场景，验证各组件协同工作。

**对应设计文档章节**: Test Plan

**前置依赖**: PR 1–9 全部（含 PR 7c）

### 测试场景

| # | 场景 | 验证目标 |
|---|---|---|
| 1 | Log 表写入过期分区 | redirect → `__historical__` → 下游消费 → 从 row 还原原始分区 |
| 2 | PK 表 upsert 过期分区 | lake old-value lookup → `UPDATE_BEFORE + UPDATE_AFTER` changelog 正确 |
| 3 | PK 表 delete 过期分区 | key-only delete 通过 `partition_name` 路由正确 |
| 4 | lookup 过期分区（数据在 lake） | 路由到 `__historical__` → lake fallback → 返回正确值 |
| 5 | 写入后立即 lookup（数据在 local） | local-first → 返回最新值（不依赖 tiering） |
| 6 | 重启 recovery | WAL replay → 状态一致 → lookup 正确 |
| 7 | tiering 完成后 cleanup | RocksDB drop → 后续 lookup fall through → lake 返回正确 |
| 8 | `dynamicPartitionEnabled = false` | `__historical__` 仍能创建和使用 |
| 9 | 非法分区名 | 不匹配 auto-partition spec → 不 redirect → `PartitionNotExistException` |
| 10 | 并发写多个 expired partition 到 `__historical__` | composite key 隔离正确，无 key 碰撞 |
| 11 | flow control | ioExecutor 队列满 → `HISTORICAL_PARTITION_THROTTLED` → 客户端 retry |

### 验收标准

- 所有场景测试通过
- 实时路径的性能和行为不受任何影响（回归验证）
