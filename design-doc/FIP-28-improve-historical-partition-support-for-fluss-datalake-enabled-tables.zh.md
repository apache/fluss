# FIP-28：支持 Paimon Lake Table 中过期分区的写入和查询

## 动机

对于启用了 lake tiering 的自动分区表（主键表和日志表），Fluss 会从元数据中过期旧分区。分区过期后，用户会遇到两个缺口：

1.  **查询缺口（主键表）**：按照当前行为，对过期分区做 point lookup 会返回 `null`。因为 Fluss 一旦判断分区不存在，就会把该行视为不存在，即使数据仍然存在于 lake 存储中。
2.  **写入缺口**：针对过期分区的迟到更新或插入会被拒绝。

这会让用户困惑，因为 batch/lake 读取仍然可以看到历史数据，但 Fluss 的在线路径无法访问这些数据。

本 FIP 为读写两条路径提出统一方案：

- 通过服务端 lake lookup fallback 读取历史分区数据（主键表）；
- 为主键表和日志表提供一个专用的特殊 Fluss 分区（`__historical__`），用于写入历史分区数据；
- 允许下游消费者从该分区消费过期原始分区的迟到记录。

范围和适用条件：

- 本 FIP 只支持启用了 Paimon lake 存储的表。
- 对于主键表，这是硬性技术要求：历史写入需要解析旧值，以生成正确的 changelog（`UPDATE_BEFORE` + `UPDATE_AFTER`），而过期分区的旧值只存在于 lake 存储中。没有 lake 后端时，无法查询旧值，写路径也无法产生正确结果。
- 对于日志表，历史写入在技术上可以不依赖 lake 存储（append-only，不需要旧值解析）。不过，本 FIP 仍然限制在启用了 lake 的表上，以避免功能矩阵碎片化：只支持启用 lake 的日志表历史写入，而不支持未启用 lake 的日志表，会在同一种表类型中制造不一致的用户预期。

说明：本 FIP 主要聚焦 Paimon 作为 lake 存储后端，因为 Paimon 通过 `LocalTableQuery` API 提供高效 point lookup 能力。Iceberg、Lance 等其他 lake 格式目前还没有可比的点查性能，因此可在后续 FIP 中再考虑支持它们；支持 Iceberg 需要完整 cache。

## 公共接口

### RPC 扩展

扩展 lookup RPC，用于历史分区的 lake lookup：

``` protobuf
message PbLookupReqForBucket {
  // existing fields...
  optional string partition_name = N;  // original partition name for historical lookup
}
```

- 当 lookup 目标是 `__historical__` 分区时，`partition_name` 携带原始分区名。
- 服务端使用该字段判断应该查询哪个 lake 分区。
- 该字段是可选字段；普通 lookup 不设置它。

扩展 put-kv RPC，用于历史 delete 的确定性路由：

``` protobuf
message PbPutKvReqForBucket {
  optional int64 partition_id = 1;
  required int32 bucket_id = 2;
  required bytes records = 3;
  optional string partition_name = 4;
}
```

- `partition_name` 携带重定向到 `__historical__` 前的原始分区名。
- 对于历史分区 put，客户端始终设置该字段。服务端使用它编码组合 key，并解析 lake fallback 分区。对于 key-only delete（`row == null`），由于没有 row payload，这是原始分区身份的唯一来源。
- 该字段是可选字段，以保持向后兼容；普通（非历史）分区 put 不设置它。

### 新增 SPI 接口

通过 `LakeStorage` 暴露表级别的 lake point lookup：

``` java
public interface LakeStorage {
    // existing methods...

    /**
     * Creates a {@link LakeTableLookuper} for point lookup against the specified table
     * in lake storage.
     */
    default LakeTableLookuper createLakeTableLookuper(TablePath tablePath) {
        throw new UnsupportedOperationException(
                "Point lookup is not supported for this lake storage.");
    }
}

/**
 * Interface for looking up a single key from lake storage. This is used for:
 * <ul>
 *   <li>PK table historical write: old-value fallback from lake to produce UPDATE_BEFORE for changelog
 *   <li>Expired partition point lookup: local-first lookup (RocksDB → lake fallback)
 * </ul>
 *
 * <p>Each instance is bound to a specific table and caches per-table resources
 * (e.g., catalog connections, table metadata) for efficient repeated lookups.
 *
 * <p>The key bytes passed to {@link #lookup} are already encoded in the lake storage's
 * native format (e.g., Paimon BinaryRow format) by the client-side encoder, so
 * implementations can use them directly without re-encoding.
 */
public interface LakeTableLookuper extends AutoCloseable {
    /**
     * Lookup a single key from lake storage.
     *
     * <p>Key bytes are already encoded in the lake storage's native key format.
     * Returned value bytes should be encoded with schema ID prefix.
     *
     * @param key encoded key bytes in lake storage's native format
     * @param context lookup context containing partition, bucket, and schema information
     * @return encoded value bytes with schema ID prefix, or null if not found
     * @throws Exception if lookup fails
     */
    @Nullable
    byte[] lookup(byte[] key, LookupContext context) throws Exception;

    /** Context for a single lake lookup operation. */
    class LookupContext {
        @Nullable private final ResolvedPartitionSpec partitionSpec;
        private final int bucketId;
        private final int schemaId;

        public LookupContext(
                @Nullable ResolvedPartitionSpec partitionSpec, int bucketId, int schemaId) {
            this.partitionSpec = partitionSpec;
            this.bucketId = bucketId;
            this.schemaId = schemaId;
        }

        /** Returns the partition spec, or null for non-partitioned tables. */
        @Nullable
        public ResolvedPartitionSpec getPartitionSpec() { return partitionSpec; }

        /** Returns the bucket ID. */
        public int getBucketId() { return bucketId; }

        /** Returns the schema ID used for value encoding. */
        public int getSchemaId() { return schemaId; }
    }
}
```

## 提议的变更

### A. 历史分区写路径（日志表 + 主键表）

#### A.1 历史分区

对于每张符合条件的表，维护特殊的 **Historical Partition**，作为过期分区的写入目标。历史分区名由自动分区 key 的值替换为 `__historical__` 得到：

- **单分区 key** `[dt]`：每张表一个历史分区，即 `__historical__`。
- **多分区 key** `[region, dt]`（`dt` 是自动分区 key）：每个静态分区前缀对应一个历史分区，例如 `us-east$__historical__`、`eu-west$__historical__`。每个静态前缀都有独立的历史分区，也有自己独立的 bucket、WAL 和 tiering 生命周期。

本 FIP 中，“历史分区”指所有这类以 `__historical__` 为后缀的分区。

属性：

- 始终可写；
- 不会自动过期；
- 使用正常的复制和 WAL 行为；
- bucket 数与该表其他分区相同。

当写入目标是过期的原始分区时，客户端将写入重定向到历史分区；原始分区身份仍然保留在 row 的分区列中。该规则适用于主键写入和日志追加。

**过期分区判定条件**：

只有当以下条件全部满足时，分区才被视为“已过期”，并且可以重定向到 `__historical__`：

1.  表是启用了 data lake 的自动分区表（符合条件的表）。
2.  分区名符合表的自动分区 spec 命名模式（例如，日期分区表需要满足合法日期格式）。
3.  分区名早于根据自动分区 spec 和当前时间计算出的 TTL 过期边界，也就是早于保留窗口。
4.  当前元数据中不存在该分区。

如果条件 4 为真，但条件 1 到 3 中任一条件不满足，客户端不能重定向到 `__historical__`，应保留原始的 `PartitionNotExistException`。这样可以避免非法分区名、未来分区或不符合条件的表被错误路由到历史路径。

**创建**：

- 当客户端第一次遇到过期分区（由上述判定条件定义）时，会懒创建 `__historical__`；无论当前操作是写入还是 lookup 都一样。客户端检查缓存元数据中是否存在 `__historical__`，如果不存在，就通过分区创建 RPC 触发创建。
- 虽然 `__historical__` 的创建复用了现有分区创建 RPC、元数据管理器和副本分配基础设施，但它被视为 **系统分区创建**，不受用户可见的 `dynamicPartitionEnabled` 设置控制。这保证 `dynamicPartitionEnabled = false` 的表仍然可以使用历史写入和 lookup；`__historical__` 是系统内部机制，不是用户创建的业务分区。服务端创建校验会对符合条件的表（自动分区、启用 Paimon lake）无条件允许 `__historical__`，不受动态分区开关影响。
- 如果创建失败（例如 coordinator 临时错误），当前操作失败，客户端下次重试时会再次触发创建。
- 如果多个客户端并发创建 `__historical__`，只有一个会成功；其他客户端收到 `PartitionAlreadyExistException` 后继续正常执行。

**名称保留**：

- 分区值 `__historical__` 在概念上由系统保留。不过，目前服务端 **不会强制校验** 这一点，因为 `__historical__` 通过标准分区创建 RPC 创建，服务端无法区分系统发起和用户发起的创建。

**AutoPartitionManager 排除逻辑**：

- `AutoPartitionManager` 会识别 `__historical__` 作为系统分区，并在 TTL 过期检查中无条件跳过它。实现方式是在应用 TTL 逻辑前先检查分区名。

**分区发现和消费者可见性**：

- `__historical__` 与普通分区一样包含在 `listPartitions` 结果中。消费者通过标准分区发现机制发现它，并可以订阅它的 bucket。
- 元数据 API（例如 `getPartition`）将它视为普通分区。不做特殊过滤，该分区对客户端完全可见。

#### A.2 日志表写路径（客户端 -> 服务端）

客户端处理：

1.  Producer 根据记录的分区列解析目标分区。
2.  如果目标分区存在，走普通日志写路径。
3.  如果目标分区不存在，客户端评估过期分区判定条件（见 A.1）。如果判定为过期分区，客户端将目标分区重写为 `__historical__`，同时保持 row 数据不变。如果不满足判定条件，则向调用方传播原始 `PartitionNotExistException`。
4.  客户端计算 `__historical__` 分区内的目标 bucket：
    - **Sticky 策略（无 bucket key）**：复用普通 sticky 行为；继续写入当前 sticky bucket，并在 sticky window 切换时轮转到下一个 bucket。
    - **Bucket-key 策略**：直接根据 `bucketKeyBytes` 计算 bucket（复用已经编码好的 bytes）。
5.  记录以普通日志追加请求发送给选中的 `__historical__` bucket leader。

具体 bucket-key 路由代码：

``` java
byte[] bucketKeyBytes = record.getBucketKey(); // already encoded by existing bucket-key encoder
int bucketId = bucketAssigner.assignBucket(bucketKeyBytes, cluster);
```

原因：对于日志表，主要考虑是与 `__historical__` 中主键表的 bucket 策略保持一致，也与现有 bucket-key 语义一致。主键表需要按 bucket key 分桶，详细原因见下面的主键表章节。代价是：来自不同原始分区但 bucket key 相同的数据可能增加热点。

服务端处理：

1.  `__historical__` 分区 leader 将记录追加到 `__historical__` 日志中，使用正常复制和 ACK 语义。
2.  Row payload 仍然包含原始分区列，因此不需要额外的 envelope 字段。
3.  下游消费者订阅 `__historical__` bucket，并通过标准日志消费路径消费这些记录。
4.  消费者可以从 row 分区列恢复原始分区身份。

这使日志表的历史写入与现有生产者/消费者协议完全兼容，并在数据 payload 中保留分区语义。

Offset 连续性和语义保证：

`__historical__` 是一个 **best-effort 的迟到数据追加路径**，不是原始分区 changelog 的严格延续。原始分区和 `__historical__` 之间 **没有 offset 连续性**：

- 当分区过期时，其元数据和日志数据都会被删除；原始 offset 已不存在。
- `__historical__` 从 0 开始拥有自己的 offset 空间；原始 offset 与历史 offset 之间没有映射关系。
- 原始分区来自 row 数据中的分区列。

**潜在 changelog 缺口**：TTL 过期由时间驱动，不会等待所有消费者消费完原始分区。如果某个消费者在原始分区过期时还没有完整消费该分区的 changelog，而新的迟到写入随后被重定向到 `__historical__`，那么原始分区 changelog 尾部与 `__historical__` 中该分区第一条记录之间可能存在缺口。这是 **TTL 过期的既有限制**；在本 FIP 之前，迟到数据会被直接拒绝，因此不会有任何数据到达。`__historical__` 捕获了原本会丢失的迟到数据，改善了这种情况。

**用户预期**：订阅 `__historical__` 的消费者应将其视为迟到记录的补充流，而不是任意原始分区 changelog 的无缝延续。对于要求跨分区过期也保持严格 changelog 完整性的场景，用户应将 TTL 配置得足够长，确保所有消费者都能消费完成，或者改为从 lake 存储消费。

#### A.3 主键表写路径（客户端 -> 服务端）

#### A.3.1 历史分区写入流程

客户端处理：

1.  Upsert writer / delete writer 根据 row 分区列解析目标分区。
2.  如果目标分区存在，走普通主键写路径。
3.  如果目标分区不存在，客户端评估过期分区判定条件（见 A.1）。如果判定为过期分区，客户端将目标分区重写为 `__historical__`，并保持 row payload 不变。如果不满足判定条件，则向调用方传播原始 `PartitionNotExistException`。
4.  客户端计算 bucket：
    - 直接根据 `bucketKeyBytes` 计算目标 `__historical__` bucket。
5.  记录发送给选中的 `__historical__` bucket leader。
6.  对于被重定向的 PK 写入，客户端在 put-kv bucket 请求元数据中携带 `partition_name`。这是 key-only delete（`row == null`）确定性路由所必需的。

具体 bucket 计算：

``` java
byte[] bucketKeyBytes = record.getBucketKey(); // already encoded by existing bucket-key encoder
int bucketId = bucketAssigner.assignBucket(bucketKeyBytes, cluster);
```

原因：这样可以保持主键表在线写入与 union-read 规划之间的 bucket-key 对齐。使用 `originalPartitionName + bucketKey` 的组合路由可以改善热点分布，但在从特定 Paimon bucket 做历史数据 union read 时会导致更宽的 fan-out。

服务端处理：

1.  `__historical__` 分区 leader 收到写入，并进入历史 PK 写 pipeline。

2.  对于主键表，PK 编码不包含分区列。不同原始分区可能产生相同 key bytes：
    - `dt=2020,id=1` -> `encode(id=1)`
    - `dt=2019,id=1` -> `encode(id=1)`

3.  为避免 key 冲突和错误的旧值解析，`__historical__` 使用 **组合 key** 编码，把原始分区名前置到原始 key 前：

        composite key = partitionName length (4 bytes, big-endian) + partitionName bytes (UTF-8) + original key bytes

    这保证来自不同原始分区的 key 不会在同一个 key 空间中冲突。

4.  每个 `__historical__` bucket 维护一个 **单一的、非持久化的 RocksDB 实例**（与普通分区的 RocksDB 分离），使用组合 key 存储所有原始分区的数据。该 RocksDB：
    - 使用单一默认 CF；
    - 不需要 snapshot 或 checkpoint，因为 lake 是事实源，恢复通过从最后一个 tiered offset 重放 WAL 完成（见 A.3.4）；
    - 存储在 bucket 普通 RocksDB 旁边的专用数据目录中（例如 `__historical_/`）。

5.  提取原始分区：
    - upsert（`row != null`）：来自 row 分区列；
    - delete（`row == null`）：来自 RPC 请求中的 `partition_name` 字段（`PbPutKvReqForBucket.partition_name`），因为没有 row payload 可用于提取分区列。

6.  根据原始分区名 + 原始 key bytes 编码组合 key。

7.  使用组合 key 在 historical RocksDB 上处理 upsert/delete：
    - upsert → 将组合 key + value 写入 prewrite buffer；
    - delete → 从 prewrite buffer 删除组合 key；
    - flush → prewrite buffer flush 到 historical RocksDB。

8.  在旧值解析中，如果本地旧值不存在，该路径会 fallback 到底层 lake 存储做 point lookup。Lake lookup 异步执行；线程模型见 C 节。

流程示意：

``` text
Incoming PK record on __historical__
        |
        v
Extract original partition:
  - upsert (row != null): from `partition_name` in RPC request
  - delete (row == null): from `partition_name` in RPC request
        |
        v
Encode composite key: partitionName + originalKey
        |
        v
Process upsert/delete with composite key
        |
        +--> prewrite buffer (composite key)
        +--> historical RocksDB (composite key, single CF)
        |
        v
Old-value lookup: prewrite buffer -> historical RocksDB -> lake
```

旧值解析链路：

1.  prewrite buffer（组合 key lookup）；
2.  historical RocksDB（组合 key lookup）；如果发现 **tombstone**（见下文），立即返回 `null`，不会继续 fallback 到 lake；
3.  lake fallback（原始分区 + 原始 key）。

**DELETE 操作的 tombstone 机制**：

在普通（非历史）分区中，RocksDB 的 `delete(key)` 会物理删除 key。删除后，`get(key)` 返回 `null`，这是正确的，因为没有 lake fallback。但在 `__historical__` 分区中，RocksDB 返回 `null` 会触发 lake fallback（上面的第 3 步），进而从 lake 存储返回删除前的旧值，相当于撤销了 delete。

为避免这种情况，`__historical__` 分区中的 DELETE 操作写入一个 **tombstone**（空 byte array `new byte[0]`），而不是物理删除 key：

- 普通编码 value 至少有 2 个 bytes（schema ID prefix），因此空 byte array 可以明确表示 tombstone marker。
- 当 `value != null && value.length == 0` 时，`isTombstone(value)` 返回 `true`。
- 旧值解析链路（第 2 步）会检查 tombstone：如果 RocksDB 结果是 tombstone，则返回 `null`，不会 fallback 到 lake。

所有写入 historical RocksDB 的路径都一致应用 tombstone：

- **写路径**（`KvPreWriteBuffer.flush()`）：flush 历史分区的 DELETE entry 时，写入 `TOMBSTONE_VALUE`，而不是调用 `delete()`。
- **恢复路径**（`KvRecoverHelper`）：恢复期间重放 WAL DELETE 记录时，将 `TOMBSTONE_VALUE` 写入 historical RocksDB。

执行说明：

- 整个历史写入处理（包括第 3 步 lake fallback）都会从 RPC thread offload 到共享 IO executor，因此不可预测的远端 lake I/O 延迟不会阻塞 RPC thread 或实时写路径。详见 C 节。

#### A.3.2 设计理由：带组合 key 的单一非持久化 RocksDB

针对 `__historical__` 中主键状态管理，考虑过四种方案：

1.  **在 bucket 现有 RocksDB 中为每个分区创建 CF**：每个原始分区在该 bucket 共享 RocksDB 中拥有一个 CF。
2.  **纯内存状态**（每个原始分区一个 HashMap，不使用 RocksDB）。
3.  **每个分区一个非持久化 RocksDB 实例**：每个原始分区一个 RocksDB。
4.  **带组合 key 的单一非持久化 RocksDB**（本 FIP 选择的方案）。

本 FIP 选择方案 4，原因如下：

- **没有 CF 膨胀风险**：方案 1 需要在单个 RocksDB 中为每个活跃历史分区创建一个 CF。每个 CF 会分配自己的 memtable（默认 64MB × 2 = 128MB），即使只有几十个分区，也可能耗尽内存。RocksDB 启动时间也会随着 CF 数量增加而下降。
- **内存使用可控**：方案 2 没有天然的 spill-to-disk 机制；如果 tiering 很慢或历史写入突增，内存会无界增长。RocksDB 会自动处理 memory → SST flush。
- **O(1) 固定资源开销**：方案 3（每个分区一个 RocksDB）需要为每个活跃分区维护一个 RocksDB 实例，每个实例都有自己的 memtable（最小 4–8 MB × 2）、MANIFEST 和后台 flush/compaction 活动。如果有 N 个活跃历史分区，总固定开销是 O(N)。单个 RocksDB 实例的固定开销不随包含历史数据的分区数量变化而变化；只有数据量增长，这是预期行为。
- **没有持久化复杂度**：由于 lake 存储是历史数据的事实源，historical RocksDB 不需要 snapshot 或 checkpoint。重启时丢弃它，并从最后一个 tiered offset 重放 WAL 重建（见 A.3.4）。这样可以消除所有 snapshot 管理开销。
- **对 codec 影响最小**：组合 key 编码完全限定在 `__historical__` bucket 自己的 RocksDB 实例中。普通分区的 key codec、snapshot 和恢复路径完全不受影响。编码/解码很简单：在原始 key 前加上 4 字节长度前缀和分区名 bytes。
- **管理简单**：每个 `__historical__` bucket 一个 RocksDB 实例，也就是一次 open、一次 close、一个目录。无需管理动态 RocksDB 实例集合或 CF handle。

**`HistoricalKvManager`**：所有 bucket 级 historical RocksDB 实例由集中式 `HistoricalKvManager` 管理，提供：

- **懒创建**：只有在某个 bucket 第一次 `put()` 时才打开 RocksDB，而不是在 bucket 分配时打开。
- **空闲超时清理**：一个调度任务定期扫描所有打开的实例，并驱逐空闲时间超过可配置超时（`kv.historical.idle-timeout`，默认 30 分钟）的实例。这样可以限制突发历史写入后的内存使用；突发结束且数据已 tiered 后，空闲 RocksDB 实例会被自动清理。

#### A.3.3 Tiering 同步后的清理

历史写入预期是突发式的：一波迟到数据之后会进入空闲期。整波数据通常会很快 tier 到 lake。基于这一特征，清理采用简单的整库 RocksDB drop 策略，而不是按分区增量清理：

**清理条件**：对于给定 bucket，当 `__historical__` 分区的 `tieredOffset >= logEndOffset` 时，该 bucket historical RocksDB 中的所有数据都已经持久地 tier 到 lake。`tieredOffset` 和 `logEndOffset` 都采用排他的 next-offset 语义，即下一条将被写入/已被 tier 的记录 offset；因此 `>=` 表示所有已有记录都已 tier。

**清理流程**：

1.  将清理作为任务提交到每个 bucket 的串行 executor（见 C.2），使其与历史写入串行化。
2.  在串行 executor 内重新检查 `tieredOffset >= logEndOffset`。如果初次检查后又有新的写入到达，该条件会不再成立，清理会被跳过。
3.  如果条件成立，关闭并删除整个 historical RocksDB 目录，然后创建一个新的空实例，或者懒到下一次历史写入到达时再创建。

**与并发 lookup 的协调**：

清理会关闭并删除 historical RocksDB，但历史 lookup（不会经过每个 bucket 的串行 executor，见 C.2）可能正在并发读取它。为避免 lookup 访问已关闭/已删除的 RocksDB 实例，每个 bucket handle 使用一个 **读写锁**（`ReentrantReadWriteLock`）：lookup 在读取本地历史状态前获取共享读锁（允许并发读取），清理在关闭实例前通过 `tryLock()` 获取独占写锁。如果无法立即获取锁，清理跳过该实例并稍后重试。如果清理先完成，后续 lookup 会看到本地实例不存在，并直接 fallback 到 lake。

**为什么整库 drop RocksDB，而不是按分区 `DeleteRange`**：

- 不需要按分区追踪 offset，只需要检查分区级 tiered offset。
- 没有 `DeleteRange` tombstone 开销，也不依赖 compaction。
- 没有复杂的锁语义；每个 bucket 的串行 executor 已经提供必要的串行化。
- 匹配突发写入模式：一波数据完全 tier 后，RocksDB 实际上已经可以视为空。

**未来考虑：按分区增量清理**：

如果历史写入不是突发式，而是持续发生（例如，多个原始分区以不同速率持续收到迟到写入），整库 RocksDB drop 可能长时间不会触发，因为随着新写入不断到达，`tieredOffset < logEndOffset` 会持续为真。那种情况下，可以考虑按分区增量清理：

1.  维护 `partitionEndOffset[partition]`，追踪 `__historical__` 中每个原始分区写入的最新日志 offset。
2.  当 `tieredOffset >= partitionEndOffset[partition]` 时，该分区的数据已经完全 tier。
3.  将清理提交到每个 bucket 的串行 executor，在内部重新检查条件，然后对该分区的组合 key 前缀执行 `DeleteRange`。
4.  由于组合 key 不包含日志 offset，`DeleteRange` 只能删除整个分区前缀，无法做局部清理（只删除到某个特定 offset 为止的记录）。因此条件必须是该分区的 **所有** 写入都已经 tier。

这会增加追踪和锁管理复杂度，因此除非实践证明突发写入假设不足，否则先延后处理。

#### A.3.4 `__historical__` 分区恢复流程

由于 historical RocksDB 是非持久化的（没有 snapshot/checkpoint），恢复完全通过从最后一个 tiered offset 重放 WAL 完成：

1.  **丢弃旧状态**：如果 historical RocksDB 数据目录存在，则删除它，然后创建一个全新的空 RocksDB 实例。
2.  **确定重放起始 offset**：从 tiering service 元数据中获取 `__historical__` 分区的 tiered offset。因为 `__historical__` 本身也是一个 tiered 分区，其 tiered offset 作为整体被追踪；无需计算每个原始分区的 offset。
3.  **重放 WAL**：从 tiered offset 到 **log end** 重放 `__historical__` 日志：
    - 对每条记录，从 row 分区列提取原始分区；
    - 编码组合 key（partitionName + originalKey）；
    - 将 upsert/delete 应用到 historical RocksDB。
4.  重放达到 **high watermark** 后，高于 high watermark 的记录会被应用到 prewrite buffer，而不是直接写入 RocksDB（与普通分区恢复语义一致）。

这种方式比普通分区恢复简单很多：

- 没有需要恢复的 snapshot，始终从干净 RocksDB 开始。
- 重放数据量由 `log end - last tiered offset` 限定；只要 tiering 正常运行，这个量就很小。
- 只需要管理一个 RocksDB 实例；重放期间不需要动态创建实例或 CF。

#### A.3.5 Changelog 缺口和已知限制

如 A.2（“Offset 连续性和语义保证”）所述，`__historical__` 是 best-effort 的迟到数据追加路径。对于主键表，这对 changelog 正确性还有额外影响：

**场景**：某个消费者同时订阅原始分区和 `__historical__`。在该消费者消费完原始分区 changelog 之前，原始分区被 TTL 过期。之后该分区的新写入被重定向到 `__historical__`。

**影响**：

- 消费者可能遗漏原始分区 changelog 的尾部，也就是它最后消费的 offset 与分区过期之间的记录。
- `__historical__` 中该分区的第一条记录可能产生一个 changelog entry（例如 `INSERT` 或 `UPDATE_AFTER`），而这个 entry 与消费者最后看到的该 key 状态在逻辑上不一致。

**为什么可以接受**：

- 这是基于 TTL 的分区过期已有的限制，不是本 FIP 引入的。此前迟到数据会被完全拒绝；`__historical__` 改善了这种情况。
- 只有当 TTL 在所有消费者完成消费前过期分区时，才会出现这个缺口。需要严格 changelog 完整性的用户应配置足够长的 TTL，或者从 lake 存储消费。
- 对大多数迟到数据场景（例如 correction、backfill），best-effort 语义已经足够。

### B. 历史分区 point lookup 路径（主键表）

#### B.1 客户端 fallback

当 `PrimaryKeyLookuper` 无法解析分区（根据 A.1 的过期分区判定，分区已过期）时：

- 按照相同的 `__historical__` 写入分桶策略（基于 bucket-key）计算 `bucketId`；
- 将 lookup 请求路由到 `__historical__` 分区 + `bucketId` 的 leader；
- 发送标准 `LookupRequest`，并额外设置 `partition_name` 字段为原始分区名；
- `LookupSender` 必须按照 `(__historical__ bucket, original partition name)` 批量聚合历史 lookup 请求；这与历史 PK 写入的 batching key 相同（见 C.1）。由于 `partition_name` 是 bucket-request 级字段，单个 `PbLookupReqForBucket` 只能携带一个原始分区的 key。把不同原始分区的 key 混入同一个 bucket 请求，会导致服务端用错误的 lake 分区查询所有 key。

**过期分区缓存**：`PrimaryKeyLookuper` 维护一个内存缓存（`Map<String, Long>`），将过期分区名映射到已解析的 `__historical__` 分区 ID。分区一旦被识别为过期，并且其历史分区 ID 被解析出来（这涉及一次 ZK 元数据 lookup），该映射就会被缓存。同一个过期分区的后续 lookup 会完全跳过 ZK lookup，避免高并发 lookup 下重复元数据请求冲击 ZooKeeper。

#### B.2 服务端：按分区类型分发

`ReplicaManager.lookups()` 检查目标是否为 `__historical__` 分区：

- **普通分区**：同步查询本地 RocksDB（现有路径，不变）。
- **`__historical__` 分区**：将 lookup 提交给 `ioExecutor` 异步处理（线程模型见 C 节）。该 lookup 遵循与写路径旧值解析相同的状态链，以保证一致性：
  1.  **Prewrite buffer**：用组合 key（partitionName + originalKey）检查内存中的 prewrite buffer。如果找到，直接返回 value。
  2.  **Historical RocksDB**：用组合 key 检查 historical RocksDB。如果找到，直接返回 value。
  3.  **Lake fallback**：如果本地未找到，则 fallback 到 lake lookup：
      - 校验已配置 lake 存储；
      - 获取（或缓存）表级别的 `LakeTableLookuper`；
      - 使用请求中的 `partition_name` 标识 lake 存储中的原始分区。

这保证已经写入 `__historical__` 但尚未 tier 到 lake 的数据，仍然对 point lookup 可见。如果没有 local-first lookup，一次成功历史写入之后立即 lookup，可能返回陈旧数据或 null。

#### B.3 Paimon 实现：`PaimonLakeTableLookuper`

Paimon 专用实现使用 Paimon 的 `LocalTableQuery` 做高效 point lookup：

**线程安全：**

- `lookup()` 方法是 `synchronized` 的，用于保证共享可变状态（Paimon `LocalTableQuery`、文件刷新追踪、scan 结果缓存）的线程安全。来自 `ioExecutor` 的多个历史 lookup 请求可能并发访问同一个 `PaimonLakeTableLookuper` 实例。

**懒初始化：**

- 资源（Catalog、FileStoreTable、LocalTableQuery 等）在第一次 `lookup()` 调用时懒初始化。
- 当 schema 演进（schema ID 变化）时，关闭所有资源并重新创建，以反映新 schema。

**基于 snapshot 失效的按（partition, bucket）文件刷新：**

- 维护一个 `Map<PaimonPartitionBucket, Long> refreshedSnapshots`，将每个（partition BinaryRow, bucketId）对映射到上次文件刷新使用的 Paimon snapshot ID。
- 每次 lookup 时，将当前最新 Paimon snapshot ID 与该（partition, bucket）的缓存 snapshot ID 比较：
  - 如果没有缓存项，或者缓存 snapshot ID 已过期（也就是存在更新的 snapshot），则扫描 Paimon 数据文件，调用 `tableQuery.refreshFiles()`，并更新缓存 snapshot ID。
  - 如果缓存 snapshot ID 与最新 snapshot 一致，则跳过 scan。
- 这保证从 `__historical__` 新 tier 的数据（会产生新的 Paimon snapshot）对后续 lookup 可见，同时在没有新 snapshot 提交时避免重复文件扫描。
- **Scan 结果缓存**：完整表 scan 结果按 snapshot ID 缓存（`lastScannedSnapshotId`）。如果同一个 snapshot 中有多个（partition, bucket）对需要刷新，只执行一次 scan。

**Lookup 流程：**

1.  Key bytes 已经由客户端 `PaimonKeyEncoder` 编码为 Paimon BinaryRow 格式，因此可以通过 `keyRow.pointTo(MemorySegment.wrap(key), 0, key.length)` 直接包装为 Paimon `BinaryRow`，不需要 decode/re-encode。
2.  调用 `tableQuery.lookup(partition, bucketId, keyRow)`。
3.  如果找到，则通过 `PaimonRowAsFlussRow` adapter 将 Paimon 结果包装为 Fluss `InternalRow`。
4.  使用 `CompactedRowEncoder` + `ValueEncoder.encodeValue(binaryRow)` 编码 value。

### C. 实时路径与历史路径之间的性能隔离

历史分区操作涉及 lake I/O，延迟不可预测。如果不做隔离，这些慢操作会阻塞实时路径共享的资源。当前 Fluss 写路径在 RPC thread 上完全同步执行：`TabletService.putKv()` → `ReplicaManager.putRecordsToKv()` → `putToLocalKv()` → `KvTablet.putAsLeader()`。如果历史 PK 写入需要在 RPC thread 上执行 lake 旧值 lookup，就会阻塞其他写入，也会阻塞该线程上的所有 RPC 处理。

隔离策略分两层：

- **服务端**（C.2、C.3）：历史操作从 RPC thread offload 到专用 `ioExecutor`。实时路径不会使用 `ioExecutor`，因此没有资源竞争；完整的线程和队列容量都可用于历史操作。
- **客户端**（C.4）：对于 **lookup 路径**，`LookupSender` 将 inflight permit 拆成 realtime 和 historical 两个 semaphore，并通过可配置比例（`client.lookup.historical-inflight-ratio`，默认 0.1）控制，避免慢 lake lookup 耗尽与实时 lookup 共享的 permit。对于 **写路径**，客户端不设置比例上限；流控完全依赖服务端 semaphore（C.3），以及客户端在 throttle error 上的指数退避。

| Layer | Resource | Historical Cap | Mechanism |
|----|----|----|----|
| Server | `ioExecutor` thread pool | No cap needed — dedicated to historical (C.2) | Real-time paths never use ioExecutor |
| Server | Historical request semaphore | `maxQueued × server.historical-request-queue-ratio` (C.3) | `tryAcquire()` fails → `HISTORICAL_PARTITION_THROTTLED` |
| Client | Write throughput | Bounded by server semaphore (C.4.1) | Server throttle + client exponential backoff (100ms, 2×, max 5s) |
| Client | Lookup inflight permits | `maxLookupInflight × client.lookup.historical-inflight-ratio` (C.4.2) | Separate historical semaphore |

#### C.1 客户端：Batching 约束

客户端 sender/accumulator 层按以下约束批量聚合记录：

1.  **实时与历史分离**：实时分区和 `__historical__` 分区会被聚合到 **不同 batch** 中，并作为 **独立请求** 发送。这保证服务端收到的请求要么纯实时、要么纯历史，从而可以在请求级别清晰分发。

2.  **按原始分区对历史 PK 做 batching**：对于主键表，历史 put-kv 请求必须进一步按 `(historical bucket, original partition name)` 聚合。这是因为 `PbPutKvReqForBucket.partition_name` 是请求级字段，单个请求只能携带一个原始分区名。对于 upsert（`row != null`），服务端可以从 row 分区列提取原始分区；但对于 key-only delete（`row == null`），服务端完全依赖请求级 `partition_name`。把不同原始分区的 key-only delete 混入同一个请求，会使它们无法区分。

    因此，历史 PK 写入的 batching key 是：`(__historical__ bucket, original partition name)`；而普通 PK 写入只按 `(partition, bucket)` batching。

#### C.2 服务端：将历史操作 offload 到 IO Executor

`ReplicaManager` 使用共享 `ioExecutor`（有界线程池）将所有历史分区操作从 RPC thread offload 出去：

- **写路径**：`putRecordsToKv()` 检查目标是否为 `__historical__` 分区。如果是，整个写入处理会异步提交到 `ioExecutor`，RPC thread 立即释放。实时写入仍像以前一样在 RPC thread 上同步执行。
- **Lookup 路径**：`lookups()` 检查目标是否为 `__historical__` 分区。如果是，将 lookup 提交到 `ioExecutor` 执行异步 lake lookup。普通 lookup 仍像以前一样同步查询本地 RocksDB。

两条路径共享同一个 `ioExecutor`，因为它们都受 lake I/O 约束。共享线程池也为每个服务端的总 lake I/O 并发提供统一上限。

``` text
Real-time write:    RPC Thread → synchronous putToLocalKv() → RocksDB
Real-time lookup:   RPC Thread → synchronous local RocksDB query

Historical write:   RPC Thread → ioExecutor → putToLocalKv() → lake old-value lookup → historical RocksDB
Historical lookup:  RPC Thread → ioExecutor → Paimon LocalTableQuery
```

实时路径不受任何新 executor 或并发控制影响，仍然像以前一样完全在 RPC thread 上同步执行。

**按 bucket 的写入顺序保证**：

Offload 到 `ioExecutor` 的历史写入必须在同一个 `__historical__` bucket 内保持严格顺序。如果不保证顺序，不同延迟的并发 lake 旧值 lookup 可能导致后提交的写入先于先提交的写入完成，从而破坏 PK 状态和 changelog 顺序。

该保证通过 **按 bucket 串行执行** 实现：写入同一个 `__historical__` bucket 的任务会提交到 `ioExecutor`，但会按顺序处理。可以用 per-bucket 队列实现（例如按 table-bucket 作为 key，每个 key 的任务在线程池内按 FIFO 顺序执行）。不同 bucket 的写入仍然可以在 `ioExecutor` 线程之间并发执行。

Lookup 不要求顺序，可以不受 bucket 限制并发执行。

#### C.3 服务端：流控

历史操作异步 offload 到 `ioExecutor`，但 Netty 服务端请求队列由实时请求和历史请求共享。如果历史请求无限积压（例如因为 lake I/O 慢），可能填满共享请求队列，并导致实时请求被拒绝。为避免这种情况，服务端使用有界 `Semaphore`（`HistoricalPartitionHandler.requestPermits`）限制并发 in-flight 历史请求数量。每个请求通过 `tryAcquire()`（非阻塞）获取一个 permit，不论请求包含多少 bucket；所有子任务完成后释放 permit。容量按 `maxQueuedRequests * server.historical-request-queue-ratio` 计算（默认比例 0.1），为实时请求保留大部分队列容量。

当所有 permit 都被占用（`tryAcquire()` 返回 `false`）时，服务端用 `HISTORICAL_PARTITION_THROTTLED` 错误码拒绝整个请求。客户端收到该错误后执行 **指数退避重试**（初始 100ms，倍数 2×，最大 5s，jitter 0.2）。对于启用了幂等性的写路径，客户端还会把该 bucket 标记为 throttled，防止后续 batch 以乱序 sequence number 发送。

**异步写提交的防御性拷贝**：在将历史写入提交到 `ioExecutor` 前，服务端将 `KvRecordBatch` 拷贝到堆内存（`copyToHeap()`）。这样可以防止异步任务仍在处理时，RPC 层释放 Netty ByteBuf 导致 `IndexOutOfBoundsException`。

被 semaphore 接受的历史请求会分发给 `ioExecutor` 处理。Semaphore 和 `ioExecutor` 是两个独立关注点：semaphore 限制准入的历史请求数量，`ioExecutor` 决定并发处理数量。

#### C.4 客户端：历史资源上限

客户端将历史操作限制在总共享资源的大约 1/10。这样可以限制历史写入/lookup 对实时路径的影响，而不需要复杂分析每类资源。

##### C.4.1 写路径：服务端 throttle + 客户端 backoff

写路径以服务端 semaphore（C.3）作为主要流控机制，并结合客户端在 throttle error 上的指数退避：

1.  当 semaphore 满时，服务端拒绝历史写入，返回 `HISTORICAL_PARTITION_THROTTLED`。
2.  客户端 `Sender` 使用指数退避（`ExponentialBackoff(100, 2, 5000, 0.2)`）处理 throttle error：
    - 基于 backoff delay 在 batch 上设置 `retryAfterMs`。
    - 如果启用了幂等性，则通过 `idempotenceManager.markBucketThrottled()` 将该 bucket 标记为 throttled，防止后续 batch 从该 bucket drain 出来（否则会导致 `OutOfOrderSequenceException`）。
    - 在 backoff 时间后重新入队该 batch 以便重试。
3.  实时 batch 永远不受影响；throttle error 和 backoff 只作用于目标为 `__historical__` 分区的历史 batch。

##### C.4.2 Lookup 路径：历史 permit 上限

`LookupSender` 将单一 inflight semaphore 拆分为两个独立 semaphore。历史比例通过 `client.lookup.historical-inflight-ratio` 配置（默认 0.1）：

``` text
historicalInflight = max(1, (int)(maxLookupInflight * historicalInflightRatio))  // e.g., 13
realtimeLookupSemaphore   = new Semaphore(maxLookupInflight - historicalInflight) // e.g., 115
historicalLookupSemaphore = new Semaphore(historicalInflight)                     // e.g., 13
```

`LookupSender` 根据 lookup 类型获取对应 semaphore：

- 实时 lookup → `realtimeLookupSemaphore.acquire()`，只与其他实时 lookup 竞争。
- 历史 lookup → `historicalLookupSemaphore.acquire()`，只与其他历史 lookup 竞争。

历史 lookup 先按 `partitionName` 分组，再在每个分区组内按 `(leader, type)` 分组。这保证每个 RPC 请求都携带服务端组合 key 编码所需的正确 `partition_name`；不同过期分区（例如 `"2000"`、`"2001"`）可能重定向到同一个 `__historical__` bucket，但必须分开发送。

当 `LookupSender` 从服务端收到 `HISTORICAL_PARTITION_THROTTLED` 错误时，使用与写路径相同的指数退避策略（`ExponentialBackoff(100, 2, 5000, 0.2)`），在 lookup query 上设置 `retryAfterMs` 以延迟重试。

## 兼容性、弃用和迁移计划

### 之前的行为（本 FIP 之前）

在自动分区表上写入过期分区已经存在问题：

- 如果 `dynamicPartitionEnabled = true`：客户端会动态创建过期分区，但 `AutoPartitionManager` 会在下一轮 TTL 检查周期立即删除它，造成 create/drop 循环。
- 如果 `dynamicPartitionEnabled = false`：客户端直接抛出 `PartitionNotExistException`。
- 对过期分区做 point lookup 会返回 `null`，即使数据存在于 lake 存储中。

两种情况下，对过期分区写入或读取都无法产生正确结果。

### 新行为（本 FIP）

- 对过期分区的写入会被重定向到 `__historical__` 分区并成功。
- 对过期分区的 point lookup 会通过带 `partition_name` 的 `LookupRequest` 路由到 `__historical__` 分区，由服务端执行 local-first lookup（prewrite buffer → historical RocksDB → lake fallback），并返回正确 value。

### 兼容性

- **旧客户端 -> 新服务端**

  - 请求兼容性：旧客户端不会在 put-kv 或 lookup 请求中发送 `partition_name`；新服务端可以接受，因为该字段是可选字段。
  - 普通写入、delete 和 lookup 不变。
  - 旧客户端没有重定向到 `__historical__` 的逻辑，因此会退回到之前的行为（动态 create/drop 循环或 `PartitionNotExistException`）。

- **新客户端 -> 旧服务端**

  - 旧服务端不支持 `__historical__` 分区（没有 historical RocksDB，没有 lake 旧值 lookup，也没有 lake lookup 分发）。
  - 不需要特殊版本检查。新客户端会尝试重定向到 `__historical__`，但由于旧服务端不会创建或正确处理 `__historical__`，写入/lookup 会失败；这等价于此前过期分区写入本就不受支持的行为。
  - 非过期分区上的普通写入、delete 和 lookup 不受影响。

## 测试计划

- **集成测试**
  - 写入过期分区会被重定向到 `__historical__` 分区。
  - 历史 key 上的 update 通过从 lake 取回旧值生成 `UPDATE_BEFORE + UPDATE_AFTER`。
  - 日志表对过期分区的写入会被重定向到 `__historical__` 分区，并且可以从 `__historical__` bucket 被下游消费。
  - 从过期分区 lookup 仍然可以正确得到 value。
  - 重启/恢复后行为保持不变。

## 被拒绝的替代方案

N/A
