# PR 8: 历史分区点查路径 + LookupSender inflight 信号量拆分

## Context

PR 7c 已完成写入路径的 lake fallback：写入过期分区时，server 通过 prewrite buffer → RocksDB → Paimon 链路获取旧值。但 **点查（lookup）路径尚未支持过期分区**——当前 `PrimaryKeyLookuper` 遇到 `PartitionNotExistException` 直接返回空结果。

本 PR 目标：
1. **客户端**：检测过期分区，重定向 lookup 到 `__historical__`，传递 `partition_name`
2. **客户端**：拆分 `LookupSender` 的全局 inflight 信号量为 realtime/historical 两个独立信号量，避免慢 lake I/O 饿死实时 lookup
3. **服务端**：在 `Replica.lookups()` 和 `KvTablet` 中支持历史分区 lookup 的 local-first 链路（prewrite buffer → RocksDB → lake fallback）
4. **RPC**：`toLookupData()` 提取 `partition_name` 并传递给 `ReplicaManager`

---

## 现状分析

### 客户端 lookup 流程（当前）

```
PrimaryKeyLookuper.lookup(key)
  → getPartitionId(key) → PartitionNotExistException → 返回空 ❌
```

### 服务端 lookup 流程（当前）

```
ReplicaManager.lookups()
  → isHistoricalRequest() 检查已存在
  → historicalPartitionHandler.submitLookup() 调度已存在
  → Replica.lookups() → kvTablet.multiGet(keys) → 只查 RocksDB ❌ (没有 lake fallback)
```

### 关键已有基础设施

| 组件 | 状态 |
|------|------|
| `PbLookupReqForBucket.partition_name` (field 4) | proto 已有 ✅ |
| `ReplicaManager.isHistoricalRequest()` | 已实现 ✅ |
| `HistoricalPartitionHandler.submitLookup()` | 已实现 ✅ |
| `KvTablet.lookupFromLake()` | 已实现 ✅（用于 write 的 old-value） |
| `DynamicPartitionCreator.isExpiredPartition()` | 已实现 ✅ |
| `CompositeKeyEncoder.encode/decode` | 已实现 ✅ |

---

## 改动范围

| # | 文件 | 操作 | 说明 |
|---|------|------|------|
| 1 | `fluss-client/.../lookup/PrimaryKeyLookuper.java` | 修改 | 过期分区检测 + 重定向到 `__historical__` |
| 2 | `fluss-client/.../lookup/LookupSender.java` | 修改 | 拆分 inflight 信号量 |
| 3 | `fluss-client/.../lookup/LookupBatch.java` | 修改 | 增加 `partitionName` 字段 |
| 4 | `fluss-client/.../lookup/AbstractLookupQuery.java` | 修改 | 增加 `partitionName` 字段标记历史查询 |
| 5 | `fluss-client/.../utils/ClientRpcMessageUtils.java` | 修改 | `makeLookupRequest()` 设置 `partition_name` |
| 6 | `fluss-server/.../utils/ServerRpcMessageUtils.java` | 修改 | `toLookupData()` 提取 `partition_name` |
| 7 | `fluss-server/.../tablet/TabletService.java` | 修改 | lookup 路径传递 `partitionNames` |
| 8 | `fluss-server/.../replica/ReplicaManager.java` | 修改 | `lookups()` 接收并传递 `partitionNames` |
| 9 | `fluss-server/.../replica/Replica.java` | 修改 | `lookups()` 支持 `partitionName` 参数 |
| 10 | `fluss-server/.../kv/KvTablet.java` | 修改 | 新增 `multiGetWithLakeFallback()` |
| 11 | `fluss-common/.../config/ConfigOptions.java` | 修改 | 新增 historical lookup permits 配置 |
| 12 | `fluss-client/...test.../HistoricalPartitionTableITCase.java` | 修改 | 新增 lookup 测试 |

---

## 详细设计

### 1. 客户端：PrimaryKeyLookuper 过期分区重定向

**文件**: `fluss-client/src/main/java/org/apache/fluss/client/lookup/PrimaryKeyLookuper.java`

当前逻辑（:106-117）捕获 `PartitionNotExistException` 并返回空。需要改为：

```java
public CompletableFuture<LookupResult> lookup(InternalRow lookupKey) {
    byte[] pkBytes = primaryKeyEncoder.encodeKey(lookupKey);
    byte[] bkBytes = ...;
    Long partitionId = null;
    String originalPartitionName = null;  // 新增

    if (partitionGetter != null) {
        String partitionName = partitionGetter.getPartition(lookupKey);
        try {
            partitionId = getPartitionId(lookupKey, partitionGetter, tableInfo.getTablePath(), metadataUpdater);
        } catch (PartitionNotExistException e) {
            // 检查是否为过期分区 + data-lake-enabled
            if (isExpiredPartition(partitionName, partitionKeys, autoPartitionStrategy, isDataLakeEnabled)) {
                // 重定向到 __historical__ 分区
                originalPartitionName = partitionName;
                String historicalName = buildHistoricalPartitionName(
                    partitionName, partitionKeys, autoPartitionStrategy);
                PhysicalTablePath historicalPath = PhysicalTablePath.of(tableInfo.getTablePath(), historicalName);
                // 确保 __historical__ 存在（复用 DynamicPartitionCreator 的逻辑）
                ensureHistoricalPartitionExists(historicalPath, partitionKeys, historicalName);
                partitionId = getHistoricalPartitionId(historicalPath);
                // 对 key 做 composite encoding
                pkBytes = CompositeKeyEncoder.encode(partitionName, pkBytes);
            } else {
                return CompletableFuture.completedFuture(new LookupResult(Collections.emptyList()));
            }
        }
    }

    int bucketId = bucketingFunction.bucketing(bkBytes, numBuckets);
    TableBucket tableBucket = new TableBucket(tableInfo.getTableId(), partitionId, bucketId);

    // 传递 originalPartitionName 以便 LookupSender 设置 partition_name RPC 字段
    // 并用于信号量选择
    lookupClient.lookup(tableInfo.getTablePath(), tableBucket, pkBytes,
            insertIfNotExists, originalPartitionName)
        .whenComplete(...);
}
```

**需要注入的依赖**（从 `TableInfo` 获取，参照写入路径 `DynamicPartitionCreator`）：
- `partitionKeys`: `tableInfo.getPartitionKeys()`
- `autoPartitionStrategy`: 从 `TableConfig` 构建
- `isDataLakeEnabled`: `tableInfo.getTableConfig().getDataLakeFormat().isPresent()`

**确保 `__historical__` 存在**：
- 复用 `DynamicPartitionCreator.createHistoricalPartitionIfNeeded()` 的逻辑
- 或者在 `PrimaryKeyLookuper` 中持有 `Admin` 引用直接创建
- 推荐：在 `PrimaryKeyLookuper` 中新增 `HistoricalPartitionResolver` 内部类（或独立类），封装过期分区检测 + `__historical__` 创建 + composite key encoding，供 lookup 和后续其他路径复用

### 2. 客户端：LookupClient / LookupQuery 增加 partitionName

**文件**: `fluss-client/src/main/java/org/apache/fluss/client/lookup/AbstractLookupQuery.java`

```java
public abstract class AbstractLookupQuery<T> {
    private final TablePath tablePath;
    private final TableBucket tableBucket;
    private final byte[] key;
    @Nullable private final String partitionName;  // 新增：原始分区名，null 表示实时查询
    private int retries;

    // partitionName != null → 历史查询
    public boolean isHistorical() {
        return partitionName != null;
    }
}
```

**文件**: `fluss-client/src/main/java/org/apache/fluss/client/lookup/LookupQuery.java`

构造函数增加 `@Nullable String partitionName` 参数。

**文件**: `fluss-client/src/main/java/org/apache/fluss/client/lookup/LookupClient.java`

```java
public CompletableFuture<byte[]> lookup(
        TablePath tablePath, TableBucket tableBucket, byte[] keyBytes,
        boolean insertIfNotExists, @Nullable String partitionName) {
    LookupQuery lookup = new LookupQuery(tablePath, tableBucket, keyBytes,
            insertIfNotExists, partitionName);
    lookupQueue.appendLookup(lookup);
    return lookup.future();
}
```

保留原 `lookup(TablePath, TableBucket, byte[], boolean)` 签名，转发 `partitionName=null`。

### 3. 客户端：LookupBatch 携带 partitionName

**文件**: `fluss-client/src/main/java/org/apache/fluss/client/lookup/LookupBatch.java`

```java
public class LookupBatch {
    private final TableBucket tableBucket;
    @Nullable private final String partitionName;  // 新增
    private final List<LookupQuery> lookups;

    public LookupBatch(TableBucket tableBucket, @Nullable String partitionName) {
        this.tableBucket = tableBucket;
        this.partitionName = partitionName;
        this.lookups = new ArrayList<>();
    }
}
```

### 4. 客户端：LookupSender 信号量拆分

**文件**: `fluss-client/src/main/java/org/apache/fluss/client/lookup/LookupSender.java`

**当前**（:76, :93）:

```java
private final Semaphore maxInFlightReuqestsSemaphore;
// → new Semaphore(128)
```

**改为两个独立信号量**:

```java
private final Semaphore realtimeSemaphore;
private final Semaphore historicalSemaphore;

LookupSender(..., int maxFlightRequests, int historicalFlightRequests, ...) {
    this.realtimeSemaphore = new Semaphore(maxFlightRequests - historicalFlightRequests);
    this.historicalSemaphore = new Semaphore(historicalFlightRequests);
}
```

**信号量选择逻辑** — 在 `sendLookupRequestAndHandleResponse()` 中：

```java
private void sendLookupRequestAndHandleResponse(
        int destination, TabletServerGateway gateway, LookupRequest lookupRequest,
        long tableId, Map<TableBucket, LookupBatch> lookupsByBucket, boolean historical) {
    Semaphore semaphore = historical ? historicalSemaphore : realtimeSemaphore;
    try {
        semaphore.acquire();
    } catch (InterruptedException e) { ... }
    gateway.lookup(lookupRequest)
        .thenAccept(resp -> { try { handleLookupResponse(...); } finally { semaphore.release(); } })
        .exceptionally(e -> { try { handleLookupRequestException(...); return null; } finally { semaphore.release(); } });
}
```

**批次分组** — 在 `sendLookupRequest()` 中，需要按 `(tableId, partitionName)` 分组 batch：

```java
private void sendLookupRequest(int destination, List<AbstractLookupQuery<?>> lookups, boolean insertIfNotExists) {
    // 按 tableId 分组
    Map<Long, Map<TableBucket, LookupBatch>> lookupByTableId = new HashMap<>();
    for (AbstractLookupQuery<?> q : lookups) {
        LookupQuery lookup = (LookupQuery) q;
        TableBucket tb = lookup.tableBucket();
        long tableId = tb.getTableId();
        lookupByTableId
            .computeIfAbsent(tableId, k -> new HashMap<>())
            .computeIfAbsent(tb, k -> new LookupBatch(tb, lookup.partitionName()))
            .addLookup(lookup);
    }
    // 发送时判断 historical
    lookupByTableId.forEach((tableId, lookupsByBucket) -> {
        boolean historical = lookupsByBucket.values().stream()
            .anyMatch(batch -> batch.partitionName() != null);
        sendLookupRequestAndHandleResponse(
            destination, gateway, makeLookupRequest(tableId, lookupsByBucket.values(), ...),
            tableId, lookupsByBucket, historical);
    });
}
```

**关键约束**：`partition_name` 是 `PbLookupReqForBucket` 级别的字段（每个 bucket 一个），同一个 bucket 的所有 key 共享同一个 `partition_name`。客户端保证同一请求不混合 historical/realtime bucket。

**分组策略** — 在 `groupByLeaderAndType()` 中增加 historical 维度：

当前按 `<leader, LookupType>` 分组。需要进一步按 `historical` 拆分，确保 historical 和 realtime 的 lookup 不在同一批次中，这样信号量选择才正确。

可以在 key 中加入 `isHistorical` flag：`Tuple2<Integer, LookupType>` → 增加考虑 `lookup.isHistorical()` 作为分组维度。简单做法：在 `sendLookups()` 方法中，先按 `isHistorical()` 拆分两个子列表再分别调 `groupByLeaderAndType` 并 dispatch。

### 5. 客户端：ClientRpcMessageUtils 设置 partition_name

**文件**: `fluss-client/src/main/java/org/apache/fluss/client/utils/ClientRpcMessageUtils.java`

`makeLookupRequest()` (:206-229) 中增加 `partition_name` 设置：

```java
batch.lookups().forEach(get -> pbLookupReqForBucket.addKey(get.key()));
// 新增：设置 partition_name
if (batch.partitionName() != null) {
    pbLookupReqForBucket.setPartitionName(batch.partitionName());
}
```

### 6. 服务端：ServerRpcMessageUtils 提取 partition_name

**文件**: `fluss-server/src/main/java/org/apache/fluss/server/utils/ServerRpcMessageUtils.java`

参照 `getPartitionNames(PutKvRequest)` (:1078-1095) 的实现，新增：

```java
public static Map<TableBucket, String> getLookupPartitionNames(LookupRequest lookupRequest) {
    long tableId = lookupRequest.getTableId();
    Map<TableBucket, String> partitionNames = null;
    for (PbLookupReqForBucket req : lookupRequest.getBucketsReqsList()) {
        if (req.hasPartitionName()) {
            if (partitionNames == null) {
                partitionNames = new HashMap<>();
            }
            TableBucket tb = new TableBucket(tableId,
                    req.hasPartitionId() ? req.getPartitionId() : null,
                    req.getBucketId());
            partitionNames.put(tb, req.getPartitionName());
        }
    }
    return partitionNames;
}
```

### 7. 服务端：TabletService 传递 partitionNames

**文件**: `fluss-server/src/main/java/org/apache/fluss/server/tablet/TabletService.java`

`lookup()` 方法（:277-303）中提取并传递 `partitionNames`：

```java
public CompletableFuture<LookupResponse> lookup(LookupRequest request) {
    Map<TableBucket, List<byte[]>> lookupData = toLookupData(request);
    Map<TableBucket, String> partitionNames = getLookupPartitionNames(request);  // 新增
    // ... 传递给 replicaManager.lookups(lookupData, partitionNames, ...)
}
```

### 8. 服务端：ReplicaManager.lookups() 传递 partitionNames

**文件**: `fluss-server/src/main/java/org/apache/fluss/server/replica/ReplicaManager.java`

`lookups()` 方法签名增加 `@Nullable Map<TableBucket, String> partitionNames`：

```java
public void lookups(
        Map<TableBucket, List<byte[]>> entriesPerBucket,
        @Nullable Map<TableBucket, String> partitionNames,  // 新增
        short apiVersion,
        Consumer<Map<TableBucket, LookupResultForBucket>> responseCallback) {
```

在 `processLookupsSync()` 中传递 `partitionName` 到 `Replica.lookups()`：

```java
private Map<TableBucket, LookupResultForBucket> processLookupsSync(
        Map<TableBucket, List<byte[]>> entriesPerBucket,
        @Nullable Map<TableBucket, String> partitionNames,
        short apiVersion) {
    for (Map.Entry<TableBucket, List<byte[]>> entry : entriesPerBucket.entrySet()) {
        TableBucket tb = entry.getKey();
        String partitionName = partitionNames != null ? partitionNames.get(tb) : null;
        List<byte[]> values = replica.lookups(entry.getValue(), partitionName);
        // ...
    }
}
```

### 9. 服务端：Replica.lookups() 支持 partitionName

**文件**: `fluss-server/src/main/java/org/apache/fluss/server/replica/Replica.java`

`lookups()` (:1314-1341) 增加 `partitionName` 参数：

```java
public List<byte[]> lookups(List<byte[]> keys, @Nullable String partitionName) {
    // ...
    if (partitionName != null) {
        // 历史分区：使用 local-first 链路
        return kvTablet.multiGetWithLakeFallback(keys, partitionName);
    } else {
        return kvTablet.multiGet(keys);
    }
}
```

保留原 `lookups(List<byte[]>)` 签名，转发 `partitionName=null`。

### 10. 服务端：KvTablet.multiGetWithLakeFallback()

**文件**: `fluss-server/src/main/java/org/apache/fluss/server/kv/KvTablet.java`

新增方法，复用已有的 `getOldValue()` (:770-793) 和 `lookupFromLake()` (:797-812) 逻辑：

```java
/**
 * Multi-get with lake fallback for historical partition lookups.
 * Lookup chain: prewrite buffer → RocksDB → lake fallback.
 */
public List<byte[]> multiGetWithLakeFallback(List<byte[]> keys, String partitionName) {
    return inReadLock(kvLock, () -> {
        rocksDBKv.checkIfRocksDBClosed();
        List<byte[]> results = new ArrayList<>(keys.size());
        for (byte[] key : keys) {
            byte[] value = getOldValue(new KvPreWriteBuffer.Key(key), partitionName);
            results.add(value);
        }
        return results;
    });
}
```

`getOldValue()` 已实现完整链路：prewrite buffer → RocksDB → `lookupFromLake()` (decode composite key, 查 Paimon)。直接复用。

### 11. 配置项

**文件**: `fluss-common/src/main/java/org/apache/fluss/config/ConfigOptions.java`

```java
public static final ConfigOption<Double> CLIENT_LOOKUP_HISTORICAL_INFLIGHT_RATIO =
        key("client.lookup.historical-inflight-ratio")
                .doubleType()
                .defaultValue(0.1)  // 历史查询占总 inflight 的 10%
                .withDescription(
                        "The ratio of max inflight lookup requests reserved for "
                                + "historical partition lookups. This isolates slow lake "
                                + "fallback lookups from real-time lookups. "
                                + "For example, with max-inflight-requests=128 and ratio=0.1, "
                                + "13 permits are reserved for historical lookups and 115 for real-time.");
```

与服务端 `server.historical-request-queue-ratio`（默认 0.1）风格一致。

`LookupClient` 构造函数中计算并传给 `LookupSender`：

```java
int totalInflight = conf.getInt(ConfigOptions.CLIENT_LOOKUP_MAX_INFLIGHT_SIZE);        // 128
double ratio = conf.get(ConfigOptions.CLIENT_LOOKUP_HISTORICAL_INFLIGHT_RATIO);         // 0.1
int historicalInflight = Math.max(1, (int) (totalInflight * ratio));                     // 13
this.lookupSender = new LookupSender(
        metadataUpdater, lookupQueue,
        totalInflight, historicalInflight,
        ...);
```

`LookupSender` 中：
- `realtimeSemaphore = new Semaphore(totalInflight - historicalInflight)` → 115
- `historicalSemaphore = new Semaphore(historicalInflight)` → 13

---

## 测试

**文件**: `fluss-client/src/test/java/org/apache/fluss/client/table/HistoricalPartitionTableITCase.java`

新增测试：

### testHistoricalLookupFromLake

```
1. 创建 lake-enabled 分区 PK 表
2. 写入 active partition "2019": upsert(1, "val", "2019")
3. 使 "2019" 过期 (alter retention + drop partition)
4. lookup(PK=1, partition="2019")
   → 客户端检测过期 → 重定向到 __historical__ → composite key
   → 服务端：prewrite buffer miss → RocksDB miss → lake fallback
   → 返回 "val"（注意：此处使用 mock lake，可能返回 null）
5. lookup(PK=999, partition="2019")
   → lake miss → 返回 null
```

注意：`HistoricalPartitionTableITCase` 使用 `TestingPaimonStoragePlugin`（mock），`NullLakeTableLookuper` 始终返回 null。所以测试主要验证：
- 客户端正确重定向到 `__historical__`
- composite key encoding 正确
- 服务端走 historical lookup 路径不报错
- 真正的 lake fallback 验证在 `HistoricalPartitionPaimonITCase`（E2E with real Paimon）中补充

### testHistoricalLookupSemaphoreIsolation

验证信号量拆分不会导致实时 lookup 被饿死（可选，需要复杂的并发测试设置）。

---

## 验证步骤

```bash
# 1. 格式化
./mvnw spotless:apply -pl fluss-client,fluss-server,fluss-common -q

# 2. 编译
./mvnw test-compile -pl fluss-client,fluss-server -am -q

# 3. 运行客户端测试
./mvnw test -Dtest=HistoricalPartitionTableITCase -pl fluss-client

# 4. 运行 lookup 相关单元测试
./mvnw test -Dtest=LookupSenderTest -pl fluss-client

# 5. 运行 E2E 测试（验证真正的 lake fallback）
./mvnw test -Dtest=HistoricalPartitionPaimonITCase -pl fluss-lake/fluss-lake-paimon
```
