# PR 7b: 服务端 composite key encoding for historical PK writes

## Context

PR7a 已将 `partition_name`（原始分区名）从客户端通过 `PbPutKvReqForBucket.partition_name` 发送到服务端。但服务端目前 **未读取** 该字段，也 **未使用** `CompositeKeyEncoder`。

**当前问题**：多个过期分区（如 "2000" 和 "2001"）的数据都写入 `__historical__` 的同一个 RocksDB，使用原始 key。如果两个分区有相同 PK 值，它们在 RocksDB 中会 **冲突**。

**本 PR 目标**：在服务端读取 `partition_name`，使用 `CompositeKeyEncoder.encode(partitionName, rawKey)` 对 key 加上分区前缀，实现隔离。

**不在本 PR 范围**：lake fallback old-value lookup（PR7c）。

---

## 改动范围

| # | 文件 | 操作 | 说明 |
|---|------|------|------|
| 1 | `fluss-server/.../utils/ServerRpcMessageUtils.java` | 修改 | 新增 `getPartitionNames()` 提取 `partition_name` |
| 2 | `fluss-server/.../tablet/TabletService.java` | 修改 | 调用 `getPartitionNames()` 并传递给 ReplicaManager |
| 3 | `fluss-server/.../replica/ReplicaManager.java` | 修改 | `putRecordsToKv()` / `putToLocalKv()` 传递 partitionNames |
| 4 | `fluss-server/.../replica/Replica.java` | 修改 | `putRecordsToLeader()` 增加 `@Nullable String partitionName` 参数 |
| 5 | `fluss-server/.../kv/KvTablet.java` | 修改 | `putAsLeader()` / `processKvRecords()` 应用 CompositeKeyEncoder；新增 `isHistoricalPartition` 字段 |
| 6 | `fluss-client/.../table/HistoricalPartitionTableITCase.java` | 扩展 | 新增多分区隔离测试 |

---

## Step 1: ServerRpcMessageUtils — 提取 partition_name

**文件**: `fluss-server/src/main/java/org/apache/fluss/server/utils/ServerRpcMessageUtils.java`

在 `getPutKvData()` 方法附近新增方法：

```java
/**
 * Extracts partition_name per bucket from a PutKvRequest.
 * Returns null if no bucket has partition_name set.
 */
@Nullable
public static Map<TableBucket, String> getPartitionNames(PutKvRequest putKvRequest) {
    long tableId = putKvRequest.getTableId();
    Map<TableBucket, String> partitionNames = null;
    for (PbPutKvReqForBucket req : putKvRequest.getBucketsReqsList()) {
        if (req.hasPartitionName()) {
            if (partitionNames == null) {
                partitionNames = new HashMap<>();
            }
            TableBucket tb =
                    new TableBucket(
                            tableId,
                            req.hasPartitionId() ? req.getPartitionId() : null,
                            req.getBucketId());
            partitionNames.put(tb, req.getPartitionName());
        }
    }
    return partitionNames;
}
```

**设计要点**：对非历史写入请求（partition_name 全部未设置），返回 null 避免创建空 Map。

---

## Step 2: TabletService — 传递 partitionNames

**文件**: `fluss-server/src/main/java/org/apache/fluss/server/tablet/TabletService.java`

修改 `putKv()` 方法，在提取 putKvData 后提取 partitionNames，传递给 replicaManager：

```java
Map<TableBucket, KvRecordBatch> putKvData = getPutKvData(request);
Map<TableBucket, String> partitionNames = getPartitionNames(request);  // 新增
...
replicaManager.putRecordsToKv(
        ..., putKvData, partitionNames, getTargetColumns(request), ...);
```

---

## Step 3: ReplicaManager — 传递 partitionNames

**文件**: `fluss-server/src/main/java/org/apache/fluss/server/replica/ReplicaManager.java`

### 3a. putRecordsToKv() 增加参数

方法签名增加 `@Nullable Map<TableBucket, String> partitionNames`，实时路径和历史路径都将 `partitionNames` 传递给 `putToLocalKv()`。

### 3b. putToLocalKv() 增加参数

在循环内提取 per-bucket partition_name，传递给 `replica.putRecordsToLeader()`：

```java
String partitionName = partitionNames != null ? partitionNames.get(tb) : null;
replica.putRecordsToLeader(entry.getValue(), targetColumns, mergeMode, requiredAcks, partitionName);
```

---

## Step 4: Replica — 传递 partitionName

**文件**: `fluss-server/src/main/java/org/apache/fluss/server/replica/Replica.java`

`putRecordsToLeader()` 增加 `@Nullable String partitionName` 参数，传递给 `kv.putAsLeader()`。

---

## Step 5: KvTablet — CompositeKeyEncoder 编码

**文件**: `fluss-server/src/main/java/org/apache/fluss/server/kv/KvTablet.java`

### 5a. isHistoricalPartition 字段

KvTablet 构造时通过 `PartitionUtils.isHistoricalPartitionName()` 判断自身是否属于历史分区，存储为 `final boolean isHistoricalPartition` 字段。

### 5b. putAsLeader() 增加参数

增加 `@Nullable String partitionName`，传递给 `processKvRecords()`。2-arg 便捷重载传 null。

### 5c. processKvRecords() — 核心编码逻辑

增加 `@Nullable String partitionName` 参数。使用 `isHistoricalPartition` 字段判断是否需要 composite key 编码：

```java
byte[] rawKeyBytes = BytesUtils.toArray(kvRecord.getKey());
byte[] keyBytes =
        isHistoricalPartition
                ? CompositeKeyEncoder.encode(partitionName, rawKeyBytes)
                : rawKeyBytes;
KvPreWriteBuffer.Key key = KvPreWriteBuffer.Key.of(keyBytes);
```

**关键**：判断条件基于 tablet 自身身份（`isHistoricalPartition`），而非参数是否为 null，语义更直接。composite key 用于 pre-write buffer 和 RocksDB 的所有操作，无需改动下游方法。

---

## Step 6: 集成测试 — 多分区隔离

**文件**: `fluss-client/src/test/java/org/apache/fluss/client/table/HistoricalPartitionTableITCase.java`

新增 `testPkTableWriteMultipleExpiredPartitions()`：写同一 PK=0 到两个不同过期分区 "2000" 和 "2001"，验证 historical 分区产生 2 条 +I（而非 1 条覆盖产生 -U/+U）。

---

## 无需改动的组件

| 组件 | 为什么无需改动 |
|------|--------------|
| **CompositeKeyEncoder** | PR3 已实现，直接复用 |
| **getFromBufferOrKv()** | 接收的 key 已是 composite key，无需感知 |
| **processUpsert() / processDeletion()** | 同上，key 已在 processKvRecords 中编码 |
| **KvPreWriteBuffer** | 存储 composite key，无需改动 |
| **HistoricalPartitionHandler** | 只负责流控和调度，不关心 key 编码 |
| **客户端** | PR7a 已完成 partition_name 发送 |
