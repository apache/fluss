# PR6: Log 表历史写入路径

## Context

FIP-28 需要 Log 表对过期分区的写入能 redirect 到 `__historical__` 并被下游正确消费。

**当前问题**: PR2 已实现 `DynamicPartitionCreator` 在检测到过期分区时异步创建 `__historical__` 分区，但 `checkAndCreatePartitionAsync()` 返回 `void`，WriteRecord 仍指向原始（不存在的）分区。Sender 找不到该分区的 leader，写入会一直在 `unknownLeaderTables` 循环中挂起。

**解决方案**: 让 `checkAndCreatePartitionAsync()` 返回实际写入路径。当检测到过期分区时，返回 `__historical__` 的 `PhysicalTablePath`；`WriterClient.doSend()` 据此重定向 WriteRecord。

**设计要点**:
- Redirect 在 `DynamicPartitionCreator` + `WriterClient` 层完成，仅当分区不存在 AND 已过期时才 redirect（分区仍存在则正常写入）
- Row payload 不变，原始分区列（如 `"20230101"`）保留在 row 中，consumer 从 row 提取原始分区身份
- RecordAccumulator / Sender / BucketAssigner / 服务端均无需改动

## 改动范围

| # | 文件 | 操作 | 说明 |
|---|------|------|------|
| 1 | `fluss-client/.../write/WriteRecord.java` | 修改 | 新增 `withPhysicalTablePath()` 方法 |
| 2 | `fluss-client/.../write/DynamicPartitionCreator.java` | 修改 | 返回值从 `void` → `PhysicalTablePath`，增加 redirect cache |
| 3 | `fluss-client/.../write/WriterClient.java` | 修改 | 使用返回路径，redirect WriteRecord |
| 4 | `fluss-client/.../write/DynamicPartitionCreatorTest.java` | 新建 | redirect 单元测试 |
| 5 | `fluss-client/...ITCase` | 新建 | 端到端集成测试 |

**无需改动**: RecordAccumulator（PhysicalTablePath 天然分离 batch）、Sender（独立发送）、BucketAssigner（per-path 缓存）、服务端（正常 replication）

---

## Step 1: WriteRecord.withPhysicalTablePath()

**文件**: `fluss-client/src/main/java/org/apache/fluss/client/write/WriteRecord.java`

在 class 末尾（约 line 300）添加 package-private 方法：

```java
/**
 * Creates a copy of this record with a different physical table path. Used for redirecting
 * expired partition writes to the historical partition.
 */
WriteRecord withPhysicalTablePath(PhysicalTablePath newPath) {
    if (newPath.equals(this.physicalTablePath)) {
        return this;
    }
    return new WriteRecord(
            this.tableInfo,
            newPath,
            this.key,
            this.bucketKey,
            this.row,
            this.writeFormat,
            this.targetColumns,
            this.estimatedSizeInBytes,
            this.mergeMode);
}
```

---

## Step 2: DynamicPartitionCreator 返回实际路径

**文件**: `fluss-client/src/main/java/org/apache/fluss/client/write/DynamicPartitionCreator.java`

### 2a. 新增 redirect cache 字段

```java
private final ConcurrentMap<PhysicalTablePath, PhysicalTablePath> expiredPartitionRedirects =
        MapUtils.newConcurrentMap();
```

### 2b. 修改 checkAndCreatePartitionAsync() 签名和实现

返回值从 `void` → `PhysicalTablePath`。重构 `!idExist` 分支，在 `forceCheckPartitionExist()` 之前插入 cache 检查：

```java
public PhysicalTablePath checkAndCreatePartitionAsync(
        PhysicalTablePath physicalTablePath,
        List<String> partitionKeys,
        AutoPartitionStrategy autoPartitionStrategy,
        boolean isDataLakeEnabled) {
    String partitionName = physicalTablePath.getPartitionName();
    if (partitionName == null) {
        return physicalTablePath;
    }

    Optional<Long> partitionIdOpt = metadataUpdater.getPartitionId(physicalTablePath);
    boolean idExist = partitionIdOpt.isPresent();
    if (!idExist) {
        if (inflightPartitionsToCreate.contains(physicalTablePath)) {
            LOG.debug("Partition {} is already being created, skipping.", physicalTablePath);
            return physicalTablePath;
        }
        // Fast path: check redirect cache before server roundtrip
        PhysicalTablePath cachedRedirect = expiredPartitionRedirects.get(physicalTablePath);
        if (cachedRedirect != null) {
            return cachedRedirect;
        }
        if (forceCheckPartitionExist(physicalTablePath)) {
            LOG.debug("Partition {} already exists, skipping.", physicalTablePath);
            return physicalTablePath;
        }
        if (isExpiredPartition(
                partitionName, partitionKeys, autoPartitionStrategy, isDataLakeEnabled)) {
            createHistoricalPartitionIfNeeded(
                    physicalTablePath, partitionKeys, autoPartitionStrategy);
            String historicalName =
                    buildHistoricalPartitionName(
                            partitionName, partitionKeys, autoPartitionStrategy);
            PhysicalTablePath historicalPath =
                    PhysicalTablePath.of(physicalTablePath.getTablePath(), historicalName);
            expiredPartitionRedirects.put(physicalTablePath, historicalPath);
            return historicalPath;
        }
        if (dynamicPartitionEnabled) {
            // ... 原有逻辑不变 ...
            return physicalTablePath;
        }
        throw new PartitionNotExistException(...);
    }
    return physicalTablePath;
}
```

**关键优化**: `expiredPartitionRedirects` cache 避免对同一过期分区反复调用 `forceCheckPartitionExist()`（server roundtrip）。cache 放在 `forceCheckPartitionExist()` 之前检查。

---

## Step 3: WriterClient.doSend() 处理重定向

**文件**: `fluss-client/src/main/java/org/apache/fluss/client/write/WriterClient.java`

修改 `doSend()` 方法（line 173）：

```java
private void doSend(WriteRecord record, WriteCallback callback) {
    try {
        throwIfWriterClosed();
        TableInfo tableInfo = record.getTableInfo();
        PhysicalTablePath physicalTablePath = record.getPhysicalTablePath();

        // 返回实际路径（过期分区 redirect 到 __historical__）
        PhysicalTablePath actualPath =
                dynamicPartitionCreator.checkAndCreatePartitionAsync(
                        physicalTablePath,
                        tableInfo.getPartitionKeys(),
                        tableInfo.getTableConfig().getAutoPartitionStrategy(),
                        tableInfo.getTableConfig().isDataLakeEnabled());

        // 路径变化则重建 WriteRecord
        if (!actualPath.equals(physicalTablePath)) {
            record = record.withPhysicalTablePath(actualPath);
        }

        Cluster cluster = metadataUpdater.getCluster();
        BucketAssigner bucketAssigner =
                bucketAssignerMap.computeIfAbsent(
                        actualPath,  // 使用 actualPath
                        k -> createBucketAssigner(tableInfo, actualPath, conf));

        int bucketId = bucketAssigner.assignBucket(record.getBucketKey(), cluster);
        // 后续逻辑不变 ...
    }
}
```

---

## Step 4: 测试

### DynamicPartitionCreatorTest（单元测试）

Mock `MetadataUpdater` 和 `Admin`，测试：

| 测试方法 | 验证目标 |
|---------|---------|
| `testNonPartitionedTableReturnsOriginalPath` | partitionName=null → 返回原路径 |
| `testExistingPartitionReturnsOriginalPath` | 分区存在 → 返回原路径 |
| `testExpiredPartitionReturnsHistoricalPath` | 分区不存在且过期 → 返回 `__historical__` 路径 |
| `testExpiredPartitionRedirectCached` | 同一过期分区第二次调用 → 跳过 forceCheck，直接返回缓存 |
| `testNonExpiredNonExistentThrows` | 分区不存在且未过期 → 抛 PartitionNotExistException |
| `testMultiKeyPartitionRedirect` | 多分区键 `us-east$20230101` → redirect 到 `us-east$__historical__` |

### 集成测试（ITCase）

创建 data-lake-enabled + auto-partition 的 Log 表，测试：

| 测试方法 | 验证目标 |
|---------|---------|
| `testWriteExpiredPartitionRedirects` | 写入过期分区 → 数据进入 `__historical__` |
| `testWriteExistingPartitionNotRedirected` | 写入现有分区 → 正常写入 |
| `testWriteNonExpiredNonExistentThrows` | 非过期不存在分区 → 抛异常 |
| `testFlushWaitsForHistoricalBatches` | flush() 等待所有 batch 包括历史 |
| `testConsumerReadsOriginalPartitionFromRow` | consumer 从 `__historical__` 消费 → row 中保留原始分区列值 |

---

## 无需改动的组件说明

| 组件 | 为什么无需改动 |
|------|--------------|
| **RecordAccumulator** | 使用 `ConcurrentMap<PhysicalTablePath, BucketAndWriteBatches>` 管理 batch，`__historical__` 作为不同 PhysicalTablePath 天然创建独立 batch |
| **Sender** | 按 (node, table, partition, bucket) 独立发送 batch；`__historical__` 有独立 partitionId → 独立 request |
| **BucketAssigner** | WriterClient 按 PhysicalTablePath 缓存 assigner，`__historical__` 自动获得独立 assigner |
| **Server (LogTablet)** | `__historical__` 的 Log append 走正常 `LogTablet.appendAsLeader()` → 标准 replication/ACK |
| **flush()** | `accumulator.beginFlush()` + `awaitFlushCompletion()` 等待所有 incomplete batch，包括 `__historical__` |

---

## 验证步骤

```bash
# 1. 格式化
./mvnw spotless:apply -pl fluss-client -q

# 2. 编译
./mvnw clean install -DskipTests -pl fluss-client -am -q

# 3. 运行单元测试
./mvnw test -Dtest=DynamicPartitionCreatorTest -pl fluss-client
./mvnw test -Dtest=WriteRecordTest -pl fluss-client

# 4. 运行集成测试
./mvnw test -Dtest=HistoricalPartitionLogTableITCase -pl fluss-client
```
