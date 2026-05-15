# PR 7a: 客户端 PK 历史写入路径

## Context

PR6 已实现 Log 表的过期分区写入重定向：`WriterClient.doSend()` 调用 `DynamicPartitionCreator.checkAndCreatePartitionAsync()` 获取实际写入路径，过期分区 redirect 到 `__historical__`。PK 表共享同一 `doSend()` 路径，redirect 本身已生效。

**PK 表的额外需求**：服务端需要知道记录的**原始分区名**（如 `"2000"`），以便：
1. `CompositeKeyEncoder` 使用原始分区名前缀 key，隔离不同原始分区的数据
2. key-only delete 能正确路由

`PbPutKvReqForBucket` 已有 `partition_name` 字段（proto 已定义，PR3 服务端已使用），但客户端尚未设置。

**核心挑战**：`partition_name` 是 per-bucket-request 级别字段，同一 `PbPutKvReqForBucket` 只能有一个 `partition_name`。因此不同原始分区的记录不能合并到同一个 KvWriteBatch。

## 改动范围

| # | 文件 | 操作 | 说明 |
|---|------|------|------|
| 1 | `fluss-client/.../write/WriteRecord.java` | 修改 | 新增 `originalPartitionName` 字段，`withPhysicalTablePath()` 自动捕获 |
| 2 | `fluss-client/.../write/KvWriteBatch.java` | 修改 | 新增 `originalPartitionName` 字段，`tryAppend()` 不匹配时返回 false（触发 batch split） |
| 3 | `fluss-client/.../write/RecordAccumulator.java` | 修改 | `createWriteBatch()` 传递 `originalPartitionName` 到 KvWriteBatch |
| 4 | `fluss-client/.../utils/ClientRpcMessageUtils.java` | 修改 | `makePutKvRequest()` 设置 `partition_name` |
| 5 | `fluss-client/.../table/HistoricalPartitionTableITCase.java` | 重命名+扩展 | 合并 Log 表和 PK 表端到端集成测试（原 HistoricalPartitionLogTableITCase） |

**无需改动**：WriterClient（PR6 redirect 已生效）、Sender、服务端（PR3 已有 CompositeKeyEncoder + HistoricalKvManager）

---

## Step 1: WriteRecord 新增 originalPartitionName

**文件**: `fluss-client/src/main/java/org/apache/fluss/client/write/WriteRecord.java`

### 1a. 新增字段和 getter

在现有字段区域（约 line 223）后添加：

```java
/** The original partition name before redirect, null if not redirected. */
private final @Nullable String originalPartitionName;
```

添加 getter：

```java
public @Nullable String getOriginalPartitionName() {
    return originalPartitionName;
}
```

### 1b. 修改 constructor

在 private constructor 参数末尾增加 `@Nullable String originalPartitionName`，赋值给字段。

### 1c. 更新所有 factory 方法

所有 `forUpsert`、`forDelete`、`forIndexedAppend`、`forArrowAppend`、`forCompactedAppend` 传 `null`（无 redirect）。

### 1d. 修改 withPhysicalTablePath()

自动捕获当前 `physicalTablePath.getPartitionName()` 作为 `originalPartitionName`：

```java
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
            this.mergeMode,
            this.physicalTablePath.getPartitionName());
}
```

**设计要点**：调用者（`WriterClient.doSend()`）无需改动——`withPhysicalTablePath()` 自动提取原始分区名。

---

## Step 2: KvWriteBatch 新增 originalPartitionName + tryAppend 检查

**文件**: `fluss-client/src/main/java/org/apache/fluss/client/write/KvWriteBatch.java`

### 2a. 新增字段

```java
private final @Nullable String originalPartitionName;
```

### 2b. 修改 constructor

参数列表末尾（`mergeMode` 之后、`createdMs` 之前）添加 `@Nullable String originalPartitionName`：

```java
public KvWriteBatch(
        int bucketId,
        PhysicalTablePath physicalTablePath,
        int schemaId,
        KvFormat kvFormat,
        int writeLimit,
        AbstractPagedOutputView outputView,
        @Nullable int[] targetColumns,
        MergeMode mergeMode,
        @Nullable String originalPartitionName,
        long createdMs) {
    super(bucketId, physicalTablePath, createdMs);
    // ... existing assignments ...
    this.originalPartitionName = originalPartitionName;
}
```

### 2c. 添加 getter

```java
@Nullable
public String getOriginalPartitionName() {
    return originalPartitionName;
}
```

### 2d. tryAppend() 添加 originalPartitionName 检查

在现有 mergeMode 检查之后、key null 检查之前（约 line 110），添加：

```java
// Records with different original partition names must go to separate batches
// because partition_name is a per-request field on PbPutKvReqForBucket.
if (!Objects.equals(this.originalPartitionName, writeRecord.getOriginalPartitionName())) {
    return false;
}
```

**返回 false（非 throw）**：不同 `originalPartitionName` 是合法场景（多个过期分区共用 `__historical__`），只是需要分批发送。`RecordAccumulator.tryAppend()` 收到 `false` 后会关闭当前 batch 并创建新 batch。

---

## Step 3: RecordAccumulator 传递 originalPartitionName

**文件**: `fluss-client/src/main/java/org/apache/fluss/client/write/RecordAccumulator.java`

修改 `createWriteBatch()` 方法（line 622-631），KvWriteBatch 构造调用增加参数：

```java
case COMPACTED_KV:
case INDEXED_KV:
    return new KvWriteBatch(
            bucketId,
            physicalTablePath,
            tableInfo.getSchemaId(),
            writeFormat.toKvFormat(),
            outputView.getPreAllocatedSize(),
            outputView,
            writeRecord.getTargetColumns(),
            writeRecord.getMergeMode(),
            writeRecord.getOriginalPartitionName(),
            clock.milliseconds());
```

---

## Step 4: ClientRpcMessageUtils 设置 partition_name

**文件**: `fluss-client/src/main/java/org/apache/fluss/client/utils/ClientRpcMessageUtils.java`

修改 `makePutKvRequest()` 方法的 forEach 块（line 185-195），在设置 `partitionId` 之后添加：

```java
readyWriteBatches.forEach(
        readyBatch -> {
            TableBucket tableBucket = readyBatch.tableBucket();
            KvWriteBatch kvBatch = (KvWriteBatch) readyBatch.writeBatch();
            PbPutKvReqForBucket pbPutKvReqForBucket =
                    request.addBucketsReq()
                            .setBucketId(tableBucket.getBucket())
                            .setRecordsBytesView(kvBatch.build());
            if (tableBucket.getPartitionId() != null) {
                pbPutKvReqForBucket.setPartitionId(tableBucket.getPartitionId());
            }
            // Set partition_name for historical partition writes.
            // Server uses this for CompositeKeyEncoder key prefix.
            String partitionName = kvBatch.getOriginalPartitionName();
            if (partitionName != null) {
                pbPutKvReqForBucket.setPartitionName(partitionName);
            }
        });
```

**注意**：forEach 中原本使用 `readyBatch.writeBatch().build()` 调用基类方法。改为先 cast 到 `KvWriteBatch`，既能调用 `build()` 也能调用 `getOriginalPartitionName()`。此 cast 安全——`makePutKvRequest` 仅处理 KV batch。

---

## Step 5: 集成测试

**文件**: `fluss-client/src/test/java/org/apache/fluss/client/table/HistoricalPartitionTableITCase.java`

将现有 `HistoricalPartitionLogTableITCase` 重命名为 `HistoricalPartitionTableITCase`，合并 Log 表和 PK 表测试到同一个类：

| 测试方法 | 验证目标 |
|---------|---------|
| `testLogTableWriteExpiredPartition` | Log 表：append 到过期分区 → 数据进入 `__historical__`，row 保留原始分区值 |
| `testLogTableWriteNonExpiredNonExistentThrows` | Log 表：非过期不存在分区 → 抛 PartitionNotExistException |
| `testPkTableWriteExpiredPartition` | PK 表：upsert 到过期分区 → `__historical__` 分区被创建，数据可从 log scanner 消费，row 保留原始分区列值 |
| `testPkTableWriteNonExpiredNonExistentThrows` | PK 表：非过期不存在分区 → 抛 PartitionNotExistException |

类中包含两个建表方法：`createDataLakeLogTable()` 和 `createDataLakePkTable()`（PK 表增加 primary key 定义）。删除原有 `HistoricalPartitionLogTableITCase`。

---

## 无需改动的组件

| 组件 | 为什么无需改动 |
|------|--------------|
| **WriterClient.doSend()** | PR6 redirect 逻辑已生效，自动调用 `withPhysicalTablePath()` 设置 `originalPartitionName` |
| **Sender** | 按 (node, table, partition, bucket) 独立发送 batch，`__historical__` 有独立 partitionId |
| **Server** | PR3 已实现 `CompositeKeyEncoder` 和 `HistoricalKvManager`，读取 `partition_name` 前缀 key |
| **LogWriteBatch** | Log 表不需要 `partition_name`，原始分区值在 row payload 中 |
| **WriteBatch 基类** | `originalPartitionName` 仅 KV 路径需要，不影响基类 |

---

## 验证步骤

```bash
# 1. 格式化
./mvnw spotless:apply -pl fluss-client -q

# 2. 编译
./mvnw clean install -DskipTests -pl fluss-client -am -q

# 3. 运行集成测试（包含 Log 表和 PK 表）
./mvnw test -Dtest=HistoricalPartitionTableITCase -pl fluss-client
```
