# PR 1: RPC 协议扩展 + `__historical__` 分区常量与生命周期

## Context

FIP-28 为启用了 DataLake 的 auto-partitioned 表支持对已过期分区的写入和点查。PR 1 是整个 feature 的基础设施层：
1. 在 protobuf 中添加 `partition_name` 字段，供后续 PR 传递原始分区名
2. 定义 `__historical__` 分区常量和工具方法
3. 服务端生命周期管理：AutoPartitionManager 跳过 `__historical__`，CoordinatorService 允许 eligible 表创建 `__historical__`

**多级分区语义**：对于多级分区表（如 keys=[region, dt]，dt 为 auto-partition key），只有 auto-partition key 的值替换为 `__historical__`，其他 key 保持原值。因此会产生多个 `__historical__` 分区（如 `us-east$__historical__`、`us-west$__historical__`）。

## 改动列表

### 1. Protobuf: 添加 `partition_name` 字段

**文件**: `fluss-rpc/src/main/proto/FlussApi.proto`

在 `PbPutKvReqForBucket`（line 890）和 `PbLookupReqForBucket`（line 906）各添加一个字段：

```protobuf
message PbPutKvReqForBucket {
  optional int64 partition_id = 1;
  required int32 bucket_id = 2;
  required bytes records = 3;
  optional string partition_name = 4;  // 新增
}

message PbLookupReqForBucket {
  optional int64 partition_id = 1;
  required int32 bucket_id = 2;
  repeated bytes keys = 3;
  optional string partition_name = 4;  // 新增
}
```

改完后执行 `./mvnw clean install -DskipTests -pl fluss-protogen,fluss-rpc` 重新生成 Java 代码。

### 2. 常量和工具方法

**文件**: `fluss-common/src/main/java/org/apache/fluss/utils/PartitionUtils.java`

添加常量：

```java
/** The partition value used for the auto-partition key in a historical partition. */
public static final String HISTORICAL_PARTITION_VALUE = "__historical__";
```

添加工具方法：

```java
/**
 * Returns true if the given partition spec represents a historical partition.
 * A historical partition has its auto-partition key value equal to
 * {@link #HISTORICAL_PARTITION_VALUE}.
 *
 * <p>For single-key tables: checks if the only value is "__historical__".
 * For multi-key tables: checks if the auto-partition key's value is "__historical__".
 */
public static boolean isHistoricalPartitionSpec(
        PartitionSpec partitionSpec,
        List<String> partitionKeys,
        AutoPartitionStrategy autoPartitionStrategy)
```

### 3. CoordinatorService: 允许 eligible 表创建 `__historical__`

**文件**: `fluss-server/src/main/java/org/apache/fluss/server/coordinator/CoordinatorService.java`

修改 `createPartition()` 方法（line 676-718），在验证逻辑前增加 `__historical__` 分支：

- 用 `isHistoricalPartitionSpec()` 判断是否历史分区
- 如果是历史分区：校验 `isDataLakeEnabled()`，跳过 `validateAutoPartitionTime()`，用 `isCreate=false` 跳过 `__` 前缀校验
- 如果不是：原有逻辑不变

### 4. AutoPartitionManager: 跳过 `__historical__`

**文件**: `fluss-server/src/main/java/org/apache/fluss/server/coordinator/AutoPartitionManager.java`

在 `addPartitionToPartitionsByTable()` 方法入口，复用 `extractAutoPartitionValue()` 判断，跳过 `__historical__` 分区。

### 5. 测试

- `PartitionUtilsTest`: 测试 `isHistoricalPartitionSpec` 单key/多key/正常/未启用
- `AutoPartitionManagerTest`: 测试 `__historical__` 不被 TTL 自动过期

## 不需要改动的文件

- `ClientRpcMessageUtils.java` — PR 7a / PR 8
- `ServerRpcMessageUtils.java` — PR 7b / PR 8
- `Errors.java` — PR 4
- `MetadataManager.java` — 无需改动

## 验证步骤

```bash
./mvnw clean install -DskipTests -pl fluss-protogen,fluss-rpc
./mvnw clean install -DskipTests -T 1C
./mvnw verify -pl fluss-common,fluss-rpc,fluss-server
./mvnw spotless:apply && ./mvnw spotless:check
```
