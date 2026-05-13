# PR 2: 客户端基础 — 过期分区判定谓词 + `__historical__` 动态创建

## Context

PR 2 基于 PR 1 的基础设施，在客户端添加：
1. 过期分区判定谓词（`isExpiredPartition`）— 判断一个不存在的分区是否"已过期"
2. 历史分区名构建工具（`buildHistoricalPartitionName`）— 将原始分区名转为 `__historical__` 分区名
3. 修改 `DynamicPartitionCreator`，让写入路径在检测到过期分区时自动创建 `__historical__` 分区

这些工具方法放在 `fluss-common` 的 `PartitionUtils` 中，write 和 lookup 路径均可复用。

## 改动列表

### 1. PartitionUtils：添加 `isExpiredPartition()` + `buildHistoricalPartitionName()`

**文件**: `fluss-common/src/main/java/org/apache/fluss/utils/PartitionUtils.java`

#### 1a. 将 `isValidPartitionTime` 从 `private` 改为包级可见

当前是 `private static`，改为 `static`（包级），为 `isExpiredPartition` 和后续测试提供访问。

#### 1b. 添加 `isExpiredPartition()` 方法

判定四条件中的 1-3（条件 4 "分区不存在" 由调用方保证）：

```java
public static boolean isExpiredPartition(
        String partitionName,
        List<String> partitionKeys,
        AutoPartitionStrategy autoPartitionStrategy,
        boolean isDataLakeEnabled) {
    // 条件 1: auto-partitioned + data-lake enabled
    if (!autoPartitionStrategy.isAutoPartitionEnabled() || !isDataLakeEnabled) {
        return false;
    }
    // numToRetain <= 0 表示不过期
    if (autoPartitionStrategy.numToRetain() <= 0) {
        return false;
    }
    // 解析分区名 → 提取 auto-partition key 对应的值
    ResolvedPartitionSpec resolvedSpec =
            ResolvedPartitionSpec.fromPartitionName(partitionKeys, partitionName);
    String autoPartitionKey = autoPartitionStrategy.key() != null
            ? autoPartitionStrategy.key() : partitionKeys.get(0);
    String partitionTime = resolvedSpec.toPartitionSpec().getSpecMap().get(autoPartitionKey);
    AutoPartitionTimeUnit timeUnit = autoPartitionStrategy.timeUnit();
    // 条件 2: 时间格式合法
    if (partitionTime == null || !isValidPartitionTime(partitionTime, timeUnit)) {
        return false;
    }
    // 条件 3: 比保留边界更老
    ZonedDateTime now = ZonedDateTime.ofInstant(
            Instant.now(), autoPartitionStrategy.timeZone().toZoneId());
    String lastRetainPartitionTime =
            generateAutoPartitionTime(now, -autoPartitionStrategy.numToRetain(), timeUnit);
    return lastRetainPartitionTime.compareTo(partitionTime) > 0;
}
```

#### 1c. 添加 `buildHistoricalPartitionName()` 方法

将原始分区名中 auto-partition key 的值替换为 `__historical__`：

```java
public static String buildHistoricalPartitionName(
        String partitionName,
        List<String> partitionKeys,
        AutoPartitionStrategy autoPartitionStrategy) {
    ResolvedPartitionSpec resolvedSpec =
            ResolvedPartitionSpec.fromPartitionName(partitionKeys, partitionName);
    String autoPartitionKey = autoPartitionStrategy.key() != null
            ? autoPartitionStrategy.key() : partitionKeys.get(0);
    int autoKeyIndex = partitionKeys.indexOf(autoPartitionKey);
    List<String> historicalValues = new ArrayList<>(resolvedSpec.getPartitionValues());
    historicalValues.set(autoKeyIndex, HISTORICAL_PARTITION_VALUE);
    return new ResolvedPartitionSpec(partitionKeys, historicalValues).getPartitionName();
}
```

示例：
- 单 key `[dt]`，`"20230101"` → `"__historical__"`
- 多 key `[region, dt]`（dt 为 auto key），`"us-east$20230101"` → `"us-east$__historical__"`

### 2. DynamicPartitionCreator：支持过期分区检测 + `__historical__` 创建

**文件**: `fluss-client/src/main/java/org/apache/fluss/client/write/DynamicPartitionCreator.java`

#### 2a. `checkAndCreatePartitionAsync` 增加 `isDataLakeEnabled` 参数

```java
public void checkAndCreatePartitionAsync(
        PhysicalTablePath physicalTablePath,
        List<String> partitionKeys,
        AutoPartitionStrategy autoPartitionStrategy,
        boolean isDataLakeEnabled)  // 新增
```

#### 2b. `forceCheckPartitionExist` 改为不直接抛异常

当前 `dynamicPartitionEnabled=false` 时直接抛 `PartitionNotExistException`。改为统一返回 `boolean`，由调用方决定后续行为：

```java
private boolean forceCheckPartitionExist(PhysicalTablePath physicalTablePath) {
    try {
        return metadataUpdater.checkAndUpdatePartitionMetadata(physicalTablePath);
    } catch (Exception e) {
        Throwable t = ExceptionUtils.stripExecutionException(e);
        if (t instanceof PartitionNotExistException) {
            return false;
        } else {
            throw new FlussRuntimeException(e.getMessage(), e);
        }
    }
}
```

#### 2c. 重构 `checkAndCreatePartitionAsync` 核心逻辑

分区确认不存在后的三分支判断：

```
if isExpiredPartition → 创建 __historical__（不受 dynamicPartitionEnabled 约束）
else if dynamicPartitionEnabled → 原有逻辑（校验时间 + 创建原始分区）
else → 抛 PartitionNotExistException
```

#### 2d. 新增 `createHistoricalPartitionIfNeeded()` 方法

- 用 `buildHistoricalPartitionName()` 构建历史分区名
- 先检查 `__historical__` 分区是否已在 metadata cache / server 中存在
- 不存在时通过 `admin.createPartition(tablePath, spec, true)` 创建
  - `ignoreIfExists=true` 保证多 client 并发创建的幂等性
- 使用现有 `inflightPartitionsToCreate` 去重机制
- 失败时调用 `fatalErrorHandler`（与现有行为一致）

### 3. WriterClient：传递 `isDataLakeEnabled`

**文件**: `fluss-client/src/main/java/org/apache/fluss/client/write/WriterClient.java`

在 `doSend()` 中更新调用（第 179 行附近）：

```java
dynamicPartitionCreator.checkAndCreatePartitionAsync(
        physicalTablePath,
        tableInfo.getPartitionKeys(),
        tableInfo.getTableConfig().getAutoPartitionStrategy(),
        tableInfo.getTableConfig().isDataLakeEnabled());  // 新增
```

### 4. 测试

#### 4.1 PartitionUtilsTest

**文件**: `fluss-common/src/test/java/org/apache/fluss/utils/PartitionUtilsTest.java`

- `testIsExpiredPartition`：
  - 合法过期分区 → true（时间格式正确 + 比 retention 边界更老）
  - 非法分区名（不匹配时间格式）→ false
  - 未来分区（在 retention window 内）→ false
  - non-eligible table（无 data lake）→ false
  - non-eligible table（非 auto-partition）→ false
  - `numToRetain <= 0` → false
- `testIsExpiredPartitionWithMultipleKeys`：
  - 多 key 表过期 → true
  - 多 key 表未来 → false
- `testBuildHistoricalPartitionName`：
  - 单 key 表 → `"__historical__"`
  - 多 key 表 → 只替换 auto-partition key（如 `"us-east$__historical__"`）

注：DynamicPartitionCreator 的端到端测试留到后续 ITCase 中覆盖，避免在单测中 hack MetadataUpdater 和 Admin。

## 关键设计决策

| 决策 | 理由 |
|------|------|
| `isExpiredPartition` 放在 `PartitionUtils`（fluss-common） | write 和 lookup 路径均可复用，无需依赖 fluss-client |
| `ignoreIfExists=true` 处理并发 | 服务端静默处理 `PartitionAlreadyExistException`，无需客户端额外 catch |
| `numToRetain <= 0` 时返回 false | 负值表示不过期，不应 redirect |
| 过期检测绕过 `dynamicPartitionEnabled` | 设计文档要求：`__historical__` 创建不依赖此配置 |
| `forceCheckPartitionExist` 改为纯返回布尔 | 让调用方统一决定抛异常还是走 expired 分支 |

## 验证步骤

```bash
# 1. 全量构建
./mvnw clean install -DskipTests -T 1C

# 2. 代码格式化
./mvnw spotless:apply && ./mvnw spotless:check

# 3. 运行受影响模块已有测试确保无回归
./mvnw verify -pl fluss-common,fluss-client

# 4. 单独运行关键测试
./mvnw test -Dtest=PartitionUtilsTest -pl fluss-common
```
