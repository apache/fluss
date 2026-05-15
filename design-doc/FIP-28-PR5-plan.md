# PR5: Historical RocksDB 基础设施 — 非持久化 RocksDB + Composite Key 编解码

## Context

FIP-28 需要 `__historical__` 分区有临时 KV 状态存储能力，作为写入 lake 前的缓冲。PR5 提供基础设施组件（CompositeKeyEncoder + HistoricalKvManager），PR7b 将它们集成到实际的写入/查询路径中。

关键设计决策：
1. **非持久化 RocksDB**：WAL 已默认关闭（`RocksDBResourceContainer.getWriteOptions().setDisableWAL(true)`），不做 snapshot
2. **ReadWriteLock**：put/lookup 取 readLock（共享），clean 用 `writeLock.tryLock()`（最低优先级）
3. **30 分钟空闲超时**后销毁，可配置

## 改动范围

| # | 模块 | 文件 | 操作 | 说明 |
|---|------|------|------|------|
| 1 | fluss-common | `config/ConfigOptions.java` | 修改 | 新增 `KV_HISTORICAL_IDLE_TIMEOUT` |
| 2 | fluss-server | `kv/CompositeKeyEncoder.java` | 新建 | composite key 编解码 |
| 3 | fluss-server | `kv/HistoricalKvManager.java` | 新建 | 管理 __historical__ bucket 的非持久化 RocksDB 实例 |
| 4 | fluss-server | `kv/CompositeKeyEncoderTest.java` | 新建 | 编解码测试 |
| 5 | fluss-server | `kv/HistoricalKvManagerTest.java` | 新建 | Manager 测试 |

---

## Step 1: 配置项

**文件**: `fluss-common/src/main/java/org/apache/fluss/config/ConfigOptions.java`

在 KV 相关配置区域（`KV_SHARED_RATE_LIMITER_BYTES_PER_SEC` 之后）添加：

```java
public static final ConfigOption<Duration> KV_HISTORICAL_IDLE_TIMEOUT =
        key("kv.historical.idle-timeout")
                .durationType()
                .defaultValue(Duration.ofMinutes(30))
                .withDescription(
                        "The idle timeout for historical partition RocksDB instances. "
                                + "When a historical KV instance has not been accessed for longer "
                                + "than this duration, it will be closed and its local data "
                                + "deleted. The default value is 30 minutes.");
```

---

## Step 2: CompositeKeyEncoder

**文件**: `fluss-server/src/main/java/org/apache/fluss/server/kv/CompositeKeyEncoder.java`（新建）

Static utility class，private constructor，`@Internal` 注解。

### Composite Key 格式

```
[4 bytes: partitionName 长度, big-endian int]
[N bytes: partitionName, UTF-8]
[剩余 bytes: original key]
```

### API

```java
@Internal
public final class CompositeKeyEncoder {
    private CompositeKeyEncoder() {}

    /** 编码 composite key: partitionName + originalKey → compositeKey */
    public static byte[] encode(String partitionName, byte[] originalKey)

    /** 解码 composite key → (partitionName, originalKey) */
    public static Tuple2<String, byte[]> decode(byte[] compositeKey)
}
```

`decode` 返回 `Tuple2<String, byte[]>`（使用已有的 `org.apache.fluss.utils.types.Tuple2`），避免解析两次。

实现：手动字节操作（`System.arraycopy`），不用 `ByteBuffer`，热路径更轻量。

`partitionName` 就是 auto-partition 的分区列值（如 `"20240101"`），不是完整的 partition spec。

---

## Step 3: HistoricalKvManager

**文件**: `fluss-server/src/main/java/org/apache/fluss/server/kv/HistoricalKvManager.java`（新建）

### 架构

```
HistoricalKvManager (per TabletServer)
  |
  +-- Map<TableBucket, HistoricalKvHandle> handles  (ConcurrentHashMap)
  |     |
  |     +-- HistoricalKvHandle
  |           +-- ReentrantReadWriteLock rwLock     (put/get=readLock, clean=writeLock)
  |           +-- volatile RocksDBKv rocksDBKv      (null when closed)
  |           +-- volatile long lastAccessTimeMs
  |           +-- File kvDir
  |           +-- Object createLock                 (guards lazy creation)
  |
  +-- Configuration conf
  +-- RateLimiter sharedRateLimiter
  +-- Clock clock
  +-- long idleTimeoutMs
```

### 构造器

```java
public HistoricalKvManager(Configuration conf, RateLimiter sharedRateLimiter)
@VisibleForTesting
HistoricalKvManager(Configuration conf, RateLimiter sharedRateLimiter, Clock clock)
```

### Public API

```java
/** 启动定期清理调度 */
public void startCleanupScheduler(ScheduledExecutorService scheduler)

/** 写入（lazy create RocksDB） */
public void put(TableBucket bucket, File dataDir, byte[] key, byte[] value) throws IOException

/** 查询（RocksDB 未 open 返回 null） */
@Nullable
public byte[] get(TableBucket bucket, byte[] key) throws IOException

/** 删除 */
public void delete(TableBucket bucket, File dataDir, byte[] key) throws IOException

/** 强制销毁指定 bucket 的 RocksDB */
public void dropInstance(TableBucket bucket)

/** 关闭所有实例 */
public void close()
```

### 并发设计

**put/get/delete 路径**:
```
1. readLock.lock()
2. handle.ensureOpen()  // synchronized(createLock) double-check lazy create
3. rocksDBKv.put/get/delete(key, value)
4. lastAccessTimeMs = clock.milliseconds()
5. readLock.unlock()
```

**清理路径**（定期调度）:
```
1. 遍历 handles
2. if (now - lastAccessTimeMs > idleTimeoutMs && rocksDBKv != null):
   a. writeLock.tryLock() → 失败则跳过
   b. double-check 超时条件
   c. rocksDBKv.close() + FileUtils.deleteDirectoryQuietly(kvDir)
   d. handles.remove(bucket)
   e. writeLock.unlock()
```

**ensureOpen()** — HistoricalKvHandle 内部方法:
```java
void ensureOpen(Configuration conf, RateLimiter sharedRateLimiter) throws IOException {
    if (rocksDBKv != null) return;
    synchronized (createLock) {
        if (rocksDBKv != null) return;
        RocksDBResourceContainer container =
                new RocksDBResourceContainer(conf, kvDir, false, sharedRateLimiter);
        RocksDBKvBuilder builder = new RocksDBKvBuilder(kvDir, container, container.getColumnOptions());
        rocksDBKv = builder.build();
    }
}
```

- `enableStatistics=false`（临时实例不需要统计）
- WAL 已在 `RocksDBResourceContainer.getWriteOptions()` 中默认关闭
- 复用已有 `RocksDBKvBuilder` 和 `RocksDBResourceContainer`，无需修改

### 目录结构

复用现有的 `FlussPaths.kvTabletDir(dataDir, physicalTablePath, tableBucket)` 模式。`__historical__` 分区有独立的 `partitionId`，与正常分区的 `partitionId` 不同，因此目录自然不冲突：

```
{dataDir}/{db}/{table-name}-{tableId}/__historical__-p{partitionId}/kv-{bucket}/db/
```

`HistoricalKvManager` 的 `put`/`delete` 方法接收 `File dataDir` 参数，由调用方负责计算正确的目录路径。

### 锁安全性分析

`synchronized(createLock)` 在 `readLock` 内部是安全的，因为：
- `createLock` 仅在 `ensureOpen()` 中使用，不存在外部持有
- `readLock` 总是在 `createLock` 之前获取
- `writeLock`（cleanup）不获取 `createLock`
- 无循环依赖

---

## Step 4: 测试

### CompositeKeyEncoderTest

| 测试方法 | 验证目标 |
|---------|---------|
| `testEncodeDecodeRoundTrip` | 正常 partitionName + key 编解码对称 |
| `testEncodeDecodeWithEmptyOriginalKey` | 空 originalKey |
| `testEncodeDecodeWithEmptyPartitionName` | 空 partitionName |
| `testEncodeDecodeWithSpecialCharacters` | Unicode / `$` 分隔符等 |
| `testDifferentPartitionNamesDifferentKeys` | 同 originalKey 不同 partition → 不同 compositeKey |

### HistoricalKvManagerTest

使用 `ManualClock`（已存在于 `fluss-common/.../utils/clock/ManualClock.java`）控制时间。
使用 `@TempDir` 管理数据目录。
使用 `KvManager.getDefaultRateLimiter()` 获取 unlimited RateLimiter。

| 测试方法 | 验证目标 |
|---------|---------|
| `testPutAndGet` | 基本写入/读取 |
| `testGetBeforePutReturnsNull` | 无数据时返回 null |
| `testPutAndDelete` | 删除后返回 null |
| `testMultipleBucketsIsolated` | 不同 bucket 数据隔离 |
| `testLazyCreation` | put 前 openInstanceCount=0，put 后 =1 |
| `testIdleTimeoutCleansUp` | put → advance clock > timeout → cleanIdleInstances → get=null |
| `testAccessResetsIdleTimer` | put → partial advance → get → partial advance → clean → 数据仍在 |
| `testDropInstance` | 强制删除后 get=null |
| `testCloseAllInstances` | close 后所有实例被清理 |
| `testConcurrentPutAndGet` | 多线程 put/get 不阻塞 |

---

## 验证步骤

```bash
# 1. 格式化
./mvnw spotless:apply -pl fluss-common,fluss-server -q

# 2. 编译
./mvnw clean install -DskipTests -pl fluss-common,fluss-server -am -q

# 3. 运行测试
./mvnw test -Dtest=CompositeKeyEncoderTest -pl fluss-server
./mvnw test -Dtest=HistoricalKvManagerTest -pl fluss-server
```
