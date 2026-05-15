# PR4: 服务端隔离基础设施 — ioExecutor + per-bucket serial executor + 历史 request queue + flow control

## Context

FIP-28 需要历史分区操作（写入/查询过期分区）在服务端异步执行，避免 lake I/O 阻塞实时路径的 RPC 线程。PR4 建立性能隔离基础设施：

- **Flow control**: 有界历史 request queue，队列满时拒绝请求（客户端自动 backoff retry）
- **Per-bucket serial execution**: 同 bucket 写入 FIFO 串行，不同 bucket 可并发
- **Async dispatch**: 历史请求提交到已有 `ioExecutor` 异步执行，RPC 线程立即释放

关键决策：
1. 复用 TabletServer 已有的 `ioExecutor`（`server.io-pool.size`，默认 10 线程），不创建新线程池
2. 历史 request queue 容量用比例配置：`server.historical-request-queue-ratio`（默认 0.1）

## 改动范围

| # | 模块 | 文件 | 操作 | 说明 |
|---|------|------|------|------|
| 1 | fluss-common | `exception/HistoricalPartitionThrottledException.java` | 新建 | 队列满时的 retriable 异常 |
| 2 | fluss-rpc | `protocol/Errors.java` | 修改 | 新增 `HISTORICAL_PARTITION_THROTTLED`（code 70）|
| 3 | fluss-common | `utils/PartitionUtils.java` | 修改 | 新增 `isHistoricalPartitionName()` 辅助方法 |
| 4 | fluss-common | `config/ConfigOptions.java` | 修改 | 新增 `SERVER_HISTORICAL_REQUEST_QUEUE_RATIO` |
| 5 | fluss-server | `replica/HistoricalPartitionHandler.java` | 新建 | 核心：per-bucket serial executor + flow control |
| 6 | fluss-server | `replica/ReplicaManager.java` | 修改 | `putRecordsToKv()` 和 `lookups()` 增加历史分区检测与异步分发 |
| 7 | fluss-server | `replica/HistoricalPartitionHandlerTest.java` | 新建 | Handler 单元测试 |
| 8 | fluss-common | `utils/PartitionUtilsTest.java` | 修改 | `isHistoricalPartitionName` 测试 |

---

## Step 1: HistoricalPartitionThrottledException

**文件**: `fluss-common/src/main/java/org/apache/fluss/exception/HistoricalPartitionThrottledException.java`（新建）

继承 `RetriableException`，客户端 `Sender.canRetry()`（`Sender.java:320`）和 `LookupSender.canRetry()`（`LookupSender.java:448`）已有 `error.exception() instanceof RetriableException` 检查，无需额外客户端改动。

```java
@PublicEvolving
public class HistoricalPartitionThrottledException extends RetriableException {
    public HistoricalPartitionThrottledException(String message) { super(message); }
}
```

**参考**: `NotEnoughReplicasException.java` — 同样继承 `RetriableException`。

## Step 2: Errors 枚举新增错误码

**文件**: `fluss-rpc/src/main/java/org/apache/fluss/rpc/protocol/Errors.java`

在 `TOO_MANY_SCANNERS`（code 69）之后添加：

```java
HISTORICAL_PARTITION_THROTTLED(
        70,
        "The historical partition request queue is full. Please retry later.",
        HistoricalPartitionThrottledException::new);
```

客户端无需改动：`HistoricalPartitionThrottledException` 继承 `RetriableException`，已有的 retry 逻辑自动覆盖。

## Step 3: isHistoricalPartitionName() 辅助方法

**文件**: `fluss-common/src/main/java/org/apache/fluss/utils/PartitionUtils.java`

用于 ReplicaManager 中根据 partition name 判断是否为 `__historical__` 分区。获取 partition name 路径：`Replica.getPhysicalTablePath().getPartitionName()`。

```java
public static boolean isHistoricalPartitionName(@Nullable String partitionName) {
    if (partitionName == null) {
        return false;
    }
    // 单 partition key: partitionName = "__historical__"
    // 多 partition key: partitionName = "us$__historical__"
    // 按 '$' 分隔后检查每个 segment
    if (partitionName.equals(HISTORICAL_PARTITION_VALUE)) {
        return true;
    }
    for (String segment : partitionName.split("\\$")) {
        if (HISTORICAL_PARTITION_VALUE.equals(segment)) {
            return true;
        }
    }
    return false;
}
```

## Step 4: 配置项

**文件**: `fluss-common/src/main/java/org/apache/fluss/config/ConfigOptions.java`

在 `SERVER_IO_POOL_SIZE`（line ~327）附近添加：

```java
public static final ConfigOption<Double> SERVER_HISTORICAL_REQUEST_QUEUE_RATIO =
        key("server.historical-request-queue-ratio")
                .doubleType()
                .defaultValue(0.1)
                .withDescription(
                        "The ratio of historical request queue capacity to the total "
                                + "netty server max queued requests ("
                                + "netty.server.max-queued-requests). When the historical "
                                + "request queue is full, new historical partition requests "
                                + "are rejected with a retriable error. The default value "
                                + "of 0.1 means 10% of the total request queue capacity.");
```

实际容量计算：`capacity = max(1, (int)(NETTY_SERVER_MAX_QUEUED_REQUESTS × ratio))`

## Step 5: HistoricalPartitionHandler

**文件**: `fluss-server/src/main/java/org/apache/fluss/server/replica/HistoricalPartitionHandler.java`（新建）

核心类，封装两个独立机制：

### 5.1 Flow Control — Semaphore

- `Semaphore requestPermits(capacity)` 控制总历史请求并发
- `submitWrite()`/`submitLookup()` 先 `tryAcquire()`，失败则抛 `HistoricalPartitionThrottledException`
- 任务完成时 `release()`（在 `finally` 中保证释放）

### 5.2 Per-bucket Serial Execution — drain loop

- `Map<TableBucket, BucketTaskQueue> bucketQueues`（`synchronized` 保护）
- 每个 `BucketTaskQueue` 包含 `Deque<Runnable> tasks` 和 `boolean running`
- 新任务入队；若该 bucket 无运行中任务，提交 `drainBucketQueue(bucket)` 到 ioExecutor
- `drainBucketQueue` 循环取任务执行，直到队列空，标记 `running=false`

```java
@Internal
@ThreadSafe
public class HistoricalPartitionHandler {

    private final Semaphore requestPermits;
    private final ExecutorService ioExecutor;
    private final int capacity;
    private final Map<TableBucket, BucketTaskQueue> bucketQueues = new HashMap<>();

    public HistoricalPartitionHandler(ExecutorService ioExecutor, int capacity) {
        this.ioExecutor = ioExecutor;
        this.capacity = Math.max(1, capacity);
        this.requestPermits = new Semaphore(this.capacity);
    }

    /** 写入：per-bucket FIFO 串行 */
    public void submitWrite(TableBucket bucket, Runnable task) {
        if (!requestPermits.tryAcquire()) {
            throw new HistoricalPartitionThrottledException("...");
        }
        enqueueWriteTask(bucket, task);
    }

    /** 查询：直接并发执行 */
    public void submitLookup(Runnable task) {
        if (!requestPermits.tryAcquire()) {
            throw new HistoricalPartitionThrottledException("...");
        }
        ioExecutor.execute(() -> {
            try { task.run(); }
            finally { requestPermits.release(); }
        });
    }

    private void enqueueWriteTask(TableBucket bucket, Runnable task) {
        boolean shouldSubmit;
        synchronized (bucketQueues) {
            BucketTaskQueue queue = bucketQueues.computeIfAbsent(
                    bucket, k -> new BucketTaskQueue());
            queue.tasks.addLast(task);
            shouldSubmit = !queue.running;
            if (shouldSubmit) { queue.running = true; }
        }
        if (shouldSubmit) {
            ioExecutor.execute(() -> drainBucketQueue(bucket));
        }
    }

    private void drainBucketQueue(TableBucket bucket) {
        while (true) {
            Runnable task;
            synchronized (bucketQueues) {
                BucketTaskQueue queue = bucketQueues.get(bucket);
                if (queue == null || queue.tasks.isEmpty()) {
                    if (queue != null) {
                        queue.running = false;
                        bucketQueues.remove(bucket);
                    }
                    return;
                }
                task = queue.tasks.pollFirst();
            }
            try { task.run(); }
            finally { requestPermits.release(); }
        }
    }

    private static final class BucketTaskQueue {
        final Deque<Runnable> tasks = new ArrayDeque<>();
        boolean running = false;
    }
}
```

**设计要点**:
- Semaphore 非阻塞 `tryAcquire()`，队列满时立即拒绝
- drain loop 避免为每个 bucket 创建 SingleThreadExecutor，共享 ioExecutor
- `synchronized(bucketQueues)` 临界区极短（仅 queue/dequeue 操作），不会造成竞争
- Lookup 跳过 per-bucket 串行，直接提交 ioExecutor 并发执行

## Step 6: ReplicaManager 集成

**文件**: `fluss-server/src/main/java/org/apache/fluss/server/replica/ReplicaManager.java`

### 6.1 新增字段 & 初始化

```java
// 新字段（line ~173 后）
private final HistoricalPartitionHandler historicalPartitionHandler;
```

在构造器中初始化（line ~332 后，`this.ioExecutor = ioExecutor;` 之后）：

```java
int maxQueued = conf.getInt(ConfigOptions.NETTY_SERVER_MAX_QUEUED_REQUESTS);
double ratio = conf.get(ConfigOptions.SERVER_HISTORICAL_REQUEST_QUEUE_RATIO);
int historicalCapacity = Math.max(1, (int) (maxQueued * ratio));
this.historicalPartitionHandler = new HistoricalPartitionHandler(ioExecutor, historicalCapacity);
```

### 6.2 历史分区检测辅助方法

通过 `TableBucket.partitionId` 定位 replica，再判断该 replica 是否属于 `__historical__` 分区。
注意：RPC 请求中的 `partition_name` 字段是原始分区名（如 "20240101"），与此处的检测无关。

```java
private boolean isHistoricalBucket(TableBucket tb) {
    // 通过 partitionId 从 allReplicas map 定位到对应的 partition replica
    HostedReplica hosted = getReplica(tb);  // 已有方法，line ~2029
    if (hosted instanceof OnlineReplica) {
        // 从 replica 的 PhysicalTablePath 获取该 partition 的 name
        String partitionName = ((OnlineReplica) hosted).getReplica()
                .getPhysicalTablePath().getPartitionName();
        return PartitionUtils.isHistoricalPartitionName(partitionName);
    }
    return false;
}
```

### 6.3 修改 putRecordsToKv()（line 654-677）

**改动策略**：在方法入口将 `entriesPerBucket` 拆分为 realtime / historical 两组：

- **全部 realtime**（快速路径）：保持原有同步逻辑不变，零开销
- **有 historical 部分**：realtime 同步处理，historical 逐 bucket 提交到 `historicalPartitionHandler.submitWrite()`
- 当 historical bucket 提交失败（队列满），该 bucket 返回 `HISTORICAL_PARTITION_THROTTLED` 错误
- 所有 bucket（sync + async）结果就绪后，合并结果调用 callback

```
putRecordsToKv() 新流程：
1. split entries → realtimeEntries + historicalEntries
2. if historicalEntries.isEmpty() → 原有同步路径（零开销）
3. else:
   a. realtime 部分 → putToLocalKv()（同步）
   b. historical 部分 → per-bucket submitWrite() → putToLocalKv()（异步）
   c. 队列满的 bucket → 直接返回 THROTTLED error
   d. AtomicInteger + synchronizedMap 协调：所有 async 完成后合并 → maybeAddDelayedWrite → callback
```

### 6.4 修改 lookups()（line 758-840）

同样的拆分策略。注意：
- `insertIfNotExists` 场景暂不对 historical 做特殊处理（PR8 再处理），走原有同步路径
- 纯 lookup（`insertIfNotExists=false`）才对 historical 做异步分发

```
lookups() 新流程：
1. if insertIfNotExists → 原有同步路径（不变）
2. split entries → realtimeEntries + historicalEntries
3. if historicalEntries.isEmpty() → 原有同步路径
4. else:
   a. realtime 部分 → 同步 per-bucket lookup
   b. historical 部分 → per-bucket submitLookup()（异步，无需串行）
   c. 队列满的 bucket → 返回 THROTTLED error
   d. 所有完成后合并 → callback
```

需要将现有 lookups() 中的 per-bucket 处理逻辑提取为可复用的 helper 方法。

## Step 7: 测试

### 7.1 HistoricalPartitionHandlerTest（新建）

**文件**: `fluss-server/src/test/java/org/apache/fluss/server/replica/HistoricalPartitionHandlerTest.java`

| 测试方法 | 验证目标 |
|---------|---------|
| `testQueueFullThrowsThrottleException` | capacity=2，提交 2 个阻塞任务后第 3 个抛 `HistoricalPartitionThrottledException` |
| `testSameBucketWritesFIFO` | 同 bucket 3 个 write，按提交顺序执行 |
| `testDifferentBucketWritesConcurrent` | 不同 bucket 2 个 write，并发执行 |
| `testLookupsConcurrent` | 多个 lookup 并发执行 |
| `testPermitReleasedOnCompletion` | 队列满 → 等任务完成 → 再提交成功 |
| `testPermitReleasedOnException` | 任务抛异常后 permit 正确释放 |

### 7.2 PartitionUtilsTest（修改）

**文件**: `fluss-common/src/test/java/org/apache/fluss/utils/PartitionUtilsTest.java`

新增 `testIsHistoricalPartitionName()`：null、单 key、多 key、普通分区、子串非 segment。

## 验证步骤

```bash
# 1. 编译
./mvnw clean install -DskipTests -pl fluss-common,fluss-rpc,fluss-server -am

# 2. 格式化
./mvnw spotless:apply -pl fluss-common,fluss-rpc,fluss-server

# 3. 运行相关测试
./mvnw test -Dtest=HistoricalPartitionHandlerTest -pl fluss-server
./mvnw test -Dtest=PartitionUtilsTest -pl fluss-common
```
