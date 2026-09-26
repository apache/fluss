<!--
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
-->

# Produce 后 DelayedFetchLog 唤醒机制分析与实施计划

## 当前实现归属（2026-09-10）

本项作为独立的 Fluss 核心修复，在 `complete-delay-fetch` 分支和已有
[PR #3412](https://github.com/apache/fluss/pull/3412) 推进，关联
[issue #3455](https://github.com/apache/fluss/issues/3455)。
纳入 [umbrella #4185](https://github.com/apache/fluss/issues/4185) 的独立核心模块开发 / review 板块，
可独立 review 和合入 Apache main，不依赖 Kafka PR01–PR03，也不占用 Kafka 功能 PR04 编号。
2026-09-10 核对 PR 为 Open、非 Draft，远端 head 仍为 `f78917765`；本地补充改动尚未推送。
整合 `codex/wake-delayed-fetch-after-append` 的候选实现 `a01fb02a5` 到原 tip `f78917765`，
本次不改写分支历史；下文保留原始分析与方案，当前代码差异以本节为准。

- `enqueueDelayedFetchCompletions` 在入队前筛选成功 bucket，每个 bucket 一个 action，
  避免某个 bucket 的普通异常跳过同请求中其他 bucket 的唤醒。
- 队列按 drain 开始时的大小限制执行次数；捕获 `Exception` 后继续执行后续 action，
  `Error` 向外传播。该边界不同于下文原方案中的 `catch (Throwable)`。
- 原生 RPC 同步调用退出后执行 drain；返回的异步响应尚未完成时也必须执行。
- 回归测试覆盖单 bucket、多 bucket、部分 append 失败、action 只执行一次、
  普通异常隔离、执行中新增 action 留待后续 drain，以及 RPC 成功/失败/异步返回时机。
- 后续 Kafka Produce 只依赖此核心能力，并补自己的 drain 调用；不重复提交核心修复。

验证记录（2026-09-10，`f78917765` 加本次工作区修改）：

```bash
JAVA_HOME=/Library/Java/JavaVirtualMachines/zulu-11.jdk/Contents/Home \
  mvn -o -pl fluss-rpc,fluss-server -am clean test \
  -Dtest=FlussRequestHandlerTest,DelayedActionQueueTest,DelayedFetchLogTest,ReplicaManagerTest \
  -Dsurefire.failIfNoSpecifiedTests=false
```

- `FlussRequestHandlerTest` 3 个、`DelayedActionQueueTest` 3 个、
  `DelayedFetchLogTest` 4 个、`ReplicaManagerTest` 34 个，共 44 个测试通过，0 failure/error/skip。
- Checkstyle、Spotless、RAT 和 `git diff --check` 通过；未执行全仓库测试。
- 首次增量构建遇到旧 class 的 JDK 签名不兼容；Java 11 clean 重编译后上述测试全部通过。
- 本次仅整合并验证工作区内容，没有 rebase、提交或推送。


## 背景

当 follower 向 leader 发送 `FetchLogRequest` 时，如果当前没有足够数据（`bytesReadable < minFetchBytes`），请求会被放入 `DelayedFetchLog` 中等待，直到有新数据或超时（`maxWaitMs`，默认 500ms）。

问题：**Produce 写入 leader 后，能否主动唤醒等待中的 follower DelayedFetchLog？**

> **范围说明**：本文仅分析 Log 表（append-only）的 produce → 复制路径。PK 表的 `putRecordsToKv` 路径存在类似问题，但不在本文讨论范围内。

---

## 第一部分：Kafka 的完整实现

Kafka 通过 **四个协同机制** 确保 produce 写入后 DelayedFetch 被及时唤醒。

### 1.1 LeaderHwChange 三态跟踪

**文件：** `storage/src/main/java/org/apache/kafka/storage/internals/log/LeaderHwChange.java`

```java
public enum LeaderHwChange {
    INCREASED,  // LEO 增长且 HW 也增长了（如单副本或 follower 已追上）
    SAME,       // LEO 增长但 HW 未变（follower 还没追上，HW 不能提升）
    NONE        // 写入失败或无变化
}
```

**文件：** `storage/src/main/java/org/apache/kafka/storage/internals/log/LogAppendInfo.java`

`LogAppendInfo` 中包含 `leaderHwChange` 字段，由 `Partition.appendRecordsToLeader()` 填充：

```java
// LogAppendInfo 关键字段
private final LeaderHwChange leaderHwChange;

public LeaderHwChange leaderHwChange() { return leaderHwChange; }

// copy 方法用于追加后设置 HW 变化状态
public LogAppendInfo copy(LeaderHwChange newLeaderHwChange) {
    return new LogAppendInfo(..., newLeaderHwChange);
}
```

### 1.2 Partition.appendRecordsToLeader() — 设置 LeaderHwChange

**文件：** `core/src/main/scala/kafka/cluster/Partition.scala:1373`

```scala
def appendRecordsToLeader(records: MemoryRecords, origin: AppendOrigin,
                          requiredAcks: Int, ...): LogAppendInfo = {
  val (info, leaderHWIncremented) = inReadLock(leaderIsrUpdateLock) {
    leaderLogIfLocal match {
      case Some(leaderLog) =>
        val minIsr = effectiveMinIsr(leaderLog)
        val inSyncSize = partitionState.isr.size
        if (inSyncSize < minIsr && requiredAcks == -1) {
          throw new NotEnoughReplicasException(...)
        }

        val info = leaderLog.appendAsLeader(records, ...)

        // ★ 关键：追加后尝试提升 HW，并记录 HW 是否变化
        (info, maybeIncrementLeaderHW(leaderLog))
      case None => throw new NotLeaderOrFollowerException(...)
    }
  }

  // ★ 将 HW 变化状态写入 LogAppendInfo 返回给上层
  info.copy(if (leaderHWIncremented) LeaderHwChange.INCREASED else LeaderHwChange.SAME)
}
```

**设计要点**：`appendRecordsToLeader()` 永远不会返回 `NONE` — 只要方法成功返回，就说明 LEO 增长了。`INCREASED` 表示 HW 也跟着涨了（常见于单副本场景），`SAME` 表示 HW 没变（follower 还没追上）。

### 1.3 ActionQueue — 延迟执行队列

**文件：** `core/src/main/scala/kafka/server/ActionQueue.scala`

```scala
trait ActionQueue {
  def add(action: () => Unit): Unit
  def tryCompleteActions(): Unit
}

class DelayedActionQueue extends Logging with ActionQueue {
  private val queue = new ConcurrentLinkedQueue[() => Unit]()

  def add(action: () => Unit): Unit = queue.add(action)

  def tryCompleteActions(): Unit = {
    val maxToComplete = queue.size()
    var count = 0
    var done = false
    while (!done && count < maxToComplete) {
      try {
        val action = queue.poll()
        if (action == null) done = true
        else action()
      } catch {
        case e: Throwable => error("failed to complete delayed actions", e)
      } finally count += 1
    }
  }
}
```

`ActionQueue` 使用 `ConcurrentLinkedQueue` 实现无锁入队，出队时执行动作。核心原则：**`ReplicaManager` 只入队，调用方负责触发执行**。

### 1.4 ReplicaManager.appendRecords() → addCompletePurgatoryAction() — 加入 ActionQueue

**文件：** `core/src/main/scala/kafka/server/ReplicaManager.scala:751`

```scala
def appendRecords(timeout: Long, requiredAcks: Short, ...,
                  actionQueue: ActionQueue = this.defaultActionQueue, ...): Unit = {
  // Step 1: 写入本地 log
  val localProduceResults = appendToLocalLog(...)

  // Step 2: ★ 将唤醒动作加入 ActionQueue（不立即执行）
  addCompletePurgatoryAction(actionQueue, localProduceResults)

  // Step 3: 若 acks=-1，创建 DelayedProduce 等待 follower 复制
  maybeAddDelayedProduce(requiredAcks, ...)
  // ★ 不在此处调用 tryCompleteActions —— 由调用方负责
}
```

**文件：** `core/src/main/scala/kafka/server/ReplicaManager.scala:923`

```scala
private def addCompletePurgatoryAction(
    actionQueue: ActionQueue,
    appendResults: Map[TopicPartition, LogAppendResult]
): Unit = {
  actionQueue.add {
    () => appendResults.foreach { case (topicPartition, result) =>
      val requestKey = TopicPartitionOperationKey(topicPartition)
      result.info.leaderHwChange match {
        case LeaderHwChange.INCREASED =>
          // HW 提升：唤醒 DelayedProduce + DelayedFetch + DelayedDeleteRecords
          delayedProducePurgatory.checkAndComplete(requestKey)
          delayedFetchPurgatory.checkAndComplete(requestKey)
          delayedDeleteRecordsPurgatory.checkAndComplete(requestKey)
        case LeaderHwChange.SAME =>
          // ★ HW 没变但 LEO 涨了：仅唤醒 DelayedFetch
          // 因为 follower 使用 LOG_END isolation，只需 LEO 增长即可
          delayedFetchPurgatory.checkAndComplete(requestKey)
        case LeaderHwChange.NONE =>
          // 无变化，不唤醒
      }
    }
  }
}
```

**设计要点**：动作被放入 `ActionQueue` 而非直接执行，目的是避免在持有 produce 流程锁时执行唤醒逻辑，防止锁竞争。

### 1.5 KafkaApis.handle() 和 KafkaRequestHandler — 统一触发

动作在两个地方被统一触发，确保每次请求处理完毕后都能执行队列中的待完成操作。

**文件：** `core/src/main/scala/kafka/server/KafkaApis.scala:171`

```scala
override def handle(request: RequestChannel.Request, requestLocal: RequestLocal): Unit = {
  try {
    request.header.apiKey match {
      case ApiKeys.PRODUCE => handleProduceRequest(request)
      case ApiKeys.FETCH   => handleFetchRequest(request)
      // ... 其他 API
    }
  } catch { ... }
  finally {
    // ★ 每次请求处理完后统一执行 ActionQueue
    replicaManager.tryCompleteActions()
  }
}
```

**文件：** `core/src/main/scala/kafka/server/KafkaRequestHandler.scala:148`

```scala
// 处理 callback（如 DelayedProduce 的 responseCallback）之后也要执行
finally {
  apis.tryCompleteActions()
}
```

触发点在 **请求处理框架层**，而非业务层。所有 API 类型（PRODUCE、FETCH 等）处理完后都会统一触发。

### 1.6 ReplicaFetcherThread — Follower 侧唤醒 Consumer Fetch

**文件：** `core/src/main/scala/kafka/server/ReplicaFetcherThread.scala`

```scala
private[server] val partitionsWithNewHighWatermark = mutable.Buffer[TopicPartition]()

override def doWork(): Unit = {
  super.doWork()                    // 执行 fetch 并处理返回数据
  completeDelayedFetchRequests()    // ★ 唤醒 consumer 的 delayed fetch
}

override def processPartitionData(topicPartition: TopicPartition,
                                   fetchOffset: Long, ...): Option[LogAppendInfo] = {
  val partition = replicaMgr.getPartitionOrException(topicPartition)
  val log = partition.localLogOrException

  // 追加 leader 的数据到 follower log
  val logAppendInfo = partition.appendRecordsToFollowerOrFutureReplica(records, ...)

  // ★ 若 follower HW 被更新，记录下来
  log.maybeUpdateHighWatermark(partitionData.highWatermark).foreach { newHighWatermark =>
    partitionsWithNewHighWatermark += topicPartition
  }
  logAppendInfo
}

private def completeDelayedFetchRequests(): Unit = {
  if (partitionsWithNewHighWatermark.nonEmpty) {
    // ★ 唤醒这些 partition 上等待 HW 推进的 consumer DelayedFetch
    replicaMgr.completeDelayedFetchRequests(partitionsWithNewHighWatermark.toSeq)
    partitionsWithNewHighWatermark.clear()
  }
}
```

**文件：** `core/src/main/scala/kafka/server/ReplicaManager.scala:456`

```scala
private[server] def completeDelayedFetchRequests(topicPartitions: Seq[TopicPartition]): Unit = {
  topicPartitions.foreach(tp =>
    delayedFetchPurgatory.checkAndComplete(TopicPartitionOperationKey(tp)))
}
```

### 1.7 DelayedFetch.tryComplete() — 判断是否可完成

**文件：** `core/src/main/scala/kafka/server/DelayedFetch.scala:74`

```scala
override def tryComplete(): Boolean = {
  var accumulatedSize = 0
  fetchPartitionStatus.foreach { case (topicIdPartition, fetchStatus) =>
    val fetchOffset = fetchStatus.startOffsetMetadata
    try {
      if (fetchOffset != LogOffsetMetadata.UNKNOWN_OFFSET_METADATA) {
        val partition = replicaManager.getPartitionOrException(...)
        val offsetSnapshot = partition.fetchOffsetSnapshot(...)

        // ★ 根据 isolation 级别选择 endOffset
        val endOffset = params.isolation match {
          case FetchIsolation.LOG_END       => offsetSnapshot.logEndOffset      // follower 用这个
          case FetchIsolation.HIGH_WATERMARK => offsetSnapshot.highWatermark    // consumer 用这个
          case FetchIsolation.TXN_COMMITTED => offsetSnapshot.lastStableOffset  // 事务 consumer 用这个
        }

        if (fetchOffset.messageOffset < endOffset.messageOffset) {
          if (fetchOffset.onOlderSegment(endOffset)) {
            return forceComplete()   // Case F: 跨 segment
          } else if (fetchOffset.onSameSegment(endOffset)) {
            val bytesAvailable = math.min(endOffset.positionDiff(fetchOffset), ...)
            accumulatedSize += bytesAvailable
          }
        }
      }
    } catch { ... }
  }
  // Case G: 累积字节数 >= minBytes
  if (accumulatedSize >= params.minBytes) forceComplete() else false
}
```

### Kafka 完整流程图

```
Producer Request
  │
  ▼
KafkaApis.handle()
  │
  ├─ handleProduceRequest()
  │    │
  │    ▼
  │  ReplicaManager.appendRecords()                          [ReplicaManager.scala:751]
  │    │
  │    ├─ (1) appendToLocalLog()                             [ReplicaManager.scala:1372]
  │    │       │
  │    │       └─ Partition.appendRecordsToLeader()           [Partition.scala:1373]
  │    │              ├─ leaderLog.appendAsLeader(records)     // LEO 增长
  │    │              ├─ maybeIncrementLeaderHW(leaderLog)     // 尝试提升 HW
  │    │              └─ return info.copy(INCREASED or SAME)   // ★ 返回 HW 变化状态
  │    │
  │    ├─ (2) addCompletePurgatoryAction(actionQueue, results)  [ReplicaManager.scala:923]
  │    │       └─ actionQueue.add { () =>                       // ★ 只入队，不执行
  │    │              INCREASED → checkAndComplete(delayedProduce + delayedFetch + delayedDelete)
  │    │              SAME      → checkAndComplete(delayedFetch)   // ★ 唤醒 follower fetch
  │    │              NONE      → (nothing)
  │    │          }
  │    │
  │    └─ (3) maybeAddDelayedProduce(...)                     // acks=-1 时等待 follower
  │
  └─ finally:
       replicaManager.tryCompleteActions()                    [KafkaApis.scala:280]
         └─ actionQueue.tryCompleteActions()                  [ActionQueue.scala:51]
              └─ 执行队列中所有 action（包括上面的 checkAndComplete）
                   └─ DelayedFetch.tryComplete()              [DelayedFetch.scala:74]
                        └─ 检查 LEO/HW 是否增长 → forceComplete()
```

---

## 第二部分：Fluss 现状分析

### 2.1 Produce 路径 — 缺失唤醒

**文件：** `fluss-server/.../replica/ReplicaManager.java:623`

```java
public void appendRecordsToLog(int timeoutMs, int requiredAcks,
        Map<TableBucket, MemoryLogRecords> entriesPerBucket,
        @Nullable UserContext userContext,
        Consumer<List<ProduceLogResultForBucket>> responseCallback) {
    // ...
    Map<TableBucket, ProduceLogResultForBucket> appendResult =
            appendToLocalLog(entriesPerBucket, requiredAcks, userContext);

    // ★ 缺失：这里没有唤醒 delayedFetchLogManager

    // 若 acks=-1，创建 DelayedWrite
    maybeAddDelayedWrite(timeoutMs, requiredAcks, entriesPerBucket.size(),
                         appendResult, responseCallback);
}
```

### 2.2 appendToLocalLog — 未传递 HW 变化状态

**文件：** `fluss-server/.../replica/ReplicaManager.java:1270`

```java
private Map<TableBucket, ProduceLogResultForBucket> appendToLocalLog(
        Map<TableBucket, MemoryLogRecords> entriesPerBucket,
        int requiredAcks, @Nullable UserContext userContext) {
    Map<TableBucket, ProduceLogResultForBucket> resultForBucketMap = new HashMap<>();
    for (Map.Entry<TableBucket, MemoryLogRecords> entry : entriesPerBucket.entrySet()) {
        TableBucket tb = entry.getKey();
        try {
            Replica replica = getReplicaOrException(tb);
            LogAppendInfo appendInfo = replica.appendRecordsToLeader(records, requiredAcks);

            // ★ 只记录了 offset，没有记录 HW 是否变化
            resultForBucketMap.put(tb,
                new ProduceLogResultForBucket(tb, baseOffset, appendInfo.lastOffset() + 1));
        } catch (Exception e) {
            resultForBucketMap.put(tb,
                new ProduceLogResultForBucket(tb, ApiError.fromThrowable(e)));
        }
    }
    return resultForBucketMap;
}
```

### 2.3 Replica.appendRecordsToLeader() — HW 变化被忽略

**文件：** `fluss-server/.../replica/Replica.java:1039`

```java
public LogAppendInfo appendRecordsToLeader(MemoryLogRecords memoryLogRecords,
                                           int requiredAcks) throws Exception {
    return inReadLock(leaderIsrUpdateLock, () -> {
        // ...
        LogAppendInfo appendInfo;
        appendInfo = logTablet.appendAsLeader(memoryLogRecords);

        // ★ maybeIncrementLeaderHW 的返回值（boolean）被忽略
        // ★ 没有将 HW 变化状态传递给上层
        // ★ 没有调用 tryCompleteDelayedOperations()
        maybeIncrementLeaderHW(logTablet, clock.milliseconds());

        return appendInfo;
    });
}
```

对比 Kafka 的 `Partition.appendRecordsToLeader()`：
- Kafka 记录 `maybeIncrementLeaderHW` 返回值，映射为 `LeaderHwChange.INCREASED` 或 `SAME`
- Kafka 通过 `info.copy(LeaderHwChange)` 把状态传回 `ReplicaManager`
- Fluss 直接丢弃了这个返回值

### 2.4 Fluss LogAppendInfo — 缺少 LeaderHwChange

**文件：** `fluss-server/.../log/LogAppendInfo.java`

Fluss 的 `LogAppendInfo` 只包含 offset、timestamp、validBytes 等基础字段，**不包含 `LeaderHwChange` 状态**，无法将 HW 变化信息传递给上层。

### 2.5 ReplicaFetcherThread.doWork() — TODO 未实现

**文件：** `fluss-server/.../replica/fetcher/ReplicaFetcherThread.java:136`

```java
@Override
public void doWork() {
    maybeFetch();
    // TODO, if we support fetch from follower, we need to complete delayed fetch log operation
    // here.
}
```

对比 Kafka 的 `ReplicaFetcherThread.doWork()`：
- Kafka 在 `processPartitionData()` 中记录 HW 更新的 partition
- `doWork()` 结束时调用 `completeDelayedFetchRequests()` 唤醒 consumer 的 DelayedFetch
- Fluss 只有一个 TODO 注释

### 2.6 TabletService — 无请求后处理

**文件：** `fluss-server/.../tablet/TabletService.java:188`

```java
@Override
public CompletableFuture<ProduceLogResponse> produceLog(ProduceLogRequest request) {
    // ...
    replicaManager.appendRecordsToLog(...);
    return response;
    // ★ 没有类似 KafkaApis.handle() finally 块中的 tryCompleteActions()
}
```

### 2.7 FlussRequestHandler — 无框架层后处理

**文件：** `fluss-rpc/.../netty/server/FlussRequestHandler.java:54`

```java
public void processRequest(FlussRequest request) {
    // ...
    CompletableFuture<?> responseFuture =
            (CompletableFuture<?>) api.getMethod().invoke(service, message);
    // ★ 没有类似 KafkaApis.handle() finally 块中的 tryCompleteActions()
    responseFuture.whenComplete(...);
}
```

Fluss 的 RPC 框架层没有 ActionQueue 机制，也没有在请求处理完毕后统一触发 delayed operations 的入口。

### 2.8 现有唤醒路径汇总

Fluss 中 `tryCompleteDelayedOperations()`（唤醒 DelayedWrite + DelayedFetchLog）的所有调用点：

| 触发场景 | 代码位置 | 说明 |
|---------|---------|------|
| Follower fetch 更新 LEO → HW 提升 | `Replica.java:1278` `updateFollowerFetchState()` | 仅在 `leaderHWIncremented == true` 时触发 |
| makeLeader 时 HW 提升 | `Replica.java:474` | 仅在 `leaderHWIncremented == true` 时触发 |
| ISR 变更时 HW 提升 | `Replica.java:1914` `submitAdjustIsr()` | 仅在 `hwIncremented == true` 时触发 |
| becomeFollower | `ReplicaManager.java:1222` `completeDelayedOperations()` | 角色转换时强制完成 |
| stopReplica | `ReplicaManager.java:1925` `completeDelayedOperations()` | 停止副本时强制完成 |
| **Produce 写入 (Log 表)** | **❌ 缺失** | **核心差距** |

### 2.9 实际影响分析

**对 follower 复制的影响（`FetchIsolation.LOG_END`）：**
- Follower fetch 携带 `minBytes = 1`，`maxWaitMs = 500ms`
- 当 follower fetch 到达 leader 时恰好无新数据 → 进入 `DelayedFetchLog`
- Producer 写入 → LEO 增长 → 但无人调用 `checkAndComplete`
- **必须等待 500ms 超时** 才能完成该 DelayedFetchLog
- 复制延迟从理想的 ~0ms 增加到最多 500ms

**对 acks=-1 producer 的影响：**
- 复制延迟增大 → `DelayedWrite` 中等待的 HW 提升更慢 → 端到端延迟增加
- 最差情况：produce 延迟增加 ~500ms

**低吞吐 vs 高吞吐：**
- 高吞吐场景影响较小：fetch 到达时通常已有数据，不会进入 delayed 状态
- 低吞吐场景影响显著：间歇性写入时 follower fetch 大概率进入 delayed

---

## 第三部分：修复方案 — ActionQueue 机制

### 方案选型

基于对 Kafka 和 Fluss 代码的深入分析，评估了三种方案：

#### 不采用：直接 checkAndComplete（方案 A）

在 `appendRecordsToLog()` 末尾直接调用 `delayedFetchLogManager.checkAndComplete()`。虽然最简单，但：
- 不够可扩展：未来新增 delayed operation 类型或写入路径时需逐一手动添加
- 不与 Kafka 对齐：缺少 ActionQueue 作为策略抽象的灵活性

#### 不采用：LeaderHwChange 三态（方案 C）

分析 `maybeIncrementLeaderHW()`（`Replica.java:1161`）在 produce 路径上的行为：

| LeaderHwChange | 场景 | 需唤醒 DelayedFetchLog? | 需唤醒 DelayedWrite? | 实际存在 DelayedWrite? |
|---|---|---|---|---|
| `INCREASED` | 单副本 produce | 无 follower，无 delayed fetch | 是 | 否（`delayedWriteRequired` = false） |
| `SAME` | 多副本 produce | **是** | 否（HW 没变） | 可能有，但 HW 没变无法完成 |
| `NONE` | produce 失败 | 否 | 否 | — |

三态跟踪的精细区分最终退化为：**produce 成功就唤醒 `delayedFetchLogManager`，失败就不唤醒** — 这正是通过 `succeeded()` 判断就能做到的事情。

此外，`ReplicaFetcherThread.completeDelayedFetchRequests()` 是为了唤醒 **consumer** 的 DelayedFetch（使用 `HIGH_WATERMARK` isolation），与当前要解决的 **follower 复制延迟**问题无关，不应混在同一个改动中。

#### 采用：ActionQueue 机制（方案 B）

参照 Kafka 的设计，引入 ActionQueue 架构：
- `ReplicaManager.appendRecordsToLog()` 中通过 `addCompletePurgatoryAction()` 将唤醒 action 入队
- 请求处理框架层（`FlussRequestHandler.processRequest()`）在方法调用后统一触发 `tryCompleteActions()`

**为什么选择 ActionQueue：**

1. **可扩展** — 未来新增 delayed operation 类型（如 DelayedDeleteRecords）或新增写入路径时，只需在 `addCompletePurgatoryAction()` 中追加即可
2. **策略灵活** — ActionQueue 是接口，调用方可选择延迟执行（`DelayedActionQueue`）或立即执行（Kafka 中 `CoordinatorPartitionWriter` 的 `directActionQueue` 模式）
3. **与 Kafka 对齐** — 维护者熟悉的设计模式，降低理解成本

**简化设计 — 不引入 LeaderHwChange 三态：**

`addCompletePurgatoryAction()` 只需检查 `succeeded()` 即可，不需要引入 `LeaderHwChange` 枚举。只唤醒 `delayedFetchLogManager`，不唤醒 `delayedWriteManager`（理由见上表分析）。

### 触发策略：请求处理框架层触发

Kafka 的 `tryCompleteActions()` 触发点在 **请求处理框架层**（`KafkaApis.handle()` finally 块），而非业务层。对应到 Fluss，触发点应在 `FlussRequestHandler.processRequest()` 中，而非 `TabletService.produceLog()`。

```
Kafka:    KafkaApis.handle()                   finally { tryCompleteActions() }
Fluss:    FlussRequestHandler.processRequest()  invoke 之后 tryCompleteActions()
```

#### 为什么 Fluss 的异步模型（CompletableFuture）下这样做是等价的

Fluss 的 `FlussRequestHandler.processRequest()` 通过反射调用 `TabletService.produceLog()` 并得到一个 `CompletableFuture`。虽然返回的是异步 future，但 **所有写入工作在 `invoke` 返回前已同步完成**：

```
RequestProcessor thread 上的执行时间线：
──────────────────────────────────────────────────────────────
FlussRequestHandler.processRequest(request)
  │
  ├─ CompletableFuture<?> responseFuture = api.invoke(service, message)
  │   │
  │   └─ 实际调用 TabletService.produceLog()
  │        └─ replicaManager.appendRecordsToLog()
  │             ├─ appendToLocalLog()              // ① 写入 log（同步）
  │             ├─ addCompletePurgatoryAction()    // ② 入队（同步）
  │             └─ maybeAddDelayedWrite()          // ③ 创建 DelayedWrite（同步）
  │
  │   ← invoke 返回时，① ② ③ 已全部在当前线程同步完成
  │
  ├─ ★ tryCompleteActions()                        // ④ 执行队列（同步）
  │
  └─ responseFuture.whenComplete(...)              // 注册响应回调
──────────────────────────────────────────────────────────────
```

- **acks=1**：`maybeAddDelayedWrite` 内部直接调用 `responseCallback`，future 在 invoke 返回前已 complete
- **acks=-1**：future 还未 complete（等 follower 复制 → HW 提升 → DelayedWrite 完成），但 action 入队 ② 已同步发生

因此在 `invoke` 之后调用 `tryCompleteActions()` ④，时机与 Kafka 的 `finally { tryCompleteActions() }` 等价——都是在写入 + 入队同步完成后、同一线程上立即执行。

#### 框架层触发 vs 业务层触发

将 `tryCompleteActions()` 放在框架层（`FlussRequestHandler`）而非业务层（`TabletService`）：
- **不遗漏** — 框架层对所有 API 请求统一后处理，新增写入 API 不需要记得手动加
- **职责清晰** — `ReplicaManager` 负责入队，框架层负责触发，`TabletService` 专注业务逻辑
- **与 Kafka 对齐** — `KafkaApis.handle()` 也是框架层，对所有 API 类型统一 finally

---

## 第四部分：Kafka vs Fluss 完整对比

| 维度 | Kafka | Fluss（修复前） | 差距 |
|------|-------|---------|------|
| **Produce 后唤醒 DelayedFetch** | ✅ `addCompletePurgatoryAction` 将动作加入 `ActionQueue` | ❌ 缺失 | 核心差距 |
| **LeaderHwChange 跟踪** | ✅ INCREASED/SAME/NONE 三态，在 `LogAppendInfo` 中传递 | ❌ `maybeIncrementLeaderHW` 返回值被忽略，`LogAppendInfo` 无此字段 | 信息丢失 |
| **ActionQueue 机制** | ✅ 请求处理完后统一执行，避免锁竞争 | ❌ 不存在 | 架构缺失 |
| **RPC 层后处理** | ✅ `KafkaApis.handle()` finally + `KafkaRequestHandler` callback finally | ❌ `FlussRequestHandler` 和 `TabletService` 均无 finally 逻辑 | 执行点缺失 |
| **Follower fetcher 唤醒 consumer fetch** | ✅ `doWork()` 中 `completeDelayedFetchRequests()` | ❌ 有 TODO 注释未实现 | 未实现 |
| **Follower 无新数据时的复制延迟** | ≈ 0ms（produce 后立即唤醒） | 最差 500ms（`maxWaitMs` 超时） | 性能差距 |
| **DelayedFetch tryComplete 逻辑** | ✅ 支持 LOG_END / HIGH_WATERMARK / TXN_COMMITTED 三种 isolation | ✅ 支持 LOG_END / HIGH_WATERMARK 两种 isolation | 基本一致 |
| **Follower fetch 参数** | `minBytes=1`, `maxWaitMs=500ms` | `minBytes=1`, `maxWaitMs=500ms` | 一致 |

---

## 第五部分：实施计划

### Step 1: 创建 ActionQueue 接口

**新建文件：** `fluss-server/src/main/java/org/apache/fluss/server/replica/delay/ActionQueue.java`

**包位置：** `org.apache.fluss.server.replica.delay` — 与 `DelayedOperationManager`、`DelayedFetchLog` 等同包

```java
package org.apache.fluss.server.replica.delay;

import org.apache.fluss.annotation.Internal;

/**
 * A queue for collecting actions which need to be executed later.
 *
 * <p>This is used to decouple the enqueuing of delayed operation completions from their execution.
 * For example, after appending records, we enqueue actions to complete delayed fetch operations,
 * then execute them after the write path is fully finished.
 */
@Internal
public interface ActionQueue {

    /** Adds an action to this queue. */
    void add(Runnable action);

    /** Tries to complete all pending actions in the queue. */
    void tryCompleteActions();
}
```

### Step 2: 创建 DelayedActionQueue 实现

**新建文件：** `fluss-server/src/main/java/org/apache/fluss/server/replica/delay/DelayedActionQueue.java`

```java
package org.apache.fluss.server.replica.delay;

import org.apache.fluss.annotation.Internal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Default implementation of {@link ActionQueue} that collects actions into a concurrent queue and
 * executes them when {@link #tryCompleteActions()} is called.
 *
 * <p>Uses {@link ConcurrentLinkedQueue} for lock-free enqueue. Actions are executed and removed
 * from the queue when {@link #tryCompleteActions()} is called.
 */
@Internal
public class DelayedActionQueue implements ActionQueue {
    private static final Logger LOG = LoggerFactory.getLogger(DelayedActionQueue.class);

    private final ConcurrentLinkedQueue<Runnable> queue = new ConcurrentLinkedQueue<>();

    @Override
    public void add(Runnable action) {
        queue.add(action);
    }

    @Override
    public void tryCompleteActions() {
        int maxToComplete = queue.size();
        int count = 0;
        while (count < maxToComplete) {
            Runnable action = queue.poll();
            if (action == null) {
                break;
            }
            try {
                action.run();
            } catch (Throwable t) {
                LOG.error("Failed to complete delayed action.", t);
            }
            count++;
        }
    }
}
```

### Step 3: 集成到 ReplicaManager

**修改文件：** `fluss-server/src/main/java/org/apache/fluss/server/replica/ReplicaManager.java`

#### 3a. 新增字段

```java
private final ActionQueue actionQueue;
```

在构造函数中初始化：

```java
this.actionQueue = new DelayedActionQueue();
```

#### 3b. 新增 addCompletePurgatoryAction 方法

```java
/**
 * Adds actions to complete delayed fetch log operations for successfully written buckets.
 *
 * <p>Actions are added to the {@link ActionQueue} rather than executed immediately. The caller
 * is responsible for invoking {@link #tryCompleteActions()} to execute the queued actions after
 * the write path is fully finished.
 */
private void addCompletePurgatoryAction(
        Map<TableBucket, ? extends WriteResultForBucket> writeResults) {
    actionQueue.add(
            () -> {
                for (Map.Entry<TableBucket, ? extends WriteResultForBucket> entry :
                        writeResults.entrySet()) {
                    if (entry.getValue().succeeded()) {
                        delayedFetchLogManager.checkAndComplete(
                                new DelayedTableBucketKey(entry.getKey()));
                    }
                }
            });
}
```

#### 3c. 修改 appendRecordsToLog()

删除现有的直接 `checkAndComplete` 循环，替换为 `addCompletePurgatoryAction` 入队调用：

```java
public void appendRecordsToLog(
        int timeoutMs,
        int requiredAcks,
        Map<TableBucket, MemoryLogRecords> entriesPerBucket,
        @Nullable UserContext userContext,
        Consumer<List<ProduceLogResultForBucket>> responseCallback) {
    // ... validation ...

    Map<TableBucket, ProduceLogResultForBucket> appendResult =
            appendToLocalLog(entriesPerBucket, requiredAcks, userContext);

    // Enqueue delayed fetch completions — not executed here.
    // Framework layer invokes tryCompleteActions() after this method returns.
    addCompletePurgatoryAction(appendResult);

    // Maybe create DelayedWrite for acks=-1, or invoke callback for acks=1.
    maybeAddDelayedWrite(
            timeoutMs, requiredAcks, entriesPerBucket.size(), appendResult, responseCallback);
}
```

#### 3d. 暴露公共方法

```java
/** Tries to complete all pending delayed actions in the action queue. */
public void tryCompleteActions() {
    actionQueue.tryCompleteActions();
}
```

### Step 4: 修改 FlussRequestHandler（框架层触发）

**修改文件：** `fluss-rpc/src/main/java/org/apache/fluss/rpc/netty/server/FlussRequestHandler.java`

在 `processRequest()` 中，`invoke` 之后、`whenComplete` 之前，调用 `tryCompleteActions()`：

```java
@Override
public void processRequest(FlussRequest request) {
    // ... session setup, leader check ...
    try {
        // invoke the corresponding method on RpcGateway instance.
        CompletableFuture<?> responseFuture =
                (CompletableFuture<?>) api.getMethod().invoke(service, message);

        // ★ 新增：执行写入路径中入队的延迟操作（如唤醒 DelayedFetchLog）
        service.tryCompleteActions();

        responseFuture.whenComplete(
                (response, throwable) -> {
                    // ... response handling ...
                });
    } catch (Throwable t) {
        // ... error handling ...
    }
}
```

这需要在 `RpcGatewayService` 接口中增加 `tryCompleteActions()` 默认方法：

**修改文件：** `fluss-rpc/src/main/java/org/apache/fluss/rpc/RpcGatewayService.java`

```java
/** Tries to complete all pending delayed actions. Default no-op for services without ActionQueue. */
default void tryCompleteActions() {}
```

**修改文件：** `fluss-server/src/main/java/org/apache/fluss/server/tablet/TabletService.java`

```java
@Override
public void tryCompleteActions() {
    replicaManager.tryCompleteActions();
}
```

### Step 5: 验证测试

**文件：** `fluss-server/src/test/java/org/apache/fluss/server/replica/delay/DelayedFetchLogTest.java`

现有测试直接调用 `replicaManager.appendRecordsToLog()`，绕过了框架层。需要在测试中 produce 后补充 `replicaManager.tryCompleteActions()` 调用。

需验证：
- `testCompleteDelayedFetchLog` — 需在 produce 后补充 `tryCompleteActions()` 调用
- `testProduceAutoCompletesDelayedFetchLog` — 同上

---

## 执行顺序分析

### 完整调用链

```
FlussRequestHandler.processRequest(request)
  │
  ├─ api.invoke(service, message)  →  TabletService.produceLog()
  │    │
  │    └─ replicaManager.appendRecordsToLog(...)
  │         ├─ appendToLocalLog()              → LEO 增长
  │         ├─ addCompletePurgatoryAction()    → 将 checkAndComplete action 入队（不执行）
  │         └─ maybeAddDelayedWrite()          → acks=1: 直接 callback
  │                                            → acks=-1: 创建 DelayedWrite
  │   ← invoke 返回（同步）
  │
  ├─ service.tryCompleteActions()              → 执行队列中的 action，唤醒 follower fetch
  │
  └─ responseFuture.whenComplete(...)          → 注册异步响应回调
```

### 为什么先 `addCompletePurgatoryAction` 后 `maybeAddDelayedWrite`？

- `addCompletePurgatoryAction` 只是入队，不执行，顺序不影响行为
- `maybeAddDelayedWrite` 需要在 `tryCompleteActions` 之前完成，确保 acks=-1 时 DelayedWrite 已注册 watch
- `tryCompleteActions` 由框架层在 `invoke` 返回后统一执行，此时 produce 响应已发出（acks=1）或 DelayedWrite 已就绪（acks=-1）

---

## 涉及文件总结

| 文件 | 操作 | 说明 |
|------|------|------|
| `.../delay/ActionQueue.java` | **新建** | 接口定义 |
| `.../delay/DelayedActionQueue.java` | **新建** | ConcurrentLinkedQueue 实现 |
| `.../replica/ReplicaManager.java` | **修改** | 集成 ActionQueue，修改 `appendRecordsToLog` |
| `.../rpc/RpcGatewayService.java` | **修改** | 新增 `tryCompleteActions()` 默认方法 |
| `.../rpc/netty/server/FlussRequestHandler.java` | **修改** | `invoke` 后调用 `service.tryCompleteActions()` |
| `.../tablet/TabletService.java` | **修改** | 实现 `tryCompleteActions()` 委托给 ReplicaManager |
| `.../delay/DelayedFetchLogTest.java` | **修改** | 补充 `tryCompleteActions()` 调用 |

**预期效果**：Log 表 follower 复制延迟从最差 500ms 降低到 ~0ms（produce 后立即唤醒）。

**后续独立优化**（不在本次范围内）：
- PK 表 `putRecordsToKv` 路径接入 ActionQueue
- `ReplicaFetcherThread.doWork()` 中实现 `completeDelayedFetchRequests()`，用于支持 consumer fetch from follower 场景

---

## 验证步骤

```bash
# 1. 运行 delayed fetch 相关测试
./mvnw test -Dtest=DelayedFetchLogTest -pl fluss-server

# 2. 代码格式检查
./mvnw spotless:check -pl fluss-server

# 3. 完整模块测试
./mvnw verify -pl fluss-server
```
