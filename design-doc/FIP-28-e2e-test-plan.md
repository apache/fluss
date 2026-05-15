# FIP-28 端到端测试计划

本文档描述如何在真实环境中测试 "Support Write and Lookup for Expired Partitions" 功能，重点覆盖历史分区与当前分区共存场景下的正确性和隔离性。

---

## 1. 环境准备

### 1.1 表定义

测试需要两种表：PK 表和 Log 表，均启用 auto-partition + lake tiering。

**PK 表**:

```sql
CREATE TABLE pk_table (
    dt STRING,
    id BIGINT,
    name STRING,
    amount DOUBLE,
    PRIMARY KEY (dt, id) NOT ENFORCED
) PARTITIONED BY (dt)
WITH (
    'table.auto-partition.enabled' = 'true',
    'table.auto-partition.time-unit' = 'DAY',
    'table.auto-partition.num-precreate' = '1',
    'table.auto-partition.num-retention' = '3',  -- 保留 3 天
    'table.datalake.enabled' = 'true',
    'bucket.num' = '4'
);
```

**Log 表**:

```sql
CREATE TABLE log_table (
    dt STRING,
    event_id BIGINT,
    payload STRING
) PARTITIONED BY (dt)
WITH (
    'table.auto-partition.enabled' = 'true',
    'table.auto-partition.time-unit' = 'DAY',
    'table.auto-partition.num-precreate' = '1',
    'table.auto-partition.num-retention' = '3',
    'table.datalake.enabled' = 'true',
    'bucket.num' = '4'
);
```

### 1.2 如何构造"历史分区与当前分区共存"

核心思路：**不需要真正等待 TTL 过期**。只需向一个命名上落在 TTL 边界之前的分区写入数据，客户端的过期谓词就会判定它为已过期并 redirect 到 `__historical__`。

**构造方法**：

```
假设 TTL = 3 天，今天是 2026-05-12

当前分区（正常路径）：dt = '2026-05-12'、'2026-05-11'、'2026-05-10' (在 retention window 内)
过期分区（历史路径）：dt = '2026-05-01'、'2026-04-15'、'2024-01-01' (在 retention window 外)
```

- **当前分区**：直接写入，走正常写入路径
- **过期分区**：写入时客户端检测到分区名过期 + metadata 中不存在 → 触发 redirect → 数据写入 `__historical__`

两者可以**在同一个 producer/writer 中交替写入**，从而构造共存场景。

### 1.3 前置确认

- Paimon catalog 已配置且可用
- Tiering 服务正常运行
- `__historical__` 分区尚未存在（首次写入过期分区时应触发自动创建）

---

## 2. 测试场景

### 场景 1: 基本功能 — PK 表写入过期分区

**目的**: 验证 PK 表对过期分区的 upsert 能正确 redirect 到 `__historical__`，并生成正确 changelog。

**步骤**:

1. 先向 lake 写入初始数据（确保 lake 中有旧值可供 old-value lookup）：
   - 创建分区 `dt = '2026-05-01'`
   - 写入 `(dt='2026-05-01', id=1, name='Alice', amount=100.0)`
   - 等待 tiering 完成，数据落入 Paimon
   - 等待 AutoPartitionManager 的 TTL 检查将该分区过期删除（或手动删除该分区模拟过期）
2. 写入过期分区的 upsert：
   - `(dt='2026-05-01', id=1, name='Alice', amount=200.0)` — 更新已有 key
   - `(dt='2026-05-01', id=2, name='Bob', amount=50.0)` — 插入新 key
3. 写入过期分区的 delete：
   - 删除 `(dt='2026-05-01', id=1)`
4. 连续写入两次同 key 的 upsert（不等待 flush），验证 old-value 来自 prewrite buffer：
   - `(dt='2026-05-01', id=3, name='V1', amount=10.0)`
   - `(dt='2026-05-01', id=3, name='V2', amount=20.0)`

**验证**:

- `__historical__` 分区被自动创建
- 从 `__historical__` 消费 changelog：
  - id=1 的 upsert 产生 `UPDATE_BEFORE(amount=100.0)` + `UPDATE_AFTER(amount=200.0)`（old-value 从 lake 获取）
  - id=2 的 insert 产生 `INSERT(name='Bob', amount=50.0)`
  - id=1 的 delete 产生 `DELETE(name='Alice', amount=200.0)`
  - id=3 的第二次 upsert 产生 `UPDATE_BEFORE(name='V1', amount=10.0)` + `UPDATE_AFTER(name='V2', amount=20.0)`（old-value 从 prewrite buffer 获取）
- composite key 隔离正确：不同 original partition 的相同 id 不会碰撞

### 场景 2: 基本功能 — Log 表写入过期分区

**步骤**:

1. 向当前分区 `dt = '2026-05-12'` 写入若干条记录
2. 同时向过期分区 `dt = '2026-04-01'` 写入若干条记录
3. 从 `__historical__` 消费

**验证**:

- 过期分区的记录出现在 `__historical__` 中
- 当前分区的记录正常出现在 `dt = '2026-05-12'` 分区中
- consumer 能从 `__historical__` 的 row payload 中还原 `dt = '2026-04-01'`

### 场景 3: 基本功能 — 过期分区 Lookup

**步骤**:

1. 准备 lake 中的数据（同场景 1 步骤 1）
2. 对已过期的 partition 执行 point lookup：
   - `lookup(dt='2026-05-01', id=1)` — 数据仅在 lake
3. 向过期分区写入新数据：
   - `(dt='2026-05-01', id=3, name='Charlie', amount=300.0)`
4. 立即 lookup：
   - `lookup(dt='2026-05-01', id=3)` — 数据在 local（尚未 tier）

**验证**:

- 步骤 2: 返回正确值（lake fallback）
- 步骤 4: 返回最新值（local-first，不依赖 tiering）
- `lookup(dt='2026-05-01', id=999)` → 返回 null

#### 场景 3a: Lookup 触发 `__historical__` 创建

**目的**: 验证 lookup 路径也能触发 `__historical__` 的自动创建（设计文档 A.1: "regardless of whether the operation is a write or a lookup"）。

**前提**: `__historical__` 尚未创建（使用独立的表，或确保在场景 1 之前执行）。

**步骤**:

1. 准备 lake 中的数据（同场景 1 步骤 1）
2. 不先写入，直接执行 lookup：`lookup(dt='2026-05-01', id=1)`

**验证**:

- `__historical__` 被 lookup 路径自动创建
- lookup 返回正确值（lake fallback）

### 场景 4: 多过期分区写入同一 `__historical__` — Composite Key 隔离

**目的**: 验证不同 original partition 的 key space 在 `__historical__` 中完全隔离。

**步骤**:

1. 向过期分区 `dt = '2026-04-01'` 写入 `(id=1, name='April-Alice', amount=100.0)`
2. 向过期分区 `dt = '2026-03-01'` 写入 `(id=1, name='March-Alice', amount=200.0)`
3. 分别 lookup：
   - `lookup(dt='2026-04-01', id=1)`
   - `lookup(dt='2026-03-01', id=1)`

**验证**:

- lookup 返回各自正确的值，不互相干扰
- `dt='2026-04-01'` 的 id=1 返回 `name='April-Alice'`
- `dt='2026-03-01'` 的 id=1 返回 `name='March-Alice'`

---

### 场景 5: 隔离性 — 同一 Pipeline 中过期分区写入不影响实时分区写入延迟

**目的**: 最核心的隔离性验证。过期分区的写入可以慢（lake I/O 耗时），但不能拖慢同一 pipeline 中实时分区的写入延迟。**实时分区的写入延迟是唯一关注指标**，过期分区写入延迟不关心。

**场景描述**:

真实业务中，一条 Flink pipeline 的上游数据流天然混合了实时数据和迟到数据。同一个 Sink 算子内的同一个 writer 会同时处理两类记录：
- 实时分区记录 → 走同步本地 RocksDB，毫秒级完成
- 过期分区记录 → redirect 到 `__historical__`，服务端需要 lake old-value lookup，可能耗时数百毫秒甚至秒级

如果隔离做得不好，过期分区的慢写入会阻塞 Sender 线程或 batch drain，导致实时分区的 batch 也被堵住，实时写入延迟从毫秒级劣化到秒级。

**步骤**:

1. 创建 Flink 作业，数据源产生混合流，写入同一张 Fluss PK 表：
   - 实时记录：目标分区 `dt = '2026-05-12'`（当前分区，存在于 metadata）
   - 迟到记录：目标分区 `dt = '2026-04-01'`（已过期，不存在于 metadata）
   - 两类记录交替产生，模拟正常业务流中夹杂少量迟到数据
2. 在作业运行期间，持续观察**实时分区的写入延迟**

**验证**:

- 混入过期分区数据后，实时分区 p99 写入延迟相对于纯实时基准的劣化 < 10%
- 过期分区写入可以慢，但最终全部写入 `__historical__` 成功
- Sink 算子不出现由过期分区写入引起的反压
- Flink checkpoint 在混合写入期间正常完成，不因历史 batch 的 slow ACK 导致 checkpoint timeout

**隔离机制 — "历史路径最多占 1/10 总资源"原则（设计文档 C.2, C.4）**:

```
客户端侧（C.4.1 historical in-flight cap）:
├── Sender: historicalWriteInFlightCap = maxInFlight / 10
│   → 历史 in-flight 达到 cap 时跳过，继续发送实时 batch
├── 内存: 历史 in-flight 内存 ≤ cap × batchSize，自然有界
└── flush: 等待所有 batch ACK（包括历史），但 cap 限制了等待量

服务端侧:
├── 历史 request queue（C.3）: 默认 ~1/10 总 request queue，满时返回 THROTTLED
├── ioExecutor（C.2）: 历史专用线程池，实时路径不使用，无资源竞争
└── 历史写入提交到 ioExecutor，RPC 线程立即释放
```

### 场景 6: 隔离性 — 同一 Pipeline 中过期分区 Lookup 不影响实时分区 Lookup 延迟

**目的**: 在 lookup join 场景中，过期分区 key 的 lake fallback 查询（慢）不影响实时分区 key 的本地 lookup（快）。**实时分区的 lookup 延迟是唯一关注指标**。

**步骤**:

1. 创建 Flink lookup join 作业：
   - 主流包含混合 join key：部分对应当前分区，部分对应过期分区
   - Fluss PK 表作为 lookup 维表
2. 观察实时分区 key 的 lookup 延迟

**验证**:

- 混入过期分区 key 后，实时分区 key 的 p99 lookup 延迟相对于纯实时基准的劣化 < 10%
- 过期分区 key 的 lookup 可以慢（lake fallback），但不阻塞同一 LookupSender 中实时 key 的返回
- 隔离机制（C.4.2）：LookupSender 拆分 inflight 信号量，历史 permit = 总量的 1/10，历史 permit 占满时实时 lookup 不受影响

---

### 场景 7: Flow Control — 过期分区 throttle 不影响实时分区

**目的**: 极端场景 — 历史 request queue 被过期分区操作打满时，throttle 只影响过期分区的写入，实时分区写入完全不受影响。

**步骤**:

1. 配置较小的历史 request queue（如容量 10）和较小的 ioExecutor（如 2 线程）
2. 在同一个 Flink 作业中，混合大量过期分区数据和少量实时数据，制造历史 request queue 过载
3. 观察实时分区写入

**验证**:

- 过期分区写入收到 `HISTORICAL_PARTITION_THROTTLED`，客户端自动 backoff retry
- **实时分区写入完全不受影响**：不返回 throttle 错误，延迟不劣化
- 过期分区的 retry 不导致 Sink 反压传导到实时路径
- 过期分区数据经过 retry 最终全部写入成功（可以慢，但不丢）

---

### 场景 8: Recovery — 重启后历史状态恢复

**步骤**:

1. 向过期分区写入 PK 数据（如 100 条）
2. 确认 lookup 能查到写入的数据
3. 重启 TabletServer（或 kill + restart `__historical__` bucket 的 leader）
4. 等待 recovery 完成
5. 再次 lookup 之前写入的数据

**验证**:

- recovery 后所有之前写入的数据都能通过 lookup 查到
- recovery 期间当前分区的读写不受影响（`__historical__` recovery 只影响历史路径）
- recovery 日志显示从 tiered offset replay WAL

### 场景 9: Cleanup — Tiering 完成后 RocksDB 清理

**步骤**:

1. 向过期分区写入一批 PK 数据
2. 确认 lookup 能查到（数据在 local RocksDB）
3. 等待 tiering 完成（`tieredOffset >= logEndOffset`）
4. 确认 cleanup 触发（观察日志或 metrics）
5. 再次 lookup 之前写入的数据

**验证**:

- cleanup 后 historical RocksDB 被清理
- lookup 仍返回正确值（fall through 到 lake）
- 再向过期分区写入新数据 → RocksDB 被 lazy 重建 → 写入成功

### 场景 10: Cleanup 与并发操作的协调

**步骤**:

1. 向过期分区写入一批数据
2. 触发 tiering，等待接近完成
3. 在 tiering 完成（即将触发 cleanup）的同时：
   - 持续 lookup 过期分区数据
   - 向过期分区写入新数据
4. 观察行为

**验证**:

- 如果 cleanup 时有新写入到达 → cleanup 被跳过（re-check `tieredOffset >= logEndOffset` 失败）
- 如果 cleanup 时有并发 lookup → reference counting 协调，lookup 不会读到已关闭的 RocksDB
- cleanup 完成后 → 后续 lookup fall through 到 lake → 返回正确值

**实现提示**:

- 可通过注入测试钩子（如在 cleanup re-check 前添加可控延迟/latch）来拉大时间窗口，确保并发操作能在 cleanup 过程中执行
- 或使用小数据量 + 短 tiering 间隔，反复触发 cleanup，同时持续进行 lookup/write，通过统计验证无异常

---

### 场景 11: 边界场景 — `dynamicPartitionEnabled = false`

**目的**: 验证 `__historical__` 作为系统分区不受 `dynamicPartitionEnabled` 配置限制。

**步骤**:

1. 创建 auto-partitioned + lake-enabled 的 PK 表，设置 `'table.auto-partition.dynamic-partition.enabled' = 'false'`
2. 向过期分区写入数据

**验证**:

- `__historical__` 分区被成功创建（系统分区创建绕过 `dynamicPartitionEnabled`）
- 写入成功，数据出现在 `__historical__` 中

### 场景 12: 边界场景 — 非过期分区的错误处理

**步骤**:

1. 向不存在的但命名合法且在 retention window 内的分区写入（如未来日期 `dt = '2099-01-01'`）
2. 向不存在的但命名非法的分区写入（如 `dt = 'invalid-date'`）
3. 向非 lake-enabled 表的不存在分区写入

**验证**:

- 三种情况都抛 `PartitionNotExistException`，不 redirect 到 `__historical__`
- 过期谓词准确区分真正过期 vs 其他不存在情况

### 场景 13: 边界场景 — `__historical__` 名称保留

**步骤**:

1. 尝试通过 DDL 创建名为 `__historical__` 的分区
2. 如果动态分区命名模式能产生 `__historical__`（极不可能），验证被拒绝

**验证**:

- 用户创建 `__historical__` 被拒绝，返回错误
- 只有系统通过内部路径才能创建

---

## 3. 性能基准测试

### 3.1 实时分区写入延迟（核心指标）

| 场景 | 关注指标 | 预期 |
|---|---|---|
| 仅实时分区写入（基准） | p99 延迟 | 作为基准值 |
| 混入 10% 过期分区数据 | 实时分区 p99 延迟 | 相对基准劣化 < 10% |
| 混入 50% 过期分区数据 | 实时分区 p99 延迟 | 相对基准劣化 < 10% |
| ioExecutor 队列接近满 | 实时分区 p99 延迟 | 不受影响 |

过期分区的写入延迟不需要关注，可以慢。

### 3.2 实时分区 Lookup 延迟（核心指标）

| 场景 | 关注指标 | 预期 |
|---|---|---|
| 仅实时分区 lookup（基准） | p99 延迟 | 作为基准值 (< 1ms 级别) |
| 混入过期分区 key 的 lookup | 实时分区 key 的 p99 延迟 | 相对基准劣化 < 10% |

过期分区 lookup 延迟（lake fallback）不需要关注。

### 3.3 Recovery 时间

| 场景 | 关注指标 |
|---|---|
| 不同数据量（1K / 10K / 100K 条） | recovery 时间与 WAL replay 数据量成线性关系 |
| recovery 期间当前分区可用性 | 当前分区不受影响 |

验证方式：测量不同数据量下的 recovery 时间，确认线性关系，无异常瓶颈。

---

## 4. 观测与验证手段

### 4.1 关键日志

- `__historical__` 创建日志
- ioExecutor 队列满 / throttle 日志
- Recovery WAL replay 进度日志
- Cleanup 触发与完成日志
- Lake fallback lookup 日志

### 4.2 关键 Metrics（如已暴露）

- `ioExecutor` 队列长度、活跃线程数
- 历史写入 / lookup 的延迟分布
- 当前分区写入 / lookup 的延迟分布
- `HISTORICAL_PARTITION_THROTTLED` 错误计数
- Lake fallback lookup 次数和延迟

### 4.3 数据一致性校验

对每个测试场景，可通过以下方式验证数据一致性：

1. **Fluss lookup vs Paimon 直接查询**: 对同一个 key，Fluss lookup 的返回值应与 Paimon 表直接查询的结果一致（tiering 完成后）
2. **Changelog 完整性**: 从 `__historical__` 消费的 changelog 应用到初始状态后，最终状态应与 lookup 结果一致
3. **跨分区隔离**: 对同一个 id，不同 original partition 的 lookup 应返回各自独立的值

---

## 5. 测试执行 Checklist

```
基本功能:
[ ] 场景 1: PK 表写入过期分区 — upsert + delete + changelog 验证（含 prewrite buffer old-value 路径）
[ ] 场景 2: Log 表写入过期分区 — redirect + 消费验证
[ ] 场景 3: 过期分区 Lookup — lake fallback + local-first
[ ] 场景 3a: Lookup 触发 __historical__ 创建
[ ] 场景 4: 多过期分区 composite key 隔离

隔离性（同一 Pipeline 验证，实时分区 p99 劣化 < 10%）:
[ ] 场景 5: 过期分区写入不影响实时分区写入延迟 + checkpoint 正常完成
[ ] 场景 6: 过期分区 Lookup 不影响实时分区 Lookup 延迟
[ ] 场景 7: Flow Control — throttle 只影响过期分区

Recovery & Cleanup:
[ ] 场景 8:  重启后历史状态恢复
[ ] 场景 9:  Tiering 完成后 RocksDB 清理
[ ] 场景 10: Cleanup 与并发操作协调

边界场景:
[ ] 场景 11: dynamicPartitionEnabled = false
[ ] 场景 12: 非过期分区错误处理
[ ] 场景 13: __historical__ 名称保留

性能基准:
[ ] 混合写入时实时分区延迟对比（0% / 10% / 50% 过期，p99 劣化 < 10%）
[ ] 混合 Lookup 时实时分区延迟对比（p99 劣化 < 10%）
[ ] Recovery 时间线性关系验证
```
