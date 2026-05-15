# PR 7c: 服务端 lake fallback old-value lookup for historical PK writes

## Context

PR 7b 完成了 composite key encoding：历史 PK 写入到 `__historical__` 时，key 被编码为 `[partitionName | originalKey]`，不同原始分区的 key 不再碰撞。

**当前问题**：`processUpsert()` 和 `processDeletion()` 通过 `getFromBufferOrKv()` 获取 old-value，查找链路为 `prewrite buffer → RocksDB`。对于历史写入，如果 key 首次出现（buffer 和 RocksDB 都没有），old-value 为 null，导致：
- upsert 总是产生 `INSERT`，无法生成 `UPDATE_BEFORE + UPDATE_AFTER` changelog
- delete 直接跳过（"key 不存在"），丢失删除操作

但实际上 old-value 可能存在于 **lake 存储** 中（该 key 之前 tier 过）。

**本 PR 目标**：扩展 old-value 查找链路，当 local（buffer + RocksDB）miss 时，对历史分区 tablet fallback 到 `LakeTableLookuper` 从 lake 获取 old-value。

**不在本 PR 范围**：历史分区 lookup 的客户端路由（PR 8）。

---

## 改动范围

| # | 文件 | 操作 | 说明 |
|---|------|------|------|
| 1 | `fluss-server/.../kv/KvTablet.java` | 修改 | `getFromBufferOrKv()` 扩展为 `getOldValue()`，历史分区 tablet 增加 lake fallback；新增 `@Nullable LakeTableLookuper` 字段 |
| 2 | `fluss-server/.../kv/KvTablet.java` | 修改 | 构建 `LookupContext` 需要的信息（partitionName、bucketId、schemaId）传入 lake fallback |
| 3 | `fluss-server/.../replica/Replica.java` | 修改 | 创建 KvTablet 时传入 `LakeTableLookuper` 实例（历史分区 tablet 非 null，普通 tablet null） |
| 4 | `fluss-server/.../replica/ReplicaManager.java` | 修改 | 持有并管理 `LakeTableLookuper` 生命周期；创建 Replica 时传递 |
| 5 | `fluss-server/.../tablet/TabletServer.java` | 修改 | 从 `LakeCatalogDynamicLoader` 获取 `LakeStorage`，调用 `createLakeTableLookuper()` |
| 6 | `fluss-server/.../coordinator/LakeCatalogDynamicLoader.java` | 修改 | 暴露 `LakeStorage` 实例供 TabletServer 获取（目前只暴露 `LakeCatalog`） |
| 7 | `fluss-server/.../lakehouse/TestingPaimonStoragePlugin.java` | 修改 | `TestingPaimonLakeStorage` 添加返回 null 的 `LakeTableLookuper`，验证 wiring |

---

## 核心设计

### Old-value resolution chain（扩展后）

```
1. prewrite buffer (composite key)  → 命中则使用
2. RocksDB (composite key)          → 命中则使用
3. lake fallback                    → 仅 isHistoricalPartition 时触发
   3a. CompositeKeyEncoder.decode(compositeKey) → (partitionName, originalKey)
   3b. 构建 LookupContext(partitionSpec, bucketId, schemaId)
   3c. LakeTableLookuper.lookup(originalKey, context)
   3d. 返回 encoded value bytes（或 null → 视为新 INSERT）
```

### key 格式兼容性

`LakeTableLookuper.lookup()` 接收的 key 是 lake 原生格式（Paimon BinaryRow）。当前 Fluss PK 表的 key 编码规则：

- kvFormatVersion=1（legacy table）：使用 lake encoder（PaimonKeyEncoder），key bytes 就是 Paimon BinaryRow 格式 → **直接使用**
- kvFormatVersion=2 + defaultBucketKey：同上 → **直接使用**
- kvFormatVersion=2 + 自定义 bucketKey（prefix lookup）：使用 CompactedKeyEncoder → **格式不同**，需要转换

本 PR 先支持前两种场景（覆盖绝大多数用例）。第三种场景需要额外的 key 转码逻辑，可后续迭代。

---

## Step 1: LakeCatalogDynamicLoader — 暴露 LakeStorage

**文件**: `fluss-server/src/main/java/org/apache/fluss/server/coordinator/LakeCatalogDynamicLoader.java`

当前 `createLakeCatalog()` 方法内部创建了 `LakeStorage` 但只取 `createLakeCatalog()`，没有保留 `LakeStorage` 引用。

改动：在 `LakeCatalogContainer` 中保存 `LakeStorage` 实例，新增 getter。

```java
public static class LakeCatalogContainer {
    private final @Nullable LakeStorage lakeStorage;  // 新增
    // ... 现有字段 ...

    @Nullable
    public LakeStorage getLakeStorage() {
        return lakeStorage;
    }
}
```

`createLakeCatalog()` 方法也需要拆分，使 `LakeStorage` 在 coordinator 和 tablet-server 场景下都可用：
- Coordinator: 需要 `LakeCatalog`（已有）
- TabletServer: 需要 `LakeStorage.createLakeTableLookuper()`（本 PR 新增）

---

## Step 2: TabletServer — 创建 LakeTableLookuper

**文件**: `fluss-server/src/main/java/org/apache/fluss/server/tablet/TabletServer.java`

从 `lakeCatalogDynamicLoader.getLakeCatalogContainer().getLakeStorage()` 获取 `LakeStorage`，调用 `createLakeTableLookuper(tablePath)` 创建 lookuper。

考虑点：
- `LakeTableLookuper` 应该 per-table 创建（构造参数是 `TablePath`）
- 多个 table 的 `__historical__` tablet 可能共享同一 lookuper
- 生命周期管理：TabletServer 关闭时 close lookuper

实现方案：在 `ReplicaManager` 中维护 `Map<TablePath, LakeTableLookuper>` 缓存，按需创建，shutdown 时全部 close。

---

## Step 3: ReplicaManager — 管理 LakeTableLookuper

**文件**: `fluss-server/src/main/java/org/apache/fluss/server/replica/ReplicaManager.java`

### 3a. 新增字段和构造参数

```java
@Nullable private final LakeStorage lakeStorage;
private final Map<TablePath, LakeTableLookuper> lakeTableLoouperCache = new HashMap<>();
```

`lakeStorage` 从 `TabletServer` 传入。非 lake-enabled 集群为 null。

### 3b. 获取 LakeTableLookuper 的方法

```java
@Nullable
private LakeTableLookuper getOrCreateLakeTableLookuper(TablePath tablePath) {
    if (lakeStorage == null) {
        return null;
    }
    return lakeTableLoouperCache.computeIfAbsent(
            tablePath, path -> lakeStorage.createLakeTableLookuper(path));
}
```

### 3c. 创建 Replica 时传递

在创建历史分区的 Replica / KvTablet 时，将 lookuper 传入。

### 3d. 关闭时清理

```java
// shutdown() 中
for (LakeTableLookuper lookuper : lakeTableLoouperCache.values()) {
    IOUtils.closeQuietly(lookuper, "lake table lookuper");
}
```

---

## Step 4: Replica — 传递 LakeTableLookuper 到 KvTablet

**文件**: `fluss-server/src/main/java/org/apache/fluss/server/replica/Replica.java`

在创建 `KvTablet` 时传入 `@Nullable LakeTableLookuper`。普通分区传 null，历史分区传实际实例。

---

## Step 5: KvTablet — lake fallback 核心逻辑

**文件**: `fluss-server/src/main/java/org/apache/fluss/server/kv/KvTablet.java`

### 5a. 新增字段

```java
@Nullable private final LakeTableLookuper lakeTableLookuper;
```

构造函数和 `create()` 工厂方法增加此参数。

### 5b. 扩展 getFromBufferOrKv → getOldValue

当前：
```java
private byte[] getFromBufferOrKv(KvPreWriteBuffer.Key key) throws IOException {
    KvPreWriteBuffer.Value value = kvPreWriteBuffer.get(key);
    if (value == null) {
        return rocksDBKv.get(key.get());
    }
    return value.get();
}
```

扩展后（仅历史分区 tablet 时触发 lake fallback）：
```java
private byte[] getOldValue(KvPreWriteBuffer.Key key, @Nullable String partitionName)
        throws Exception {
    // 1. prewrite buffer
    KvPreWriteBuffer.Value value = kvPreWriteBuffer.get(key);
    if (value != null) {
        return value.get();
    }
    // 2. RocksDB
    byte[] rocksResult = rocksDBKv.get(key.get());
    if (rocksResult != null) {
        return rocksResult;
    }
    // 3. lake fallback (only for historical partition)
    if (!isHistoricalPartition || lakeTableLookuper == null || partitionName == null) {
        return null;
    }
    return lookupFromLake(key.get(), partitionName);
}
```

### 5c. lookupFromLake 方法

```java
private byte[] lookupFromLake(byte[] compositeKey, String partitionName) throws Exception {
    // Decode composite key to get original key
    Tuple2<String, byte[]> decoded = CompositeKeyEncoder.decode(compositeKey);
    byte[] originalKey = decoded.f1;

    // Build LookupContext with original partition spec
    List<String> partitionKeys = tableInfo.getPartitionKeys();
    ResolvedPartitionSpec partitionSpec =
            ResolvedPartitionSpec.fromPartitionName(partitionKeys, partitionName);
    int bucketId = tableBucket.getBucket();
    int schemaId = schemaGetter.getLatestSchemaId();

    LakeTableLookuper.LookupContext context =
            new LakeTableLookuper.LookupContext(partitionSpec, bucketId, schemaId);

    return lakeTableLookuper.lookup(originalKey, context);
}
```

### 5d. 调用方修改

`processUpsert()` 和 `processDeletion()` 中将 `getFromBufferOrKv(key)` 替换为 `getOldValue(key, partitionName)`。

这意味着 `processUpsert` 和 `processDeletion` 也需要接收 `partitionName` 参数。修改方法签名：

```java
private long processUpsert(
        KvPreWriteBuffer.Key key,
        BinaryValue currentValue,
        RowMerger currentMerger,
        AutoIncrementUpdater autoIncrementUpdater,
        ValueDecoder valueDecoder,
        WalBuilder walBuilder,
        PaddingRow latestSchemaRow,
        long logOffset,
        @Nullable String partitionName)  // 新增
        throws Exception {
    ...
    byte[] oldValueBytes = getOldValue(key, partitionName);
    ...
}
```

`processDeletion` 同理。

`processKvRecords` 调用 `processUpsert/processDeletion` 时传递已有的 `partitionName`。

### 5e. tableInfo 和 partitionKeys

`lookupFromLake` 需要 `partitionKeys` 来构建 `ResolvedPartitionSpec`。当前 KvTablet 没有 `TableInfo`。

两种方案：
1. KvTablet 构造时传入 `List<String> partitionKeys`
2. KvTablet 构造时传入 `TableInfo`

建议方案 1（更小依赖面）：只传 `@Nullable List<String> partitionKeys`，历史分区 tablet 非 null。

---

## Step 6: 测试

### 6a. TestingPaimonLakeStorage — 添加返回 null 的 LakeTableLookuper

**文件**: `fluss-server/src/test/java/org/apache/fluss/server/lakehouse/TestingPaimonStoragePlugin.java`

在 `TestingPaimonLakeStorage` 中 override `createLakeTableLookuper()`，返回一个永远返回 null 的 lookuper：

```java
public static class TestingPaimonLakeStorage implements LakeStorage {
    // ... 现有方法 ...

    @Override
    public LakeTableLookuper createLakeTableLookuper(TablePath tablePath) {
        return new NullLakeTableLookuper();
    }
}

private static class NullLakeTableLookuper implements LakeTableLookuper {
    @Override
    public byte[] lookup(byte[] key, LookupContext context) {
        return null;  // 永远查不到数据
    }

    @Override
    public void close() {}
}
```

**好处**：
- 验证整条链路的 wiring 正确：`TabletServer → ReplicaManager → Replica → KvTablet → LakeTableLookuper`
- lookuper 返回 null → 历史写入仍产生 `INSERT`（与当前行为一致）→ 现有 `HistoricalPartitionTableITCase` 测试无需改动即可通过
- 不需要真实 Paimon lake 环境

### 6b. KvTablet 单元测试

在 `KvTabletTest` 中新增测试，mock `LakeTableLookuper`，验证：
- historical tablet + local miss + lake hit → `UPDATE_BEFORE + UPDATE_AFTER`
- historical tablet + local miss + lake miss → `INSERT`
- 非 historical tablet → 不触发 lake fallback

### 6c. 集成测试

现有 `HistoricalPartitionTableITCase` 通过 `TestingPaimonLakeStorage` 的 null lookuper 验证 wiring。

真正验证 lake 返回 old-value 的端到端测试（tiering → 写入 → UPDATE changelog）放在 `fluss-lake-paimon` 模块下，使用真实 `PaimonLakeTableLookuper`，可后续 PR 或在本 PR 中按需补充。

---

## 无需改动的组件

| 组件 | 为什么无需改动 |
|------|--------------|
| **CompositeKeyEncoder** | PR3/7b 已实现 encode/decode |
| **LakeTableLookuper SPI** | PR3 已定义 |
| **PaimonLakeTableLookuper** | PR3 已实现 |
| **客户端** | 不感知 old-value resolution |
| **HistoricalKvManager** | 本 PR 暂不涉及（recovery/cleanup 是 PR 9） |

---

## 验证步骤

```bash
# 1. 格式化
./mvnw spotless:apply -pl fluss-server -q

# 2. 编译
./mvnw clean install -DskipTests -pl fluss-server -am -q

# 3. 运行 KvTablet 单元测试
./mvnw test -Dtest=KvTabletTest -pl fluss-server

# 4. 运行 Replica 测试
./mvnw test -Dtest=ReplicaTest -pl fluss-server

# 5. 运行集成测试（如果有 Paimon 环境）
./mvnw test -Dtest=HistoricalPartitionTableITCase -pl fluss-client
```

---

## 风险和待确认点

1. **key 格式兼容性**：kvFormatVersion=2 + 自定义 bucketKey 场景，raw key 是 CompactedKeyEncoder 格式而非 Paimon BinaryRow，需要转码后才能传给 `LakeTableLookuper`。本 PR 先不支持此场景，后续 PR 补充。
2. **LakeStorage 可用性**：TabletServer 是否总是能获取到 `LakeStorage`？需要确认 `LakeCatalogDynamicLoader` 在 tablet-server 模式下是否创建 `LakeStorage`。当前代码显示 `isCoordinator=false` 时不创建 `LakeCatalog`，但 `LakeStorage` 本身可以独立创建。
3. **线程安全**：`LakeTableLookuper` 是否线程安全？`PaimonLakeTableLookuper` 内部有状态（Catalog、LocalTableQuery 等），需确认是否支持并发调用。如果不支持，需要 per-bucket 或加锁。
4. **schemaId 来源**：`LookupContext` 需要 `schemaId`。在 `processKvRecords` 中有 `schemaIdOfNewData`，但 lake fallback 应使用 lake 端的 schema — 需确认 `PaimonLakeTableLookuper` 如何处理 schema 映射。


todo: 还有一个问题，如果没有重新创建 table connection，metadata cache 了某个分区的metadata，然后这个分区被drop 了，会不会还是用这个分区的 metadata