# PR3: LakeStorage SPI 扩展 + PaimonLakeTableLookuper 实现

## Context

FIP-28 需要在 lake 侧提供点查能力，用于：
- PK 表历史写入时的 old-value fallback（从 lake 获取旧值以生成正确的 changelog）
- 过期分区的 point lookup（local-first: RocksDB → lake fallback）

PR3 定义 `LakeTableLookuper` SPI 接口并提供 Paimon 实现，使用 Paimon 的 `LocalTableQuery` API 进行高效点查。

## 改动列表

### 1. 新增 `LakeTableLookuper` 接口

**文件**: `fluss-common/src/main/java/org/apache/fluss/lake/lakestorage/LakeTableLookuper.java`（新建）

按设计文档定义 SPI 接口：

```java
@PublicEvolving
public interface LakeTableLookuper extends AutoCloseable {
    /**
     * Lookup a single key from lake storage for an expired partition.
     *
     * Key bytes are already encoded in the lake storage's native key format.
     * Returned value bytes should be encoded with schema ID prefix.
     *
     * @param key     encoded key bytes in lake storage's native format
     * @param context lookup context (partition, bucket, schemaId)
     * @return encoded value bytes, or null if not found
     */
    @Nullable
    byte[] lookup(byte[] key, LookupContext context) throws Exception;

    /** Context for a single lake lookup operation. */
    class LookupContext {
        private final ResolvedPartitionSpec partitionSpec;
        private final int bucketId;
        private final int schemaId;
        // constructor, getters
    }
}
```

**注意**: `LookupContext` 作为 `LakeTableLookuper` 的静态内部类，包含：
- `ResolvedPartitionSpec partitionSpec` — 原始分区 spec（用于定位 lake 中的分区）；非分区表传 null
- `int bucketId` — bucket ID
- `int schemaId` — 用于 value 编码的 schema ID

### 2. 扩展 `LakeStorage` 接口

**文件**: `fluss-common/src/main/java/org/apache/fluss/lake/lakestorage/LakeStorage.java`

添加 default 方法：

```java
default LakeTableLookuper createLakeTableLookuper(TablePath tablePath) {
    throw new UnsupportedOperationException(
            "Point lookup is not supported for this lake storage.");
}
```

### 3. 扩展 `PluginLakeStorageWrapper`

**文件**: `fluss-common/src/main/java/org/apache/fluss/lake/lakestorage/PluginLakeStorageWrapper.java`

在 `ClassLoaderFixingLakeStorage` 内部类中添加对 `createLakeTableLookuper` 的 classloader-fixing 包装：

```java
@Override
public LakeTableLookuper createLakeTableLookuper(TablePath tablePath) {
    try (TemporaryClassLoaderContext ignored = TemporaryClassLoaderContext.of(loader)) {
        return new ClassLoaderFixingLakeTableLookuper(
                inner.createLakeTableLookuper(tablePath), loader);
    }
}
```

新增 `ClassLoaderFixingLakeTableLookuper` 内部类，对 `lookup()` 和 `close()` 使用 `TemporaryClassLoaderContext` 包装。

### 4. 实现 `PaimonLakeTableLookuper`

**文件**: `fluss-lake/fluss-lake-paimon/src/main/java/org/apache/fluss/lake/paimon/PaimonLakeTableLookuper.java`（新建）

**核心实现**:

1. **懒初始化**: `Catalog`、`FileStoreTable`、`LocalTableQuery` 在首次 `lookup()` 调用时创建
2. **Snapshot-based 文件刷新**:
   - 维护 `Map<PaimonPartitionBucket, Long> refreshedBuckets`（复用现有 `PaimonPartitionBucket` 类）
   - 每次 lookup 比较最新 snapshot ID 与缓存值，有新 snapshot 时 refresh files
3. **Lookup 流程**:
   - key bytes 是 Paimon BinaryRow 格式，直接 `keyRow.pointTo(MemorySegment.wrap(key), 0, key.length)`
   - 调用 `tableQuery.lookup(partition, bucketId, keyRow)` 获取 Paimon InternalRow
   - 通过 `PaimonRowAsFlussRow` 适配为 Fluss InternalRow
   - 使用 `CompactedRowEncoder` 编码为 `BinaryRow`，再用 `ValueEncoder.encodeValue(schemaId, binaryRow)` 加 schema ID 前缀

**构造器参数**: `Configuration paimonConfig, TablePath tablePath`

**可复用的已有工具**:
- `PaimonConversions.toPaimon(TablePath)` — TablePath → Paimon Identifier（`fluss-lake-paimon/.../utils/PaimonConversions.java`）
- `PaimonRowAsFlussRow` — Paimon row → Fluss row 适配器（`fluss-lake-paimon/.../utils/PaimonRowAsFlussRow.java`）
- `PaimonPartitionBucket` — (partition, bucket) 复合 key（`fluss-lake-paimon/.../utils/PaimonPartitionBucket.java`）
- `CatalogFactory.createCatalog()` — 同 `PaimonLakeSource` 中的模式
- `CompactedRowEncoder` — Fluss row 编码器（`fluss-common/.../row/encode/CompactedRowEncoder.java`）
- `ValueEncoder.encodeValue()` — schema ID + row 编码（`fluss-common/.../row/encode/ValueEncoder.java`）

**partition BinaryRow 构建**: Paimon 的 `LocalTableQuery.lookup()` 需要 `BinaryRow partition` 参数。对于分区表，需要从 `ResolvedPartitionSpec` 构建 Paimon 的 partition BinaryRow。参考 Paimon 的 partition encoding 方式，使用 `BinaryRowWriter` 将 partition values 写入 `BinaryRow`。非分区表传空 `BinaryRow(0)`。

### 5. 扩展 `PaimonLakeStorage`

**文件**: `fluss-lake/fluss-lake-paimon/src/main/java/org/apache/fluss/lake/paimon/PaimonLakeStorage.java`

添加 `createLakeTableLookuper` 实现：

```java
@Override
public LakeTableLookuper createLakeTableLookuper(TablePath tablePath) {
    return new PaimonLakeTableLookuper(paimonConfig, tablePath);
}
```

### 6. 测试

**文件**: `fluss-lake/fluss-lake-paimon/src/test/java/org/apache/fluss/lake/paimon/PaimonLakeTableLookuperTest.java`（新建）

继承 `PaimonSourceTestBase`（复用 Paimon catalog 创建和表管理基础设施），测试：

- **点查存在的 key** → 返回正确 value bytes（含 schema ID 前缀）
- **点查不存在的 key** → 返回 null
- **新数据写入后**（新 snapshot）→ 重新 refresh → 查到最新值
- **snapshot 无变化时** → 跳过 refresh（通过检查内部 refreshedBuckets map 或观察行为）
- **分区表点查** → partition routing 正确

## 关键文件路径

| 文件 | 操作 |
|------|------|
| `fluss-common/.../lake/lakestorage/LakeTableLookuper.java` | 新建 |
| `fluss-common/.../lake/lakestorage/LakeStorage.java` | 修改（加 default method） |
| `fluss-common/.../lake/lakestorage/PluginLakeStorageWrapper.java` | 修改（加 classloader 包装） |
| `fluss-lake/fluss-lake-paimon/.../PaimonLakeTableLookuper.java` | 新建 |
| `fluss-lake/fluss-lake-paimon/.../PaimonLakeStorage.java` | 修改（加 override） |
| `fluss-lake/fluss-lake-paimon/.../PaimonLakeTableLookuperTest.java` | 新建 |

## 关键设计决策

| 决策 | 理由 |
|------|------|
| `LakeTableLookuper` 放在 `fluss-common` 而非 `fluss-server` | 作为 SPI 接口，lake 实现模块需要实现它，必须在公共模块 |
| `LookupContext` 作为静态内部类 | 只在 `LakeTableLookuper` 上下文中使用，避免顶层类膨胀 |
| 懒初始化 Catalog/TableQuery | 避免构造时加载，减少资源占用 |
| Snapshot-based refresh 策略 | 复用 Paimon 已有机制，有新 snapshot 时才刷新文件，避免不必要的 IO |
| default 方法抛 UnsupportedOperationException | 不强制所有 LakeStorage 实现点查，保持向后兼容 |

## 验证步骤

```bash
# 1. 代码格式化
./mvnw spotless:apply -pl fluss-common,fluss-lake/fluss-lake-paimon

# 2. 编译
./mvnw clean install -DskipTests -pl fluss-common,fluss-lake/fluss-lake-paimon -am

# 3. 运行测试
./mvnw test -Dtest=PaimonLakeTableLookuperTest -pl fluss-lake/fluss-lake-paimon
```
