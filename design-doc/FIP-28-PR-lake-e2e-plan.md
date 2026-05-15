# E2E Test: Historical Partition Lake Fallback in fluss-lake-paimon

## Context

PR 7b+7c 已实现：
- 服务端 composite key encoding 隔离不同过期分区的 key
- 服务端 lake fallback old-value lookup：`processUpsert()`/`processDeletion()` 在 prewrite buffer 和 RocksDB 都 miss 时，回退到 `PaimonLakeTableLookuper` 查询 lake 中的旧值

`fluss-client` 中的 `HistoricalPartitionTableITCase` 使用的是 `TestingPaimonStoragePlugin`（mock），其 `NullLakeTableLookuper` 始终返回 null，**无法验证真正的 lake fallback 路径**。

**本 PR 目标**：在 `fluss-lake-paimon` 模块新增 e2e 测试，使用**真实 Paimon 存储**验证：
1. 写入过期分区时 old-value 能从 Paimon lake 正确获取
2. 多过期分区 composite key 隔离 + lake fallback 各自正确
3. Lake miss 时正确生成 INSERT 而非 UPDATE

---

## 改动范围

| # | 文件 | 操作 | 说明 |
|---|------|------|------|
| 1 | `fluss-lake/fluss-lake-paimon/src/test/.../tiering/HistoricalPartitionPaimonITCase.java` | 新增 | E2E 测试类 |

只新增一个测试文件，不修改任何生产代码。

---

## 测试类设计

**文件**: `fluss-lake/fluss-lake-paimon/src/test/java/org/apache/fluss/lake/paimon/tiering/HistoricalPartitionPaimonITCase.java`

继承 `FlinkPaimonTieringTestBase`，复用其 Fluss 集群 + Paimon catalog 基础设施。

```java
class HistoricalPartitionPaimonITCase extends FlinkPaimonTieringTestBase {

    @RegisterExtension
    public static final FlussClusterExtension FLUSS_CLUSTER_EXTENSION =
            FlussClusterExtension.builder()
                    .setClusterConf(initConfig())
                    .setNumOfTabletServers(3)
                    .build();

    @BeforeAll
    static void beforeAll() {
        FlinkPaimonTieringTestBase.beforeAll(FLUSS_CLUSTER_EXTENSION.getClientConfig());
    }

    @Override
    protected FlussClusterExtension getFlussClusterExtension() {
        return FLUSS_CLUSTER_EXTENSION;
    }
}
```

---

## 测试场景

### Test 1: `testHistoricalPkWriteWithLakeFallbackOldValue`

**目的**：验证写入过期分区时，old-value 能从 Paimon lake 正确获取，生成 UPDATE_BEFORE + UPDATE_AFTER changelog。

**完整流程**：

```
1. 创建 lake-enabled 分区 PK 表
   Schema: (a INT, b STRING, c STRING)，PK=(a,c)，partitionBy=c，YEAR 分区，7 年保留

2. 等待自动分区就绪

3. 写入当前分区数据 + 触发 snapshot + 启动 tiering job → 在 Paimon 创建表结构

4. 直接用 Paimon BatchWriteBuilder 向 Paimon 表的 partition "2000" 写入数据：
   (pt="2000", pk=1, val="old_val", __bucket=0, __offset=0, __timestamp=now)
   这模拟 "2000" 分区之前活跃时被 tier 到 lake 的数据

5. 关闭动态分区创建（clientConf.set(CLIENT_WRITER_DYNAMIC_CREATE_PARTITION_ENABLED, false)）

6. 用 Fluss UpsertWriter 写入过期分区 "2000"，key=1，val="new_val"
   → 客户端检测过期 → 重定向到 __historical__
   → 服务端 composite key encoding
   → old-value lookup: buffer miss → RocksDB miss → lake fallback
   → PaimonLakeTableLookuper 在 Paimon partition "2000" 找到 old value

7. 验证 __historical__ 的 changelog：
   - UPDATE_BEFORE(a=1, b="old_val", c="2000")
   - UPDATE_AFTER(a=1, b="new_val", c="2000")
   （不是 INSERT，因为 lake 中有旧值）

8. 取消 tiering job
```

### Test 2: `testHistoricalPkWriteLakeMiss`

**目的**：验证 lake 中没有旧值时，生成 INSERT changelog。

**流程**：

```
1. 同 Test 1 的步骤 1-4（复用同一个表）

2. 写入过期分区 "2000"，key=999（Paimon 中不存在）
   → lake fallback miss → 生成 INSERT

3. 验证 __historical__ changelog：
   - INSERT(a=999, b="brand_new", c="2000")
```

### Test 3: `testMultiExpiredPartitionsCompositeKeyIsolationWithLake`

**目的**：验证不同过期分区的相同 key 在 lake fallback 时各自获取正确的旧值。

**流程**：

```
1. 同 Test 1 的表

2. 向 Paimon 写入两个分区的数据：
   - partition "2000": (pk=1, val="val_2000")
   - partition "2001": (pk=1, val="val_2001")

3. 用 Fluss UpsertWriter 写入：
   - upsert(1, "new_2000", "2000")  → lake fallback 找到 "val_2000"
   - upsert(1, "new_2001", "2001")  → lake fallback 找到 "val_2001"

4. 验证 __historical__ changelog：
   - UPDATE_BEFORE(1, "val_2000", "2000") + UPDATE_AFTER(1, "new_2000", "2000")
   - UPDATE_BEFORE(1, "val_2001", "2001") + UPDATE_AFTER(1, "new_2001", "2001")
   （而非 INSERT，证明 composite key 隔离 + lake fallback 各自正确）
```

---

## 关键实现细节

### 创建分区 PK 表

```java
private long createPartitionedPkTable(TablePath tablePath) throws Exception {
    Schema schema = Schema.newBuilder()
            .column("a", DataTypes.INT())
            .column("b", DataTypes.STRING())
            .column("c", DataTypes.STRING())
            .primaryKey("a", "c")
            .build();
    TableDescriptor descriptor = TableDescriptor.builder()
            .schema(schema)
            .distributedBy(1)
            .partitionedBy("c")
            .property(ConfigOptions.TABLE_AUTO_PARTITION_ENABLED, true)
            .property(ConfigOptions.TABLE_AUTO_PARTITION_TIME_UNIT, AutoPartitionTimeUnit.YEAR)
            .property(ConfigOptions.TABLE_AUTO_PARTITION_NUM_RETENTION, 7)
            .property(ConfigOptions.TABLE_DATALAKE_ENABLED.key(), "true")
            .property(ConfigOptions.TABLE_DATALAKE_FRESHNESS, Duration.ofMillis(500))
            .build();
    return createTable(tablePath, descriptor);
}
```

### 直接写入 Paimon 表

需要包含 system columns (`__bucket`, `__offset`, `__timestamp`)：

```java
private void writeToPaimon(TablePath tablePath, List<GenericRow> rows) throws Exception {
    Identifier id = Identifier.create(tablePath.getDatabaseName(), tablePath.getTableName());
    Table table = paimonCatalog.getTable(id);
    BatchWriteBuilder writeBuilder = table.newBatchWriteBuilder();
    try (BatchTableWrite writer = writeBuilder.newWrite()) {
        for (GenericRow row : rows) {
            writer.write(row);
        }
        List<CommitMessage> messages = writer.prepareCommit();
        try (BatchTableCommit commit = writeBuilder.newCommit()) {
            commit.commit(messages);
        }
    }
}
```

Paimon row 格式 (与 `PaimonLakeTableLookuperTest` 一致)：
```java
GenericRow.of(
    BinaryString.fromString("2000"),   // c (partition key)
    1,                                  // a (pk)
    BinaryString.fromString("old_val"), // b (value)
    0,                                  // __bucket
    0L,                                 // __offset
    Timestamp.fromEpochMillis(0))       // __timestamp
```

### 扫描 __historical__ changelog

复用 `HistoricalPartitionTableITCase` 的 pattern：
```java
private List<ScanRecord> scanHistoricalPartition(Table table, long partitionId, int expected) {
    try (LogScanner scanner = table.newScan().createLogScanner()) {
        scanner.subscribeFromBeginning(partitionId, 0);
        List<ScanRecord> records = new ArrayList<>();
        while (records.size() < expected) {
            ScanRecords batch = scanner.poll(Duration.ofSeconds(2));
            for (TableBucket bucket : batch.buckets()) {
                for (ScanRecord record : batch.records(bucket)) {
                    records.add(record);
                }
            }
        }
        return records;
    }
}
```

---

## 复用的现有组件

| 组件 | 文件 | 用途 |
|------|------|------|
| `FlinkPaimonTieringTestBase` | `fluss-lake-paimon/.../testutils/FlinkPaimonTieringTestBase.java` | 集群/Paimon 基础设施 |
| `FlussClusterExtension` | `fluss-server/.../testutils/FlussClusterExtension.java` | Fluss 集群管理 |
| `PaimonTieringITCase` pattern | `fluss-lake-paimon/.../tiering/PaimonTieringITCase.java` | 参考 tiering job 启动/等待模式 |
| `HistoricalPartitionTableITCase` pattern | `fluss-client/.../table/HistoricalPartitionTableITCase.java` | 参考 changelog 扫描验证模式 |
| `PaimonLakeTableLookuperTest` pattern | `fluss-lake-paimon/.../PaimonLakeTableLookuperTest.java` | 参考 Paimon 直接写入模式 |

---

## 验证步骤

```bash
# 1. 格式化
./mvnw spotless:apply -pl fluss-lake/fluss-lake-paimon -q

# 2. 编译
./mvnw test-compile -pl fluss-lake/fluss-lake-paimon -am -q

# 3. 运行测试
./mvnw test -Dtest=HistoricalPartitionPaimonITCase -pl fluss-lake/fluss-lake-paimon
```
