/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.lake.paimon.tiering;

import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.FlussConnection;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.writer.UpsertWriter;
import org.apache.fluss.config.AutoPartitionTimeUnit;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.lake.paimon.testutils.FlinkPaimonTieringTestBase;
import org.apache.fluss.metadata.PartitionSpec;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableChange;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.testutils.common.CommonTestUtils;
import org.apache.fluss.types.DataTypes;

import org.apache.flink.core.execution.JobClient;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.time.Duration;
import java.time.Year;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.apache.fluss.flink.FlinkConnectorOptions.BOOTSTRAP_SERVERS;
import static org.apache.fluss.flink.source.testutils.FlinkRowAssertionsUtils.assertResultsIgnoreOrder;
import static org.apache.fluss.testutils.DataTestUtils.row;

/**
 * E2E integration test for historical partition lake fallback with real Paimon storage. Verifies
 * that when writing to expired partitions, the old-value lookup falls back to Paimon lake storage
 * and correctly generates changelog entries.
 *
 * <p>Data flows entirely through Fluss → tiering → Paimon, exercising the full lake fallback path
 * that cannot be tested with mock lake storage plugins.
 *
 * <p>Uses YEAR partitions with dynamically computed partition names (currentYear - 3 and
 * currentYear - 2) to avoid calendar-dependent flakiness. Partitions are expired by altering
 * retention and manual drops.
 */
class HistoricalPartitionPaimonITCase extends FlinkPaimonTieringTestBase {

    @RegisterExtension
    public static final FlussClusterExtension FLUSS_CLUSTER_EXTENSION =
            FlussClusterExtension.builder()
                    .setClusterConf(initConfig())
                    .setNumOfTabletServers(3)
                    .build();

    private StreamTableEnvironment streamTEnv;

    @BeforeAll
    static void beforeAll() {
        FlinkPaimonTieringTestBase.beforeAll(FLUSS_CLUSTER_EXTENSION.getClientConfig());
    }

    @BeforeEach
    public void beforeEach() {
        super.beforeEach();
        String bootstrapServers = String.join(",", clientConf.get(ConfigOptions.BOOTSTRAP_SERVERS));
        streamTEnv = StreamTableEnvironment.create(execEnv, EnvironmentSettings.inStreamingMode());
        streamTEnv.executeSql(
                String.format(
                        "create catalog %s with ('type' = 'fluss', '%s' = '%s')",
                        CATALOG_NAME, BOOTSTRAP_SERVERS.key(), bootstrapServers));
        streamTEnv.executeSql("use catalog " + CATALOG_NAME);
        streamTEnv.executeSql("use " + DEFAULT_DB);
    }

    @Override
    protected FlussClusterExtension getFlussClusterExtension() {
        return FLUSS_CLUSTER_EXTENSION;
    }

    /**
     * Verifies lake fallback old-value lookup and composite key isolation for expired partitions.
     *
     * <p>Phase 1: Dynamically create and write data to two past partitions (within retention=5),
     * tier to Paimon. Phase 2: Alter retention to 1 and manually drop both partitions. Phase 3:
     * Write to expired partitions and verify via Flink streaming read:
     *
     * <ul>
     *   <li>Lake hit: same key in different expired partitions retrieves correct old values
     *       (composite key isolation), producing UPDATE_BEFORE + UPDATE_AFTER.
     *   <li>Lake miss: a new key not in Paimon produces INSERT.
     * </ul>
     */
    @Test
    void testHistoricalPkWriteWithLakeFallback() throws Exception {
        // Compute partition names relative to the current year so the test never becomes flaky.
        // retention=5 keeps (currentYear-5) and beyond; both partitions are well within range.
        // After altering retention to 1, (currentYear-1) becomes the boundary, expiring both.
        int currentYear = Year.now().getValue();
        String p1 = String.valueOf(currentYear - 3);
        String p2 = String.valueOf(currentYear - 2);

        TablePath tablePath = TablePath.of(DEFAULT_DB, "pk_lake_fallback");
        long tableId = createPartitionedPkTable(tablePath);
        FLUSS_CLUSTER_EXTENSION.waitUntilPartitionAllReady(tablePath);

        // Phase 1: Write data to past partitions p1 and p2.
        // With retention=5, both are within retention (currentYear - 5 < p1 < p2).
        Configuration writerConf = new Configuration(clientConf);
        writerConf.set(ConfigOptions.CLIENT_WRITER_DYNAMIC_CREATE_PARTITION_ENABLED, true);

        int precreateCount = ConfigOptions.TABLE_AUTO_PARTITION_NUM_PRECREATE.defaultValue();

        try (Connection writerConn = ConnectionFactory.createConnection(writerConf);
                Table writerTable = writerConn.getTable(tablePath)) {
            UpsertWriter writer = writerTable.newUpsert().createWriter();
            writer.upsert(row(1, "val_p1", p1));
            writer.upsert(row(1, "val_p2", p2));
            writer.flush();
        }

        Map<Long, String> partitions =
                waitUntilPartitions(
                        FLUSS_CLUSTER_EXTENSION.getZooKeeperClient(),
                        tablePath,
                        precreateCount + 2);

        // Start tiering job to sync partitions to Paimon
        JobClient jobClient = buildTieringJob(execEnv);

        // Wait for tiering to sync both partitions to Paimon
        for (Map.Entry<Long, String> entry : partitions.entrySet()) {
            if (p1.equals(entry.getValue()) || p2.equals(entry.getValue())) {
                TableBucket tb = new TableBucket(tableId, entry.getKey(), 0);
                waitUntilBucketSynced(tb);
            }
        }

        // Stop tiering job after Paimon data is ready. Lake fallback during
        // Phase 3 reads Paimon directly and does not need the tiering job.
        // Stopping it ensures Flink streaming read discovers __historical__
        // from log (not from a lake snapshot).
        jobClient.cancel().get();

        // Start streaming query BEFORE dropping partitions to first verify the
        // Paimon snapshot data, then continue to capture __historical__ changelog.
        CloseableIterator<Row> rowIter =
                streamTEnv
                        .executeSql(
                                "SELECT * FROM pk_lake_fallback"
                                        + " /*+ OPTIONS('scan.partition.discovery.interval'='100ms') */")
                        .collect();

        // Verify initial data from Paimon snapshot
        List<String> snapshotRows =
                Arrays.asList(
                        String.format("+I[1, val_p1, %s]", p1),
                        String.format("+I[1, val_p2, %s]", p2));
        assertResultsIgnoreOrder(rowIter, snapshotRows, false);

        // Phase 2: Alter retention to 1 so both partitions are expired
        // (currentYear - 1 > p1 and p2), then manually drop them.
        admin.alterTable(
                        tablePath,
                        Collections.singletonList(
                                TableChange.set(
                                        ConfigOptions.TABLE_AUTO_PARTITION_NUM_RETENTION.key(),
                                        "1")),
                        false)
                .get();
        admin.dropPartition(tablePath, new PartitionSpec(Collections.singletonMap("c", p1)), true)
                .get();
        admin.dropPartition(tablePath, new PartitionSpec(Collections.singletonMap("c", p2)), true)
                .get();

        // Wait for tablet server metadata cache to reflect partition drops.
        // The coordinator sends UpdateMetadataRequest asynchronously after the ZK
        // node is deleted; until tablet servers process the deletion markers, their
        // cache may return stale partition data, causing the client to bypass expired
        // partition detection and write to the old (dropped) partitionId.
        waitUntilPartitionDropPropagated(tablePath, p1, p2);

        // Phase 3: Write to expired partitions with dynamic creation disabled
        Configuration historicalConf = new Configuration(clientConf);
        historicalConf.set(ConfigOptions.CLIENT_WRITER_DYNAMIC_CREATE_PARTITION_ENABLED, false);

        try (Connection historicalConn = ConnectionFactory.createConnection(historicalConf);
                Table historicalTable = historicalConn.getTable(tablePath)) {
            UpsertWriter writer = historicalTable.newUpsert().createWriter();
            // Lake hit: PK=1 exists in Paimon for both p1 and p2
            writer.upsert(row(1, "new_p1", p1));
            writer.upsert(row(1, "new_p2", p2));
            // Lake miss: PK=999 does not exist in Paimon
            writer.upsert(row(999, "brand_new", p1));
            writer.flush();
        }

        // Verify changelog records from __historical__ partition.
        // Partition discovery detects __historical__ and reads its log.
        // Lake hit: -U (old value from Paimon) + +U (new value) for each partition
        // Lake miss: +I for a key not found in Paimon
        List<String> changelogRows =
                Arrays.asList(
                        String.format("-U[1, val_p1, %s]", p1),
                        String.format("+U[1, new_p1, %s]", p1),
                        String.format("-U[1, val_p2, %s]", p2),
                        String.format("+U[1, new_p2, %s]", p2),
                        String.format("+I[999, brand_new, %s]", p1));

        assertResultsIgnoreOrder(rowIter, changelogRows, true);
    }

    // ---- Helper methods ----

    /**
     * Waits until dropped partitions are no longer returned by the tablet server's metadata cache.
     *
     * <p>After {@code admin.dropPartition()}, the coordinator asynchronously sends {@code
     * UpdateMetadataRequest} with deletion markers to tablet servers. Until a tablet server
     * processes the marker, its {@code ServerMetadataCache} still contains the old partition entry.
     * A client {@code MetadataRequest} hitting this stale cache will re-populate the client-side
     * metadata, causing {@code DynamicPartitionCreator} to skip expired-partition detection.
     */
    private void waitUntilPartitionDropPropagated(
            TablePath tablePath, String... droppedPartitions) {
        CommonTestUtils.waitUntil(
                () -> {
                    try (Connection conn = ConnectionFactory.createConnection(clientConf)) {
                        // Initialize table-level metadata so partition lookups can proceed.
                        conn.getTable(tablePath).close();
                        for (String partition : droppedPartitions) {
                            PhysicalTablePath path = PhysicalTablePath.of(tablePath, partition);
                            boolean found;
                            try {
                                found =
                                        ((FlussConnection) conn)
                                                .getMetadataUpdater()
                                                .checkAndUpdatePartitionMetadata(path);
                            } catch (Exception ignored) {
                                // PartitionNotExistException means the partition is truly gone.
                                found = false;
                            }
                            if (found) {
                                return false;
                            }
                        }
                        return true;
                    }
                },
                Duration.ofSeconds(30),
                "partition drops to propagate to tablet server cache");
    }

    /**
     * Creates a lake-enabled, auto-partitioned primary key table with YEAR time unit and 5-year
     * retention.
     */
    private long createPartitionedPkTable(TablePath tablePath) throws Exception {
        Schema schema =
                Schema.newBuilder()
                        .column("a", DataTypes.INT())
                        .column("b", DataTypes.STRING())
                        .column("c", DataTypes.STRING())
                        .primaryKey("a", "c")
                        .build();
        TableDescriptor descriptor =
                TableDescriptor.builder()
                        .schema(schema)
                        .distributedBy(1)
                        .partitionedBy("c")
                        .property(ConfigOptions.TABLE_AUTO_PARTITION_ENABLED, true)
                        .property(
                                ConfigOptions.TABLE_AUTO_PARTITION_TIME_UNIT,
                                AutoPartitionTimeUnit.YEAR)
                        .property(ConfigOptions.TABLE_AUTO_PARTITION_NUM_RETENTION, 5)
                        .property(ConfigOptions.TABLE_DATALAKE_ENABLED.key(), "true")
                        .property(ConfigOptions.TABLE_DATALAKE_FRESHNESS, Duration.ofMillis(500))
                        .build();
        return createTable(tablePath, descriptor);
    }
}
