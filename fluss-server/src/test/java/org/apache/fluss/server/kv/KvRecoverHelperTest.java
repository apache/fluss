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

package org.apache.fluss.server.kv;

import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.TableConfig;
import org.apache.fluss.memory.TestingMemorySegmentPool;
import org.apache.fluss.metadata.KvFormat;
import org.apache.fluss.metadata.LogFormat;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.SchemaInfo;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.KvRecord;
import org.apache.fluss.record.KvRecordBatch;
import org.apache.fluss.record.KvRecordTestUtils;
import org.apache.fluss.record.TestingSchemaGetter;
import org.apache.fluss.row.encode.KeyEncoder;
import org.apache.fluss.rpc.protocol.MergeMode;
import org.apache.fluss.server.kv.autoinc.AutoIncrementManager;
import org.apache.fluss.server.kv.autoinc.TestingSequenceGeneratorFactory;
import org.apache.fluss.server.kv.rowmerger.RowMerger;
import org.apache.fluss.server.log.LogTablet;
import org.apache.fluss.server.log.LogTestUtils;
import org.apache.fluss.server.metrics.group.TestingMetricGroups;
import org.apache.fluss.server.zk.NOPErrorHandler;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.ZooKeeperExtension;
import org.apache.fluss.server.zk.data.TableRegistration;
import org.apache.fluss.shaded.arrow.org.apache.arrow.memory.RootAllocator;
import org.apache.fluss.testutils.common.AllCallbackWrapper;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.utils.clock.SystemClock;
import org.apache.fluss.utils.concurrent.FlussScheduler;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import static org.apache.fluss.compression.ArrowCompressionInfo.DEFAULT_COMPRESSION;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link KvRecoverHelper} with historical partition composite key encoding. */
class KvRecoverHelperTest {

    @RegisterExtension
    public static final AllCallbackWrapper<ZooKeeperExtension> ZK_EXTENSION =
            new AllCallbackWrapper<>(new ZooKeeperExtension());

    private static ZooKeeperClient zkClient;

    /** Schema: id INT (PK), name STRING, dt STRING (partition column). */
    private static final Schema PARTITIONED_SCHEMA =
            Schema.newBuilder()
                    .column("id", DataTypes.INT())
                    .column("name", DataTypes.STRING())
                    .column("dt", DataTypes.STRING())
                    .primaryKey("id")
                    .build();

    private static final short SCHEMA_ID = 1;
    private static final long TABLE_ID = 10001L;
    private static final TablePath TABLE_PATH = TablePath.of("test_db", "test_table");
    private static final Configuration CONF = new Configuration();

    @BeforeAll
    static void beforeAll() throws Exception {
        zkClient = ZK_EXTENSION.getCustomExtension().getZooKeeperClient(NOPErrorHandler.INSTANCE);

        // Register the partitioned table info in ZooKeeper so KvRecoverHelper.initSchema()
        // can look it up during recovery.
        // Register table first (creates the table node), then register schema underneath it.
        TableRegistration tableRegistration =
                new TableRegistration(
                        TABLE_ID,
                        null,
                        Collections.singletonList("dt"),
                        new TableDescriptor.TableDistribution(1, Arrays.asList("id")),
                        new HashMap<>(),
                        new HashMap<>(),
                        null,
                        System.currentTimeMillis(),
                        System.currentTimeMillis());
        zkClient.registerTable(TABLE_PATH, tableRegistration);
        zkClient.registerFirstSchema(TABLE_PATH, PARTITIONED_SCHEMA);
    }

    /**
     * Tests that recovery for a historical partition encodes keys using {@link
     * CompositeKeyEncoder}. After recovery, lookups with composite keys should succeed and lookups
     * with plain keys should fail.
     */
    @Test
    void testHistoricalRecoveryUsesCompositeKeys(@TempDir File tempDir) throws Exception {
        // Create a __historical__ partition
        PhysicalTablePath historicalPath =
                PhysicalTablePath.of("test_db", "test_table", "__historical__");
        TableBucket tableBucket = new TableBucket(TABLE_ID, 1L, 0);
        TestingSchemaGetter schemaGetter =
                new TestingSchemaGetter(new SchemaInfo(PARTITIONED_SCHEMA, SCHEMA_ID));

        // Create LogTablet
        LogTablet logTablet = createLogTablet(tempDir, historicalPath);

        // Create KvTablet A: write data through it to populate the WAL
        File kvDirA = new File(tempDir, "kv-a");
        kvDirA.mkdirs();
        KvTablet kvTabletA =
                createKvTablet(historicalPath, tableBucket, logTablet, kvDirA, schemaGetter);

        // Write records with different original partition names.
        // Use PKBasedKvRecordFactory so that the KvRecord key bytes are encoded from the PK
        // column (id), matching what KvRecoverHelper.applyLogRecordBatch() produces during
        // recovery via keyEncoder.encodeKey(logRow).
        KvRecordTestUtils.KvRecordBatchFactory batchFactory =
                KvRecordTestUtils.KvRecordBatchFactory.of(SCHEMA_ID);
        KvRecordTestUtils.PKBasedKvRecordFactory recordFactory =
                KvRecordTestUtils.PKBasedKvRecordFactory.of(
                        PARTITIONED_SCHEMA.getRowType(), new int[] {0});

        // Write a record for partition "2024-01-01"
        KvRecord record1 = recordFactory.ofRecord(new Object[] {1, "alice", "2024-01-01"});
        KvRecordBatch batch1 = batchFactory.ofRecords(Collections.singletonList(record1));
        kvTabletA.putAsLeader(batch1, null, MergeMode.DEFAULT, "2024-01-01");

        // Write a record for partition "2024-01-02"
        KvRecord record2 = recordFactory.ofRecord(new Object[] {2, "bob", "2024-01-02"});
        KvRecordBatch batch2 = batchFactory.ofRecords(Collections.singletonList(record2));
        kvTabletA.putAsLeader(batch2, null, MergeMode.DEFAULT, "2024-01-02");

        // Advance HW so recovery reads from WAL into RocksDB directly (Phase 1)
        logTablet.updateHighWatermark(logTablet.localLogEndOffset());

        // Flush pre-write buffer to RocksDB so data is visible for sanity check
        kvTabletA.flush(Long.MAX_VALUE, NOPErrorHandler.INSTANCE);

        // Prepare expected composite keys
        KeyEncoder keyEncoder =
                KeyEncoder.ofPrimaryKeyEncoder(
                        PARTITIONED_SCHEMA.getRowType(),
                        Arrays.asList("id"),
                        new TableConfig(new Configuration()),
                        false);
        byte[] plainKey1 = keyEncoder.encodeKey(record1.getRow());
        byte[] plainKey2 = keyEncoder.encodeKey(record2.getRow());
        byte[] compositeKey1 = CompositeKeyEncoder.encode("2024-01-01", plainKey1);
        byte[] compositeKey2 = CompositeKeyEncoder.encode("2024-01-02", plainKey2);

        // Sanity check: the original kvTablet should have composite keys
        List<byte[]> valuesA = kvTabletA.multiGet(Arrays.asList(compositeKey1, compositeKey2));
        assertThat(valuesA.get(0)).as("original KvTablet should have composite key 1").isNotNull();
        assertThat(valuesA.get(1)).as("original KvTablet should have composite key 2").isNotNull();

        // Close the original KvTablet
        kvTabletA.close();

        // Create a fresh KvTablet B for recovery
        File kvDirB = new File(tempDir, "kv-b");
        kvDirB.mkdirs();
        KvTablet kvTabletB =
                createKvTablet(historicalPath, tableBucket, logTablet, kvDirB, schemaGetter);

        // Run recovery with historical partition mode
        KvRecoverHelper.KvRecoverContext recoverContext =
                new KvRecoverHelper.KvRecoverContext(TABLE_PATH, zkClient, 1024 * 1024);
        // RemoteLogFetcher won't be used since all data is in local log.
        // Pass null for RemoteLogManager — the fetcher is only invoked when
        // nextFetchOffset < localLogStartOffset, which won't happen here.
        RemoteLogFetcher remoteLogFetcher =
                new RemoteLogFetcher(null, tableBucket, new File(tempDir, "remote-tmp").toPath());

        try {
            KvRecoverHelper kvRecoverHelper =
                    new KvRecoverHelper(
                            kvTabletB,
                            logTablet,
                            0L,
                            null,
                            null,
                            recoverContext,
                            KvFormat.COMPACTED,
                            LogFormat.ARROW,
                            schemaGetter,
                            remoteLogFetcher,
                            true,
                            Collections.singletonList("dt"));
            kvRecoverHelper.recover();
        } finally {
            remoteLogFetcher.close();
        }

        // Flush pre-write buffer to make data visible in RocksDB
        kvTabletB.flush(Long.MAX_VALUE, NOPErrorHandler.INSTANCE);

        // Verify: composite keys should be found after recovery
        List<byte[]> recoveredComposite =
                kvTabletB.multiGet(Arrays.asList(compositeKey1, compositeKey2));
        assertThat(recoveredComposite.get(0))
                .as("recovered KvTablet should have composite key 1")
                .isNotNull();
        assertThat(recoveredComposite.get(1))
                .as("recovered KvTablet should have composite key 2")
                .isNotNull();

        // Verify: plain keys should NOT be found (proving composite encoding was used)
        List<byte[]> recoveredPlain = kvTabletB.multiGet(Arrays.asList(plainKey1, plainKey2));
        assertThat(recoveredPlain.get(0))
                .as("recovered KvTablet should NOT have plain key 1")
                .isNull();
        assertThat(recoveredPlain.get(1))
                .as("recovered KvTablet should NOT have plain key 2")
                .isNull();

        kvTabletB.close();
    }

    /**
     * Tests that recovery for a normal (non-historical) partition continues to use plain keys,
     * unaffected by the historical partition changes.
     */
    @Test
    void testNormalRecoveryUnchanged(@TempDir File tempDir) throws Exception {
        // Create a normal (non-historical) partition
        PhysicalTablePath normalPath = PhysicalTablePath.of("test_db", "test_table", "2024-01-01");
        TableBucket tableBucket = new TableBucket(TABLE_ID, 2L, 0);
        TestingSchemaGetter schemaGetter =
                new TestingSchemaGetter(new SchemaInfo(PARTITIONED_SCHEMA, SCHEMA_ID));

        // Create LogTablet
        LogTablet logTablet = createLogTablet(tempDir, normalPath);

        // Create KvTablet A: write data to populate WAL
        File kvDirA = new File(tempDir, "kv-a");
        kvDirA.mkdirs();
        KvTablet kvTabletA =
                createKvTablet(normalPath, tableBucket, logTablet, kvDirA, schemaGetter);

        // Write records (normal partition — no partition name passed).
        // Use PKBasedKvRecordFactory for consistency with recovery key encoding.
        KvRecordTestUtils.KvRecordBatchFactory batchFactory =
                KvRecordTestUtils.KvRecordBatchFactory.of(SCHEMA_ID);
        KvRecordTestUtils.PKBasedKvRecordFactory recordFactory =
                KvRecordTestUtils.PKBasedKvRecordFactory.of(
                        PARTITIONED_SCHEMA.getRowType(), new int[] {0});

        KvRecord record1 = recordFactory.ofRecord(new Object[] {1, "alice", "2024-01-01"});
        KvRecordBatch batch1 = batchFactory.ofRecords(Collections.singletonList(record1));
        kvTabletA.putAsLeader(batch1, null);

        // Advance HW
        logTablet.updateHighWatermark(logTablet.localLogEndOffset());

        // Prepare expected plain key
        KeyEncoder keyEncoder =
                KeyEncoder.ofPrimaryKeyEncoder(
                        PARTITIONED_SCHEMA.getRowType(),
                        Arrays.asList("id"),
                        new TableConfig(new Configuration()),
                        false);
        byte[] plainKey1 = keyEncoder.encodeKey(record1.getRow());

        // Close original KvTablet
        kvTabletA.close();

        // Create fresh KvTablet B for recovery
        File kvDirB = new File(tempDir, "kv-b");
        kvDirB.mkdirs();
        KvTablet kvTabletB =
                createKvTablet(normalPath, tableBucket, logTablet, kvDirB, schemaGetter);

        // Run recovery with normal (non-historical) mode
        KvRecoverHelper.KvRecoverContext recoverContext =
                new KvRecoverHelper.KvRecoverContext(TABLE_PATH, zkClient, 1024 * 1024);
        RemoteLogFetcher remoteLogFetcher =
                new RemoteLogFetcher(null, tableBucket, new File(tempDir, "remote-tmp").toPath());

        try {
            KvRecoverHelper kvRecoverHelper =
                    new KvRecoverHelper(
                            kvTabletB,
                            logTablet,
                            0L,
                            null,
                            null,
                            recoverContext,
                            KvFormat.COMPACTED,
                            LogFormat.ARROW,
                            schemaGetter,
                            remoteLogFetcher,
                            false,
                            null);
            kvRecoverHelper.recover();
        } finally {
            remoteLogFetcher.close();
        }

        // Flush pre-write buffer
        kvTabletB.flush(Long.MAX_VALUE, NOPErrorHandler.INSTANCE);

        // Verify: plain key should be found after normal recovery
        List<byte[]> recovered = kvTabletB.multiGet(Collections.singletonList(plainKey1));
        assertThat(recovered.get(0))
                .as("recovered KvTablet should have plain key after normal recovery")
                .isNotNull();

        kvTabletB.close();
    }

    // ----- helper methods -----

    private LogTablet createLogTablet(File tempDir, PhysicalTablePath physicalPath)
            throws Exception {
        File logTabletDir =
                LogTestUtils.makeRandomLogTabletDir(
                        tempDir,
                        physicalPath.getDatabaseName(),
                        TABLE_ID,
                        physicalPath.getTableName());
        return LogTablet.create(
                tempDir,
                physicalPath,
                logTabletDir,
                CONF,
                TestingMetricGroups.TABLET_SERVER_METRICS,
                0,
                new FlussScheduler(1),
                LogFormat.ARROW,
                1,
                true,
                SystemClock.getInstance(),
                true);
    }

    private KvTablet createKvTablet(
            PhysicalTablePath physicalPath,
            TableBucket tableBucket,
            LogTablet logTablet,
            File kvDir,
            TestingSchemaGetter schemaGetter)
            throws Exception {
        TableConfig tableConf = new TableConfig(new Configuration());
        RowMerger rowMerger = RowMerger.create(tableConf, KvFormat.COMPACTED, schemaGetter);
        AutoIncrementManager autoIncrementManager =
                new AutoIncrementManager(
                        schemaGetter,
                        physicalPath.getTablePath(),
                        tableConf,
                        new TestingSequenceGeneratorFactory());

        return KvTablet.create(
                physicalPath,
                tableBucket,
                logTablet,
                kvDir,
                CONF,
                TestingMetricGroups.TABLET_SERVER_METRICS,
                new RootAllocator(Long.MAX_VALUE),
                new TestingMemorySegmentPool(10 * 1024),
                KvFormat.COMPACTED,
                rowMerger,
                DEFAULT_COMPRESSION,
                schemaGetter,
                tableConf.getChangelogImage(),
                KvManager.getDefaultRateLimiter(),
                autoIncrementManager);
    }
}
