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

package org.apache.fluss.server.replica.historical;

import org.apache.fluss.cluster.Endpoint;
import org.apache.fluss.cluster.ServerType;
import org.apache.fluss.config.AutoPartitionTimeUnit;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.HistoricalPartitionThrottledException;
import org.apache.fluss.exception.InvalidPartitionException;
import org.apache.fluss.lake.lakestorage.LakeTableLookuper;
import org.apache.fluss.memory.MemorySegment;
import org.apache.fluss.metadata.ChangelogImage;
import org.apache.fluss.metadata.DataLakeFormat;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.ResolvedPartitionSpec;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.SchemaGetter;
import org.apache.fluss.metadata.SchemaInfo;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.BinaryValue;
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.record.KvRecordBatch;
import org.apache.fluss.record.LogRecords;
import org.apache.fluss.record.TestingSchemaGetter;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.encode.CompactedKeyEncoder;
import org.apache.fluss.row.encode.KvValueLayout;
import org.apache.fluss.row.encode.ValueDecoder;
import org.apache.fluss.row.encode.ValueEncoder;
import org.apache.fluss.rpc.entity.FetchLogResultForBucket;
import org.apache.fluss.rpc.entity.LookupResultForBucket;
import org.apache.fluss.rpc.entity.PutKvResultForBucket;
import org.apache.fluss.rpc.messages.NotifyLakeTableOffsetResponse;
import org.apache.fluss.rpc.protocol.ApiKeys;
import org.apache.fluss.rpc.protocol.Errors;
import org.apache.fluss.rpc.protocol.MergeMode;
import org.apache.fluss.server.entity.FetchReqInfo;
import org.apache.fluss.server.entity.LakeBucketOffset;
import org.apache.fluss.server.entity.LookupDataForBucket;
import org.apache.fluss.server.entity.NotifyLakeTableOffsetData;
import org.apache.fluss.server.entity.NotifyLeaderAndIsrData;
import org.apache.fluss.server.entity.NotifyLeaderAndIsrResultForBucket;
import org.apache.fluss.server.entity.PutKvDataForBucket;
import org.apache.fluss.server.kv.KvStateLookupResult;
import org.apache.fluss.server.kv.KvTablet;
import org.apache.fluss.server.kv.historical.HistoricalKvKeyEncoder;
import org.apache.fluss.server.kv.historical.HistoricalKvTombstone;
import org.apache.fluss.server.log.FetchParams;
import org.apache.fluss.server.log.LogAppendInfo;
import org.apache.fluss.server.metadata.BucketMetadata;
import org.apache.fluss.server.metadata.ClusterMetadata;
import org.apache.fluss.server.metadata.PartitionMetadata;
import org.apache.fluss.server.metadata.ServerInfo;
import org.apache.fluss.server.metadata.TableMetadata;
import org.apache.fluss.server.replica.Replica;
import org.apache.fluss.server.replica.ReplicaTestBase;
import org.apache.fluss.server.zk.data.LeaderAndIsr;
import org.apache.fluss.server.zk.data.TableRegistration;
import org.apache.fluss.server.zk.data.lake.LakeTableHelper;
import org.apache.fluss.server.zk.data.lake.LakeTableSnapshot;
import org.apache.fluss.testutils.common.ManuallyTriggeredScheduledExecutorService;
import org.apache.fluss.types.DataField;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.ByteArraySlice;
import org.apache.fluss.utils.types.Tuple2;

import com.github.benmanes.caffeine.cache.Scheduler;
import com.github.benmanes.caffeine.cache.Ticker;
import org.junit.jupiter.api.Test;
import org.rocksdb.FlushOptions;

import javax.annotation.Nullable;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.fluss.record.TestData.DEFAULT_REMOTE_DATA_DIR;
import static org.apache.fluss.server.coordinator.CoordinatorContext.INITIAL_COORDINATOR_EPOCH;
import static org.apache.fluss.server.kv.KvTabletTestUtils.flushAndWait;
import static org.apache.fluss.server.zk.data.LeaderAndIsr.INITIAL_BUCKET_EPOCH;
import static org.apache.fluss.server.zk.data.LeaderAndIsr.INITIAL_LEADER_EPOCH;
import static org.apache.fluss.testutils.DataTestUtils.assertLogRecordsEqualsWithRowKind;
import static org.apache.fluss.testutils.DataTestUtils.compactedRow;
import static org.apache.fluss.testutils.DataTestUtils.genKvRecordBatch;
import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.apache.fluss.testutils.InternalRowAssert.assertThatRow;
import static org.apache.fluss.testutils.common.CommonTestUtils.retry;
import static org.apache.fluss.utils.PartitionUtils.HISTORICAL_PARTITION_VALUE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for historical primary-key writes coordinated by {@link HistoricalPartitionManager}. */
class HistoricalPartitionManagerTest extends ReplicaTestBase {

    private static final long TABLE_ID = 987654L;
    private static final long PARTITION_ID = 123L;
    private static final TablePath TABLE_PATH =
            TablePath.of("historical_write_db", "historical_write_table");
    private static final String ORIGINAL_PARTITION = "20240107";
    private static final String ANOTHER_ORIGINAL_PARTITION = "20240108";
    private static final String HISTORICAL_PARTITION = HISTORICAL_PARTITION_VALUE;
    private static final TableBucket TABLE_BUCKET = new TableBucket(TABLE_ID, PARTITION_ID, 0);
    private static final RowType KEY_TYPE =
            DataTypes.ROW(
                    new DataField("id", DataTypes.INT()),
                    new DataField("region", DataTypes.STRING()));

    @Test
    void testResolvesMultipleLakeMissesWithoutPrewriteRollback() throws Exception {
        TableInfo tableInfo = registerHistoricalTableAndBecomeLeader();
        Replica replica = replicaManager.getReplicaOrException(TABLE_BUCKET);
        KvTablet kvTablet = replica.getKvTablet();
        assertThat(kvTablet).isNotNull();
        TestingHistoricalLakeLookupManager lakeLookupManager =
                new TestingHistoricalLakeLookupManager(lookupConfiguration());

        byte[] firstKey = key(1, "us");
        byte[] secondKey = key(2, "eu");
        byte[] thirdKey = key(3, "ap");
        KvRecordBatch insertBatch =
                batch(
                        tableInfo.getRowType(),
                        upsert(1, "us", ORIGINAL_PARTITION, "v1"),
                        // The second record reuses the first record's staged state and must not
                        // trigger another lake lookup for the same key.
                        upsert(1, "us", ORIGINAL_PARTITION, "v1-updated"),
                        upsert(2, "eu", ORIGINAL_PARTITION, "v2"),
                        // The absent delete is a no-op. The following upsert must still see the
                        // resolved absence without another lake lookup.
                        delete(3, "ap"),
                        upsert(3, "ap", ORIGINAL_PARTITION, "v3"));
        long truncateCount =
                replicaManager.getServerMetricGroup().kvTruncateAsErrorCount().getCount();

        try (HistoricalPartitionManager historicalPartitionManager =
                createHistoricalPartitionManager(lakeLookupManager)) {
            writeBatch(historicalPartitionManager, replica, ORIGINAL_PARTITION, insertBatch);

            assertThat(lakeLookupManager.lookupCount).hasValue(3);
            assertThat(lakeLookupManager.lookupBatchCount).hasValue(1);
            assertThat(replicaManager.getServerMetricGroup().kvTruncateAsErrorCount().getCount())
                    .isEqualTo(truncateCount);
            flushAndWait(kvTablet, Long.MAX_VALUE);
            assertHistoricalValue(
                    kvTablet,
                    ORIGINAL_PARTITION,
                    firstKey,
                    tableInfo,
                    row(1, "us", ORIGINAL_PARTITION, "v1-updated"));
            assertHistoricalValue(
                    kvTablet,
                    ORIGINAL_PARTITION,
                    secondKey,
                    tableInfo,
                    row(2, "eu", ORIGINAL_PARTITION, "v2"));
            assertHistoricalValue(
                    kvTablet,
                    ORIGINAL_PARTITION,
                    thirdKey,
                    tableInfo,
                    row(3, "ap", ORIGINAL_PARTITION, "v3"));
        }
    }

    @Test
    void testReusesLocalProbeResultsWhenEntriesDisappearBeforeApply() throws Exception {
        TableInfo tableInfo = registerHistoricalTableAndBecomeLeader();
        Replica replica = replicaManager.getReplicaOrException(TABLE_BUCKET);
        KvTablet kvTablet = replica.getKvTablet();
        assertThat(kvTablet).isNotNull();
        TestingHistoricalLakeLookupManager lakeLookupManager =
                new TestingHistoricalLakeLookupManager(lookupConfiguration());

        RowType rowType = tableInfo.getRowType();
        byte[] valueKey = key(1, "us");
        byte[] tombstoneKey = key(2, "eu");
        byte[] missingKey = key(3, "ap");

        try (HistoricalPartitionManager historicalPartitionManager =
                createHistoricalPartitionManager(lakeLookupManager)) {
            // Seed exactly the two local states that compaction can remove between phases.
            kvTablet.getRocksDBKv()
                    .put(
                            HistoricalKvKeyEncoder.encode(ORIGINAL_PARTITION, valueKey),
                            ValueEncoder.forLayout(KvValueLayout.TAGGED)
                                    .encodeValue(
                                            new BinaryValue(
                                                    (short) tableInfo.getSchemaId(),
                                                    compactedRow(
                                                            rowType,
                                                            new Object[] {
                                                                1, "us", ORIGINAL_PARTITION, "v1"
                                                            })),
                                            0L));
            kvTablet.getRocksDBKv()
                    .put(
                            HistoricalKvKeyEncoder.encode(ORIGINAL_PARTITION, tombstoneKey),
                            HistoricalKvTombstone.encode(1L));

            // The third key forces lake I/O after all three keys have been probed. Remove the
            // value and tombstone during that I/O to model compaction between probe and apply.
            lakeLookupManager.setLookupHook(
                    () -> {
                        deleteHistoricalRocksDbKey(kvTablet, valueKey);
                        deleteHistoricalRocksDbKey(kvTablet, tombstoneKey);
                    });

            LogAppendInfo appendInfo =
                    writeBatch(
                            historicalPartitionManager,
                            replica,
                            ORIGINAL_PARTITION,
                            batch(
                                    rowType,
                                    upsert(1, "us", ORIGINAL_PARTITION, "v2"),
                                    upsert(2, "eu", ORIGINAL_PARTITION, "recreated"),
                                    upsert(3, "ap", ORIGINAL_PARTITION, "inserted")));

            assertThat(appendInfo.numMessages()).isEqualTo(4);
            assertThat(lakeLookupManager.lookupCount).hasValue(1);
            assertThat(lakeLookupManager.lookupBatchCount).hasValue(1);
            flushAndWait(kvTablet, Long.MAX_VALUE);
            assertHistoricalValue(
                    kvTablet,
                    ORIGINAL_PARTITION,
                    valueKey,
                    tableInfo,
                    row(1, "us", ORIGINAL_PARTITION, "v2"));
            assertHistoricalValue(
                    kvTablet,
                    ORIGINAL_PARTITION,
                    tombstoneKey,
                    tableInfo,
                    row(2, "eu", ORIGINAL_PARTITION, "recreated"));
            assertHistoricalValue(
                    kvTablet,
                    ORIGINAL_PARTITION,
                    missingKey,
                    tableInfo,
                    row(3, "ap", ORIGINAL_PARTITION, "inserted"));
        }
    }

    @Test
    void testWalFullRowUpsertDoesNotLookupLake() throws Exception {
        TableInfo tableInfo = registerHistoricalTableAndBecomeLeader(ChangelogImage.WAL);
        Replica replica = replicaManager.getReplicaOrException(TABLE_BUCKET);
        KvTablet kvTablet = replica.getKvTablet();
        assertThat(kvTablet).isNotNull();
        TestingHistoricalLakeLookupManager lakeLookupManager =
                new TestingHistoricalLakeLookupManager(lookupConfiguration());

        byte[] primaryKey = key(1, "us");
        KvRecordBatch insertBatch =
                batch(tableInfo.getRowType(), upsert(1, "us", ORIGINAL_PARTITION, "v1"));

        try (HistoricalPartitionManager historicalPartitionManager =
                createHistoricalPartitionManager(lakeLookupManager)) {
            writeBatch(historicalPartitionManager, replica, ORIGINAL_PARTITION, insertBatch);

            assertThat(lakeLookupManager.lookupCount).hasValue(0);
            assertThat(lakeLookupManager.lookupBatchCount).hasValue(0);
            flushAndWait(kvTablet, Long.MAX_VALUE);
            assertHistoricalValue(
                    kvTablet,
                    ORIGINAL_PARTITION,
                    primaryKey,
                    tableInfo,
                    row(1, "us", ORIGINAL_PARTITION, "v1"));
        }
    }

    @Test
    void testHistoricalInsertUpdateAndDelete() throws Exception {
        TableInfo tableInfo = registerHistoricalTableAndBecomeLeader(true);
        Replica replica = replicaManager.getReplicaOrException(TABLE_BUCKET);
        KvTablet kvTablet = replica.getKvTablet();
        assertThat(kvTablet).isNotNull();
        assertThat(kvManager.getKv(TABLE_BUCKET)).contains(kvTablet);
        TestingHistoricalLakeLookupManager lakeLookupManager =
                new TestingHistoricalLakeLookupManager(lookupConfiguration());

        RowType rowType = tableInfo.getRowType();
        byte[] primaryKey = key(1, "us");

        try (HistoricalPartitionManager historicalPartitionManager =
                createHistoricalPartitionManager(lakeLookupManager)) {
            // The first write misses both local state and lake, so it creates a local overlay.
            KvRecordBatch insertBatch = batch(rowType, upsert(1, "us", "20240107", "v1"));
            assertThat(
                            writeBatch(
                                            historicalPartitionManager,
                                            replica,
                                            ORIGINAL_PARTITION,
                                            insertBatch)
                                    .lastOffset())
                    .isZero();
            flushAndWait(kvTablet, Long.MAX_VALUE);

            assertHistoricalValue(
                    kvTablet,
                    ORIGINAL_PARTITION,
                    primaryKey,
                    tableInfo,
                    row(1, "us", "20240107", "v1"));
            assertThat(lakeLookupManager.lookupCount).hasValue(1);

            // The same primary key in another original partition must use a separate state entry.
            KvRecordBatch anotherPartitionBatch =
                    batch(rowType, upsert(1, "us", "20240108", "another"));
            writeBatch(
                    historicalPartitionManager,
                    replica,
                    ANOTHER_ORIGINAL_PARTITION,
                    anotherPartitionBatch);
            flushAndWait(kvTablet, Long.MAX_VALUE);
            assertHistoricalValue(
                    kvTablet,
                    ORIGINAL_PARTITION,
                    primaryKey,
                    tableInfo,
                    row(1, "us", "20240107", "v1"));
            assertHistoricalValue(
                    kvTablet,
                    ANOTHER_ORIGINAL_PARTITION,
                    primaryKey,
                    tableInfo,
                    row(1, "us", "20240108", "another"));
            assertThat(lakeLookupManager.lookupCount).hasValue(2);

            // Exercise the ReplicaManager entry point; the update should reuse the local overlay.
            KvRecordBatch updateBatch = batch(rowType, upsert(1, "us", "20240107", "v2"));
            CompletableFuture<List<PutKvResultForBucket>> updateResponse =
                    new CompletableFuture<>();
            assertThat(replica.tableMetrics().totalHistoricalPutKvRequests().getCount()).isZero();
            assertThat(replica.tableMetrics().failedHistoricalPutKvRequests().getCount()).isZero();
            replicaManager.putHistoricalRecordsToKv(
                    10_000,
                    1,
                    Collections.singletonList(
                            new PutKvDataForBucket(TABLE_BUCKET, updateBatch, ORIGINAL_PARTITION)),
                    null,
                    MergeMode.DEFAULT,
                    ApiKeys.PUT_KV.highestSupportedVersion,
                    updateResponse::complete);
            Map<TableBucket, PutKvResultForBucket> updateResults =
                    updateResponse.get(10, TimeUnit.SECONDS).stream()
                            .collect(
                                    java.util.stream.Collectors.toMap(
                                            PutKvResultForBucket::getTableBucket,
                                            result -> result));
            assertThat(updateResults.get(TABLE_BUCKET).failed()).isFalse();
            assertThat(replica.tableMetrics().totalHistoricalPutKvRequests().getCount()).isOne();
            assertThat(replica.tableMetrics().failedHistoricalPutKvRequests().getCount()).isZero();
            assertHistoricalValue(
                    kvTablet,
                    ORIGINAL_PARTITION,
                    primaryKey,
                    tableInfo,
                    row(1, "us", "20240107", "v2"));
            assertHistoricalValue(
                    kvTablet,
                    ANOTHER_ORIGINAL_PARTITION,
                    primaryKey,
                    tableInfo,
                    row(1, "us", "20240108", "another"));
            assertThat(lakeLookupManager.lookupCount).hasValue(2);

            // Historical lookup should observe the updated value from the local overlay.
            CompletableFuture<List<LookupResultForBucket>> lookupResponse =
                    new CompletableFuture<>();
            replicaManager.historicalLookups(
                    Collections.singletonList(
                            new LookupDataForBucket(
                                    TABLE_BUCKET,
                                    Collections.singletonList(primaryKey),
                                    ORIGINAL_PARTITION)),
                    lookupResponse::complete);
            LookupResultForBucket lookupResult = lookupResponse.get(10, TimeUnit.SECONDS).get(0);
            assertThat(lookupResult.failed()).isFalse();
            BinaryValue lookedUpValue =
                    new ValueDecoder(
                                    schemaGetter(tableInfo),
                                    tableInfo.getTableConfig().getKvFormat(),
                                    KvValueLayout.PLAIN)
                            .decodeValue(lookupResult.lookupValues().get(0).toByteArray());
            assertThat(lookedUpValue.row.getString(3)).isEqualTo(BinaryString.fromString("v2"));

            // Keep a tombstone locally so a later lookup cannot resurrect the value from lake.
            KvRecordBatch deleteBatch = batch(rowType, delete(1, "us"));
            writeBatch(historicalPartitionManager, replica, ORIGINAL_PARTITION, deleteBatch);
            flushAndWait(kvTablet, Long.MAX_VALUE);
            assertThat(kvTablet.lookupHistoricalLocal(ORIGINAL_PARTITION, primaryKey))
                    .isEqualTo(KvStateLookupResult.deleted());
            assertHistoricalValue(
                    kvTablet,
                    ANOTHER_ORIGINAL_PARTITION,
                    primaryKey,
                    tableInfo,
                    row(1, "us", "20240108", "another"));
            assertThat(lakeLookupManager.lookupCount).hasValue(2);

            CompletableFuture<List<LookupResultForBucket>> deletedLookupResponse =
                    new CompletableFuture<>();
            replicaManager.historicalLookups(
                    Collections.singletonList(
                            new LookupDataForBucket(
                                    TABLE_BUCKET,
                                    Collections.singletonList(primaryKey),
                                    ORIGINAL_PARTITION)),
                    deletedLookupResponse::complete);
            LookupResultForBucket deletedLookup =
                    deletedLookupResponse.get(10, TimeUnit.SECONDS).get(0);
            assertThat(deletedLookup.failed()).isFalse();
            assertThat(deletedLookup.lookupValues()).containsExactly((ByteArraySlice) null);

            // Historical replicas must reject the normal KV write path.
            assertThatThrownBy(
                            () ->
                                    replica.putRecordsToLeader(
                                            insertBatch, null, MergeMode.DEFAULT, 1))
                    .isInstanceOf(InvalidPartitionException.class);
        }
    }

    @Test
    void testHistoricalMutationsUseProducingWalOffsetsAsTags() throws Exception {
        TableInfo tableInfo = registerHistoricalTableAndBecomeLeader();
        Replica replica = replicaManager.getReplicaOrException(TABLE_BUCKET);
        KvTablet kvTablet = replica.getKvTablet();
        assertThat(kvTablet).isNotNull();
        RowType rowType = tableInfo.getRowType();
        byte[] primaryKey = key(1, "us");

        try (HistoricalPartitionManager historicalPartitionManager =
                createHistoricalPartitionManager(
                        new TestingHistoricalLakeLookupManager(lookupConfiguration()))) {
            LogAppendInfo insertAppend =
                    writeBatch(
                            historicalPartitionManager,
                            replica,
                            ORIGINAL_PARTITION,
                            batch(rowType, upsert(1, "us", ORIGINAL_PARTITION, "v1")));
            flushAndWait(kvTablet, Long.MAX_VALUE);
            assertHistoricalValueTag(
                    kvTablet, ORIGINAL_PARTITION, primaryKey, insertAppend.lastOffset());

            LogAppendInfo updateAppend =
                    writeBatch(
                            historicalPartitionManager,
                            replica,
                            ORIGINAL_PARTITION,
                            batch(rowType, upsert(1, "us", ORIGINAL_PARTITION, "v2")));
            assertThat(updateAppend.numMessages()).isEqualTo(2);
            flushAndWait(kvTablet, Long.MAX_VALUE);
            // In FULL changelog mode, UPDATE_AFTER is the second WAL record and produces state.
            assertHistoricalValueTag(
                    kvTablet, ORIGINAL_PARTITION, primaryKey, updateAppend.lastOffset());

            LogAppendInfo deleteAppend =
                    writeBatch(
                            historicalPartitionManager,
                            replica,
                            ORIGINAL_PARTITION,
                            batch(rowType, delete(1, "us")));
            flushAndWait(kvTablet, Long.MAX_VALUE);
            byte[] tombstone = historicalRawValue(kvTablet, ORIGINAL_PARTITION, primaryKey);
            assertThat(tombstone).hasSize(Long.BYTES);
            assertHistoricalValueTag(
                    kvTablet, ORIGINAL_PARTITION, primaryKey, deleteAppend.lastOffset());
        }
    }

    @Test
    void testCompactionRespectsRetentionAndFallsBackToLake() throws Exception {
        TableInfo tableInfo = registerHistoricalTableAndBecomeLeader(ChangelogImage.WAL);
        Replica replica = replicaManager.getReplicaOrException(TABLE_BUCKET);
        KvTablet kvTablet = replica.getKvTablet();
        assertThat(kvTablet).isNotNull();
        TestingHistoricalLakeLookupManager lakeLookupManager =
                new TestingHistoricalLakeLookupManager(lookupConfiguration());
        byte[] coveredKey = key(1, "us");
        byte[] retainedKey = key(2, "eu");
        int retentionOffsetDistance = 60_000;
        long cleanupOffset = retentionOffsetDistance + 1L;

        try (HistoricalPartitionManager historicalPartitionManager =
                createHistoricalPartitionManager(lakeLookupManager)) {
            writeBatch(
                    historicalPartitionManager,
                    replica,
                    ORIGINAL_PARTITION,
                    batch(
                            tableInfo.getRowType(),
                            upsert(1, "us", ORIGINAL_PARTITION, "covered"),
                            upsert(2, "eu", ORIGINAL_PARTITION, "retained")));
            // Repeated WAL-mode updates advance the log by one offset each without creating
            // thousands of distinct local keys. This fills the hardcoded retention window.
            writeBatch(
                    historicalPartitionManager,
                    replica,
                    ORIGINAL_PARTITION,
                    genKvRecordBatch(
                            KEY_TYPE,
                            tableInfo.getRowType(),
                            Collections.nCopies(
                                    retentionOffsetDistance,
                                    upsert(3, "ap", ORIGINAL_PARTITION, "padding"))));
            assertThat(replica.getLocalLogEndOffset()).isEqualTo(cleanupOffset + 1L);
            flushAndWait(kvTablet, Long.MAX_VALUE);
            assertHistoricalValueTag(kvTablet, ORIGINAL_PARTITION, coveredKey, 0L);
            assertHistoricalValueTag(kvTablet, ORIGINAL_PARTITION, retainedKey, 1L);

            historicalPartitionManager.onLakeProgress(replica, 6L, retentionOffsetDistance);

            // Lake coverage alone does not remove values still within the retained offset range.
            compactHistoricalKv(kvTablet);
            assertHistoricalValueTag(kvTablet, ORIGINAL_PARTITION, coveredKey, 0L);
            assertHistoricalValueTag(kvTablet, ORIGINAL_PARTITION, retainedKey, 1L);

            historicalPartitionManager.onLakeProgress(replica, 7L, cleanupOffset);

            assertThat(kvTablet.getHistoricalCleanupOffset()).isEqualTo(cleanupOffset);
            compactHistoricalKv(kvTablet);
            assertThat(historicalRawValue(kvTablet, ORIGINAL_PARTITION, coveredKey)).isNull();
            assertHistoricalValueTag(kvTablet, ORIGINAL_PARTITION, retainedKey, 1L);

            // A miss caused by cleanup must enter lake lookup only after the covering snapshot
            // token has been published.
            lakeLookupManager.putLakeValue(
                    ORIGINAL_PARTITION,
                    ValueEncoder.encodeValue(
                            (short) tableInfo.getSchemaId(),
                            compactedRow(
                                    tableInfo.getRowType(),
                                    new Object[] {
                                        1, "us", ORIGINAL_PARTITION, "covered-from-lake"
                                    })));
            lakeLookupManager.setLookupHook(
                    () -> {
                        assertThat(lakeLookupManager.requiredSnapshotCount).hasValue(2);
                        assertThat(kvTablet.getHistoricalCleanupOffset()).isEqualTo(cleanupOffset);
                    });
            LookupResultForBucket fallbackResult =
                    historicalPartitionManager
                            .lookup(
                                    replica,
                                    new LookupDataForBucket(
                                            TABLE_BUCKET,
                                            Collections.singletonList(coveredKey),
                                            ORIGINAL_PARTITION),
                                    (lookupTimeNanos, lookupFileDownloaded) -> {})
                            .get(10, TimeUnit.SECONDS);
            assertThat(fallbackResult.failed()).isFalse();
            BinaryValue fallbackValue =
                    new ValueDecoder(
                                    schemaGetter(tableInfo),
                                    tableInfo.getTableConfig().getKvFormat(),
                                    KvValueLayout.PLAIN)
                            .decodeValue(fallbackResult.lookupValues().get(0).toByteArray());
            assertThat(fallbackValue.row.getString(3))
                    .isEqualTo(BinaryString.fromString("covered-from-lake"));

            historicalPartitionManager.onLakeProgress(replica, 8L, cleanupOffset + 1L);
            compactHistoricalKv(kvTablet);
            assertThat(historicalRawValue(kvTablet, ORIGINAL_PARTITION, retainedKey)).isNull();
        }
    }

    @Test
    void testLakeProgressPublishesSnapshotBeforeCleanupOffset() throws Exception {
        TableInfo tableInfo = registerHistoricalTableAndBecomeLeader(ChangelogImage.WAL);
        Replica replica = replicaManager.getReplicaOrException(TABLE_BUCKET);
        KvTablet kvTablet = replica.getKvTablet();
        assertThat(kvTablet).isNotNull();
        TestingHistoricalLakeLookupManager lakeLookupManager =
                new TestingHistoricalLakeLookupManager(lookupConfiguration());

        try (HistoricalPartitionManager historicalPartitionManager =
                createHistoricalPartitionManager(lakeLookupManager)) {
            writeBatch(
                    historicalPartitionManager,
                    replica,
                    ORIGINAL_PARTITION,
                    batch(
                            tableInfo.getRowType(),
                            upsert(1, "us", ORIGINAL_PARTITION, "v1"),
                            upsert(2, "eu", ORIGINAL_PARTITION, "v2")));

            // The covering snapshot must be published before entries become eligible for cleanup.
            lakeLookupManager.setRequireSnapshotHook(
                    () -> assertThat(kvTablet.getHistoricalCleanupOffset()).isZero());
            historicalPartitionManager.onLakeProgress(replica, 7L, 1L);
            assertThat(lakeLookupManager.requiredSnapshotCount).hasValue(1);
            assertThat(kvTablet.getHistoricalCleanupOffset()).isOne();

            // A new opaque snapshot token at the same offset is still published to the lookuper.
            lakeLookupManager.setRequireSnapshotHook(
                    () -> assertThat(kvTablet.getHistoricalCleanupOffset()).isOne());
            historicalPartitionManager.onLakeProgress(replica, 8L, 1L);
            assertThat(lakeLookupManager.requiredSnapshotCount).hasValue(2);
            assertThat(kvTablet.getHistoricalCleanupOffset()).isOne();

            // A regressing cleanup offset is ignored before it can publish an older required
            // snapshot.
            historicalPartitionManager.onLakeProgress(replica, 9L, 0L);
            assertThat(lakeLookupManager.requiredSnapshotCount).hasValue(2);
            assertThat(kvTablet.getHistoricalCleanupOffset()).isOne();

            // The RPC entry point uses ReplicaManager's own historical manager.
            CompletableFuture<NotifyLakeTableOffsetResponse> notifyFuture =
                    new CompletableFuture<>();
            replicaManager.notifyLakeTableOffset(
                    new NotifyLakeTableOffsetData(
                            1,
                            Collections.singletonMap(
                                    TABLE_BUCKET, new LakeBucketOffset(10L, null, 2L, null))),
                    notifyFuture::complete);
            notifyFuture.get(10, TimeUnit.SECONDS);
            assertThat(kvTablet.getHistoricalCleanupOffset()).isEqualTo(2L);
        }
    }

    @Test
    void testUpdateAndDeleteFromLakeFallback() throws Exception {
        TableInfo tableInfo = registerHistoricalTableAndBecomeLeader();
        Replica replica = replicaManager.getReplicaOrException(TABLE_BUCKET);
        KvTablet kvTablet = replica.getKvTablet();
        assertThat(kvTablet).isNotNull();

        TestingHistoricalLakeLookupManager lakeLookupManager =
                new TestingHistoricalLakeLookupManager(lookupConfiguration());

        RowType rowType = tableInfo.getRowType();
        String updatePartition = "20240109";
        String deletePartition = "20240110";
        byte[] updateKey = key(1, "us");
        byte[] deleteKey = key(2, "eu");
        short schemaId = (short) tableInfo.getSchemaId();
        // Seed lake-only values so the first local operations must use lake fallback.
        lakeLookupManager.putLakeValue(
                updatePartition,
                ValueEncoder.encodeValue(
                        schemaId,
                        compactedRow(rowType, new Object[] {1, "us", updatePartition, "lake-v1"})));
        lakeLookupManager.putLakeValue(
                deletePartition,
                ValueEncoder.encodeValue(
                        schemaId,
                        compactedRow(rowType, new Object[] {2, "eu", deletePartition, "lake-v1"})));

        try (HistoricalPartitionManager historicalPartitionManager =
                createHistoricalPartitionManager(lakeLookupManager)) {
            // Same-key records must see this batch's buffered values and deletes, even though
            // the saved lake result still contains lake-v1. The repeated delete is a no-op.
            KvRecordBatch updateBatch =
                    batch(
                            rowType,
                            upsert(1, "us", updatePartition, "lake-v2"),
                            delete(1, "us"),
                            delete(1, "us"),
                            upsert(1, "us", updatePartition, "recreated-v3"),
                            upsert(1, "us", updatePartition, "updated-v4"));
            assertThat(
                            writeBatch(
                                            historicalPartitionManager,
                                            replica,
                                            updatePartition,
                                            updateBatch)
                                    .numMessages())
                    .isEqualTo(6);

            // Deleting a lake-only value emits a delete and leaves a local tombstone.
            KvRecordBatch deleteBatch = batch(rowType, delete(2, "eu"));
            assertThat(
                            writeBatch(
                                            historicalPartitionManager,
                                            replica,
                                            deletePartition,
                                            deleteBatch)
                                    .numMessages())
                    .isOne();

            // Verify both lake fallbacks are materialized into the shared historical tablet.
            flushAndWait(kvTablet, Long.MAX_VALUE);
            assertHistoricalValue(
                    kvTablet,
                    updatePartition,
                    updateKey,
                    tableInfo,
                    row(1, "us", updatePartition, "updated-v4"));
            assertThat(kvTablet.lookupHistoricalLocal(deletePartition, deleteKey))
                    .isEqualTo(KvStateLookupResult.deleted());
            assertThat(lakeLookupManager.lookupCount).hasValue(2);

            assertLogRecordsEqualsWithRowKind(
                    tableInfo.getSchemaId(),
                    rowType,
                    fetchLog(0L),
                    Arrays.asList(
                            Tuple2.of(
                                    ChangeType.UPDATE_BEFORE,
                                    new Object[] {1, "us", updatePartition, "lake-v1"}),
                            Tuple2.of(
                                    ChangeType.UPDATE_AFTER,
                                    new Object[] {1, "us", updatePartition, "lake-v2"}),
                            Tuple2.of(
                                    ChangeType.DELETE,
                                    new Object[] {1, "us", updatePartition, "lake-v2"}),
                            Tuple2.of(
                                    ChangeType.INSERT,
                                    new Object[] {1, "us", updatePartition, "recreated-v3"}),
                            Tuple2.of(
                                    ChangeType.UPDATE_BEFORE,
                                    new Object[] {1, "us", updatePartition, "recreated-v3"}),
                            Tuple2.of(
                                    ChangeType.UPDATE_AFTER,
                                    new Object[] {1, "us", updatePartition, "updated-v4"}),
                            Tuple2.of(
                                    ChangeType.DELETE,
                                    new Object[] {2, "eu", deletePartition, "lake-v1"})),
                    schemaGetter(tableInfo));
        }
    }

    @Test
    void testLeaderChangeDuringLakeLookupFencesHistoricalWrite() throws Exception {
        TableInfo tableInfo = registerHistoricalTableAndBecomeLeader();
        Replica replica = replicaManager.getReplicaOrException(TABLE_BUCKET);
        TestingHistoricalLakeLookupManager lakeLookupManager =
                new TestingHistoricalLakeLookupManager(lookupConfiguration());
        HistoricalPartitionManager historicalPartitionManager =
                createHistoricalPartitionManager(lakeLookupManager);
        CountDownLatch lakeLookupStarted = new CountDownLatch(1);
        CountDownLatch finishLakeLookup = new CountDownLatch(1);
        lakeLookupManager.setLookupHook(
                () -> {
                    lakeLookupStarted.countDown();
                    await(finishLakeLookup);
                });

        KvRecordBatch insertBatch =
                batch(tableInfo.getRowType(), upsert(1, "us", ORIGINAL_PARTITION, "v1"));
        long logEndOffsetBeforeWrite = replica.getLocalLogEndOffset();

        try {
            CompletableFuture<PutKvResultForBucket> writeFuture =
                    historicalPartitionManager.putKv(
                            replica,
                            new PutKvDataForBucket(TABLE_BUCKET, insertBatch, ORIGINAL_PARTITION),
                            null,
                            MergeMode.DEFAULT,
                            1);
            assertThat(lakeLookupStarted.await(10, TimeUnit.SECONDS)).isTrue();

            // Both replica locks must be available while the lake lookup is blocked. Move the
            // replica away and back so the retry observes a different leader epoch.
            assertThat(replica.makeFollower(followerState())).isTrue();
            replica.makeLeader(leaderStateAfterFollower());
            finishLakeLookup.countDown();

            PutKvResultForBucket result = writeFuture.get(10, TimeUnit.SECONDS);
            assertThat(result.failed()).isTrue();
            assertThat(result.getError().error()).isEqualTo(Errors.FENCED_LEADER_EPOCH_EXCEPTION);
            assertThat(replica.getLocalLogEndOffset()).isEqualTo(logEndOffsetBeforeWrite);
        } finally {
            finishLakeLookup.countDown();
            historicalPartitionManager.close();
        }
    }

    @Test
    void testRecoversHistoricalOverlayFromLakeCommitOffset() throws Exception {
        TableInfo tableInfo = registerHistoricalTableAndBecomeLeader();
        Replica replica = replicaManager.getReplicaOrException(TABLE_BUCKET);
        TestingHistoricalLakeLookupManager lakeLookupManager =
                new TestingHistoricalLakeLookupManager(lookupConfiguration());

        byte[] tieredPrimaryKey = key(1, "us");
        byte[] deletedPrimaryKey = key(2, "eu");

        try (HistoricalPartitionManager historicalPartitionManager =
                createHistoricalPartitionManager(lakeLookupManager)) {
            LogAppendInfo firstAppend =
                    writeBatch(
                            historicalPartitionManager,
                            replica,
                            ORIGINAL_PARTITION,
                            batch(
                                    tableInfo.getRowType(),
                                    upsert(1, "us", ORIGINAL_PARTITION, "v1")));
            LogAppendInfo anotherPartitionAppend =
                    writeBatch(
                            historicalPartitionManager,
                            replica,
                            ANOTHER_ORIGINAL_PARTITION,
                            batch(
                                    tableInfo.getRowType(),
                                    upsert(1, "us", ANOTHER_ORIGINAL_PARTITION, "another")));
            writeBatch(
                    historicalPartitionManager,
                    replica,
                    ORIGINAL_PARTITION,
                    batch(
                            tableInfo.getRowType(),
                            upsert(2, "eu", ORIGINAL_PARTITION, "delete-me")));
            LogAppendInfo deleteAppend =
                    writeBatch(
                            historicalPartitionManager,
                            replica,
                            ORIGINAL_PARTITION,
                            batch(tableInfo.getRowType(), delete(2, "eu")));
            KvTablet kvTabletBeforeFollower = replica.getKvTablet();
            assertThat(kvTabletBeforeFollower).isNotNull();
            flushAndWait(kvTabletBeforeFollower, Long.MAX_VALUE);
            // The flush listener advances the high watermark after releasing the KV lock.
            retry(
                    Duration.ofSeconds(10),
                    () ->
                            assertThat(replica.getLogHighWatermark())
                                    .isEqualTo(replica.getLocalLogEndOffset()));

            // Persist the exclusive end offset of the first write as the lake recovery point. The
            // replica has not received this offset locally, so becoming leader must load it before
            // creating the historical overlay.
            long lakeCommitOffset = firstAppend.lastOffset() + 1;
            new LakeTableHelper(zkClient, DEFAULT_REMOTE_DATA_DIR)
                    .registerLakeTableSnapshotV1(
                            TABLE_ID,
                            new LakeTableSnapshot(
                                    1L, Collections.singletonMap(TABLE_BUCKET, lakeCommitOffset)));
            assertThat(replica.getLakeLogEndOffset()).isEqualTo(-1L);

            // Dropping and recreating the leader KV tablet forces the overlay to be rebuilt only
            // from WAL after the lake commit offset. The recovered tombstone must remain
            // authoritative over lake fallback.
            assertThat(replica.makeFollower(followerState())).isTrue();
            CompletableFuture<List<NotifyLeaderAndIsrResultForBucket>> leaderFuture =
                    new CompletableFuture<>();
            replicaManager.becomeLeaderOrFollower(
                    INITIAL_COORDINATOR_EPOCH,
                    Collections.singletonList(leaderStateAfterFollower()),
                    leaderFuture::complete);
            assertThat(leaderFuture.get(10, TimeUnit.SECONDS))
                    .containsOnly(new NotifyLeaderAndIsrResultForBucket(TABLE_BUCKET));

            KvTablet recoveredKvTablet = replica.getKvTablet();
            assertThat(recoveredKvTablet).isNotNull();
            assertThat(replica.getLakeLogEndOffset()).isEqualTo(lakeCommitOffset);
            assertThat(recoveredKvTablet.getHistoricalCleanupOffset()).isEqualTo(lakeCommitOffset);
            // Recovery retries can reuse the same tablet and restore offset.
            assertThat(recoveredKvTablet.advanceHistoricalCleanupOffset(lakeCommitOffset))
                    .isFalse();
            assertThat(replica.getKvSnapshotManager()).isNull();
            assertThat(recoveredKvTablet.getFlushedLogOffset())
                    .isEqualTo(replica.getLogHighWatermark());
            assertThat(recoveredKvTablet.getRocksDBKv().limitScan(10)).hasSize(2);
            // The first record is covered by the lake commit offset and is not replayed locally.
            assertThat(
                            recoveredKvTablet.lookupHistoricalLocal(
                                    ORIGINAL_PARTITION, tieredPrimaryKey))
                    .isEqualTo(KvStateLookupResult.notFound());
            assertThat(
                            recoveredKvTablet.lookupHistoricalLocal(
                                    ORIGINAL_PARTITION, deletedPrimaryKey))
                    .isEqualTo(KvStateLookupResult.deleted());
            assertHistoricalValue(
                    recoveredKvTablet,
                    ANOTHER_ORIGINAL_PARTITION,
                    tieredPrimaryKey,
                    tableInfo,
                    row(1, "us", ANOTHER_ORIGINAL_PARTITION, "another"));
            assertHistoricalValueTag(
                    recoveredKvTablet,
                    ANOTHER_ORIGINAL_PARTITION,
                    tieredPrimaryKey,
                    anotherPartitionAppend.lastOffset());
            assertHistoricalValueTag(
                    recoveredKvTablet,
                    ORIGINAL_PARTITION,
                    deletedPrimaryKey,
                    deleteAppend.lastOffset());
        }
    }

    @Test
    void testHistoricalLookupThrottledWhenPermitsExhausted() throws Exception {
        registerHistoricalTableAndBecomeLeader();
        Replica replica = replicaManager.getReplicaOrException(TABLE_BUCKET);
        ManuallyTriggeredScheduledExecutorService executor =
                new ManuallyTriggeredScheduledExecutorService();
        HistoricalPartitionManager historicalPartitionManager =
                new HistoricalPartitionManager(
                        new HistoricalPartitionTaskExecutor(lookupConfiguration(), executor),
                        new TestingHistoricalLakeLookupManager(lookupConfiguration()));

        byte[] primaryKey = key(1, "us");
        LookupDataForBucket lookupData =
                new LookupDataForBucket(
                        TABLE_BUCKET, Collections.singletonList(primaryKey), ORIGINAL_PARTITION);

        try {
            // Keep the first lookup queued so it retains the only available request permit.
            CompletableFuture<LookupResultForBucket> first =
                    historicalPartitionManager.lookup(
                            replica, lookupData, (lookupTimeNanos, lookupFileDownloaded) -> {});
            assertThat(first).isNotDone();
            assertThat(executor.numQueuedRunnables()).isOne();
            assertThat(historicalPartitionManager.numInflightRequests()).isOne();

            LookupResultForBucket throttled =
                    historicalPartitionManager
                            .lookup(
                                    replica,
                                    lookupData,
                                    (lookupTimeNanos, lookupFileDownloaded) -> {})
                            .get(10, TimeUnit.SECONDS);
            assertThat(throttled.failed()).isTrue();
            assertThat(throttled.getError().error())
                    .isEqualTo(Errors.HISTORICAL_PARTITION_THROTTLED);
            assertThat(throttled.getError().exception())
                    .isInstanceOf(HistoricalPartitionThrottledException.class);
            assertThat(executor.numQueuedRunnables()).isOne();
            assertThat(historicalPartitionManager.numInflightRequests()).isOne();
        } finally {
            historicalPartitionManager.close();
        }
    }

    private HistoricalPartitionManager createHistoricalPartitionManager(
            TestingHistoricalLakeLookupManager lakeLookupManager) {
        return new HistoricalPartitionManager(
                new HistoricalPartitionTaskExecutor(lookupConfiguration()), lakeLookupManager);
    }

    private static LogAppendInfo writeBatch(
            HistoricalPartitionManager manager,
            Replica replica,
            String originalPartition,
            KvRecordBatch records)
            throws Exception {
        return manager.processPut(
                replica,
                new PutKvDataForBucket(TABLE_BUCKET, records, originalPartition),
                null,
                MergeMode.DEFAULT,
                1);
    }

    private static byte[] key(int id, String region) {
        return new CompactedKeyEncoder(KEY_TYPE).encodeKey(row(id, region));
    }

    private static Tuple2<Object[], Object[]> upsert(
            int id, String region, String partition, String value) {
        return Tuple2.of(new Object[] {id, region}, new Object[] {id, region, partition, value});
    }

    private static Tuple2<Object[], Object[]> delete(int id, String region) {
        return Tuple2.of(new Object[] {id, region}, null);
    }

    private LogRecords fetchLog(long fetchOffset) throws Exception {
        CompletableFuture<Map<TableBucket, FetchLogResultForBucket>> future =
                new CompletableFuture<>();
        replicaManager.fetchLogRecords(
                new FetchParams(-1, Integer.MAX_VALUE),
                Collections.singletonMap(
                        TABLE_BUCKET, new FetchReqInfo(TABLE_ID, fetchOffset, Integer.MAX_VALUE)),
                null,
                future::complete);
        FetchLogResultForBucket result = future.get(10, TimeUnit.SECONDS).get(TABLE_BUCKET);
        assertThat(result.failed()).isFalse();
        return result.records();
    }

    private TableInfo registerHistoricalTableAndBecomeLeader() throws Exception {
        return registerHistoricalTableAndBecomeLeader(ChangelogImage.FULL);
    }

    private TableInfo registerHistoricalTableAndBecomeLeader(boolean rowTtlEnabled)
            throws Exception {
        return registerHistoricalTableAndBecomeLeader(ChangelogImage.FULL, rowTtlEnabled);
    }

    private TableInfo registerHistoricalTableAndBecomeLeader(ChangelogImage changelogImage)
            throws Exception {
        return registerHistoricalTableAndBecomeLeader(changelogImage, false);
    }

    private TableInfo registerHistoricalTableAndBecomeLeader(
            ChangelogImage changelogImage, boolean rowTtlEnabled) throws Exception {
        replicaManager.getDiskUsageMonitor().update(0.10);
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("region", DataTypes.STRING())
                        .column("dt", DataTypes.STRING())
                        .column("value", DataTypes.STRING())
                        .primaryKey("id", "region", "dt")
                        .build();
        TableDescriptor.Builder descriptorBuilder =
                TableDescriptor.builder()
                        .schema(schema)
                        .distributedBy(1, "id")
                        .partitionedBy("dt")
                        .property(ConfigOptions.TABLE_AUTO_PARTITION_ENABLED, true)
                        .property(ConfigOptions.TABLE_AUTO_PARTITION_KEY, "dt")
                        .property(
                                ConfigOptions.TABLE_AUTO_PARTITION_TIME_UNIT,
                                AutoPartitionTimeUnit.DAY)
                        .property(ConfigOptions.TABLE_AUTO_PARTITION_NUM_RETENTION, 2)
                        .property(ConfigOptions.TABLE_AUTO_PARTITION_TIMEZONE, "UTC")
                        .property(ConfigOptions.TABLE_DATALAKE_ENABLED, true)
                        .property(ConfigOptions.TABLE_DATALAKE_FORMAT, DataLakeFormat.PAIMON)
                        .property(ConfigOptions.TABLE_DATALAKE_HISTORICAL_PARTITION_ENABLED, true)
                        .property(ConfigOptions.TABLE_CHANGELOG_IMAGE, changelogImage)
                        .property(
                                ConfigOptions.TABLE_KV_FORMAT_VERSION,
                                ConfigOptions.KV_FORMAT_VERSION_2);
        if (rowTtlEnabled) {
            descriptorBuilder
                    .property(ConfigOptions.TABLE_KV_TTL, Duration.ofDays(1))
                    .property(
                            ConfigOptions.TABLE_KV_VALUE_LAYOUT_VERSION,
                            KvValueLayout.TAGGED.version());
        }
        TableDescriptor descriptor = descriptorBuilder.build();
        TableInfo tableInfo =
                TableInfo.of(TABLE_PATH, TABLE_ID, 1, descriptor, DEFAULT_REMOTE_DATA_DIR, 1L, 1L);
        zkClient.registerTable(
                TABLE_PATH,
                TableRegistration.newTable(TABLE_ID, DEFAULT_REMOTE_DATA_DIR, descriptor));
        zkClient.registerFirstSchema(TABLE_PATH, schema);

        BucketMetadata bucketMetadata =
                new BucketMetadata(
                        TABLE_BUCKET.getBucket(),
                        TABLET_SERVER_ID,
                        INITIAL_LEADER_EPOCH,
                        Collections.singletonList(TABLET_SERVER_ID));
        ServerInfo tabletServer =
                new ServerInfo(
                        TABLET_SERVER_ID,
                        "rack1",
                        Endpoint.fromListenersString("CLIENT://localhost:90"),
                        ServerType.TABLET_SERVER);
        serverMetadataCache.updateClusterMetadata(
                new ClusterMetadata(
                        null,
                        Collections.singleton(tabletServer),
                        Collections.singletonList(
                                new TableMetadata(tableInfo, Collections.emptyList())),
                        Collections.singletonList(
                                new PartitionMetadata(
                                        TABLE_ID,
                                        HISTORICAL_PARTITION,
                                        PARTITION_ID,
                                        Collections.singletonList(bucketMetadata)))));

        CompletableFuture<List<NotifyLeaderAndIsrResultForBucket>> leaderFuture =
                new CompletableFuture<>();
        replicaManager.becomeLeaderOrFollower(
                INITIAL_COORDINATOR_EPOCH,
                Collections.singletonList(
                        new NotifyLeaderAndIsrData(
                                PhysicalTablePath.of(TABLE_PATH, HISTORICAL_PARTITION),
                                TABLE_BUCKET,
                                Collections.singletonList(TABLET_SERVER_ID),
                                new LeaderAndIsr(
                                        TABLET_SERVER_ID,
                                        INITIAL_LEADER_EPOCH,
                                        Collections.singletonList(TABLET_SERVER_ID),
                                        Collections.emptyList(),
                                        INITIAL_COORDINATOR_EPOCH,
                                        INITIAL_BUCKET_EPOCH),
                                3,
                                0L)),
                leaderFuture::complete);
        assertThat(leaderFuture.get(10, TimeUnit.SECONDS))
                .containsOnly(new NotifyLeaderAndIsrResultForBucket(TABLE_BUCKET));
        return tableInfo;
    }

    private static NotifyLeaderAndIsrData followerState() {
        int newLeaderId = TABLET_SERVER_ID + 1;
        List<Integer> replicas = Arrays.asList(TABLET_SERVER_ID, newLeaderId);
        return new NotifyLeaderAndIsrData(
                PhysicalTablePath.of(TABLE_PATH, HISTORICAL_PARTITION),
                TABLE_BUCKET,
                replicas,
                new LeaderAndIsr(
                        newLeaderId,
                        INITIAL_LEADER_EPOCH + 1,
                        replicas,
                        Collections.emptyList(),
                        INITIAL_COORDINATOR_EPOCH,
                        INITIAL_BUCKET_EPOCH + 1),
                3,
                0L);
    }

    private static NotifyLeaderAndIsrData leaderStateAfterFollower() {
        List<Integer> replicas = Collections.singletonList(TABLET_SERVER_ID);
        return new NotifyLeaderAndIsrData(
                PhysicalTablePath.of(TABLE_PATH, HISTORICAL_PARTITION),
                TABLE_BUCKET,
                replicas,
                new LeaderAndIsr(
                        TABLET_SERVER_ID,
                        INITIAL_LEADER_EPOCH + 2,
                        replicas,
                        Collections.emptyList(),
                        INITIAL_COORDINATOR_EPOCH,
                        INITIAL_BUCKET_EPOCH + 2),
                3,
                0L);
    }

    private static void await(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    @SafeVarargs
    private static KvRecordBatch batch(RowType rowType, Tuple2<Object[], Object[]>... keyAndValues)
            throws Exception {
        List<Tuple2<Object[], Object[]>> records = Arrays.asList(keyAndValues);
        return genKvRecordBatch(KEY_TYPE, rowType, records);
    }

    private static void assertHistoricalValue(
            KvTablet kvTablet,
            String originalPartition,
            byte[] primaryKey,
            TableInfo tableInfo,
            InternalRow expectedRow)
            throws Exception {
        KvStateLookupResult result = kvTablet.lookupHistoricalLocal(originalPartition, primaryKey);
        assertThat(result.isPresent()).isTrue();
        BinaryValue value =
                new ValueDecoder(
                                schemaGetter(tableInfo),
                                tableInfo.getTableConfig().getKvFormat(),
                                KvValueLayout.TAGGED)
                        .decodeValue(result.value());
        assertThatRow(value.row).withSchema(tableInfo.getRowType()).isEqualTo(expectedRow);
    }

    private static void assertHistoricalValueTag(
            KvTablet kvTablet, String originalPartition, byte[] primaryKey, long expectedLogOffset)
            throws IOException {
        byte[] rawValue = historicalRawValue(kvTablet, originalPartition, primaryKey);
        assertThat(rawValue).isNotNull();
        assertThat(KvValueLayout.TAGGED.readValueTag(MemorySegment.wrap(rawValue)))
                .isEqualTo(expectedLogOffset);
    }

    private static byte[] historicalRawValue(
            KvTablet kvTablet, String originalPartition, byte[] primaryKey) throws IOException {
        return kvTablet.getRocksDBKv()
                .get(HistoricalKvKeyEncoder.encode(originalPartition, primaryKey));
    }

    private static void compactHistoricalKv(KvTablet kvTablet) throws Exception {
        try (FlushOptions flushOptions = new FlushOptions().setWaitForFlush(true)) {
            kvTablet.getRocksDBKv().getDb().flush(flushOptions);
            kvTablet.getRocksDBKv().getDb().compactRange();
        }
    }

    private static void deleteHistoricalRocksDbKey(KvTablet kvTablet, byte[] primaryKey) {
        try {
            kvTablet.getRocksDBKv()
                    .delete(HistoricalKvKeyEncoder.encode(ORIGINAL_PARTITION, primaryKey));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static SchemaGetter schemaGetter(TableInfo tableInfo) {
        return new TestingSchemaGetter(
                new SchemaInfo(tableInfo.getSchema(), tableInfo.getSchemaId()));
    }

    private final class TestingHistoricalLakeLookupManager extends HistoricalLakeLookupManager {
        private final AtomicInteger lookupCount = new AtomicInteger();
        private final AtomicInteger lookupBatchCount = new AtomicInteger();
        private final AtomicInteger requiredSnapshotCount = new AtomicInteger();
        private final Map<String, byte[]> lakeValuesByPartition = new HashMap<>();
        private volatile @Nullable Runnable lookupHook;
        private volatile @Nullable Runnable requireSnapshotHook;

        private TestingHistoricalLakeLookupManager(Configuration configuration) {
            super(
                    configuration,
                    null,
                    new java.io.File(tempDir, "historical-lookup"),
                    1L,
                    Ticker.systemTicker(),
                    Scheduler.disabledScheduler(),
                    () -> {});
        }

        private void putLakeValue(String partitionName, byte[] value) {
            lakeValuesByPartition.put(partitionName, value);
        }

        private void setLookupHook(Runnable lookupHook) {
            this.lookupHook = lookupHook;
        }

        private void setRequireSnapshotHook(Runnable requireSnapshotHook) {
            this.requireSnapshotHook = requireSnapshotHook;
        }

        @Override
        void requireLakeSnapshot(long tableId, long snapshotId) {
            super.requireLakeSnapshot(tableId, snapshotId);
            requiredSnapshotCount.incrementAndGet();
            Runnable hook = requireSnapshotHook;
            if (hook != null) {
                hook.run();
            }
        }

        @Override
        List<byte[]> lookup(
                LookupDataForBucket lookupData,
                TableInfo tableInfo,
                SchemaInfo schemaInfo,
                ResolvedPartitionSpec originalPartitionSpec,
                LakeTableLookuper.LookupMetricRecorder lookupMetricRecorder) {
            lookupBatchCount.incrementAndGet();
            lookupCount.addAndGet(lookupData.keys().size());
            Runnable hook = lookupHook;
            if (hook != null) {
                hook.run();
            }
            List<byte[]> values = new ArrayList<>(lookupData.keys().size());
            for (int i = 0; i < lookupData.keys().size(); i++) {
                values.add(lakeValuesByPartition.get(originalPartitionSpec.getPartitionName()));
            }
            return values;
        }
    }

    private Configuration lookupConfiguration() {
        Configuration configuration = new Configuration();
        configuration.set(ConfigOptions.NETTY_SERVER_MAX_QUEUED_HISTORICAL_REQUESTS, 1);
        return configuration;
    }
}
