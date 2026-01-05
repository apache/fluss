/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.lake.paimon.utils;

import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.FlussRuntimeException;
import org.apache.fluss.flink.tiering.committer.FlussTableLakeSnapshotCommitter;
import org.apache.fluss.lake.committer.LakeCommitResult;
import org.apache.fluss.metadata.PartitionInfo;
import org.apache.fluss.metadata.PartitionSpec;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.types.DataTypes;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.FileStoreTable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

import javax.annotation.Nullable;

import java.io.File;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.fluss.lake.paimon.utils.PaimonConversions.toPaimon;
import static org.apache.fluss.lake.paimon.utils.PaimonTestUtils.CompactCommitter;
import static org.apache.fluss.lake.paimon.utils.PaimonTestUtils.CompactHelper;
import static org.apache.fluss.lake.paimon.utils.PaimonTestUtils.writeAndCommitData;
import static org.apache.fluss.server.utils.LakeStorageUtils.extractLakeProperties;
import static org.assertj.core.api.Assertions.assertThat;

/** Unit test for {@link DvTableReadableSnapshotRetriever} using real Paimon tables. */
class DvTableReadableSnapshotRetrieverTest {

    @RegisterExtension
    public static final FlussClusterExtension FLUSS_CLUSTER_EXTENSION =
            FlussClusterExtension.builder()
                    .setNumOfTabletServers(3)
                    .setClusterConf(initConfig())
                    .build();

    private static Configuration initConfig() {
        Configuration conf = new Configuration();
        conf.setString("datalake.format", "paimon");
        conf.setString("datalake.paimon.metastore", "filesystem");
        String warehousePath;
        try {
            warehousePath =
                    Files.createTempDirectory("fluss-testing-readable-snapshot")
                            .resolve("warehouse")
                            .toString();
        } catch (Exception e) {
            throw new FlussRuntimeException("Failed to create warehouse path");
        }
        conf.setString("datalake.paimon.warehouse", warehousePath);
        paimonCatalog =
                CatalogFactory.createCatalog(
                        CatalogContext.create(Options.fromMap(extractLakeProperties(conf))));

        return conf;
    }

    private static final String DEFAULT_DB = "fluss";

    @TempDir private File compactionTempDir;
    private static Catalog paimonCatalog;
    private Connection flussConnection;
    private Admin flussAdmin;
    private long tableId;
    private Configuration flussConf;

    private FlussTableLakeSnapshotCommitter lakeSnapshotCommitter;

    @BeforeEach
    void setUp() {
        flussConnection =
                ConnectionFactory.createConnection(FLUSS_CLUSTER_EXTENSION.getClientConfig());
        flussAdmin = flussConnection.getAdmin();
        flussConf = FLUSS_CLUSTER_EXTENSION.getClientConfig();
        lakeSnapshotCommitter = new FlussTableLakeSnapshotCommitter(flussConf);
        lakeSnapshotCommitter.open();
    }

    @AfterEach
    void tearDown() throws Exception {
        if (flussAdmin != null) {
            flussAdmin.close();
        }
        if (flussConnection != null) {
            flussConnection.close();
        }
        if (paimonCatalog != null) {
            paimonCatalog.close();
        }
    }

    @Test
    void testGetReadableSnapshotAndOffsets() throws Exception {
        int bucket0 = 0;
        int bucket1 = 1;
        int bucket2 = 2;

        // Given: create a DV table with 3 buckets: b1, b2, b3
        TablePath tablePath = TablePath.of(DEFAULT_DB, "test_dv_flow");
        tableId = createDvTable(tablePath, 3);
        FileStoreTable fileStoreTable = getPaimonTable(tablePath);

        CompactHelper compactHelper = new CompactHelper(fileStoreTable, compactionTempDir);
        TableBucket tb0 = new TableBucket(tableId, bucket0);
        TableBucket tb1 = new TableBucket(tableId, bucket1);
        TableBucket tb2 = new TableBucket(tableId, bucket2);

        // Step 1: APPEND snapshot 1 - write data to all buckets
        // Snapshot 1 state:
        //   ┌─────────┬─────────────────────────────┐
        //   │ Bucket  │ Files                       │
        //   ├─────────┼─────────────────────────────┤
        //   │ bucket0 │ L0: [rows 0-2]              │
        //   │ bucket1 │ L0: [rows 0-2]              │
        //   │ bucket2 │ L0: [rows 0-2]              │
        //   └─────────┴─────────────────────────────┘
        Map<Integer, List<GenericRow>> appendRows = new HashMap<>();
        appendRows.put(bucket0, generateRows(bucket0, 0, 3));
        appendRows.put(bucket1, generateRows(bucket1, 0, 3));
        appendRows.put(bucket2, generateRows(bucket2, 0, 3));
        long snapshot1 = writeAndCommitData(fileStoreTable, appendRows);
        Map<TableBucket, Long> tieredLakeSnapshotEndOffset = new HashMap<>();
        tieredLakeSnapshotEndOffset.put(tb0, 3L);
        tieredLakeSnapshotEndOffset.put(tb1, 3L);
        tieredLakeSnapshotEndOffset.put(tb2, 3L);

        // No readable_snapshot yet (all buckets have L0 files)
        DvTableReadableSnapshotRetriever.ReadableSnapshotResult readableSnapshotAndOffsets =
                retriveReadableSnapshotAndOffsets(tablePath, fileStoreTable, snapshot1);
        assertThat(readableSnapshotAndOffsets).isNull();
        // commit tiered snapshot and readable snapshot to fluss
        // (simulate TieringCommitOperator) behavior
        commitSnapshot(
                tableId,
                tablePath,
                snapshot1,
                tieredLakeSnapshotEndOffset,
                readableSnapshotAndOffsets);

        // Step 2: COMPACT snapshot 2, 3 - compact bucket1 and bucket2
        // Prepare compactions for all buckets (b0, b1, b2), but only commit b1 and b2
        CompactCommitter compactCommitter2B0 = compactHelper.compactBucket(bucket0); // compact b0
        CompactCommitter compactCommitter2B1 = compactHelper.compactBucket(bucket1); // compact b1
        CompactCommitter compactCommitter2B2 = compactHelper.compactBucket(bucket2); // compact b2

        // Commit compact b1 -> snapshot 2
        // Snapshot 2 state (after compacting bucket1):
        //   ┌─────────┬─────────────────────────────┐
        //   │ Bucket  │ Files                       │
        //   ├─────────┼─────────────────────────────┤
        //   │ bucket0 │ L0: [rows 0-2]              │ ← unchanged
        //   │ bucket1 │ L1: [compacted from s1 L0]  │ ← L0 flushed to L1 (rows 0-2)
        //   │ bucket2 │ L0: [rows 0-2]              │ ← unchanged
        //   └─────────┴─────────────────────────────┘
        compactCommitter2B1.commit();
        // Commit compact b2 -> snapshot 3
        // Snapshot 3 state (after compacting bucket2):
        //   ┌─────────┬─────────────────────────────┐
        //   │ Bucket  │ Files                       │
        //   ├─────────┼─────────────────────────────┤
        //   │ bucket0 │ L0: [rows 0-2]              │ ← unchanged
        //   │ bucket1 │ L1: [from s2 (rows 0-2)]    │ ← unchanged
        //   │ bucket2 │ L1: [compacted from s1 L0]  │ ← L0 flushed to L1 (rows 0-2)
        //   └─────────┴─────────────────────────────┘
        compactCommitter2B2.commit();

        // Step 3: APPEND snapshot 4 - write more data to bucket0
        // Snapshot 4 state:
        //   ┌─────────┬─────────────────────────────┐
        //   │ Bucket  │ Files                       │
        //   ├─────────┼─────────────────────────────┤
        //   │ bucket0 │ L0: [rows 0-2 from s1]      │ ← from snapshot1
        //   │         │ L0: [rows 3-7] ← new        │ ← added in snapshot4
        //   │ bucket1 │ L1: [from s2 (rows 0-2)]    │ ← unchanged
        //   │ bucket2 │ L1: [from s3 (rows 0-2)]    │ ← unchanged
        //   └─────────┴─────────────────────────────┘
        long snapshot4 =
                writeAndCommitData(
                        fileStoreTable,
                        Collections.singletonMap(bucket0, generateRows(bucket0, 3, 8)));
        tieredLakeSnapshotEndOffset.put(tb0, 8L);
        // retrive readable snapshot and offsets
        readableSnapshotAndOffsets =
                retriveReadableSnapshotAndOffsets(tablePath, fileStoreTable, snapshot4);
        // (simulate TieringCommitOperator) behavior
        commitSnapshot(
                tableId,
                tablePath,
                snapshot4,
                tieredLakeSnapshotEndOffset,
                readableSnapshotAndOffsets);

        // Should still be null since bucket0 still has L0 files,
        // and no L0 files were flushed until snapshot3 (which only flushed b1 and b2)
        // we don't just use 0 as the log end offset for safety,
        // todo: maybe consider use 0 as the log end offset for bucket0 no any L0 files
        // flushed from the first tiered snapshot to latest compacted snapshot
        assertThat(readableSnapshotAndOffsets).isNull();

        // Step 4: COMPACT snapshot 5 - commit b0 compact (prepared based on snapshot1)
        // Snapshot 5 state (after compacting bucket0):
        //   ┌─────────┬─────────────────────────────┐
        //   │ Bucket  │ Files                       │
        //   ├─────────┼─────────────────────────────┤
        //   │ bucket0 │ L1: [compacted from s1 L0]  │ ← s1's L0 (rows 0-2) flushed to L1
        //   │         │ L0: [rows 3-7 from s4]      │ ← s4's L0 still remains
        //   │ bucket1 │ L1: [from s2 (rows 0-2)]    │ ← unchanged
        //   │ bucket2 │ L1: [from s3 (rows 0-2)]    │ ← unchanged
        //   └─────────┴─────────────────────────────┘
        compactCommitter2B0.commit();
        long snapshot5 = latestSnapshot(fileStoreTable);

        // Step 5: APPEND snapshot 6 - write more data to bucket1
        // Snapshot 6 state:
        //   ┌─────────┬─────────────────────────────┐
        //   │ Bucket  │ Files                       │
        //   ├─────────┼─────────────────────────────┤
        //   │ bucket0 │ L1: [from s5 (rows 0-2)]    │
        //   │         │ L0: [rows 3-7 from s4]      │ ← unchanged
        //   │ bucket1 │ L1: [from s2 (rows 0-2)]    │
        //   │         │ L0: [rows 3-9] ← new        │ ← added in snapshot6
        //   │ bucket2 │ L1: [from s3 (rows 0-2)]    │ ← unchanged
        //   └─────────┴─────────────────────────────┘
        // Now readable_snapshot can advance: bucket0 and bucket1 have L0, but bucket2 has no L0
        // However, bucket0 and bucket1's L0 files were flushed in snapshot5 and snapshot2
        // respectively, so we can use their base snapshots' offsets
        long snapshot6 =
                writeAndCommitData(
                        fileStoreTable,
                        Collections.singletonMap(bucket1, generateRows(bucket1, 3, 10)));
        tieredLakeSnapshotEndOffset.put(tb1, 10L);
        readableSnapshotAndOffsets =
                retriveReadableSnapshotAndOffsets(tablePath, fileStoreTable, snapshot6);
        // readable_snapshot = snapshot5 (latest compacted snapshot)
        // readable_offsets: all buckets use snapshot1's offsets (base snapshot for flushed L0)
        assertThat(readableSnapshotAndOffsets.getReadableSnapshotId()).isEqualTo(snapshot5);
        Map<TableBucket, Long> expectedReadableOffsets = new HashMap<>();
        expectedReadableOffsets.put(tb0, 3L);
        expectedReadableOffsets.put(tb1, 3L);
        expectedReadableOffsets.put(tb2, 3L);
        assertThat(readableSnapshotAndOffsets.getReadableOffsets())
                .isEqualTo(expectedReadableOffsets);
        commitSnapshot(
                tableId,
                tablePath,
                snapshot6,
                tieredLakeSnapshotEndOffset,
                readableSnapshotAndOffsets);

        // Step 6: COMPACT snapshot 7 - compact bucket0 again (flushes snapshot4's L0)
        // Snapshot 7 state (after compacting bucket0):
        //   ┌─────────┬─────────────────────────────┐
        //   │ Bucket  │ Files                       │
        //   ├─────────┼─────────────────────────────┤
        //   │ bucket0 │ L1: [merged s5 L1 + s4 L0]  │ ← s4's L0 (rows 3-7) flushed to L1
        //   │         │     [rows 0-7 total]        │
        //   │ bucket1 │ L1: [from s2 (rows 0-2)]    │
        //   │         │ L0: [rows 3-9 from s6]      │ ← unchanged
        //   │ bucket2 │ L1: [from s3 (rows 0-2)]    │ ← unchanged
        //   └─────────┴─────────────────────────────┘
        // readable_snapshot advances: bucket0's L0 flushed, use snapshot4's offset (8L)
        compactHelper.compactBucket(bucket0).commit();
        long snapshot7 = latestSnapshot(fileStoreTable);

        // Create an empty tiered snapshot (snapshot8) to simulate tiered snapshot commit
        long snapshot8 = writeAndCommitData(fileStoreTable, Collections.emptyMap());

        // retrieve the readable snapshot and offsets
        readableSnapshotAndOffsets =
                retriveReadableSnapshotAndOffsets(tablePath, fileStoreTable, snapshot8);
        // readable_snapshot = snapshot7
        // readable_offsets: bucket0 uses snapshot4's offset (8L), others use snapshot1's offset
        assertThat(readableSnapshotAndOffsets.getReadableSnapshotId()).isEqualTo(snapshot7);
        expectedReadableOffsets = new HashMap<>();
        expectedReadableOffsets.put(tb0, 8L);
        expectedReadableOffsets.put(tb1, 3L);
        expectedReadableOffsets.put(tb2, 3L);
        assertThat(readableSnapshotAndOffsets.getReadableOffsets())
                .isEqualTo(expectedReadableOffsets);
        commitSnapshot(
                tableId, tablePath, snapshot8, Collections.emptyMap(), readableSnapshotAndOffsets);

        // Step 7: COMPACT snapshot 9, 10, 11 - compact all buckets
        // Snapshot 9: compact bucket0 (no new L0 to flush)
        // Snapshot 10: compact bucket1 (flushes snapshot6's L0)
        // Snapshot 11: compact bucket2 (no new L0 to flush)
        // Snapshot 11 state (after compacting all buckets):
        //   ┌─────────┬─────────────────────────────┐
        //   │ Bucket  │ Files                       │
        //   ├─────────┼─────────────────────────────┤
        //   │ bucket0 │ L1: [from s7 (rows 0-7)]    │ ← unchanged
        //   │ bucket1 │ L1: [merged s2 L1 + s6 L0]  │ ← s6's L0 (rows 3-9) flushed to L1
        //   │         │     [rows 0-9 total]        │
        //   │ bucket2 │ L1: [from s3 (rows 0-2)]    │ ← unchanged
        //   └─────────┴─────────────────────────────┘
        compactHelper.compactBucket(bucket0).commit();
        compactHelper.compactBucket(bucket1).commit();
        compactHelper.compactBucket(bucket2).commit();

        long snapshot11 = latestSnapshot(fileStoreTable);

        // Step 8: APPEND snapshot 12 - write data to all buckets
        // Snapshot 12 state:
        //   ┌─────────┬─────────────────────────────┐
        //   │ Bucket  │ Files                       │
        //   ├─────────┼─────────────────────────────┤
        //   │ bucket0 │ L1: [from s11 (rows 0-7)]   │
        //   │         │ L0: [rows 8-12] ← new       │ ← added in snapshot12
        //   │ bucket1 │ L1: [from s11 (rows 0-9)]   │
        //   │         │ L0: [rows 10-19] ← new      │ ← added in snapshot12
        //   │ bucket2 │ L1: [from s11 (rows 0-2)]   │
        //   │         │ L0: [rows 3-10] ← new       │ ← added in snapshot12
        //   └─────────┴─────────────────────────────┘
        // readable_snapshot = snapshot11 (latest compacted snapshot)
        // readable_offsets: bucket0 uses snapshot4's offset (8L), bucket1 uses snapshot6's offset
        // (10L), bucket2 uses snapshot1's offset (3L)
        Map<Integer, List<GenericRow>> appendRows12 = new HashMap<>();
        appendRows12.put(bucket0, generateRows(bucket0, 8, 13));
        appendRows12.put(bucket1, generateRows(bucket1, 10, 20));
        appendRows12.put(bucket2, generateRows(bucket2, 3, 11));
        long snapshot12 = writeAndCommitData(fileStoreTable, appendRows12);
        readableSnapshotAndOffsets =
                retriveReadableSnapshotAndOffsets(tablePath, fileStoreTable, snapshot12);
        assertThat(readableSnapshotAndOffsets.getReadableSnapshotId()).isEqualTo(snapshot11);
        tieredLakeSnapshotEndOffset.put(tb0, 13L);
        tieredLakeSnapshotEndOffset.put(tb1, 20L);
        tieredLakeSnapshotEndOffset.put(tb2, 11L);
        expectedReadableOffsets = new HashMap<>();
        expectedReadableOffsets.put(tb0, 8L);
        expectedReadableOffsets.put(tb1, 10L);
        expectedReadableOffsets.put(tb2, 3L);
        assertThat(readableSnapshotAndOffsets.getReadableOffsets())
                .isEqualTo(expectedReadableOffsets);
        commitSnapshot(
                tableId,
                tablePath,
                snapshot12,
                tieredLakeSnapshotEndOffset,
                readableSnapshotAndOffsets);

        // Step 9: COMPACT snapshot 13 - compact bucket2 (flushes snapshot12's L0)
        // Snapshot 13 state (after compacting bucket2):
        //   ┌─────────┬─────────────────────────────┐
        //   │ Bucket  │ Files                       │
        //   ├─────────┼─────────────────────────────┤
        //   │ bucket0 │ L1: [from s11 (rows 0-7)]   │
        //   │         │ L0: [rows 8-12 from s12]    │ ← unchanged
        //   │ bucket1 │ L1: [from s11 (rows 0-9)]   │
        //   │         │ L0: [rows 10-19 from s12]   │ ← unchanged
        //   │ bucket2 │ L1: [merged s11 L1 + s12 L0]│ ← s12's L0 (rows 3-10) flushed to L1
        //   │         │     [rows 0-10 total]       │
        //   └─────────┴─────────────────────────────┘
        // readable_snapshot advances: bucket2's L0 flushed, use snapshot12's offset (11L)
        compactHelper.compactBucket(bucket2).commit();
        long snapshot13 = latestSnapshot(fileStoreTable);
        // Create an empty tiered snapshot (snapshot14) to simulate tiered snapshot commit
        long snapshot14 = writeAndCommitData(fileStoreTable, Collections.emptyMap());
        readableSnapshotAndOffsets =
                retriveReadableSnapshotAndOffsets(tablePath, fileStoreTable, snapshot14);
        // readable_snapshot = snapshot13
        // readable_offsets: bucket0 uses snapshot4's offset (8L), bucket1 uses snapshot6's offset
        // (10L), bucket2 uses snapshot12's offset (11L)
        assertThat(readableSnapshotAndOffsets.getReadableSnapshotId()).isEqualTo(snapshot13);
        expectedReadableOffsets = new HashMap<>();
        expectedReadableOffsets.put(tb0, 8L);
        expectedReadableOffsets.put(tb1, 10L);
        expectedReadableOffsets.put(tb2, 11L);
        assertThat(readableSnapshotAndOffsets.getReadableOffsets())
                .isEqualTo(expectedReadableOffsets);
        // all buckets L0 level in snapshot6 has been flushed, we can delete all snapshots prior to
        // snapshot6 safely since we won't need to search for any earlier snapshots to get readable
        // offsets
        assertThat(readableSnapshotAndOffsets.getEarliestSnapshotIdToKeep()).isEqualTo(6);
    }

    @Test
    void testGetReadableSnapshotAndOffsetsForPartitionedTable() throws Exception {
        String partition0 = "p0";
        String partition1 = "p1";
        int bucket0 = 0;
        int bucket1 = 1;

        // Given: create a partitioned DV table with 2 buckets and partition column "dt"
        TablePath tablePath = TablePath.of(DEFAULT_DB, "test_dv_flow_partitioned");
        tableId = createPartitionedDvTable(tablePath, 2);
        FileStoreTable fileStoreTable = getPaimonTable(tablePath);

        // Get partition IDs from Fluss
        Map<Long, String> partitionIdToName = createTwoPartitions(tablePath);
        Long partitionId0 = getPartitionIdByName(partitionIdToName, partition0);
        Long partitionId1 = getPartitionIdByName(partitionIdToName, partition1);

        PaimonTestUtils.CompactHelper compactHelper =
                new PaimonTestUtils.CompactHelper(fileStoreTable, compactionTempDir);
        TableBucket tbP0B0 = new TableBucket(tableId, partitionId0, bucket0);
        TableBucket tbP0B1 = new TableBucket(tableId, partitionId0, bucket1);
        TableBucket tbP1B0 = new TableBucket(tableId, partitionId1, bucket0);
        TableBucket tbP1B1 = new TableBucket(tableId, partitionId1, bucket1);

        // Step 1: APPEND snapshot 1 - write data to partition0, bucket0 and bucket1
        // Snapshot 1 state:
        //   ┌─────────────┬─────────┬─────────────────────────────┐
        //   │ Partition   │ Bucket  │ Files                       │
        //   ├─────────────┼─────────┼─────────────────────────────┤
        //   │ partition0  │ bucket0 │ L0: [rows 0-2]              │
        //   │ partition0  │ bucket1 │ L0: [rows 0-2]              │
        //   │ partition1  │ (empty) │                             │
        //   └─────────────┴─────────┴─────────────────────────────┘
        Map<Integer, List<GenericRow>> appendRowsP0 = new HashMap<>();
        appendRowsP0.put(bucket0, generateRowsForPartition(partition0, bucket0, 0, 3));
        appendRowsP0.put(bucket1, generateRowsForPartition(partition0, bucket1, 0, 3));
        long snapshot1 = writeAndCommitData(fileStoreTable, appendRowsP0);
        Map<TableBucket, Long> tieredLakeSnapshotEndOffset = new HashMap<>();
        tieredLakeSnapshotEndOffset.put(tbP0B0, 3L);
        tieredLakeSnapshotEndOffset.put(tbP0B1, 3L);

        // No readable_snapshot yet (all buckets have L0 files)
        DvTableReadableSnapshotRetriever.ReadableSnapshotResult readableSnapshotAndOffsets =
                retriveReadableSnapshotAndOffsets(tablePath, fileStoreTable, snapshot1);
        assertThat(readableSnapshotAndOffsets).isNull();
        commitSnapshot(
                tableId,
                tablePath,
                snapshot1,
                tieredLakeSnapshotEndOffset,
                readableSnapshotAndOffsets);

        // Step 2: COMPACT snapshot 2 - compact partition0, bucket0
        // Snapshot 2 state (after compacting partition0, bucket0):
        //   ┌─────────────┬─────────┬─────────────────────────────┐
        //   │ Partition   │ Bucket  │ Files                       │
        //   ├─────────────┼─────────┼─────────────────────────────┤
        //   │ partition0  │ bucket0 │ L1: [compacted from s1 L0]  │ ← L0 flushed to L1
        //   │             │         │     [rows 0-2]              │
        //   │ partition0  │ bucket1 │ L0: [rows 0-2]              │ ← unchanged
        //   │ partition1  │ (empty) │                             │
        //   └─────────────┴─────────┴─────────────────────────────┘
        BinaryRow partition0BinaryRow = toPartitionBinaryRow(partition0);
        compactHelper.compactBucket(partition0BinaryRow, bucket0).commit();
        long snapshot2 = latestSnapshot(fileStoreTable);

        // Step 3: APPEND snapshot 3 - write data to partition1, bucket0
        // Snapshot 3 state:
        //   ┌─────────────┬─────────┬─────────────────────────────┐
        //   │ Partition   │ Bucket  │ Files                       │
        //   ├─────────────┼─────────┼─────────────────────────────┤
        //   │ partition0  │ bucket0 │ L1: [from s2 (rows 0-2)]    │ ← unchanged
        //   │ partition0  │ bucket1 │ L0: [rows 0-2]              │ ← unchanged
        //   │ partition1  │ bucket0 │ L0: [rows 0-2]              │ ← new partition
        //   └─────────────┴─────────┴─────────────────────────────┘
        Map<Integer, List<GenericRow>> appendRowsP1 =
                Collections.singletonMap(
                        bucket0, generateRowsForPartition(partition1, bucket0, 0, 3));
        long snapshot3 = writeAndCommitData(fileStoreTable, appendRowsP1);
        tieredLakeSnapshotEndOffset.put(tbP1B0, 3L);

        // Should still be null since partition0 bucket1 still has L0 files,
        // and no L0 files for this bucket were flushed until snapshot3
        // todo: maybe consider use 0 as the log end offset for partition0 bucket1 if no any L0
        // files flushed from the first tiered snapshot to latest compacted snapshot
        assertThat(readableSnapshotAndOffsets).isNull();
        readableSnapshotAndOffsets =
                retriveReadableSnapshotAndOffsets(tablePath, fileStoreTable, snapshot3);
        assertThat(readableSnapshotAndOffsets).isNull();
        commitSnapshot(
                tableId,
                tablePath,
                snapshot3,
                tieredLakeSnapshotEndOffset,
                readableSnapshotAndOffsets);

        // Step 4: COMPACT snapshot 5 - compact partition0, bucket1 and partition1, bucket0
        // Snapshot 4 state (after compacting partition0, bucket1 and partition1, bucket0):
        //   ┌─────────────┬─────────┬─────────────────────────────┐
        //   │ Partition   │ Bucket  │ Files                       │
        //   ├─────────────┼─────────┼─────────────────────────────┤
        //   │ partition0  │ bucket0 │ L1: [from s2 (rows 0-2)]    │ ← unchanged
        //   │ partition0  │ bucket1 │ L1: [compacted from s1 L0]  │ ← L0 flushed to L1
        //   │             │         │     [rows 0-2]              │
        //   │ partition1  │ bucket0 │ L1: [compacted from s3 L0]  │ ← L0 flushed to L1
        //   │             │         │     [rows 0-2]              │
        //   └─────────────┴─────────┴─────────────────────────────┘
        compactHelper.compactBucket(partition0BinaryRow, bucket1).commit();
        BinaryRow partition1BinaryRow = toPartitionBinaryRow(partition1);
        compactHelper.compactBucket(partition1BinaryRow, bucket0).commit();
        long snapshot5 = latestSnapshot(fileStoreTable);

        // Step 5: APPEND snapshot 6 - write more data to partition0, bucket0
        // Snapshot 5 state:
        //   ┌─────────────┬─────────┬─────────────────────────────┐
        //   │ Partition   │ Bucket  │ Files                       │
        //   ├─────────────┼─────────┼─────────────────────────────┤
        //   │ partition0  │ bucket0 │ L1: [from s4]               │
        //   │             │         │ L0: [rows 3-5] ← new        │ ← added in snapshot5
        //   │ partition0  │ bucket1 │ L1: [from s4 (rows 0-2)]    │ ← unchanged
        //   │ partition1  │ bucket0 │ L1: [from s4 (rows 0-2)]    │ ← unchanged
        //   └─────────────┴─────────┴─────────────────────────────┘
        Map<Integer, List<GenericRow>> appendRowsP0More =
                Collections.singletonMap(
                        bucket0, generateRowsForPartition(partition0, bucket0, 3, 6));
        long snapshot6 = writeAndCommitData(fileStoreTable, appendRowsP0More);
        tieredLakeSnapshotEndOffset.put(tbP0B0, 6L);
        readableSnapshotAndOffsets =
                retriveReadableSnapshotAndOffsets(tablePath, fileStoreTable, snapshot6);
        // readable_snapshot = snapshot5
        // readable_offsets: partition0/bucket0 uses snapshot1's offset (3L),
        //                   partition0/bucket1 uses snapshot1's offset (3L),
        //                   partition1/bucket0 uses snapshot3's offset (3L)
        assertThat(readableSnapshotAndOffsets.getReadableSnapshotId()).isEqualTo(snapshot5);
        Map<TableBucket, Long> expectedReadableOffsets = new HashMap<>();
        expectedReadableOffsets.put(tbP0B0, 3L);
        expectedReadableOffsets.put(tbP0B1, 3L);
        expectedReadableOffsets.put(tbP1B0, 3L);
        assertThat(readableSnapshotAndOffsets.getReadableOffsets())
                .isEqualTo(expectedReadableOffsets);
        commitSnapshot(
                tableId,
                tablePath,
                snapshot6,
                tieredLakeSnapshotEndOffset,
                readableSnapshotAndOffsets);

        // Step 6: COMPACT snapshot 7 - compact partition0, bucket0 again (flushes snapshot6's L0)
        // Snapshot 7 state (after compacting partition0, bucket0):
        //   ┌─────────────┬─────────┬─────────────────────────────┐
        //   │ Partition   │ Bucket  │ Files                       │
        //   ├─────────────┼─────────┼─────────────────────────────┤
        //   │ partition0  │ bucket0 │ L1: [merged s5 L1 + s6 L0]  │ ← s6's L0 flushed to L1
        //   │             │         │     [rows 0-5 total]        │
        //   │ partition0  │ bucket1 │ L1: [from s5 (rows 0-2)]    │ ← unchanged
        //   │ partition1  │ bucket0 │ L1: [from s5 (rows 0-2)]    │ ← unchanged (already L1 from
        // s5)
        //   └─────────────┴─────────┴─────────────────────────────┘
        compactHelper.compactBucket(partition0BinaryRow, bucket0).commit();
        long snapshot7 = latestSnapshot(fileStoreTable);

        // Create an empty tiered snapshot (snapshot8) to simulate tiered snapshot commit
        long snapshot8 = writeAndCommitData(fileStoreTable, Collections.emptyMap());

        readableSnapshotAndOffsets =
                retriveReadableSnapshotAndOffsets(tablePath, fileStoreTable, snapshot8);
        // readable_snapshot = snapshot7
        // readable_offsets: partition0/bucket0 uses snapshot6's offset (6L),
        //                   partition0/bucket1 uses snapshot1's offset (3L),
        //                   partition1/bucket0 uses snapshot3's offset (3L) - from snapshot5
        // compaction
        assertThat(readableSnapshotAndOffsets.getReadableSnapshotId()).isEqualTo(snapshot7);
        expectedReadableOffsets = new HashMap<>();
        expectedReadableOffsets.put(tbP0B0, 6L);
        expectedReadableOffsets.put(tbP0B1, 3L);
        expectedReadableOffsets.put(tbP1B0, 3L);
        assertThat(readableSnapshotAndOffsets.getReadableOffsets())
                .isEqualTo(expectedReadableOffsets);
        commitSnapshot(
                tableId,
                tablePath,
                snapshot8,
                tieredLakeSnapshotEndOffset,
                readableSnapshotAndOffsets);

        // Step 7: APPEND snapshot 9 - write more data to partition1, bucket0
        // Snapshot 9 state:
        //   ┌─────────────┬─────────┬─────────────────────────────┐
        //   │ Partition   │ Bucket  │ Files                       │
        //   ├─────────────┼─────────┼─────────────────────────────┤
        //   │ partition0  │ bucket0 │ L1: [from s7 (rows 0-5)]    │ ← unchanged
        //   │ partition0  │ bucket1 │ L1: [from s7 (rows 0-2)]    │ ← unchanged
        //   │ partition1  │ bucket0 │ L1: [from s5 (rows 0-2)]    │
        //   │             │         │ L0: [rows 3-5] ← new        │ ← added in snapshot9
        //   └─────────────┴─────────┴─────────────────────────────┘
        Map<Integer, List<GenericRow>> appendRowsP1More =
                Collections.singletonMap(
                        bucket0, generateRowsForPartition(partition1, bucket0, 3, 6));
        long snapshot9 = writeAndCommitData(fileStoreTable, appendRowsP1More);
        tieredLakeSnapshotEndOffset.put(tbP1B0, 6L);
        readableSnapshotAndOffsets =
                retriveReadableSnapshotAndOffsets(tablePath, fileStoreTable, snapshot9);
        // readable_snapshot = snapshot7 (unchanged, partition1/bucket0 still has L0 from snapshot9)
        // readable_offsets: partition0/bucket0 uses snapshot6's offset (6L),
        //                   partition0/bucket1 uses snapshot1's offset (3L),
        //                   partition1/bucket0 uses snapshot3's offset (3L) - from snapshot5
        // compaction
        assertThat(readableSnapshotAndOffsets.getReadableSnapshotId()).isEqualTo(snapshot7);
        expectedReadableOffsets = new HashMap<>();
        expectedReadableOffsets.put(tbP0B0, 6L);
        expectedReadableOffsets.put(tbP0B1, 3L);
        expectedReadableOffsets.put(tbP1B0, 3L);
        assertThat(readableSnapshotAndOffsets.getReadableOffsets())
                .isEqualTo(expectedReadableOffsets);
        // all buckets L0 level has been flushed in snapshot3, we can delete all snapshots prior to
        // snapshot3 safely since we won't need to search for any earlier snapshots to get readable
        // offsets
        assertThat(readableSnapshotAndOffsets.getEarliestSnapshotIdToKeep()).isEqualTo(3);
        commitSnapshot(
                tableId,
                tablePath,
                snapshot9,
                tieredLakeSnapshotEndOffset,
                readableSnapshotAndOffsets);

        // Step 8: COMPACT snapshot 10 - compact partition1, bucket0 again (flushes snapshot9's L0)
        // Snapshot 10 state (after compacting partition1, bucket0):
        //   ┌─────────────┬─────────┬─────────────────────────────┐
        //   │ Partition   │ Bucket  │ Files                       │
        //   ├─────────────┼─────────┼─────────────────────────────┤
        //   │ partition0  │ bucket0 │ L1: [from s7 (rows 0-5)]    │ ← unchanged
        //   │ partition0  │ bucket1 │ L1: [from s7 (rows 0-2)]    │ ← unchanged
        //   │ partition1  │ bucket0 │ L1: [merged s5 L1 + s9 L0]  │ ← s9's L0 flushed to L1
        //   │             │         │     [rows 0-5 total]        │
        //   └─────────────┴─────────┴─────────────────────────────┘
        compactHelper.compactBucket(partition1BinaryRow, bucket0).commit();
        long snapshot10 = latestSnapshot(fileStoreTable);

        // Create an empty tiered snapshot (snapshot11) to simulate tiered snapshot commit
        long snapshot11 = writeAndCommitData(fileStoreTable, Collections.emptyMap());

        readableSnapshotAndOffsets =
                retriveReadableSnapshotAndOffsets(tablePath, fileStoreTable, snapshot11);
        // readable_snapshot = snapshot10
        // readable_offsets: partition0/bucket0 uses snapshot6's offset (6L),
        //                   partition0/bucket1 uses snapshot1's offset (3L),
        //                   partition1/bucket0 uses snapshot9's offset (6L)
        assertThat(readableSnapshotAndOffsets.getReadableSnapshotId()).isEqualTo(snapshot10);
        expectedReadableOffsets = new HashMap<>();
        expectedReadableOffsets.put(tbP0B0, 6L);
        expectedReadableOffsets.put(tbP0B1, 3L);
        expectedReadableOffsets.put(tbP1B0, 6L);
        assertThat(readableSnapshotAndOffsets.getReadableOffsets())
                .isEqualTo(expectedReadableOffsets);
    }

    private long latestSnapshot(FileStoreTable fileStoreTable) {
        return fileStoreTable.latestSnapshot().get().id();
    }

    // Helper methods
    private long createDvTable(TablePath tablePath, int numBuckets) throws Exception {
        TableDescriptor tableDescriptor =
                TableDescriptor.builder()
                        .schema(
                                org.apache.fluss.metadata.Schema.newBuilder()
                                        .column("c1", DataTypes.INT())
                                        .column("c2", DataTypes.STRING())
                                        .primaryKey("c1")
                                        .build())
                        .distributedBy(numBuckets)
                        .property(ConfigOptions.TABLE_DATALAKE_ENABLED, true)
                        .customProperty("paimon.deletion-vectors.enabled", "true")
                        .build();
        flussAdmin.createTable(tablePath, tableDescriptor, false).get();

        return flussAdmin.getTableInfo(tablePath).get().getTableId();
    }

    /**
     * Create a partitioned DV table with partition column "dt".
     *
     * @param tablePath the table path
     * @param numBuckets the number of buckets
     * @return the table ID
     */
    private long createPartitionedDvTable(TablePath tablePath, int numBuckets) throws Exception {
        TableDescriptor tableDescriptor =
                TableDescriptor.builder()
                        .schema(
                                org.apache.fluss.metadata.Schema.newBuilder()
                                        .column("c1", DataTypes.INT())
                                        .column("c2", DataTypes.STRING())
                                        .column("dt", DataTypes.STRING())
                                        .primaryKey("c1", "dt")
                                        .build())
                        .distributedBy(numBuckets)
                        .partitionedBy("dt")
                        .property(ConfigOptions.TABLE_DATALAKE_ENABLED, true)
                        .customProperty("paimon.deletion-vectors.enabled", "true")
                        .build();
        flussAdmin.createTable(tablePath, tableDescriptor, false).get();

        return flussAdmin.getTableInfo(tablePath).get().getTableId();
    }

    /**
     * Wait until partitions are created and return the mapping from partition ID to partition name.
     *
     * @param tablePath the table path
     * @return map from partition ID to partition name
     */
    private Map<Long, String> createTwoPartitions(TablePath tablePath) throws Exception {
        // For partitioned tables, partitions are created dynamically when data is written
        // We need to manually create partitions first
        flussAdmin
                .createPartition(
                        tablePath, new PartitionSpec(Collections.singletonMap("dt", "p0")), true)
                .get();
        flussAdmin
                .createPartition(
                        tablePath, new PartitionSpec(Collections.singletonMap("dt", "p1")), true)
                .get();

        List<PartitionInfo> partitionInfos = flussAdmin.listPartitionInfos(tablePath).get();
        Map<Long, String> partitionIdToName = new HashMap<>();
        for (PartitionInfo partitionInfo : partitionInfos) {
            partitionIdToName.put(partitionInfo.getPartitionId(), partitionInfo.getPartitionName());
        }
        return partitionIdToName;
    }

    /**
     * Get partition ID by partition name.
     *
     * @param partitionIdToName map from partition ID to partition name
     * @param partitionName the partition name to look up
     * @return the partition ID
     */
    private Long getPartitionIdByName(Map<Long, String> partitionIdToName, String partitionName) {
        return partitionIdToName.entrySet().stream()
                .filter(entry -> entry.getValue().equals(partitionName))
                .map(Map.Entry::getKey)
                .findFirst()
                .orElseThrow(
                        () ->
                                new IllegalStateException(
                                        "Partition name " + partitionName + " not found"));
    }

    /**
     * Generate rows for a specific partition with schema: (c1: INT, c2: STRING, dt: STRING, bucket:
     * INT, c4: BIGINT, ts: TIMESTAMP).
     *
     * @param partitionName the partition name (value for "dt" column)
     * @param bucket the bucket number
     * @param from the starting value (inclusive)
     * @param to the ending value (exclusive)
     * @return a list of GenericRow instances
     */
    private List<GenericRow> generateRowsForPartition(
            String partitionName, int bucket, int from, int to) {
        List<GenericRow> rows = new ArrayList<>();
        for (int i = from; i < to; i++) {
            rows.add(
                    GenericRow.of(
                            i,
                            BinaryString.fromString("value" + i),
                            BinaryString.fromString(partitionName),
                            bucket,
                            (long) i,
                            org.apache.paimon.data.Timestamp.now()));
        }
        return rows;
    }

    private BinaryRow toPartitionBinaryRow(String partitionName) {
        return BinaryRow.singleColumn(partitionName);
    }

    private DvTableReadableSnapshotRetriever.ReadableSnapshotResult
            retriveReadableSnapshotAndOffsets(
                    TablePath tablePath, FileStoreTable fileStoreTable, long tieredSnapshot)
                    throws Exception {
        try (DvTableReadableSnapshotRetriever retriever =
                new DvTableReadableSnapshotRetriever(
                        tablePath, tableId, fileStoreTable, flussConf)) {
            return retriever.getReadableSnapshotAndOffsets(tieredSnapshot);
        }
    }

    private FileStoreTable getPaimonTable(TablePath tablePath) throws Exception {
        Identifier identifier = toPaimon(tablePath);
        return (FileStoreTable) paimonCatalog.getTable(identifier);
    }

    private void commitSnapshot(
            long tableId,
            TablePath tablePath,
            long tieredSnapshot,
            Map<TableBucket, Long> lakeSnapshotTieredEndOffset,
            @Nullable
                    DvTableReadableSnapshotRetriever.ReadableSnapshotResult
                            readableSnapshotAndOffsets)
            throws Exception {
        String tieredSnapshotOffsetPath =
                lakeSnapshotCommitter.prepareLakeSnapshot(
                        tableId, tablePath, lakeSnapshotTieredEndOffset);
        LakeCommitResult lakeCommitResult;
        if (readableSnapshotAndOffsets != null) {
            lakeCommitResult =
                    LakeCommitResult.withReadableSnapshot(
                            tieredSnapshot,
                            readableSnapshotAndOffsets.getReadableSnapshotId(),
                            lakeSnapshotTieredEndOffset,
                            readableSnapshotAndOffsets.getReadableOffsets(),
                            readableSnapshotAndOffsets.getEarliestSnapshotIdToKeep());
        } else {
            lakeCommitResult = LakeCommitResult.unknownReadableSnapshot(tieredSnapshot);
        }
        lakeSnapshotCommitter.commit(
                tableId,
                tablePath,
                lakeCommitResult,
                tieredSnapshotOffsetPath,
                lakeSnapshotTieredEndOffset,
                Collections.emptyMap());
    }

    /**
     * Generate rows for testing with a schema that includes bucket information.
     *
     * <p>This is useful for tests that need to verify bucket-specific behavior. The generated rows
     * have the schema: (c1: INT, c2: STRING, bucket: INT, c4: BIGINT, ts: TIMESTAMP).
     *
     * @param bucket the bucket number
     * @param from the starting value (inclusive)
     * @param to the ending value (exclusive)
     * @return a list of GenericRow instances
     */
    public static List<GenericRow> generateRows(int bucket, int from, int to) {
        List<GenericRow> rows = new java.util.ArrayList<>();
        for (int i = from; i < to; i++) {
            rows.add(
                    GenericRow.of(
                            i,
                            BinaryString.fromString("value" + i),
                            bucket,
                            (long) i,
                            org.apache.paimon.data.Timestamp.now()));
        }
        return rows;
    }
}
