/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.lakehouse.paimon.flink;

import com.alibaba.fluss.flink.lakehouse.paimon.split.PaimonSnapshotAndFlussLogSplit;
import com.alibaba.fluss.flink.lakehouse.paimon.split.PaimonSnapshotSplit;
import com.alibaba.fluss.flink.source.enumerator.FlinkSourceEnumerator;
import com.alibaba.fluss.flink.source.enumerator.initializer.OffsetsInitializer;
import com.alibaba.fluss.flink.source.split.LogSplit;
import com.alibaba.fluss.flink.source.split.SourceSplitBase;
import com.alibaba.fluss.lakehouse.paimon.sink.PaimonDataBaseSyncSinkBuilder;
import com.alibaba.fluss.lakehouse.paimon.testutils.PaimonSyncTestBase;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.row.InternalRow;
import com.alibaba.fluss.server.replica.Replica;

import org.apache.flink.api.connector.source.ReaderInfo;
import org.apache.flink.api.connector.source.SplitsAssignment;
import org.apache.flink.api.connector.source.mocks.MockSplitEnumeratorContext;
import org.apache.flink.core.execution.JobClient;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.flink.source.FileStoreSourceSplit;
import org.apache.paimon.flink.source.FileStoreSourceSplitGenerator;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.DataSplit;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.alibaba.fluss.testutils.DataTestUtils.row;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for enumerating for datalake enabled table in batch mode. */
class LakeTableEnumeratorTest extends PaimonSyncTestBase {

    private static final int DEFAULT_BUCKET_NUM = 3;

    @BeforeAll
    protected static void beforeAll() {
        PaimonSyncTestBase.beforeAll();
    }

    @Test
    void testLogTableEnumerator() throws Throwable {
        // first of all, start database sync job
        PaimonDataBaseSyncSinkBuilder builder = getDatabaseSyncSinkBuilder(execEnv);
        builder.build();
        JobClient jobClient = execEnv.executeAsync();

        TablePath t1 = TablePath.of(DEFAULT_DB, "logTable");
        Map<Integer, Long> bucketLogEndOffset = new HashMap<>();
        long t1Id = prepareLogTable(t1, DEFAULT_BUCKET_NUM, bucketLogEndOffset);

        // check the status of replica after synced
        for (int i = 0; i < DEFAULT_BUCKET_NUM; i++) {
            TableBucket tableBucket = new TableBucket(t1Id, i);
            if (bucketLogEndOffset.get(i) > 0) {
                assertReplicaStatus(tableBucket, bucketLogEndOffset.get(i));
            }
        }

        // start to run enumerator
        int numSubtasks = 3;
        try (MockSplitEnumeratorContext<SourceSplitBase> context =
                new MockSplitEnumeratorContext<>(numSubtasks)) {
            FlinkSourceEnumerator enumerator =
                    new FlinkSourceEnumerator(
                            t1,
                            clientConf,
                            false,
                            false,
                            context,
                            OffsetsInitializer.initial(),
                            1000,
                            // use batch mode
                            false);
            enumerator.start();
            // register all read
            for (int i = 0; i < 3; i++) {
                registerReader(context, enumerator, i);
            }
            assertThat(context.getSplitsAssignmentSequence()).isEmpty();
            // make enumerate to get splits and assign
            context.runNextOneTimeCallable();

            // get expected assignment
            List<FileStoreSourceSplit> expectedFileStoreSourceSplits =
                    getExpectedFileStoreSourceSplit(t1);
            Map<Integer, List<SourceSplitBase>> expectedAssignment = new HashMap<>();
            for (FileStoreSourceSplit fileStoreSourceSplit : expectedFileStoreSourceSplits) {
                PaimonSnapshotSplit paimonSnapshotSplit =
                        new PaimonSnapshotSplit(
                                new TableBucket(t1Id, null, -1), null, fileStoreSourceSplit);
                expectedAssignment
                        .computeIfAbsent(
                                (paimonSnapshotSplit.splitId().hashCode() & 0x7FFFFFFF)
                                        % numSubtasks,
                                (k) -> new ArrayList<>())
                        .add(paimonSnapshotSplit);
            }

            for (int i = 0; i < numSubtasks; i++) {
                TableBucket tableBucket = new TableBucket(t1Id, i);
                // no any paimon data written for the bucket
                if (bucketLogEndOffset.get(i) <= 0) {
                    expectedAssignment
                            .computeIfAbsent(i, (k) -> new ArrayList<>())
                            .add(new LogSplit(tableBucket, null, -2, 0));
                }
            }
            Map<Integer, List<SourceSplitBase>> actualAssignment =
                    getLastReadersAssignments(context);
            verifyAssignment(actualAssignment, expectedAssignment);
        }
        jobClient.cancel().get();
    }

    @Test
    void testPrimaryKeyTableEnumerator() throws Throwable {
        TablePath t1 = TablePath.of(DEFAULT_DB, "pkTable");
        PaimonDataBaseSyncSinkBuilder builder = getDatabaseSyncSinkBuilder(execEnv);
        builder.build();
        JobClient jobClient = execEnv.executeAsync();
        Map<Integer, Long> bucketLogEndOffset = new HashMap<>();
        long tableId = preparePkTable(t1, DEFAULT_BUCKET_NUM, bucketLogEndOffset);
        // wait unit records has has been synced
        for (int i = 0; i < DEFAULT_BUCKET_NUM; i++) {
            TableBucket tableBucket = new TableBucket(tableId, i);
            waitUtilBucketSynced(tableBucket);
        }
        // write records again
        writeRowsToPkTable(t1, tableId, DEFAULT_BUCKET_NUM, bucketLogEndOffset);
        // check the status of replica after synced
        for (int i = 0; i < DEFAULT_BUCKET_NUM; i++) {
            TableBucket tableBucket = new TableBucket(tableId, i);
            assertReplicaStatus(tableBucket, bucketLogEndOffset.get(i));
        }
        // start to run enumerator
        int numSubtasks = 3;
        try (MockSplitEnumeratorContext<SourceSplitBase> context =
                new MockSplitEnumeratorContext<>(numSubtasks)) {
            FlinkSourceEnumerator enumerator =
                    new FlinkSourceEnumerator(
                            t1,
                            clientConf,
                            false,
                            false,
                            context,
                            OffsetsInitializer.initial(),
                            1000,
                            // use batch mode
                            false);
            enumerator.start();
            // register all read
            for (int i = 0; i < 3; i++) {
                registerReader(context, enumerator, i);
            }
            assertThat(context.getSplitsAssignmentSequence()).isEmpty();
            // make enumerate to get splits and assign
            context.runNextOneTimeCallable();
            Map<Integer, List<SourceSplitBase>> expectedAssignment = new HashMap<>();
            for (int i = 0; i < numSubtasks; i++) {
                PaimonSnapshotAndFlussLogSplit paimonSnapshotAndFlussLogSplit =
                        new PaimonSnapshotAndFlussLogSplit(
                                new TableBucket(tableId, i),
                                // don't care file store source split
                                null,
                                bucketLogEndOffset.get(i),
                                bucketLogEndOffset.get(i));
                expectedAssignment.put(
                        i, Collections.singletonList(paimonSnapshotAndFlussLogSplit));
            }
            Map<Integer, List<SourceSplitBase>> actualAssignment =
                    getLastReadersAssignments(context);
            verifyAssignment(actualAssignment, expectedAssignment);
        }
        jobClient.cancel().get();
    }

    private List<FileStoreSourceSplit> getExpectedFileStoreSourceSplit(TablePath tablePath)
            throws Exception {
        FileStoreTable fileStoreTable =
                (FileStoreTable)
                        paimonCatalog.getTable(
                                Identifier.create(
                                        tablePath.getDatabaseName(), tablePath.getTableName()));
        FileStoreSourceSplitGenerator splitGenerator = new FileStoreSourceSplitGenerator();
        return splitGenerator.createSplits(fileStoreTable.newScan().plan());
    }

    private void verifyAssignment(
            Map<Integer, List<SourceSplitBase>> actualAssignment,
            Map<Integer, List<SourceSplitBase>> expectedAssignment) {
        assertThat(actualAssignment).hasSize(expectedAssignment.size());
        for (Map.Entry<Integer, List<SourceSplitBase>> splitsEntry : actualAssignment.entrySet()) {
            List<SourceSplitBase> actualSplits = splitsEntry.getValue();
            List<SourceSplitBase> expectedSplits = expectedAssignment.get(splitsEntry.getKey());
            assertThat(actualSplits).hasSize(expectedSplits.size());
            for (int i = 0; i < actualSplits.size(); i++) {
                SourceSplitBase actualSplit = actualSplits.get(i);
                if (actualSplit instanceof PaimonSnapshotSplit) {
                    PaimonSnapshotSplit paimonSnapshotSplit =
                            (PaimonSnapshotSplit) actualSplits.get(i);
                    PaimonSnapshotSplit expectedPaimonSnapshotSplit =
                            (PaimonSnapshotSplit) expectedSplits.get(i);
                    // ignore file store, only check table bucket
                    assertThat(paimonSnapshotSplit.getTableBucket())
                            .isEqualTo(expectedPaimonSnapshotSplit.getTableBucket());
                    DataSplit actualDataSplit =
                            (DataSplit) paimonSnapshotSplit.getFileStoreSourceSplit().split();
                    DataSplit expectedDataSplit =
                            (DataSplit)
                                    expectedPaimonSnapshotSplit.getFileStoreSourceSplit().split();
                    // just compare data files
                    assertThat(actualDataSplit.dataFiles())
                            .isEqualTo(expectedDataSplit.dataFiles());
                } else if (actualSplit instanceof PaimonSnapshotAndFlussLogSplit) {
                    PaimonSnapshotAndFlussLogSplit paimonActualSplit =
                            (PaimonSnapshotAndFlussLogSplit) actualSplit;
                    PaimonSnapshotAndFlussLogSplit paimonExpectedSplit =
                            (PaimonSnapshotAndFlussLogSplit) expectedSplits.get(i);
                    // ignore file store, only check table bucket, starting, stopping offset
                    assertThat(paimonActualSplit.getTableBucket())
                            .isEqualTo(paimonExpectedSplit.getTableBucket());
                    assertThat(paimonActualSplit.getStartingOffset())
                            .isEqualTo(paimonExpectedSplit.getStartingOffset());
                    assertThat(paimonActualSplit.getStoppingOffset())
                            .isEqualTo(paimonExpectedSplit.getStoppingOffset());
                } else {
                    assertThat(actualSplit).isEqualTo(expectedSplits.get(i));
                }
            }
        }
    }

    // ---------------------
    private long prepareLogTable(
            TablePath tablePath, int bucketNum, Map<Integer, Long> bucketLogEndOffset)
            throws Exception {
        long t1Id = createLogTable(tablePath, bucketNum);
        for (int i = 0; i < 30; i++) {
            List<InternalRow> rows = Arrays.asList(row(1, "v1"), row(2, "v2"), row(3, "v3"));
            // write records
            writeRows(tablePath, rows, true);
        }
        for (int i = 0; i < bucketNum; i++) {
            TableBucket tableBucket = new TableBucket(t1Id, i);
            Replica replica = getLeaderReplica(tableBucket);
            bucketLogEndOffset.put(i, replica.getLocalLogEndOffset());
        }
        return t1Id;
    }

    private long preparePkTable(
            TablePath tablePath, int bucketNum, Map<Integer, Long> bucketLogEndOffset)
            throws Exception {
        long tableId = createPkTable(tablePath, bucketNum);
        writeRowsToPkTable(tablePath, tableId, bucketNum, bucketLogEndOffset);
        return tableId;
    }

    private void writeRowsToPkTable(
            TablePath tablePath, long tableId, int bucketNum, Map<Integer, Long> bucketLogEndOffset)
            throws Exception {
        for (int i = 0; i < 10; i++) {
            List<InternalRow> rows =
                    Arrays.asList(row(0, "v0"), row(1, "v1"), row(2, "v2"), row(3, "v3"));
            // write records
            writeRows(tablePath, rows, false);
        }
        for (int i = 0; i < bucketNum; i++) {
            TableBucket tableBucket = new TableBucket(tableId, i);
            Replica replica = getLeaderReplica(tableBucket);
            if (replica.getLocalLogEndOffset() > 0) {
                bucketLogEndOffset.put(i, replica.getLocalLogEndOffset());
            }
        }
    }

    private void registerReader(
            MockSplitEnumeratorContext<SourceSplitBase> context,
            FlinkSourceEnumerator enumerator,
            int readerId) {
        context.registerReader(new ReaderInfo(readerId, "location " + readerId));
        enumerator.addReader(readerId);
    }

    private Map<Integer, List<SourceSplitBase>> getLastReadersAssignments(
            MockSplitEnumeratorContext<SourceSplitBase> context) {
        List<SplitsAssignment<SourceSplitBase>> splitsAssignments =
                context.getSplitsAssignmentSequence();
        // get the last one
        SplitsAssignment<SourceSplitBase> splitAssignment =
                splitsAssignments.get(splitsAssignments.size() - 1);
        return splitAssignment.assignment();
    }
}
