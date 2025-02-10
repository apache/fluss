/*
 * Copyright (c) 2024 Alibaba Group Holding Ltd.
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

package com.alibaba.fluss.connector.flink.source.enumerator;

import com.alibaba.fluss.client.table.Table;
import com.alibaba.fluss.client.table.writer.UpsertWriter;
import com.alibaba.fluss.client.write.HashBucketAssigner;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.connector.flink.FlinkConnectorOptions;
import com.alibaba.fluss.connector.flink.source.enumerator.initializer.OffsetsInitializer;
import com.alibaba.fluss.connector.flink.source.event.PartitionBucketsUnsubscribedEvent;
import com.alibaba.fluss.connector.flink.source.event.PartitionsRemovedEvent;
import com.alibaba.fluss.connector.flink.source.split.HybridSnapshotLogSplit;
import com.alibaba.fluss.connector.flink.source.split.LogSplit;
import com.alibaba.fluss.connector.flink.source.split.SnapshotSplit;
import com.alibaba.fluss.connector.flink.source.split.SourceSplitBase;
import com.alibaba.fluss.connector.flink.source.testutils.FlinkTestBase;
import com.alibaba.fluss.metadata.Schema;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.metadata.TableDescriptor;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.row.InternalRow;
import com.alibaba.fluss.row.encode.KeyEncoder;
import com.alibaba.fluss.server.zk.ZooKeeperClient;
import com.alibaba.fluss.types.DataTypes;

import org.apache.flink.api.connector.source.ReaderInfo;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SplitsAssignment;
import org.apache.flink.api.connector.source.mocks.MockSplitEnumeratorContext;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.alibaba.fluss.client.table.scanner.log.LogScanner.EARLIEST_OFFSET;
import static com.alibaba.fluss.testutils.DataTestUtils.row;
import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for {@link FlinkSourceEnumerator}. */
class FlinkSourceEnumeratorTest extends FlinkTestBase {

    private static final int PARTITION_DISCOVERY_CALLABLE_INDEX = 0;
    private static Configuration flussConf;
    private static final long DEFAULT_SCAN_PARTITION_DISCOVERY_INTERVAL_MS = 10000L;
    private static final boolean streaming = true;

    @BeforeAll
    protected static void beforeAll() {
        FlinkTestBase.beforeAll();
        flussConf = new Configuration(clientConf);
        flussConf.setString(
                FlinkConnectorOptions.SCAN_PARTITION_DISCOVERY_INTERVAL.key(),
                Duration.ofSeconds(10).toString());
    }

    @Test
    void testPkTableNoSnapshotSplits() throws Throwable {
        long tableId = createTable(DEFAULT_TABLE_PATH, DEFAULT_PK_TABLE_DESCRIPTOR);
        int numSubtasks = 3;
        // test get snapshot split & log split and the assignment
        try (MockSplitEnumeratorContext<SourceSplitBase> context =
                new MockSplitEnumeratorContext<>(numSubtasks)) {
            FlinkSourceEnumerator enumerator =
                    new FlinkSourceEnumerator(
                            DEFAULT_TABLE_PATH,
                            flussConf,
                            true,
                            false,
                            context,
                            OffsetsInitializer.initial(),
                            DEFAULT_SCAN_PARTITION_DISCOVERY_INTERVAL_MS,
                            streaming);

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
                // one split for one subtask
                expectedAssignment.put(i, Collections.singletonList(genLogSplit(tableId, i)));
            }

            Map<Integer, List<SourceSplitBase>> actualAssignment =
                    getLastReadersAssignments(context);
            assertThat(actualAssignment).isEqualTo(expectedAssignment);
        }
    }

    @Test
    void testPkTableWithSnapshotSplits() throws Throwable {
        long tableId = createTable(DEFAULT_TABLE_PATH, DEFAULT_PK_TABLE_DESCRIPTOR);
        int numSubtasks = 5;
        // write data and wait snapshot finish to make sure
        // we can hava snapshot split
        Map<Integer, Integer> bucketIdToNumRecords = putRows(DEFAULT_TABLE_PATH, 10);
        waitUntilSnapshot(tableId, 0);

        try (MockSplitEnumeratorContext<SourceSplitBase> context =
                new MockSplitEnumeratorContext<>(numSubtasks)) {
            FlinkSourceEnumerator enumerator =
                    new FlinkSourceEnumerator(
                            DEFAULT_TABLE_PATH,
                            flussConf,
                            true,
                            false,
                            context,
                            OffsetsInitializer.initial(),
                            DEFAULT_SCAN_PARTITION_DISCOVERY_INTERVAL_MS,
                            streaming);
            enumerator.start();
            // register all read
            for (int i = 0; i < numSubtasks; i++) {
                registerReader(context, enumerator, i);
            }
            assertThat(context.getSplitsAssignmentSequence()).isEmpty();
            // make enumerate to get splits and assign
            context.runNextOneTimeCallable();

            Map<Integer, List<SourceSplitBase>> actualAssignment =
                    getLastReadersAssignments(context);

            Map<Integer, List<SourceSplitBase>> expectedAssignment = new HashMap<>();

            // the split with same bucket should be assigned to same task
            TableBucket bucket0 = new TableBucket(tableId, 0);
            TableBucket bucket1 = new TableBucket(tableId, 1);
            TableBucket bucket2 = new TableBucket(tableId, 2);

            expectedAssignment.put(
                    0,
                    Collections.singletonList(
                            new HybridSnapshotLogSplit(
                                    bucket0, null, 0L, bucketIdToNumRecords.get(0))));
            expectedAssignment.put(
                    1,
                    Collections.singletonList(
                            new HybridSnapshotLogSplit(
                                    bucket1, null, 0L, bucketIdToNumRecords.get(1))));
            expectedAssignment.put(
                    2,
                    Collections.singletonList(
                            new HybridSnapshotLogSplit(
                                    bucket2, null, 0L, bucketIdToNumRecords.get(2))));
            checkSplitAssignmentIgnoreSnapshotFiles(expectedAssignment, actualAssignment);
        }
    }

    @Test
    void testNonPkTable() throws Throwable {
        int numSubtasks = 3;
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("name", DataTypes.STRING())
                        .build();

        TableDescriptor nonPkTableDescriptor =
                TableDescriptor.builder()
                        .schema(schema)
                        .distributedBy(DEFAULT_BUCKET_NUM, "id")
                        .build();

        TablePath path1 = TablePath.of(DEFAULT_DB, "test-non-pk-table");
        admin.createTable(path1, nonPkTableDescriptor, true).get();
        long tableId = admin.getTableInfo(path1).get().getTableId();

        // test get snapshot log split and the assignment
        try (MockSplitEnumeratorContext<SourceSplitBase> context =
                new MockSplitEnumeratorContext<>(numSubtasks)) {
            FlinkSourceEnumerator enumerator =
                    new FlinkSourceEnumerator(
                            path1,
                            flussConf,
                            false,
                            false,
                            context,
                            OffsetsInitializer.initial(),
                            DEFAULT_SCAN_PARTITION_DISCOVERY_INTERVAL_MS,
                            streaming);

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
                // one split for one subtask
                expectedAssignment.put(
                        i,
                        Collections.singletonList(
                                new LogSplit(new TableBucket(tableId, i), null, -2L)));
            }

            Map<Integer, List<SourceSplitBase>> actualAssignment =
                    getLastReadersAssignments(context);
            assertThat(actualAssignment).isEqualTo(expectedAssignment);
        }
    }

    @Test
    void testReaderRegistrationTriggerAssignments() throws Throwable {
        long tableId = createTable(DEFAULT_TABLE_PATH, DEFAULT_PK_TABLE_DESCRIPTOR);
        int numSubtasks = 3;
        // test get snapshot split & log split and the assignment
        try (MockSplitEnumeratorContext<SourceSplitBase> context =
                new MockSplitEnumeratorContext<>(numSubtasks)) {
            FlinkSourceEnumerator enumerator =
                    new FlinkSourceEnumerator(
                            DEFAULT_TABLE_PATH,
                            flussConf,
                            true,
                            false,
                            context,
                            OffsetsInitializer.initial(),
                            DEFAULT_SCAN_PARTITION_DISCOVERY_INTERVAL_MS,
                            streaming);

            enumerator.start();

            context.runNextOneTimeCallable();

            assertThat(context.getSplitsAssignmentSequence()).isEmpty();

            registerReader(context, enumerator, 0);

            // check assignment then
            Map<Integer, List<SourceSplitBase>> actualAssignment =
                    getLastReadersAssignments(context);
            Map<Integer, List<SourceSplitBase>> expectedAssignment = new HashMap<>();
            expectedAssignment.put(0, Collections.singletonList(genLogSplit(tableId, 0)));
            assertThat(actualAssignment).isEqualTo(expectedAssignment);
        }
    }

    @Test
    void testAddSplitBack() throws Throwable {
        long tableId = createTable(DEFAULT_TABLE_PATH, DEFAULT_PK_TABLE_DESCRIPTOR);
        int numSubtasks = 3;
        // test get snapshot split & log split and the assignment
        try (MockSplitEnumeratorContext<SourceSplitBase> context =
                new MockSplitEnumeratorContext<>(numSubtasks)) {
            FlinkSourceEnumerator enumerator =
                    new FlinkSourceEnumerator(
                            DEFAULT_TABLE_PATH,
                            flussConf,
                            true,
                            false,
                            context,
                            OffsetsInitializer.initial(),
                            DEFAULT_SCAN_PARTITION_DISCOVERY_INTERVAL_MS,
                            streaming);

            enumerator.start();

            context.runNextOneTimeCallable();

            registerReader(context, enumerator, 0);

            // Simulate a reader failure.
            int readerId = 0;
            context.unregisterReader(readerId);

            enumerator.addSplitsBack(
                    context.getSplitsAssignmentSequence().get(0).assignment().get(readerId),
                    readerId);
            assertThat(context.getSplitsAssignmentSequence())
                    .as("The added back splits should have not been assigned")
                    .hasSize(1);

            // Simulate a reader recovery.
            registerReader(context, enumerator, readerId);

            assertThat(context.getSplitsAssignmentSequence())
                    .as("The added back splits should have been assigned")
                    .hasSize(2);

            Map<Integer, List<SourceSplitBase>> actualAssignment =
                    getLastReadersAssignments(context);
            Map<Integer, List<SourceSplitBase>> expectedAssignment = new HashMap<>();
            expectedAssignment.put(0, Collections.singletonList(genLogSplit(tableId, 0)));
            assertThat(actualAssignment).isEqualTo(expectedAssignment);
        }
    }

    @Test
    void testRestore() throws Throwable {
        long tableId = createTable(DEFAULT_TABLE_PATH, DEFAULT_PK_TABLE_DESCRIPTOR);
        int numSubtasks = 3;
        // test get snapshot split & log split and the assignment
        try (MockSplitEnumeratorContext<SourceSplitBase> context =
                new MockSplitEnumeratorContext<>(numSubtasks)) {

            // mock bucket1 has been assigned
            TableBucket bucket1 = new TableBucket(tableId, 1);
            Set<TableBucket> assignedBuckets = new HashSet<>(Collections.singletonList(bucket1));

            // mock restore with assigned buckets
            FlinkSourceEnumerator enumerator =
                    new FlinkSourceEnumerator(
                            DEFAULT_TABLE_PATH,
                            flussConf,
                            false,
                            false,
                            context,
                            assignedBuckets,
                            Collections.emptyMap(),
                            OffsetsInitializer.earliest(),
                            DEFAULT_SCAN_PARTITION_DISCOVERY_INTERVAL_MS,
                            streaming);

            enumerator.start();
            assertThat(context.getSplitsAssignmentSequence()).isEmpty();

            context.runNextOneTimeCallable();

            // register all readers
            for (int i = 0; i < numSubtasks; i++) {
                registerReader(context, enumerator, i);
            }
            // check assignment then, should contain bucket0 and bucket2
            // which are not in assigned buckets
            Map<Integer, List<SourceSplitBase>> expectedAssignment = new HashMap<>();
            expectedAssignment.put(0, Collections.singletonList(genLogSplit(tableId, 0)));
            expectedAssignment.put(2, Collections.singletonList(genLogSplit(tableId, 2)));
            Map<Integer, List<SourceSplitBase>> actualAssignment = getReadersAssignments(context);
            assertThat(actualAssignment).isEqualTo(expectedAssignment);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testDiscoverPartitionsPeriodically(boolean isPrimaryKeyTable) throws Throwable {
        int numSubtasks = 3;
        TableDescriptor tableDescriptor =
                isPrimaryKeyTable
                        ? DEFAULT_AUTO_PARTITIONED_PK_TABLE_DESCRIPTOR
                        : DEFAULT_AUTO_PARTITIONED_LOG_TABLE_DESCRIPTOR;
        long tableId = createTable(DEFAULT_TABLE_PATH, tableDescriptor);
        ZooKeeperClient zooKeeperClient = FLUSS_CLUSTER_EXTENSION.getZooKeeperClient();
        try (MockSplitEnumeratorContext<SourceSplitBase> context =
                        new MockSplitEnumeratorContext<>(numSubtasks);
                FlinkSourceEnumerator enumerator =
                        new FlinkSourceEnumerator(
                                DEFAULT_TABLE_PATH,
                                flussConf,
                                isPrimaryKeyTable,
                                true,
                                context,
                                OffsetsInitializer.initial(),
                                DEFAULT_SCAN_PARTITION_DISCOVERY_INTERVAL_MS,
                                streaming)) {
            Map<Long, String> partitionNameByIds =
                    waitUntilPartitions(zooKeeperClient, DEFAULT_TABLE_PATH);
            enumerator.start();

            // invoke partition discovery callable again and there should be pending assignments.
            runPeriodicPartitionDiscovery(context);

            // register two readers
            registerReader(context, enumerator, 0);
            registerReader(context, enumerator, 1);

            // invoke partition discovery callable again, shouldn't produce RemovePartitionEvent.
            runPeriodicPartitionDiscovery(context);
            assertThat(context.getSentSourceEvent()).isEmpty();

            // now, register the third reader
            registerReader(context, enumerator, 2);

            // check the assignments
            Map<Integer, List<SourceSplitBase>> expectedAssignment =
                    expectAssignments(enumerator, tableId, partitionNameByIds);
            Map<Integer, List<SourceSplitBase>> actualAssignments = getReadersAssignments(context);
            checkAssignmentIgnoreOrder(actualAssignments, expectedAssignment);

            // now, create a new partition and runPeriodicPartitionDiscovery again,
            // there should be new assignments
            List<String> newPartitions = Arrays.asList("newPartition1", "newPartition2");

            Map<Long, String> newPartitionNameIds =
                    createPartitions(zooKeeperClient, DEFAULT_TABLE_PATH, newPartitions);

            /// invoke partition discovery callable again and there should assignments.
            runPeriodicPartitionDiscovery(context);

            expectedAssignment = expectAssignments(enumerator, tableId, newPartitionNameIds);
            actualAssignments = getLastReadersAssignments(context);
            checkAssignmentIgnoreOrder(actualAssignments, expectedAssignment);

            // drop + create partitions;
            Set<String> dropPartitions = new HashSet<>(newPartitions);
            Map<Long, String> expectedRemovedPartitions = newPartitionNameIds;
            newPartitions = Collections.singletonList("newPartition3");

            dropPartitions(zooKeeperClient, DEFAULT_TABLE_PATH, dropPartitions);
            newPartitionNameIds =
                    createPartitions(zooKeeperClient, DEFAULT_TABLE_PATH, newPartitions);

            // invoke partition discovery callable again
            runPeriodicPartitionDiscovery(context);

            // there should be partition removed events
            Map<Integer, List<SourceEvent>> sentSourceEvents = context.getSentSourceEvent();
            assertThat(sentSourceEvents).hasSize(numSubtasks);
            for (int subtask = 0; subtask < numSubtasks; subtask++) {
                // get the source event send to reader
                List<SourceEvent> sourceEvents = sentSourceEvents.get(subtask);
                assertThat(sourceEvents).hasSize(1);
                SourceEvent sourceEvent = sourceEvents.get(0);
                PartitionsRemovedEvent partitionsRemovedEvent =
                        (PartitionsRemovedEvent) sourceEvent;

                // get the partition infos in the event
                Map<Long, String> removedPartitions = partitionsRemovedEvent.getRemovedPartitions();
                assertThat(removedPartitions).isEqualTo(expectedRemovedPartitions);
            }

            // check new assignments.
            expectedAssignment = expectAssignments(enumerator, tableId, newPartitionNameIds);
            actualAssignments = getLastReadersAssignments(context);
            checkAssignmentIgnoreOrder(actualAssignments, expectedAssignment);

            Map<Long, String> assignedPartitions =
                    new HashMap<>(enumerator.getAssignedPartitions());

            // mock enumerator receive PartitionBucketsUnsubscribedEvent,
            // partitions should be removed from the enumerator's assigned partition
            int removedPartitionsCount = 2;
            Set<Long> removedPartitions = new HashSet<>();
            Iterator<Long> partitionIdIterator = assignedPartitions.keySet().iterator();
            for (int i = 0; i < removedPartitionsCount; i++) {
                removedPartitions.add(partitionIdIterator.next());
                partitionIdIterator.remove();
            }

            Set<TableBucket> tableBuckets = new HashSet<>();
            for (long removedPartition : removedPartitions) {
                for (int bucket = 0; bucket < DEFAULT_BUCKET_NUM; bucket++) {
                    tableBuckets.add(new TableBucket(tableId, removedPartition, bucket));
                }
            }

            enumerator.handleSourceEvent(0, new PartitionBucketsUnsubscribedEvent(tableBuckets));

            // check the assigned partitions, should equal to the assignment with removed partition
            assertThat(enumerator.getAssignedPartitions()).isEqualTo(assignedPartitions);
        }
    }

    @Test
    void testGetSplitOwner() throws Exception {
        int numSubtasks = 3;
        long tableId = createTable(DEFAULT_TABLE_PATH, DEFAULT_PK_TABLE_DESCRIPTOR);
        try (MockSplitEnumeratorContext<SourceSplitBase> context =
                        new MockSplitEnumeratorContext<>(numSubtasks);
                FlinkSourceEnumerator enumerator =
                        new FlinkSourceEnumerator(
                                DEFAULT_TABLE_PATH,
                                flussConf,
                                false,
                                true,
                                context,
                                OffsetsInitializer.initial(),
                                DEFAULT_SCAN_PARTITION_DISCOVERY_INTERVAL_MS,
                                streaming)) {

            // test splits for same non-partitioned bucket, should assign to same task
            TableBucket t1 = new TableBucket(tableId, 0);
            SourceSplitBase s1 = new LogSplit(t1, null, 1);
            SourceSplitBase s2 = new HybridSnapshotLogSplit(t1, null, 0L, 1);
            assertThat(enumerator.getSplitOwner(s1)).isEqualTo(enumerator.getSplitOwner(s2));

            // test splits for same partitioned bucket, should assign to same task
            t1 = new TableBucket(tableId, 1L, 0);
            s1 = new LogSplit(t1, "p1", 1);
            s2 = new HybridSnapshotLogSplit(t1, "p1", 0L, 2);
            assertThat(enumerator.getSplitOwner(s1)).isEqualTo(enumerator.getSplitOwner(s2));

            // test splits for partitioned bucket
            // splits are with same partition id
            t1 = new TableBucket(tableId, 0L, 0);
            TableBucket t2 = new TableBucket(tableId, 0L, 1);
            s1 = new LogSplit(t1, "p0", 0);
            s2 = new LogSplit(t2, "p0", 0);
            assertThat(enumerator.getSplitOwner(s1)).isEqualTo(0);
            assertThat(enumerator.getSplitOwner(s2)).isEqualTo(1);

            // splits are with different partitions
            t1 = new TableBucket(tableId, 1L, 0);
            t2 = new TableBucket(tableId, 2L, 0);
            s1 = new LogSplit(t1, "p1", 0);
            s2 = new LogSplit(t2, "p2", 0);
            assertThat(enumerator.getSplitOwner(s1)).isEqualTo(1);
            assertThat(enumerator.getSplitOwner(s2)).isEqualTo(2);
        }
    }

    // ---------------------
    private void registerReader(
            MockSplitEnumeratorContext<SourceSplitBase> context,
            FlinkSourceEnumerator enumerator,
            int readerId) {
        context.registerReader(new ReaderInfo(readerId, "location " + readerId));
        enumerator.addReader(readerId);
    }

    private Map<Integer, List<SourceSplitBase>> expectAssignments(
            FlinkSourceEnumerator enumerator, long tableId, Map<Long, String> partitionNameIds) {
        Map<Integer, List<SourceSplitBase>> expectedAssignment = new HashMap<>();
        for (Long partitionId : partitionNameIds.keySet()) {
            for (int i = 0; i < DEFAULT_BUCKET_NUM; i++) {
                TableBucket tableBucket = new TableBucket(tableId, partitionId, i);
                LogSplit logSplit =
                        new LogSplit(
                                tableBucket, partitionNameIds.get(partitionId), EARLIEST_OFFSET);
                int task = enumerator.getSplitOwner(logSplit);
                expectedAssignment.computeIfAbsent(task, k -> new ArrayList<>()).add(logSplit);
            }
        }
        return expectedAssignment;
    }

    private void checkAssignmentIgnoreOrder(
            Map<Integer, List<SourceSplitBase>> actualAssignment,
            Map<Integer, List<SourceSplitBase>> expectedAssignment) {
        assertThat(actualAssignment).hasSameSizeAs(expectedAssignment);
        for (Map.Entry<Integer, List<SourceSplitBase>> actualSplitAssignEntry :
                actualAssignment.entrySet()) {
            List<SourceSplitBase> actualSplits =
                    expectedAssignment.get(actualSplitAssignEntry.getKey());
            List<SourceSplitBase> expectedSplits = actualSplitAssignEntry.getValue();
            assertThat(actualSplits).containsExactlyInAnyOrderElementsOf(expectedSplits);
        }
    }

    private void runPeriodicPartitionDiscovery(MockSplitEnumeratorContext<SourceSplitBase> context)
            throws Throwable {
        // Fetch potential topic descriptions
        context.runPeriodicCallable(PARTITION_DISCOVERY_CALLABLE_INDEX);
        // Initialize offsets for discovered partitions
        if (!context.getOneTimeCallables().isEmpty()) {
            context.runNextOneTimeCallable();
        }
    }

    private LogSplit genLogSplit(long tableId, int bucketId) {
        return new LogSplit(new TableBucket(tableId, bucketId), null, -2L);
    }

    private Map<Integer, List<SourceSplitBase>> getReadersAssignments(
            MockSplitEnumeratorContext<SourceSplitBase> context) {
        List<SplitsAssignment<SourceSplitBase>> splitsAssignments =
                context.getSplitsAssignmentSequence();
        Map<Integer, List<SourceSplitBase>> assignment = new HashMap<>();
        for (SplitsAssignment<SourceSplitBase> splitAssignment : splitsAssignments) {
            assignment.putAll(splitAssignment.assignment());
        }
        return assignment;
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

    private void checkSplitAssignmentIgnoreSnapshotFiles(
            Map<Integer, List<SourceSplitBase>> expectedAssignment,
            Map<Integer, List<SourceSplitBase>> actualAssignment) {
        assertThat(expectedAssignment).hasSameSizeAs(actualAssignment);
        for (Map.Entry<Integer, List<SourceSplitBase>> splitAssignEntry :
                expectedAssignment.entrySet()) {
            int subtaskId = splitAssignEntry.getKey();
            List<SourceSplitBase> expectedSplits = splitAssignEntry.getValue();
            List<SourceSplitBase> actualSplits = actualAssignment.get(subtaskId);
            assertThat(expectedSplits).hasSameSizeAs(actualSplits);

            for (int i = 0; i < expectedSplits.size(); i++) {
                SourceSplitBase sourceSplitBase = expectedSplits.get(i);
                if (sourceSplitBase.isHybridSnapshotLogSplit()) {
                    SnapshotSplit expected = sourceSplitBase.asHybridSnapshotLogSplit();
                    SnapshotSplit actual = actualSplits.get(i).asHybridSnapshotLogSplit();
                    // note: in here, we skip the check of the snapshot files
                    TableBucket expectedBucket = expected.getTableBucket();
                    TableBucket actualBucket = actual.getTableBucket();
                    assertThat(expectedBucket).isEqualTo(actualBucket);
                    assertThat(expected.recordsToSkip()).isEqualTo(actual.recordsToSkip());
                } else {
                    assertThat(expectedSplits.get(i)).isEqualTo(actualSplits.get(i));
                }
            }
        }
    }

    private static Map<Integer, Integer> putRows(TablePath tablePath, int rowsNum)
            throws Exception {
        KeyEncoder keyEncoder =
                new KeyEncoder(
                        DEFAULT_PK_TABLE_SCHEMA.getRowType(),
                        DEFAULT_PK_TABLE_SCHEMA.getPrimaryKeyIndexes());
        HashBucketAssigner hashBucketAssigner = new HashBucketAssigner(DEFAULT_BUCKET_NUM);
        Map<Integer, Integer> bucketRows = new HashMap<>();
        try (Table table = conn.getTable(tablePath)) {
            UpsertWriter upsertWriter = table.newUpsert().createWriter();
            for (int i = 0; i < rowsNum; i++) {
                InternalRow row = row(i, "v" + i);
                upsertWriter.upsert(row);

                byte[] key = keyEncoder.encode(row);
                int bucketId = hashBucketAssigner.assignBucket(key, null);

                bucketRows.merge(bucketId, 1, Integer::sum);
            }
            upsertWriter.flush();
        }
        return bucketRows;
    }
}
