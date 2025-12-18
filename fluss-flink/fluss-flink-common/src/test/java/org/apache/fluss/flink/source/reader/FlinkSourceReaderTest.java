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

package org.apache.fluss.flink.source.reader;

import org.apache.fluss.config.Configuration;
import org.apache.fluss.flink.lake.split.LakeSnapshotSplit;
import org.apache.fluss.flink.source.deserializer.DeserializerInitContextImpl;
import org.apache.fluss.flink.source.deserializer.RowDataDeserializationSchema;
import org.apache.fluss.flink.source.emitter.FlinkRecordEmitter;
import org.apache.fluss.flink.source.event.PartitionBucketsFinishedEvent;
import org.apache.fluss.flink.source.event.PartitionBucketsUnsubscribedEvent;
import org.apache.fluss.flink.source.event.PartitionsRemovedEvent;
import org.apache.fluss.flink.source.metrics.FlinkSourceReaderMetrics;
import org.apache.fluss.flink.source.split.LogSplit;
import org.apache.fluss.flink.source.split.SourceSplitBase;
import org.apache.fluss.flink.utils.FlinkTestBase;
import org.apache.fluss.lake.source.LakeSource;
import org.apache.fluss.lake.source.LakeSplit;
import org.apache.fluss.lake.source.TestingLakeSource;
import org.apache.fluss.lake.source.TestingLakeSplit;
import org.apache.fluss.metadata.PartitionInfo;
import org.apache.fluss.metadata.ResolvedPartitionSpec;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.types.RowType;

import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.connector.testutils.source.reader.TestingReaderContext;
import org.apache.flink.connector.testutils.source.reader.TestingReaderOutput;
import org.apache.flink.table.data.RowData;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.fluss.testutils.common.CommonTestUtils.retry;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link FlinkSourceReader}. */
class FlinkSourceReaderTest extends FlinkTestBase {

    @Test
    void testHandlePartitionsRemovedEvent() throws Exception {
        TablePath tablePath = TablePath.of(DEFAULT_DB, "test_partitioned_table");

        TableDescriptor tableDescriptor = DEFAULT_AUTO_PARTITIONED_PK_TABLE_DESCRIPTOR;
        long tableId = createTable(tablePath, tableDescriptor);

        // wait until partitions are created
        ZooKeeperClient zooKeeperClient = FLUSS_CLUSTER_EXTENSION.getZooKeeperClient();
        Map<Long, String> partitionNameByIds = waitUntilPartitions(zooKeeperClient, tablePath);

        // now, write rows to the table
        Map<Long, List<String>> partitionWrittenRows = new HashMap<>();
        for (Map.Entry<Long, String> partitionIdAndName : partitionNameByIds.entrySet()) {
            partitionWrittenRows.put(
                    partitionIdAndName.getKey(),
                    writeRowsToPartition(
                            conn, tablePath, Collections.singleton(partitionIdAndName.getValue())));
        }

        // try to write some rows to the table
        TestingReaderContext readerContext = new TestingReaderContext();
        try (final FlinkSourceReader reader =
                createReader(
                        clientConf,
                        tablePath,
                        tableDescriptor.getSchema().getRowType(),
                        readerContext,
                        null)) {

            // first of all, add all splits of all partitions to the reader
            Map<Long, Set<TableBucket>> assignedBuckets = new HashMap<>();
            for (Long partitionId : partitionNameByIds.keySet()) {
                for (int i = 0; i < DEFAULT_BUCKET_NUM; i++) {
                    TableBucket tableBucket = new TableBucket(tableId, partitionId, i);
                    reader.addSplits(
                            Collections.singletonList(
                                    new LogSplit(
                                            tableBucket, partitionNameByIds.get(partitionId), 0)));
                    assignedBuckets
                            .computeIfAbsent(partitionId, k -> new HashSet<>())
                            .add(tableBucket);
                }
            }

            // then, mock partition removed;
            Map<Long, String> removedPartitions = new HashMap<>();
            Set<TableBucket> unsubscribedBuckets = new HashSet<>();
            Set<Long> removedPartitionIds = new HashSet<>();
            int numberOfRemovedPartitions = 2;
            Iterator<Long> partitionIdIterator = partitionNameByIds.keySet().iterator();
            for (int i = 0; i < numberOfRemovedPartitions; i++) {
                long partitionId = partitionIdIterator.next();
                removedPartitions.put(partitionId, partitionNameByIds.get(partitionId));
                removedPartitionIds.add(partitionId);
                unsubscribedBuckets.addAll(assignedBuckets.get(partitionId));
            }
            // reader receives the partition removed event
            reader.handleSourceEvents(new PartitionsRemovedEvent(removedPartitions));

            retry(
                    Duration.ofMinutes(2),
                    () -> {
                        // check the ack event
                        PartitionBucketsUnsubscribedEvent expectedEvent =
                                new PartitionBucketsUnsubscribedEvent(unsubscribedBuckets);
                        List<SourceEvent> gotSourceEvents = readerContext.getSentEvents();
                        assertThat(gotSourceEvents).hasSize(1);
                        assertThat(gotSourceEvents).contains(expectedEvent);
                    });

            TestingReaderOutput<RowData> output = new TestingReaderOutput<>();

            // shouldn't read the rows from the partition that is removed
            List<String> expectRows = new ArrayList<>();
            for (Map.Entry<Long, List<String>> partitionIdAndWrittenRows :
                    partitionWrittenRows.entrySet()) {
                // isn't removed, should read the rows
                if (!removedPartitionIds.contains(partitionIdAndWrittenRows.getKey())) {
                    expectRows.addAll(partitionIdAndWrittenRows.getValue());
                }
            }

            while (output.getEmittedRecords().size() < expectRows.size()) {
                reader.pollNext(output);
            }

            // get the actual rows, the row format will be +I(x,x,x)
            // we need to convert to +I[x, x, x] to match the expected rows format
            List<String> actualRows =
                    output.getEmittedRecords().stream()
                            .map(Object::toString)
                            .map(row -> row.replace("(", "[").replace(")", "]").replace(",", ", "))
                            .collect(Collectors.toList());
            assertThat(actualRows).containsExactlyInAnyOrderElementsOf(expectRows);
        }
    }

    @Test
    void testOnSplitFinished() throws Exception {
        TablePath tablePath = TablePath.of(DEFAULT_DB, "test_on_split_finished");

        TableDescriptor tableDescriptor = DEFAULT_AUTO_PARTITIONED_PK_TABLE_DESCRIPTOR;
        long tableId = createTable(tablePath, tableDescriptor);

        // wait until partitions are created
        ZooKeeperClient zooKeeperClient = FLUSS_CLUSTER_EXTENSION.getZooKeeperClient();
        Map<Long, String> partitionNameByIds = waitUntilPartitions(zooKeeperClient, tablePath);

        // Get the first partition for testing
        Long testPartitionId = partitionNameByIds.keySet().iterator().next();
        String testPartitionName = partitionNameByIds.get(testPartitionId);

        // Create a reader context to capture events
        TestingReaderContext readerContext = new TestingReaderContext();
        ResolvedPartitionSpec partitionSpec =
                ResolvedPartitionSpec.fromPartitionName(
                        Collections.singletonList("date"), testPartitionName);
        PartitionInfo partitionInfo = new PartitionInfo(testPartitionId, partitionSpec);
        LakeSource<LakeSplit> lakeSource =
                new TestingLakeSource(DEFAULT_BUCKET_NUM, Collections.singletonList(partitionInfo));
        try (final FlinkSourceReader reader =
                createReader(
                        clientConf,
                        tablePath,
                        tableDescriptor.getSchema().getRowType(),
                        readerContext,
                        lakeSource)) {

            TableBucket tableBucket = new TableBucket(tableId, testPartitionId, 0);
            SourceSplitBase split =
                    new LakeSnapshotSplit(
                            tableBucket,
                            testPartitionName,
                            new TestingLakeSplit(0, Collections.singletonList(testPartitionName)),
                            0);

            reader.addSplits(Collections.singletonList(split));

            // Poll until the split is finished
            TestingReaderOutput<RowData> output = new TestingReaderOutput<>();

            while (true) {
                reader.pollNext(output);

                // Check if the event has been sent
                List<SourceEvent> sentEvents = readerContext.getSentEvents();
                if (!sentEvents.isEmpty()) {
                    break;
                }
            }

            // Verify that PartitionBucketsFinishedEvent was sent
            List<SourceEvent> sentEvents = readerContext.getSentEvents();
            assertThat(sentEvents).hasSize(1);

            SourceEvent event = sentEvents.get(0);
            assertThat(event).isInstanceOf(PartitionBucketsFinishedEvent.class);

            PartitionBucketsFinishedEvent finishedEvent = (PartitionBucketsFinishedEvent) event;
            assertThat(finishedEvent.getFinishedTableBuckets()).containsExactly(tableBucket);
        }
    }

    private FlinkSourceReader createReader(
            Configuration flussConf,
            TablePath tablePath,
            RowType sourceOutputType,
            SourceReaderContext context,
            LakeSource<LakeSplit> lakeSource)
            throws Exception {
        FutureCompletingBlockingQueue<RecordsWithSplitIds<RecordAndPos>> elementsQueue =
                new FutureCompletingBlockingQueue<>();

        RowDataDeserializationSchema deserializationSchema = new RowDataDeserializationSchema();
        deserializationSchema.open(
                new DeserializerInitContextImpl(
                        context.metricGroup().addGroup("deserializer"),
                        context.getUserCodeClassLoader(),
                        sourceOutputType));
        FlinkRecordEmitter<RowData> recordEmitter = new FlinkRecordEmitter<>(deserializationSchema);

        return new FlinkSourceReader<>(
                elementsQueue,
                flussConf,
                tablePath,
                sourceOutputType,
                context,
                null,
                new FlinkSourceReaderMetrics(context.metricGroup()),
                recordEmitter,
                lakeSource);
    }
}
