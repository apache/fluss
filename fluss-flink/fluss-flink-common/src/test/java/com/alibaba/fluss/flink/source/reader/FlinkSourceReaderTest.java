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

package com.alibaba.fluss.flink.source.reader;

import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.flink.source.deserializer.DeserializerInitContextImpl;
import com.alibaba.fluss.flink.source.deserializer.RowDataDeserializationSchema;
import com.alibaba.fluss.flink.source.emitter.FlinkRecordEmitter;
import com.alibaba.fluss.flink.source.event.PartitionBucketsUnsubscribedEvent;
import com.alibaba.fluss.flink.source.event.PartitionsRemovedEvent;
import com.alibaba.fluss.flink.source.metrics.FlinkSourceReaderMetrics;
import com.alibaba.fluss.flink.source.split.LogSplit;
import com.alibaba.fluss.flink.source.testutils.FlinkTestBase;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.metadata.TableDescriptor;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.server.zk.ZooKeeperClient;
import com.alibaba.fluss.types.RowType;

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

import static com.alibaba.fluss.testutils.common.CommonTestUtils.retry;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link FlinkSourceReader}. */
class FlinkSourceReaderTest extends FlinkTestBase {

    @Test
    void testHandlePartitionsRemovedEvent() throws Exception {
        TablePath tablePath = TablePath.of(DEFAULT_DB, "test_partitioned_table");

        TableDescriptor tableDescriptor = DEFAULT_AUTO_PARTITIONED_PK_TABLE_DESCRIPTOR;
        long tableId = createTable(tablePath, tableDescriptor);

        // wait util partitions are created
        ZooKeeperClient zooKeeperClient = FLUSS_CLUSTER_EXTENSION.getZooKeeperClient();
        Map<Long, String> partitionNameByIds = waitUntilPartitions(zooKeeperClient, tablePath);

        // now, write rows to the table
        Map<Long, List<String>> partitionWrittenRows = new HashMap<>();
        for (Map.Entry<Long, String> partitionIdAndName : partitionNameByIds.entrySet()) {
            partitionWrittenRows.put(
                    partitionIdAndName.getKey(),
                    writeRowsToPartition(
                            tablePath, Collections.singleton(partitionIdAndName.getValue())));
        }

        // try to write some rows to the table
        TestingReaderContext readerContext = new TestingReaderContext();
        try (final FlinkSourceReader reader =
                createReader(
                        clientConf,
                        tablePath,
                        tableDescriptor.getSchema().getRowType(),
                        readerContext)) {

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

    private FlinkSourceReader createReader(
            Configuration flussConf,
            TablePath tablePath,
            RowType sourceOutputType,
            SourceReaderContext context)
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
                recordEmitter);
    }
}
