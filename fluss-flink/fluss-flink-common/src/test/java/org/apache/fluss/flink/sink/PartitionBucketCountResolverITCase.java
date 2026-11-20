/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.flink.sink;

import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.scanner.ScanRecord;
import org.apache.fluss.client.table.scanner.log.LogScanner;
import org.apache.fluss.client.table.scanner.log.ScanRecords;
import org.apache.fluss.flink.sink.serializer.RowDataSerializationSchema;
import org.apache.fluss.flink.utils.FlinkTestBase;
import org.apache.fluss.metadata.PartitionInfo;
import org.apache.fluss.metadata.ResolvedPartitionSpec;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableChange;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.types.DataTypes;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for {@link PartitionBucketCountResolver} against a real Fluss cluster.
 *
 * <p>The scenario reproduces the failure this component fixes: a partitioned table is rescaled
 * (bucket.num 4 -> 8), so pre- and post-rescale partitions with different bucket counts coexist in
 * one table. Before the fix, the bucket shuffle used the stale table-level count, which scattered
 * the records of one bucket across multiple writer subtasks and eventually broke sink recovery
 * (conflicting per-bucket offsets in the WriterState).
 */
class PartitionBucketCountResolverITCase extends FlinkTestBase {

    // Parallelism 3 does not divide either bucket count (2 % 3 != 0, 4 % 3 != 0), so both
    // the pre-rescale and the post-rescale partitions exercise the combine-mode sharding
    // formula (hash(partition) + bucket) % parallelism — the path that a fixed table-level
    // numBuckets could not cover.
    private static final int OLD_BUCKET_NUM = 2;
    private static final int NEW_BUCKET_NUM = 4;

    private static final Schema schema =
            Schema.newBuilder()
                    .column("a", DataTypes.INT())
                    .column("b", DataTypes.STRING())
                    .column("c", DataTypes.STRING())
                    .build();

    private static final TableDescriptor tableDescriptor =
            TableDescriptor.builder()
                    .schema(schema)
                    .distributedBy(OLD_BUCKET_NUM, "a")
                    .partitionedBy("c")
                    .build();

    private static final List<String> PARTITION_KEYS = Collections.singletonList("c");

    private static final int PARALLELISM = 3;

    private StreamExecutionEnvironment env;

    @BeforeEach
    void setup() throws Exception {
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(PARALLELISM);
    }

    @Test
    void testResolveBucketCountsAndCachingWithRealCluster() throws Exception {
        TablePath tablePath = TablePath.of(DEFAULT_DB, "resolver_it_" + System.currentTimeMillis());
        createTable(tablePath, tableDescriptor);
        // Old partition, created before the rescale: 2 buckets.
        createPartition(tablePath, "2024-01");
        // Rescale: partitions created afterwards use 4 buckets.
        alterBucketCount(tablePath, NEW_BUCKET_NUM);
        createPartition(tablePath, "2024-02");

        // Cold start with an empty cache: the resolver must hit real cluster metadata. A single
        // listPartitionInfos call warms up every existing partition of the table.
        CountingResolver resolver = new CountingResolver(tablePath, admin, NEW_BUCKET_NUM);

        // The pre-rescale partition keeps its own count — not the rescaled table-level value.
        assertThat(resolver.bucketCountOf("2024-01")).isEqualTo(OLD_BUCKET_NUM);
        // The post-rescale partition resolves to the new count.
        assertThat(resolver.bucketCountOf("2024-02")).isEqualTo(NEW_BUCKET_NUM);
        assertThat(resolver.listCalls.get()).isEqualTo(1);

        // Cache hits: repeated lookups never trigger further metadata fetches.
        assertThat(resolver.bucketCountOf("2024-01")).isEqualTo(OLD_BUCKET_NUM);
        assertThat(resolver.bucketCountOf("2024-02")).isEqualTo(NEW_BUCKET_NUM);
        assertThat(resolver.listCalls.get()).isEqualTo(1);

        // A partition that doesn't exist yet falls back to the current table-level count; the
        // miss costs one list call (to confirm the partition is absent) plus one table lookup.
        assertThat(resolver.bucketCountOf("2024-03")).isEqualTo(NEW_BUCKET_NUM);
        assertThat(resolver.listCalls.get()).isEqualTo(2);
        assertThat(resolver.tableCalls.get()).isEqualTo(1);

        // The fallback matches the count the partition actually gets once created.
        createPartition(tablePath, "2024-03");
        assertThat(bucketCountByPartitionName(tablePath)).containsEntry("2024-03", NEW_BUCKET_NUM);

        // The fallback value is cached: repeated lookups of the same partition cost nothing.
        assertThat(resolver.bucketCountOf("2024-03")).isEqualTo(NEW_BUCKET_NUM);
        assertThat(resolver.listCalls.get()).isEqualTo(2);
        assertThat(resolver.tableCalls.get()).isEqualTo(1);
    }

    /**
     * Core end-to-end verification: after a rescale, writing through the real sink topology must
     * aggregate one bucket into exactly one writer subtask, for pre- and post-rescale partitions
     * alike. Before the fix, the stale table-level count scattered the rows of one bucket across
     * multiple subtasks, which made the WriterState conflict and the sink unrecoverable.
     */
    @Test
    void testBucketShuffleAggregatesOneBucketToOneSubtaskAfterRescale() throws Exception {
        TablePath tablePath = TablePath.of(DEFAULT_DB, "shuffle_it_" + System.currentTimeMillis());
        createTable(tablePath, tableDescriptor);
        createPartition(tablePath, "2024-01");
        alterBucketCount(tablePath, NEW_BUCKET_NUM);
        createPartition(tablePath, "2024-02");

        List<PartitionInfo> partitionInfos = admin.listPartitionInfos(tablePath).get();
        Map<String, Integer> countByPartitionName = bucketCountByPartitionName(tablePath);
        Map<Long, String> partitionNameById = partitionNameById(partitionInfos);

        // The production-shaped channel computer backed by real cluster metadata.
        RowDataSerializationSchema serializationSchema =
                new RowDataSerializationSchema(true, false);
        FlinkRowDataChannelComputer<RowData> channelComputer =
                new FlinkRowDataChannelComputer<>(
                        schema.getRowType(),
                        Collections.singletonList("a"),
                        PARTITION_KEYS,
                        null,
                        tablePath,
                        clientConf,
                        NEW_BUCKET_NUM,
                        serializationSchema);
        channelComputer.setup(PARALLELISM);

        // Rows for both partitions: 10 distinct bucket keys, each submitted twice, so the sharded
        // topology has to aggregate duplicates of one bucket into the same writer subtask.
        List<RowData> rows = new ArrayList<>();
        Map<String, Integer> channelByRowKey = new HashMap<>();
        for (String partitionName : Arrays.asList("2024-01", "2024-02")) {
            for (int a = 0; a < 10; a++) {
                for (int dup = 0; dup < 2; dup++) {
                    RowData row =
                            org.apache.flink.table.data.GenericRowData.of(
                                    a,
                                    org.apache.flink.table.data.StringData.fromString("v" + a),
                                    org.apache.flink.table.data.StringData.fromString(
                                            partitionName));
                    rows.add(row);
                    channelByRowKey.put(rowKey(row), channelComputer.channel(row));
                }
            }
        }

        // Write through the real sink topology: the partitioner inside it calls the same
        // channel logic, so the recorded channels match the actual writer assignment.
        FlussSink<RowData> flussSink =
                FlussSink.<RowData>builder()
                        .setBootstrapServers(bootstrapServers)
                        .setDatabase(DEFAULT_DB)
                        .setTable(tablePath.getTableName())
                        .setSerializationSchema(serializationSchema)
                        .build();
        DataStream<RowData> stream = env.fromElements(rows.toArray(new RowData[0]));
        stream.sinkTo(flussSink).name("Fluss Sink");
        env.execute("test bucket shuffle aggregates one bucket to one subtask after rescale");

        // Scan every bucket of every partition and map each row back to its sharding channel.
        Table table = conn.getTable(tablePath);
        LogScanner logScanner = table.newScan().createLogScanner();
        for (PartitionInfo info : partitionInfos) {
            for (int b = 0; b < info.getBucketCount(); b++) {
                logScanner.subscribeFromBeginning(info.getPartitionId(), b);
            }
        }

        Map<TableBucket, Set<Integer>> channelsPerBucket = new HashMap<>();
        int collected = 0;
        long deadline = System.currentTimeMillis() + 60_000;
        while (collected < rows.size() && System.currentTimeMillis() < deadline) {
            ScanRecords scanRecords = logScanner.poll(Duration.ofSeconds(1));
            for (TableBucket bucket : scanRecords.buckets()) {
                for (ScanRecord record : scanRecords.records(bucket)) {
                    Integer channel = channelByRowKey.get(rowKey(record.getRow()));
                    assertThat(channel).isNotNull();
                    channelsPerBucket.computeIfAbsent(bucket, k -> new HashSet<>()).add(channel);
                    collected++;
                }
            }
        }
        logScanner.close();

        // Data completeness: every submitted row landed exactly once.
        assertThat(collected).isEqualTo(rows.size());

        // Core invariant: every bucket is written by exactly one writer subtask — all rows of a
        // bucket share one sharding channel, and the channel matches the sharding formula derived
        // from the partition's own bucket count (4 for the pre-rescale partition, 8 for the new).
        for (Map.Entry<TableBucket, Set<Integer>> entry : channelsPerBucket.entrySet()) {
            TableBucket bucket = entry.getKey();
            String partitionName = partitionNameById.get(bucket.getPartitionId());
            int bucketCount = countByPartitionName.get(partitionName);
            int expectedChannel;
            if (ChannelComputer.shouldCombinePartitionInSharding(true, bucketCount, PARALLELISM)) {
                expectedChannel =
                        ChannelComputer.select(partitionName, bucket.getBucket(), PARALLELISM);
            } else {
                expectedChannel = ChannelComputer.select(bucket.getBucket(), PARALLELISM);
            }
            assertThat(entry.getValue())
                    .as("bucket %s of partition %s", bucket.getBucket(), partitionName)
                    .containsExactly(expectedChannel);
        }
    }

    // --------------------------------------------------------------------------------------------

    private static String rowKey(org.apache.flink.table.data.RowData row) {
        return row.getInt(0) + "#" + row.getString(2).toString();
    }

    private static String rowKey(org.apache.fluss.row.InternalRow row) {
        return row.getInt(0) + "#" + row.getString(2).toString();
    }

    private void createPartition(TablePath tablePath, String partitionName) throws Exception {
        admin.createPartition(
                        tablePath,
                        ResolvedPartitionSpec.fromPartitionName(PARTITION_KEYS, partitionName)
                                .toPartitionSpec(),
                        false)
                .get();
    }

    private void alterBucketCount(TablePath tablePath, int newBucketCount) throws Exception {
        admin.alterTable(
                        tablePath,
                        Collections.singletonList(TableChange.modifyBucketCount(newBucketCount)),
                        false)
                .get();
    }

    private Map<String, Integer> bucketCountByPartitionName(TablePath tablePath) throws Exception {
        Map<String, Integer> map = new HashMap<>();
        for (PartitionInfo info : admin.listPartitionInfos(tablePath).get()) {
            map.put(info.getPartitionName(), info.getBucketCount());
        }
        return map;
    }

    private Map<Long, String> partitionNameById(List<PartitionInfo> partitionInfos) {
        Map<Long, String> map = new HashMap<>();
        for (PartitionInfo info : partitionInfos) {
            map.put(info.getPartitionId(), info.getPartitionName());
        }
        return map;
    }

    /** Resolver variant that delegates to the test cluster's admin and counts metadata calls. */
    private static final class CountingResolver extends PartitionBucketCountResolver {

        private static final long serialVersionUID = 1L;

        private final Admin admin;
        private final AtomicInteger listCalls = new AtomicInteger();
        private final AtomicInteger tableCalls = new AtomicInteger();

        private CountingResolver(TablePath tablePath, Admin admin, int defaultBucketCount) {
            super(tablePath, null, defaultBucketCount);
            this.admin = admin;
        }

        @Override
        protected PartitionMetadataFetcher fetcher() {
            return new PartitionMetadataFetcher() {
                @Override
                public List<PartitionInfo> listPartitionInfos(TablePath tablePath)
                        throws Exception {
                    listCalls.incrementAndGet();
                    return admin.listPartitionInfos(tablePath).get();
                }

                @Override
                public int tableBucketCount(TablePath tablePath) throws Exception {
                    tableCalls.incrementAndGet();
                    return admin.getTableInfo(tablePath).get().getNumBuckets();
                }
            };
        }
    }
}
