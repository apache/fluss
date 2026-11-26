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

package org.apache.fluss.client.table.scanner.log;

import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.FlussConnection;
import org.apache.fluss.client.admin.ClientToServerITCaseBase;
import org.apache.fluss.client.admin.OffsetMetadata;
import org.apache.fluss.client.admin.OffsetSpec;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.scanner.MultiTableRecord;
import org.apache.fluss.client.table.scanner.ScanRecord;
import org.apache.fluss.client.table.scanner.log.internals.ConsumerGroupClient;
import org.apache.fluss.client.table.writer.AppendWriter;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.InvalidConfigException;
import org.apache.fluss.metadata.LogFormat;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.ArrowBatchData;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static org.apache.fluss.testutils.common.CommonTestUtils.waitUntil;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** End-to-end tests for committing, resuming, and resetting log scanner offsets. */
class ConsumerOffsetLogScannerITCase extends ClientToServerITCaseBase {

    private static final Schema SCHEMA =
            Schema.newBuilder()
                    .column("id", DataTypes.INT())
                    .column("value", DataTypes.STRING())
                    .build();

    @Test
    void testCommitAndResumeFromCommittedOffset() throws Exception {
        TablePath tablePath = uniqueTablePath("resume");
        long tableId = createTable(tablePath, descriptor(1), false);
        appendRows(tablePath, 5);
        String groupId = "scanner-resume-" + System.nanoTime();
        TableBucket tableBucket = new TableBucket(tableId, null, 0);

        try (Connection groupConnection = groupConnection(groupId, false);
                Table table = groupConnection.getTable(tablePath);
                LogScanner scanner = table.newScan().createLogScanner()) {
            scanner.subscribeFromBeginning(0);
            scanner.commitSync(
                    Collections.singletonMap(tableBucket, new OffsetMetadata(1L, "warmup")));
            Map<TableBucket, OffsetMetadata> checkpoint =
                    Collections.singletonMap(tableBucket, new OffsetMetadata(3L, "checkpoint"));
            assertThat(
                            awaitCommit(
                                    callback -> scanner.commitAsync(checkpoint, callback),
                                    () -> scanner.commitSync(Collections.emptyMap())))
                    .isEqualTo(checkpoint);
        }

        try (Connection groupConnection = groupConnection(groupId, false);
                Table table = groupConnection.getTable(tablePath);
                LogScanner scanner = table.newScan().createLogScanner()) {
            scanner.subscribeFromCommittedOffset(0);
            assertThat(pollIds(scanner, 2)).containsExactly(3, 4);
        }
    }

    @Test
    void testCloseAutoCommitsCurrentPositionWhenIntervalElapses() throws Exception {
        TablePath tablePath = uniqueTablePath("close");
        long tableId = createTable(tablePath, descriptor(1), false);
        appendRows(tablePath, 10);
        String groupId = "scanner-close-" + System.nanoTime();
        TableBucket tableBucket = new TableBucket(tableId, null, 0);

        Duration interval = Duration.ofMillis(1);
        Configuration configuration =
                new Configuration(clientConf)
                        .set(ConfigOptions.CLIENT_CONSUMER_GROUP_ID, groupId)
                        .set(ConfigOptions.CLIENT_CONSUMER_AUTO_COMMIT_ENABLED, true)
                        .set(ConfigOptions.CLIENT_CONSUMER_AUTO_COMMIT_INTERVAL, interval);
        try (Connection groupConnection = ConnectionFactory.createConnection(configuration);
                Table table = groupConnection.getTable(tablePath);
                LogScanner scanner = table.newScan().createLogScanner()) {
            long autoCommitDueMs = System.currentTimeMillis() + interval.toMillis();
            scanner.subscribe(0, 0L);
            scanner.commitSync(
                    Collections.singletonMap(tableBucket, new OffsetMetadata(1L, "warmup")));
            scanner.subscribe(0, 7L);
            waitUntil(
                    () -> System.currentTimeMillis() >= autoCommitDueMs,
                    Duration.ofSeconds(5),
                    Duration.ofMillis(1),
                    "Auto-commit interval did not elapse before close");
        }

        try (Connection groupConnection = groupConnection(groupId, false);
                Table table = groupConnection.getTable(tablePath);
                LogScanner scanner = table.newScan().createLogScanner()) {
            scanner.subscribeFromCommittedOffset(0);
            assertThat(pollIds(scanner, 3)).containsExactly(7, 8, 9);
        }
    }

    @Test
    void testCommitSyncWithoutExplicitOffsetsResumesAfterConsumption() throws Exception {
        TablePath tablePath = uniqueTablePath("sync");
        createTable(tablePath, descriptor(1), false);
        appendRows(tablePath, 3);
        String groupId = "scanner-sync-" + System.nanoTime();

        try (Connection groupConnection = groupConnection(groupId, false);
                Table table = groupConnection.getTable(tablePath);
                LogScanner scanner = table.newScan().createLogScanner()) {
            scanner.subscribeFromBeginning(0);
            assertThat(pollIds(scanner, 3)).containsExactly(0, 1, 2);
            scanner.commitSync();
        }

        appendRows(tablePath, 3, 2);
        try (Connection groupConnection = groupConnection(groupId, false);
                Table table = groupConnection.getTable(tablePath);
                LogScanner scanner = table.newScan().createLogScanner()) {
            scanner.subscribeFromCommittedOffset(0);
            assertThat(pollIds(scanner, 2)).containsExactly(3, 4);
        }
    }

    @Test
    void testCommitAsyncWithoutExplicitOffsetsWithAndWithoutCallback() throws Exception {
        TablePath tablePath = uniqueTablePath("async");
        long tableId = createTable(tablePath, descriptor(1), false);
        appendRows(tablePath, 3);
        String groupId = "scanner-async-" + System.nanoTime();
        TableBucket bucket = new TableBucket(tableId, 0);

        try (FlussConnection groupConnection = (FlussConnection) groupConnection(groupId, false);
                Table table = groupConnection.getTable(tablePath);
                LogScanner scanner = table.newScan().createLogScanner();
                ConsumerGroupClient offsetReader =
                        new ConsumerGroupClient(
                                groupConnection.getConfiguration(),
                                groupConnection.getMetadataUpdater(),
                                new LogScannerStatus())) {
            scanner.subscribeFromBeginning(0);
            // Wait for sys.consumer_offsets to be ready before issuing a single asynchronous
            // commit.
            scanner.commitSync(Collections.singletonMap(bucket, new OffsetMetadata(0L)));
            assertThat(pollIds(scanner, 3)).containsExactly(0, 1, 2);

            Map<TableBucket, OffsetMetadata> committed =
                    awaitCommit(
                            scanner::commitAsync, () -> scanner.commitSync(Collections.emptyMap()));
            assertThat(committed).containsEntry(bucket, new OffsetMetadata(3L));

            appendRows(tablePath, 3, 2);
            assertThat(pollIds(scanner, 2)).containsExactly(3, 4);
            scanner.commitAsync();
            waitUntil(
                    () -> {
                        Map<TableBucket, OffsetMetadata> stored =
                                offsetReader.fetchCommittedOffsets(
                                        Collections.singleton(bucket), 1_000L);
                        return stored != null && new OffsetMetadata(5L).equals(stored.get(bucket));
                    },
                    Duration.ofSeconds(30),
                    Duration.ofMillis(100),
                    "Asynchronous commit without callback did not persist the consumed offset");
        }

        appendRows(tablePath, 5, 1);
        try (Connection groupConnection = groupConnection(groupId, false);
                Table table = groupConnection.getTable(tablePath);
                LogScanner scanner = table.newScan().createLogScanner()) {
            scanner.subscribeFromCommittedOffset(0);
            assertThat(pollIds(scanner, 1)).containsExactly(5);
        }
    }

    @Test
    void testMultiTableCommitsAndResumesBothTables() throws Exception {
        TablePath first = uniqueTablePath("multi_first");
        TablePath second = uniqueTablePath("multi_second");
        long firstId = createTable(first, descriptor(1), false);
        long secondId = createTable(second, descriptor(1), true);
        appendRows(first, 3);
        appendRows(second, 3);
        String groupId = "scanner-multi-" + System.nanoTime();

        try (Connection groupConnection = groupConnection(groupId, false);
                MultiTableLogScanner scanner =
                        groupConnection.getMultiTable().newMultiTableScan().createLogScanner()) {
            scanner.subscribeFromBeginning(first, 0);
            scanner.subscribeFromBeginning(second, 0);
            Map<TablePath, List<Integer>> ids = pollMultiTableIds(scanner, 6, first, second);
            assertThat(ids.get(first)).containsExactly(0, 1, 2);
            assertThat(ids.get(second)).containsExactly(0, 1, 2);
            scanner.commitSync();
        }

        appendRows(first, 3, 1);
        appendRows(second, 3, 1);
        try (Connection groupConnection = groupConnection(groupId, false);
                MultiTableLogScanner scanner =
                        groupConnection.getMultiTable().newMultiTableScan().createLogScanner()) {
            scanner.subscribeFromCommittedOffset(first, 0);
            scanner.subscribeFromCommittedOffset(second, 0);
            Map<TablePath, List<Integer>> ids = pollMultiTableIds(scanner, 2, first, second);
            assertThat(ids.get(first)).containsExactly(3);
            assertThat(ids.get(second)).containsExactly(3);

            Map<TableBucket, OffsetMetadata> committed =
                    awaitCommit(
                            scanner::commitAsync, () -> scanner.commitSync(Collections.emptyMap()));
            assertThat(committed)
                    .containsEntry(new TableBucket(firstId, 0), new OffsetMetadata(4L))
                    .containsEntry(new TableBucket(secondId, 0), new OffsetMetadata(4L));
        }

        appendRows(first, 4, 1);
        appendRows(second, 4, 1);
        try (Connection groupConnection = groupConnection(groupId, false);
                MultiTableLogScanner scanner =
                        groupConnection.getMultiTable().newMultiTableScan().createLogScanner()) {
            scanner.subscribeFromCommittedOffset(first, 0);
            scanner.subscribeFromCommittedOffset(second, 0);
            Map<TablePath, List<Integer>> ids = pollMultiTableIds(scanner, 2, first, second);
            assertThat(ids.get(first)).containsExactly(4);
            assertThat(ids.get(second)).containsExactly(4);
        }
    }

    @Test
    void testPartitionedTableResumesWithSingleAndMultiTableScanners() throws Exception {
        TablePath tablePath = uniqueTablePath("partitioned");
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("value", DataTypes.STRING())
                        .column("pt", DataTypes.STRING())
                        .build();
        long tableId =
                createTable(
                        tablePath,
                        TableDescriptor.builder()
                                .schema(schema)
                                .distributedBy(1)
                                .partitionedBy("pt")
                                .build(),
                        false);
        admin.createPartition(tablePath, newPartitionSpec("pt", "p1"), false).get();
        long partitionId = admin.listPartitionInfos(tablePath).get().get(0).getPartitionId();
        FLUSS_CLUSTER_EXTENSION.waitUntilAllReplicaReady(new TableBucket(tableId, partitionId, 0));
        appendPartitionRows(tablePath, 0, 4);
        String groupId = "scanner-partitioned-" + System.nanoTime();

        try (Connection groupConnection = groupConnection(groupId, false);
                Table table = groupConnection.getTable(tablePath);
                LogScanner scanner = table.newScan().createLogScanner()) {
            scanner.subscribe(partitionId, 0, 0L);
            scanner.commitSync(
                    Collections.singletonMap(
                            new TableBucket(tableId, partitionId, 0), new OffsetMetadata(2L)));
        }

        try (Connection groupConnection = groupConnection(groupId, false);
                Table table = groupConnection.getTable(tablePath);
                LogScanner scanner = table.newScan().createLogScanner()) {
            scanner.subscribeFromCommittedOffset(partitionId, 0);
            assertThat(pollIds(scanner, 2)).containsExactly(2, 3);
        }

        try (Connection groupConnection = groupConnection(groupId, false);
                MultiTableLogScanner scanner =
                        groupConnection.getMultiTable().newMultiTableScan().createLogScanner()) {
            scanner.subscribeFromCommittedOffset(tablePath, partitionId, 0);
            Map<TablePath, List<Integer>> ids = pollMultiTableIds(scanner, 2, tablePath);
            assertThat(ids.get(tablePath)).containsExactly(2, 3);
            scanner.commitSync();
        }

        appendPartitionRows(tablePath, 4, 1);
        try (Connection groupConnection = groupConnection(groupId, false);
                Table table = groupConnection.getTable(tablePath);
                LogScanner scanner = table.newScan().createLogScanner()) {
            scanner.subscribeFromCommittedOffset(partitionId, 0);
            assertThat(pollIds(scanner, 1)).containsExactly(4);
        }
    }

    @Test
    void testExplicitAutoCommitRequiresConsumerGroup() throws Exception {
        TablePath tablePath = uniqueTablePath("config");
        createTable(tablePath, descriptor(1), false);
        Configuration configuration = new Configuration(clientConf);
        configuration.setBoolean(ConfigOptions.CLIENT_CONSUMER_AUTO_COMMIT_ENABLED.key(), true);

        try (Connection connection = ConnectionFactory.createConnection(configuration);
                Table table = connection.getTable(tablePath)) {
            assertThatThrownBy(() -> table.newScan().createLogScanner())
                    .isInstanceOf(InvalidConfigException.class)
                    .hasMessageContaining(ConfigOptions.CLIENT_CONSUMER_GROUP_ID.key());
            assertThatThrownBy(
                            () -> connection.getMultiTable().newMultiTableScan().createLogScanner())
                    .isInstanceOf(InvalidConfigException.class)
                    .hasMessageContaining(ConfigOptions.CLIENT_CONSUMER_GROUP_ID.key());
        }
    }

    @Test
    void testNewGroupWithLatestResetStartsFromLatestPosition() throws Exception {
        TablePath tablePath = uniqueTablePath("latest");
        TableBucket bucket = new TableBucket(createTable(tablePath, descriptor(1), false), 0);
        appendRows(tablePath, 3);
        awaitHighWatermark(tablePath, 3L);
        String groupId = "scanner-latest-" + System.nanoTime();

        Configuration configuration = new Configuration(clientConf);
        configuration.setString(ConfigOptions.CLIENT_CONSUMER_GROUP_ID.key(), groupId);
        configuration.setBoolean(ConfigOptions.CLIENT_CONSUMER_AUTO_COMMIT_ENABLED.key(), false);
        configuration.setString(ConfigOptions.CLIENT_CONSUMER_AUTO_OFFSET_RESET.key(), "latest");

        try (Connection groupConnection = ConnectionFactory.createConnection(configuration);
                Table table = groupConnection.getTable(tablePath);
                LogScannerImpl scanner = (LogScannerImpl) table.newScan().createLogScanner()) {
            scanner.subscribeFromCommittedOffset(0);
            awaitLatestPosition(scanner, bucket, 3L, false);

            appendRows(tablePath, 3, 1);
            assertThat(pollIds(scanner, 1)).containsExactly(3);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testExplicitLatestOffsetSubscriptionStartsFromLatestPosition(boolean arrowBatches)
            throws Exception {
        TablePath tablePath = uniqueTablePath("explicit-latest-" + arrowBatches);
        long tableId =
                createTable(
                        tablePath,
                        TableDescriptor.builder()
                                .schema(SCHEMA)
                                .distributedBy(1)
                                .logFormat(LogFormat.ARROW)
                                .build(),
                        false);
        TableBucket bucket = new TableBucket(tableId, 0);
        appendRows(tablePath, 3);
        awaitHighWatermark(tablePath, 3L);

        try (Connection connection = ConnectionFactory.createConnection(clientConf);
                Table table = connection.getTable(tablePath);
                LogScannerImpl scanner = (LogScannerImpl) table.newScan().createLogScanner()) {
            scanner.subscribe(0, LogScanner.LATEST_OFFSET);
            awaitLatestPosition(scanner, bucket, 3L, arrowBatches);
            appendRows(tablePath, 3, 1);
            if (arrowBatches) {
                waitUntil(
                        () -> {
                            try (ArrowScanRecords records =
                                    scanner.pollRecordBatch(Duration.ofMillis(200))) {
                                if (records.count() == 0) {
                                    return false;
                                }
                                assertThat(records.count()).isOne();
                                ArrowBatchData batch = records.iterator().next();
                                assertThat(batch.getBaseLogOffset()).isEqualTo(3L);
                                assertThat(batch.getVectorSchemaRoot().getVector(0).getObject(0))
                                        .isEqualTo(3);
                                return true;
                            }
                        },
                        Duration.ofSeconds(30),
                        Duration.ofMillis(10),
                        "Appended Arrow record was not returned");
            } else {
                assertThat(pollIds(scanner, 1)).containsExactly(3);
            }
        }
    }

    private void awaitHighWatermark(TablePath tablePath, long expected) {
        // An acknowledged append can precede the high watermark observed by a latest reset.
        waitUntil(
                () ->
                        admin.listOffsets(
                                                tablePath,
                                                Collections.singleton(0),
                                                new OffsetSpec.LatestSpec())
                                        .bucketResult(0)
                                        .get(5, TimeUnit.SECONDS)
                                == expected,
                Duration.ofSeconds(30),
                Duration.ofMillis(100),
                "Appended records did not become visible");
    }

    private static void awaitLatestPosition(
            LogScannerImpl scanner, TableBucket bucket, long expected, boolean arrowBatches) {
        waitUntil(
                () -> {
                    if (arrowBatches) {
                        try (ArrowScanRecords records = scanner.pollRecordBatch(Duration.ZERO)) {
                            assertThat(records.count()).isZero();
                        }
                    } else {
                        assertThat(scanner.poll(Duration.ZERO).isEmpty()).isTrue();
                    }
                    return Long.valueOf(expected)
                            .equals(scanner.logScannerStatus.getBucketOffset(bucket));
                },
                Duration.ofSeconds(30),
                Duration.ofMillis(10),
                "Latest offset was not resolved");
    }

    private Connection groupConnection(String groupId, boolean autoCommit) {
        Configuration configuration = new Configuration(clientConf);
        configuration.setString(ConfigOptions.CLIENT_CONSUMER_GROUP_ID.key(), groupId);
        configuration.setBoolean(
                ConfigOptions.CLIENT_CONSUMER_AUTO_COMMIT_ENABLED.key(), autoCommit);
        return ConnectionFactory.createConnection(configuration);
    }

    private static Map<TableBucket, OffsetMetadata> awaitCommit(
            Consumer<OffsetCommitCallback> submit, Runnable invokeCallbacks) throws Exception {
        CompletableFuture<Map<TableBucket, OffsetMetadata>> result = new CompletableFuture<>();
        submit.accept(
                (offsets, error) -> {
                    if (error == null) {
                        result.complete(offsets);
                    } else {
                        result.completeExceptionally(error);
                    }
                });
        waitUntil(
                () -> {
                    invokeCallbacks.run();
                    return result.isDone();
                },
                Duration.ofSeconds(35),
                Duration.ofMillis(100),
                "Asynchronous commit callback was not delivered");
        return result.get();
    }

    private void appendRows(TablePath tablePath, int count) throws Exception {
        appendRows(tablePath, 0, count);
    }

    private void appendRows(TablePath tablePath, int firstId, int count) throws Exception {
        try (Table table = conn.getTable(tablePath)) {
            AppendWriter writer = table.newAppend().createWriter();
            for (int i = 0; i < count; i++) {
                int id = firstId + i;
                writer.append(GenericRow.of(id, BinaryString.fromString("v" + id)));
            }
            writer.flush();
        }
    }

    private void appendPartitionRows(TablePath tablePath, int firstId, int count) throws Exception {
        try (Table table = conn.getTable(tablePath)) {
            AppendWriter writer = table.newAppend().createWriter();
            for (int i = 0; i < count; i++) {
                int id = firstId + i;
                writer.append(
                        GenericRow.of(
                                id,
                                BinaryString.fromString("v" + id),
                                BinaryString.fromString("p1")));
            }
            writer.flush();
        }
    }

    private static List<Integer> pollIds(LogScanner scanner, int count) throws Exception {
        List<Integer> ids = new ArrayList<>();
        long deadline = System.currentTimeMillis() + Duration.ofSeconds(15).toMillis();
        while (ids.size() < count && System.currentTimeMillis() < deadline) {
            for (ScanRecord record : scanner.poll(Duration.ofMillis(200))) {
                ids.add(record.getRow().getInt(0));
            }
        }
        assertThat(ids).hasSize(count);
        return ids;
    }

    private static Map<TablePath, List<Integer>> pollMultiTableIds(
            MultiTableLogScanner scanner, int count, TablePath... tablePaths) throws Exception {
        Map<TablePath, List<Integer>> ids = new HashMap<>();
        long deadline = System.currentTimeMillis() + Duration.ofSeconds(15).toMillis();
        int received = 0;
        while (received < count && System.currentTimeMillis() < deadline) {
            for (MultiTableRecord record : scanner.poll(Duration.ofMillis(200))) {
                ids.computeIfAbsent(record.getTablePath(), ignored -> new ArrayList<>())
                        .add(record.getRow().getInt(0));
                received++;
            }
        }
        assertThat(received).isEqualTo(count);
        assertThat(ids.keySet()).containsExactlyInAnyOrderElementsOf(Arrays.asList(tablePaths));
        return ids;
    }

    private static TableDescriptor descriptor(int buckets) {
        return TableDescriptor.builder().schema(SCHEMA).distributedBy(buckets, "id").build();
    }

    private static TablePath uniqueTablePath(String suffix) {
        return TablePath.of("consumer_offset_it", suffix + "_" + System.nanoTime());
    }
}
