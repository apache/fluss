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
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.scanner.MultiTableRecord;
import org.apache.fluss.client.table.scanner.ScanRecord;
import org.apache.fluss.client.table.scanner.log.internals.ConsumerConfig;
import org.apache.fluss.client.table.scanner.log.internals.ConsumerGroupClient;
import org.apache.fluss.client.table.writer.AppendWriter;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.InvalidConfigException;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.fluss.testutils.common.CommonTestUtils.waitUntil;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * End-to-end tests for consumer offset commit and resume via {@link LogScanner}.
 *
 * <p>These tests verify commit/resume behavior purely through the {@link LogScanner} public API:
 * commit offsets on one scanner, then open a fresh scanner with {@code
 * subscribeFromCommittedOffset} and verify that it resumes from the committed position.
 */
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

        // Commit offsets 0..2 (3 records consumed), then close the scanner.
        try (Connection groupConnection = groupConnection(groupId, false);
                Table table = groupConnection.getTable(tablePath);
                LogScanner scanner = table.newScan().createLogScanner()) {
            scanner.subscribeFromBeginning(0);
            commitAsyncWithRetry(
                    scanner,
                    Collections.singletonMap(tableBucket, new OffsetMetadata(1L, "warmup")));
            commitSyncWithRetry(
                    scanner,
                    Collections.singletonMap(tableBucket, new OffsetMetadata(3L, "checkpoint")));
        }

        // Open a fresh scanner and resume from the committed offset.
        // If the commit failed, subscribeFromCommittedOffset would start from the beginning
        // and we would get 5 records instead of 2.
        try (Connection groupConnection = groupConnection(groupId, false);
                Table table = groupConnection.getTable(tablePath);
                LogScanner scanner = table.newScan().createLogScanner()) {
            scanner.subscribeFromCommittedOffset(0);
            int records = 0;
            long deadline = System.currentTimeMillis() + 10_000L;
            while (records < 2 && System.currentTimeMillis() < deadline) {
                ScanRecords scanRecords = scanner.poll(Duration.ofMillis(200));
                for (ScanRecord ignored : scanRecords) {
                    records++;
                }
            }
            assertThat(records).isEqualTo(2);
        }
    }

    @Test
    void testClosePerformsFinalAutoCommit() throws Exception {
        TablePath tablePath = uniqueTablePath("close");
        long tableId = createTable(tablePath, descriptor(1), false);
        appendRows(tablePath, 10);
        String groupId = "scanner-close-" + System.nanoTime();
        TableBucket tableBucket = new TableBucket(tableId, null, 0);

        // Subscribe from offset 0, then advance to offset 7 and close.
        // Auto-commit is enabled, so close() should persist the consumed position.
        try (Connection groupConnection = groupConnection(groupId, true);
                Table table = groupConnection.getTable(tablePath);
                LogScanner scanner = table.newScan().createLogScanner()) {
            scanner.subscribe(0, 0L);
            commitAsyncWithRetry(
                    scanner,
                    Collections.singletonMap(tableBucket, new OffsetMetadata(1L, "warmup")));
            scanner.subscribe(0, 7L);
        }

        // Open a fresh scanner and resume from the committed offset.
        // If close() did not auto-commit, subscribeFromCommittedOffset would start from
        // the warmup offset (1) and we would get 9 records instead of 3.
        try (Connection groupConnection = groupConnection(groupId, false);
                Table table = groupConnection.getTable(tablePath);
                LogScanner scanner = table.newScan().createLogScanner()) {
            scanner.subscribeFromCommittedOffset(0);
            int records = 0;
            long deadline = System.currentTimeMillis() + 10_000L;
            while (System.currentTimeMillis() < deadline) {
                ScanRecords scanRecords = scanner.poll(Duration.ofMillis(200));
                for (ScanRecord ignored : scanRecords) {
                    records++;
                }
                if (records >= 3) {
                    break;
                }
            }
            assertThat(records).isEqualTo(3);
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

        try (Connection groupConnection = groupConnection(groupId, false);
                Table table = groupConnection.getTable(tablePath);
                LogScanner scanner = table.newScan().createLogScanner();
                ConsumerGroupClient offsetReader =
                        new ConsumerGroupClient(
                                groupId,
                                ((FlussConnection) groupConnection).getMetadataUpdater())) {
            scanner.subscribeFromBeginning(0);
            assertThat(pollIds(scanner, 3)).containsExactly(0, 1, 2);

            Map<TableBucket, OffsetMetadata> committed = commitAsyncWithRetry(scanner);
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

            CountDownLatch callback = new CountDownLatch(1);
            AtomicReference<Map<TableBucket, OffsetMetadata>> committed = new AtomicReference<>();
            AtomicReference<Exception> failure = new AtomicReference<>();
            scanner.commitAsync(
                    (offsets, error) -> {
                        committed.set(offsets);
                        failure.set(error);
                        callback.countDown();
                    });
            assertThat(callback.await(30, TimeUnit.SECONDS)).isTrue();
            assertThat(failure.get()).isNull();
            assertThat(committed.get())
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
            commitSyncWithRetry(
                    scanner,
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
        configuration.setBoolean(ConsumerConfig.ENABLE_AUTO_COMMIT.key(), true);

        try (Connection connection = ConnectionFactory.createConnection(configuration);
                Table table = connection.getTable(tablePath)) {
            assertThatThrownBy(() -> table.newScan().createLogScanner())
                    .isInstanceOf(InvalidConfigException.class)
                    .hasMessageContaining(ConsumerConfig.GROUP_ID.key());
            assertThatThrownBy(
                            () -> connection.getMultiTable().newMultiTableScan().createLogScanner())
                    .isInstanceOf(InvalidConfigException.class)
                    .hasMessageContaining(ConsumerConfig.GROUP_ID.key());
        }
    }

    private Connection groupConnection(String groupId, boolean autoCommit) {
        Configuration configuration = new Configuration(clientConf);
        configuration.setString(ConsumerConfig.GROUP_ID.key(), groupId);
        configuration.setBoolean(ConsumerConfig.ENABLE_AUTO_COMMIT.key(), autoCommit);
        return ConnectionFactory.createConnection(configuration);
    }

    private static void commitSyncWithRetry(
            LogScanner scanner, Map<TableBucket, OffsetMetadata> offsets) throws Exception {
        long deadline = System.currentTimeMillis() + Duration.ofMinutes(2).toMillis();
        while (true) {
            try {
                scanner.commitSync(offsets);
                return;
            } catch (RuntimeException failure) {
                if (System.currentTimeMillis() >= deadline) {
                    throw failure;
                }
                Thread.sleep(200L);
            }
        }
    }

    private static AtomicReference<Exception> commitAsyncWithRetry(
            LogScanner scanner, Map<TableBucket, OffsetMetadata> offsets) throws Exception {
        long deadline = System.currentTimeMillis() + Duration.ofMinutes(2).toMillis();
        AtomicReference<Exception> failure = new AtomicReference<>();
        while (System.currentTimeMillis() < deadline) {
            CountDownLatch callback = new CountDownLatch(1);
            failure.set(null);
            scanner.commitAsync(
                    offsets,
                    (committed, error) -> {
                        failure.set(error);
                        callback.countDown();
                    });
            if (callback.await(35, TimeUnit.SECONDS) && failure.get() == null) {
                return failure;
            }
            Thread.sleep(200L);
        }
        throw new AssertionError("Asynchronous offset commit did not succeed", failure.get());
    }

    private static Map<TableBucket, OffsetMetadata> commitAsyncWithRetry(LogScanner scanner)
            throws Exception {
        long deadline = System.currentTimeMillis() + Duration.ofMinutes(2).toMillis();
        while (System.currentTimeMillis() < deadline) {
            CountDownLatch callback = new CountDownLatch(1);
            AtomicReference<Map<TableBucket, OffsetMetadata>> committed = new AtomicReference<>();
            AtomicReference<Exception> failure = new AtomicReference<>();
            scanner.commitAsync(
                    (offsets, error) -> {
                        committed.set(offsets);
                        failure.set(error);
                        callback.countDown();
                    });
            if (callback.await(35, TimeUnit.SECONDS) && failure.get() == null) {
                return committed.get();
            }
            Thread.sleep(200L);
        }
        throw new AssertionError("Asynchronous offset commit did not succeed");
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
