/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.client.write;

import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.admin.FlussAdmin;
import org.apache.fluss.client.metadata.MetadataUpdater;
import org.apache.fluss.client.metadata.TestingMetadataUpdater;
import org.apache.fluss.client.metrics.TestingWriterMetricGroup;
import org.apache.fluss.cluster.BucketLocation;
import org.apache.fluss.cluster.Cluster;
import org.apache.fluss.cluster.ServerNode;
import org.apache.fluss.config.AutoPartitionTimeUnit;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.MemorySize;
import org.apache.fluss.exception.BucketRescaleException;
import org.apache.fluss.exception.FlussRuntimeException;
import org.apache.fluss.exception.InvalidPartitionException;
import org.apache.fluss.exception.PartitionNotExistException;
import org.apache.fluss.exception.TableNotExistException;
import org.apache.fluss.exception.TooManyPartitionsException;
import org.apache.fluss.metadata.PartitionSpec;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TableOrPartition;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.BinaryRow;
import org.apache.fluss.row.encode.CompactedKeyEncoder;
import org.apache.fluss.rpc.entity.ProduceLogResultForBucket;
import org.apache.fluss.rpc.entity.PutKvResultForBucket;
import org.apache.fluss.rpc.messages.ProduceLogRequest;
import org.apache.fluss.rpc.messages.PutKvRequest;
import org.apache.fluss.rpc.messages.PutKvResponse;
import org.apache.fluss.rpc.protocol.Errors;
import org.apache.fluss.server.tablet.TestTabletServerGateway;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.utils.clock.SystemClock;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.fluss.client.metadata.TestingMetadataUpdater.NODE1;
import static org.apache.fluss.client.metadata.TestingMetadataUpdater.NODE2;
import static org.apache.fluss.record.TestData.DATA1_PHYSICAL_TABLE_PATH;
import static org.apache.fluss.record.TestData.DATA1_TABLE_ID;
import static org.apache.fluss.record.TestData.DATA1_TABLE_ID_PK;
import static org.apache.fluss.record.TestData.DATA1_TABLE_INFO;
import static org.apache.fluss.record.TestData.DATA1_TABLE_PATH_PK;
import static org.apache.fluss.record.TestData.DEFAULT_REMOTE_DATA_DIR;
import static org.apache.fluss.server.utils.ServerRpcMessageUtils.makeProduceLogResponse;
import static org.apache.fluss.server.utils.ServerRpcMessageUtils.makePutKvResponse;
import static org.apache.fluss.testutils.DataTestUtils.compactedRow;
import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests how {@link Sender} handles write targets of a table without historical partition support
 * whose partition was dropped, and possibly re-created.
 */
final class SenderMissingPartitionTest {

    private static final TableInfo TABLE_INFO = tableInfo(DATA1_TABLE_ID_PK);

    // Out of the retention that TABLE_INFO has. Only the coordinator, which knows the table's
    // current retention, decides whether the partition can be re-created.
    private static final PhysicalTablePath MISSING_PATH =
            PhysicalTablePath.of(DATA1_TABLE_PATH_PK, "20000101");
    private static final PhysicalTablePath NEW_PATH =
            PhysicalTablePath.of(DATA1_TABLE_PATH_PK, "20990102");

    private static final TableBucket MISSING_BUCKET = new TableBucket(DATA1_TABLE_ID_PK, 11L, 0);
    private static final TableBucket NEW_BUCKET = new TableBucket(DATA1_TABLE_ID_PK, 12L, 0);
    private static final TableBucket RECREATED_BUCKET = new TableBucket(DATA1_TABLE_ID_PK, 13L, 0);
    private static final long RECREATED_TABLE_ID = DATA1_TABLE_ID_PK + 100;
    private static final List<TableBucket> DATA1_TABLE_BUCKETS =
            IntStream.range(0, DATA1_TABLE_INFO.getNumBuckets())
                    .mapToObj(bucket -> new TableBucket(DATA1_TABLE_ID, bucket))
                    .collect(Collectors.toList());

    private final Map<PhysicalTablePath, List<TableBucket>> serverPartitions = new HashMap<>();
    private final List<PhysicalTablePath> createdPartitions = new ArrayList<>();

    private final TestingMetadataUpdater metadataUpdater = new ServerPartitionsMetadataUpdater();

    private IdempotenceManager idempotenceManager;
    private RecordAccumulator accumulator;
    private Sender sender;

    @AfterEach
    void teardown() {
        if (sender != null) {
            sender.destroyResources();
        }
    }

    @Test
    void testMissingPartitionDoesNotBlockOtherTargets() throws Exception {
        setupSender(partitionCreator(null, false));
        CompletableFuture<Exception> missingFuture = upsertToDroppedPartition(1);
        servePartition(NEW_PATH, NEW_BUCKET.getPartitionId(), 1);
        CompletableFuture<Exception> newFuture = upsert(NEW_PATH, 2);
        metadataUpdater.invalidPhysicalTableBucketMeta(
                Collections.singleton(DATA1_PHYSICAL_TABLE_PATH));
        CompletableFuture<Exception> tableFuture = new CompletableFuture<>();
        accumulator.append(
                WriteRecord.forArrowAppend(
                        DATA1_TABLE_INFO, DATA1_PHYSICAL_TABLE_PATH, row(1, "a"), null),
                (tableBucket, logEndOffset, error) -> tableFuture.complete(error),
                metadataUpdater.getCluster());

        sender.runOnce();

        assertThat(missingFuture).isDone();
        assertThat(missingFuture.get())
                .isInstanceOf(PartitionNotExistException.class)
                .hasMessageContaining(MISSING_PATH.toString());
        assertThat(newFuture).isNotDone();

        sender.runOnce();
        TestTabletServerGateway partitionGateway = gateway(NODE1);
        assertThat(partitionGateway.pendingRequestSize()).isOne();
        PutKvRequest putKvRequest = (PutKvRequest) partitionGateway.getRequest(0);
        assertThat(putKvRequest.getBucketsReqAt(0).getPartitionId())
                .isEqualTo(NEW_BUCKET.getPartitionId());
        partitionGateway.response(0, putKvResponse(NEW_BUCKET, 1L));
        assertThat(newFuture).isDone();
        assertThat(newFuture.get()).isNull();
        TestTabletServerGateway tableGateway = gateway(NODE2);
        assertThat(tableGateway.pendingRequestSize()).isOne();
        ProduceLogRequest produceLogRequest = (ProduceLogRequest) tableGateway.getRequest(0);
        assertThat(produceLogRequest.getTableId()).isEqualTo(DATA1_TABLE_ID);
        TableBucket logBucket =
                new TableBucket(DATA1_TABLE_ID, produceLogRequest.getBucketsReqAt(0).getBucketId());
        tableGateway.response(
                0,
                makeProduceLogResponse(
                        Collections.singletonList(
                                new ProduceLogResultForBucket(logBucket, 0L, 1L))));
        assertThat(tableFuture).isDone();
        assertThat(tableFuture.get()).isNull();
    }

    @ParameterizedTest
    @EnumSource(CreationOutcome.class)
    void testCreatesMissingPartitionOnceWithoutBlockingOtherTargets(CreationOutcome outcome)
            throws Exception {
        try (PartitionCreatingAdmin admin = new PartitionCreatingAdmin(metadataUpdater)) {
            setupSender(partitionCreator(admin, true));
            CompletableFuture<Exception> missingFuture = upsertToDroppedPartition(1);
            servePartition(NEW_PATH, NEW_BUCKET.getPartitionId(), 1);
            CompletableFuture<Exception> newFuture = upsert(NEW_PATH, 2);

            sender.runOnce();
            sender.runOnce();

            TestTabletServerGateway gateway = gateway(NODE1);
            PutKvRequest request = (PutKvRequest) gateway.getRequest(0);
            assertThat(request.getBucketsReqAt(0).getPartitionId())
                    .isEqualTo(NEW_BUCKET.getPartitionId());
            gateway.response(0, putKvResponse(NEW_BUCKET, 1L));
            assertThat(newFuture).isDone();
            assertThat(newFuture.get()).isNull();
            assertThat(missingFuture).isNotDone();
            assertThat(admin.createdPartitions)
                    .containsExactly(
                            new PartitionSpec(
                                    Collections.singletonMap(
                                            "dt", MISSING_PATH.getPartitionName())));

            if (outcome == CreationOutcome.INVALID) {
                admin.creation.completeExceptionally(
                        new InvalidPartitionException("Partition value is out-of-date."));
                sender.runOnce();

                assertThat(missingFuture).isDone();
                assertThat(missingFuture.get())
                        .isInstanceOf(InvalidPartitionException.class)
                        .hasMessageContaining("out-of-date");
                assertThat(sender.getFatalError()).isNull();
                assertThat(accumulator.hasIncomplete()).isFalse();
                sender.runOnce();
                assertThat(admin.createdPartitions).hasSize(1);
            } else if (outcome == CreationOutcome.FAILED) {
                admin.creation.completeExceptionally(
                        new TooManyPartitionsException("Too many partitions."));
                sender.runOnce();

                assertThat(sender.getFatalError())
                        .isInstanceOf(FlussRuntimeException.class)
                        .hasRootCauseInstanceOf(TooManyPartitionsException.class);
                assertThat(missingFuture).isDone();
                assertThat(missingFuture.get())
                        .isInstanceOf(FlussRuntimeException.class)
                        .hasMessageContaining("Failed to dynamically create partition");
                assertThat(accumulator.hasIncomplete()).isFalse();
            } else {
                admin.creation.complete(null);
                servePartition(MISSING_PATH, RECREATED_BUCKET.getPartitionId(), 1);
                sender.runOnce();
                sender.runOnce();

                request = (PutKvRequest) gateway.getRequest(0);
                assertThat(request.getBucketsReqAt(0).getPartitionId())
                        .isEqualTo(RECREATED_BUCKET.getPartitionId());
                gateway.response(0, putKvResponse(RECREATED_BUCKET, 1L));
                assertThat(missingFuture).isDone();
                assertThat(missingFuture.get()).isNull();
                assertThat(admin.createdPartitions).hasSize(1);
            }
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testCreatesMissingPartitionForSentWritesUnlessIdempotent(boolean idempotenceEnabled)
            throws Exception {
        setupSender(recordingPartitionCreator(), idempotenceEnabled);
        servePartition(MISSING_PATH, MISSING_BUCKET.getPartitionId(), 1);
        metadataUpdater.checkAndUpdatePartitionMetadata(MISSING_PATH);
        CompletableFuture<Exception> future = upsert(MISSING_PATH, 1);
        sender.runOnce();

        serverPartitions.remove(MISSING_PATH);
        gateway(NODE1)
                .response(
                        0, putKvResponse(MISSING_BUCKET, Errors.UNKNOWN_TABLE_OR_BUCKET_EXCEPTION));
        assertThat(future).isNotDone();

        sender.runOnce();

        if (idempotenceEnabled) {
            assertThat(future).isDone();
            assertThat(future.get())
                    .isInstanceOf(PartitionNotExistException.class)
                    .hasMessageContaining("idempotence");
            assertThat(accumulator.hasIncomplete()).isFalse();
            assertThat(sender.numOfInFlightBatches(MISSING_BUCKET)).isZero();
            assertThat(idempotenceManager.inflightBatchSize(MISSING_BUCKET)).isZero();
            assertThat(createdPartitions).isEmpty();
        } else {
            assertThat(future).isNotDone();
            assertThat(createdPartitions).containsExactly(MISSING_PATH);

            servePartition(MISSING_PATH, RECREATED_BUCKET.getPartitionId(), 1);
            sender.runOnce();
            sender.runOnce();
            TestTabletServerGateway gateway = gateway(NODE1);
            PutKvRequest request = (PutKvRequest) gateway.getRequest(0);
            assertThat(request.getBucketsReqAt(0).getPartitionId())
                    .isEqualTo(RECREATED_BUCKET.getPartitionId());
            gateway.response(0, putKvResponse(RECREATED_BUCKET, 1L));
            assertThat(future).isDone();
            assertThat(future.get()).isNull();
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testRecreatesDroppedPartitionOnlyForQueuedWrites(boolean partitionIdCached)
            throws Exception {
        setupSender(recordingPartitionCreator());
        servePartition(MISSING_PATH, MISSING_BUCKET.getPartitionId(), 1);
        metadataUpdater.checkAndUpdatePartitionMetadata(MISSING_PATH);
        CompletableFuture<Exception> future = upsert(MISSING_PATH, 1);
        sender.runOnce();
        serverPartitions.remove(MISSING_PATH);
        gateway(NODE1)
                .response(
                        0, putKvResponse(MISSING_BUCKET, Errors.UNKNOWN_TABLE_OR_BUCKET_EXCEPTION));
        sender.runOnce();
        assertThat(future).isDone();
        assertThat(future.get()).isInstanceOf(PartitionNotExistException.class);
        assertThat(accumulator.hasIncomplete()).isFalse();
        if (partitionIdCached) {
            // A reader of the same connection learns a re-created partition, and invalidates only
            // its bucket metadata after it is dropped again.
            servePartition(MISSING_PATH, RECREATED_BUCKET.getPartitionId(), 1);
            metadataUpdater.updatePhysicalTableMetadata(Collections.singleton(MISSING_PATH));
            serverPartitions.remove(MISSING_PATH);
            metadataUpdater.invalidPhysicalTableBucketMeta(Collections.singleton(MISSING_PATH));
        }
        assertThat(metadataUpdater.getCluster().getPartitionId(MISSING_PATH).isPresent())
                .isEqualTo(partitionIdCached);

        sender.runOnce();
        assertThat(createdPartitions).isEmpty();

        upsert(MISSING_PATH, 2);
        sender.runOnce();
        assertThat(createdPartitions).containsExactly(MISSING_PATH);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testFailsWritesToDroppedPartitionWithRequestsInFlight(boolean recreatedByAnotherClient)
            throws Exception {
        setupSender(recordingPartitionCreator(), false);
        servePartition(MISSING_PATH, MISSING_BUCKET.getPartitionId(), 1);
        metadataUpdater.checkAndUpdatePartitionMetadata(MISSING_PATH);
        CompletableFuture<Exception> sentFuture = upsert(MISSING_PATH, 1);
        sender.runOnce();
        CompletableFuture<Exception> queuedFuture = upsert(MISSING_PATH, 2);
        if (recreatedByAnotherClient) {
            // A reader of the same connection learns the re-created partition.
            servePartition(MISSING_PATH, RECREATED_BUCKET.getPartitionId(), 1);
            metadataUpdater.updatePhysicalTableMetadata(Collections.singleton(MISSING_PATH));
        } else {
            serverPartitions.remove(MISSING_PATH);
            metadataUpdater.invalidPhysicalTableBucketMeta(Collections.singleton(MISSING_PATH));
        }

        sender.runOnce();

        assertThat(sentFuture).isDone();
        assertThat(sentFuture.get())
                .isInstanceOf(PartitionNotExistException.class)
                .hasMessageContaining("in flight");
        assertThat(queuedFuture).isDone();
        assertThat(queuedFuture.get())
                .isInstanceOf(PartitionNotExistException.class)
                .hasMessageContaining("in flight");
        assertThat(createdPartitions).isEmpty();
        TestTabletServerGateway gateway = gateway(NODE1);
        assertThat(gateway.pendingRequestSize()).isOne();

        gateway.response(
                0, putKvResponse(MISSING_BUCKET, Errors.UNKNOWN_TABLE_OR_BUCKET_EXCEPTION));
        assertThat(accumulator.hasIncomplete()).isFalse();
        assertThat(sender.numOfInFlightBatches(MISSING_BUCKET)).isZero();
    }

    @ParameterizedTest
    @CsvSource({
        "1, 1, true",
        "1, 2, true",
        "2, 1, true",
        "1, 1, false",
        "1, 2, false",
        "2, 1, false"
    })
    void testResolvesLayoutOfRecreatedPartition(
            int droppedBuckets, int recreatedBuckets, boolean recreatedByWriter) throws Exception {
        setupSender(
                recreatedByWriter ? recordingPartitionCreator() : partitionCreator(null, false));
        CompletableFuture<Exception> future = upsertToDroppedPartition(droppedBuckets);
        if (recreatedByWriter) {
            sender.runOnce();
            assertThat(createdPartitions).containsExactly(MISSING_PATH);
            assertThat(future).isNotDone();
            servePartition(MISSING_PATH, RECREATED_BUCKET.getPartitionId(), recreatedBuckets);
        } else {
            servePartition(MISSING_PATH, RECREATED_BUCKET.getPartitionId(), recreatedBuckets);
            // Another missing target fails the bulk refresh, so the partition is refreshed alone.
            upsert(NEW_PATH, 2);
            sender.runOnce();
        }

        sender.runOnce();
        sender.runOnce();

        TestTabletServerGateway gateway = gateway(NODE1);
        if (droppedBuckets != recreatedBuckets) {
            assertThat(future).isDone();
            assertThat(future.get()).isInstanceOf(BucketRescaleException.class);
            assertThat(gateway.pendingRequestSize()).isZero();
            future = upsert(MISSING_PATH, 1);
            sender.runOnce();
        }
        PutKvRequest request = (PutKvRequest) gateway.getRequest(0);
        assertThat(request.getBucketsReqAt(0).getPartitionId())
                .isEqualTo(RECREATED_BUCKET.getPartitionId());
        assertThat(request.getBucketsReqAt(0).getRoutingBucketCount()).isEqualTo(recreatedBuckets);
        TableBucket bucket =
                new TableBucket(
                        DATA1_TABLE_ID_PK,
                        RECREATED_BUCKET.getPartitionId(),
                        request.getBucketsReqAt(0).getBucketId());
        gateway.response(0, putKvResponse(bucket, 1L));
        assertThat(future).isDone();
        assertThat(future.get()).isNull();
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testFailsSequencedWritesToPartitionRecreatedByAnotherClient(boolean anotherTargetMissing)
            throws Exception {
        setupSender(partitionCreator(null, false));
        servePartition(MISSING_PATH, MISSING_BUCKET.getPartitionId(), 1);
        metadataUpdater.checkAndUpdatePartitionMetadata(MISSING_PATH);
        CompletableFuture<Exception> future = upsert(MISSING_PATH, 1);
        sender.runOnce();
        servePartition(MISSING_PATH, RECREATED_BUCKET.getPartitionId(), 1);
        TestTabletServerGateway gateway = gateway(NODE1);
        gateway.response(
                0, putKvResponse(MISSING_BUCKET, Errors.UNKNOWN_TABLE_OR_BUCKET_EXCEPTION));
        assertThat(future).isNotDone();
        if (anotherTargetMissing) {
            upsert(NEW_PATH, 2);
        }

        sender.runOnce();
        sender.runOnce();

        assertThat(future).isDone();
        assertThat(future.get())
                .isInstanceOf(PartitionNotExistException.class)
                .hasMessageContaining("re-created with ID " + RECREATED_BUCKET.getPartitionId())
                .hasMessageContaining("idempotence");
        assertThat(accumulator.hasIncomplete()).isFalse();
        assertThat(idempotenceManager.inflightBatchSize(MISSING_BUCKET)).isZero();
        assertThat(gateway.pendingRequestSize()).isZero();

        CompletableFuture<Exception> retriedFuture = upsert(MISSING_PATH, 3);
        sender.runOnce();
        PutKvRequest request = (PutKvRequest) gateway.getRequest(0);
        assertThat(request.getBucketsReqAt(0).getPartitionId())
                .isEqualTo(RECREATED_BUCKET.getPartitionId());
        gateway.response(0, putKvResponse(RECREATED_BUCKET, 1L));
        assertThat(retriedFuture).isDone();
        assertThat(retriedFuture.get()).isNull();
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testSwitchesTargetToRecreatedTable(boolean retriedBeforeSwitch) throws Exception {
        setupSender(partitionCreator(null, false));
        servePartition(MISSING_PATH, MISSING_BUCKET.getPartitionId(), 1);
        metadataUpdater.checkAndUpdatePartitionMetadata(MISSING_PATH);
        TestTabletServerGateway gateway = gateway(NODE1);
        CompletableFuture<Exception> future = upsert(MISSING_PATH, 1);
        sender.runOnce();
        gateway.response(0, putKvResponse(MISSING_BUCKET, 1L));
        assertThat(future.get()).isNull();
        CompletableFuture<Exception> droppedTableFuture = upsert(MISSING_PATH, 2);
        sender.runOnce();

        TableInfo recreatedTable = tableInfo(RECREATED_TABLE_ID);
        metadataUpdater.updateTableInfos(
                Collections.singletonMap(DATA1_TABLE_PATH_PK, recreatedTable));
        TableBucket bucket = new TableBucket(RECREATED_TABLE_ID, 21L, 0);
        serverPartitions.put(MISSING_PATH, Collections.singletonList(bucket));
        metadataUpdater.checkAndUpdatePartitionMetadata(MISSING_PATH);
        if (retriedBeforeSwitch) {
            gateway.response(
                    0, putKvResponse(MISSING_BUCKET, Errors.UNKNOWN_TABLE_OR_BUCKET_EXCEPTION));
            future = upsert(recreatedTable, MISSING_PATH, 1);
            sender.runOnce();
            sender.runOnce();
        } else {
            future = upsert(recreatedTable, MISSING_PATH, 1);
            sender.runOnce();
            gateway.response(
                    0, putKvResponse(MISSING_BUCKET, Errors.UNKNOWN_TABLE_OR_BUCKET_EXCEPTION));
        }
        gateway.response(0, putKvResponse(bucket, 1L));
        assertThat(future.get()).isNull();
        sender.runOnce();
        sender.runOnce();

        assertThat(droppedTableFuture).isDone();
        assertThat(droppedTableFuture.get()).isInstanceOf(TableNotExistException.class);
        assertThat(idempotenceManager.inflightBatchSize(MISSING_BUCKET)).isZero();

        // The partition of the re-created table is re-created as well while a write is retried.
        future = upsert(recreatedTable, MISSING_PATH, 2);
        sender.runOnce();
        TableBucket recreatedBucket = new TableBucket(RECREATED_TABLE_ID, 22L, 0);
        serverPartitions.put(MISSING_PATH, Collections.singletonList(recreatedBucket));
        gateway.response(0, putKvResponse(bucket, Errors.UNKNOWN_TABLE_OR_BUCKET_EXCEPTION));
        sender.runOnce();
        sender.runOnce();

        assertThat(future).isDone();
        assertThat(future.get())
                .isInstanceOf(PartitionNotExistException.class)
                .hasMessageContaining("idempotence");
        assertThat(idempotenceManager.inflightBatchSize(bucket)).isZero();
        assertThat(accumulator.hasIncomplete()).isFalse();
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testIgnoresRejectedPartitionOfDroppedTable(boolean recreatedTableWriteSent)
            throws Exception {
        try (PartitionCreatingAdmin admin = new PartitionCreatingAdmin(metadataUpdater)) {
            setupSender(partitionCreator(admin, true));
            CompletableFuture<Exception> droppedTableFuture = upsertToDroppedPartition(1);
            sender.runOnce();
            assertThat(admin.createdPartitions).hasSize(1);

            TableInfo recreatedTable = tableInfo(RECREATED_TABLE_ID);
            metadataUpdater.updateTableInfos(
                    Collections.singletonMap(DATA1_TABLE_PATH_PK, recreatedTable));
            TableBucket bucket = new TableBucket(RECREATED_TABLE_ID, 21L, 0);
            serverPartitions.put(MISSING_PATH, Collections.singletonList(bucket));
            metadataUpdater.checkAndUpdatePartitionMetadata(MISSING_PATH);
            CompletableFuture<Exception> future = upsert(recreatedTable, MISSING_PATH, 1);
            if (recreatedTableWriteSent) {
                sender.runOnce();
            }
            admin.creation.completeExceptionally(
                    new InvalidPartitionException("Partition value is out-of-date."));
            sender.runOnce();

            assertThat(droppedTableFuture).isDone();
            assertThat(droppedTableFuture.get()).isNotNull();
            TestTabletServerGateway gateway = gateway(NODE1);
            PutKvRequest request = (PutKvRequest) gateway.getRequest(0);
            assertThat(request.getBucketsReqAt(0).getPartitionId())
                    .isEqualTo(bucket.getPartitionId());
            gateway.response(0, putKvResponse(bucket, 1L));
            assertThat(future.get()).isNull();
            assertThat(sender.getFatalError()).isNull();
        }
    }

    @Test
    void testIgnoresRejectionOfPartitionCreatedSince() throws Exception {
        setupSender(partitionCreator(null, false));
        CompletableFuture<Exception> future = upsert(NEW_PATH, 1);
        sender.onPartitionCreationCompleted(
                NEW_PATH, DATA1_TABLE_ID_PK, new InvalidPartitionException("out-of-date"));
        sender.onPartitionCreationCompleted(NEW_PATH, DATA1_TABLE_ID_PK, null);
        servePartition(NEW_PATH, NEW_BUCKET.getPartitionId(), 1);

        sender.runOnce();
        sender.runOnce();

        TestTabletServerGateway gateway = gateway(NODE1);
        gateway.response(0, putKvResponse(NEW_BUCKET, 1L));
        assertThat(future.get()).isNull();
    }

    @Test
    void testFailsOnlyWritesToPartitionRejectedForNewWrite() throws Exception {
        try (PartitionCreatingAdmin admin = new PartitionCreatingAdmin(metadataUpdater)) {
            DynamicPartitionCreator partitionCreator = partitionCreator(admin, true);
            setupSender(partitionCreator);
            // A new write requests the partition before the Sender finds it missing.
            partitionCreator.checkAndCreatePartitionAsync(NEW_PATH, TABLE_INFO);
            CompletableFuture<Exception> future = upsert(NEW_PATH, 1);
            sender.runOnce();
            assertThat(admin.createdPartitions).hasSize(1);

            admin.creation.completeExceptionally(
                    new InvalidPartitionException("Partition value is out-of-date."));
            sender.runOnce();

            assertThat(future).isDone();
            assertThat(future.get()).isInstanceOf(InvalidPartitionException.class);
            assertThat(sender.getFatalError()).isNull();
            assertThat(accumulator.hasIncomplete()).isFalse();
        }
    }

    private void setupSender(DynamicPartitionCreator partitionCreator) {
        setupSender(partitionCreator, true);
    }

    private void setupSender(DynamicPartitionCreator partitionCreator, boolean idempotenceEnabled) {
        Configuration conf = new Configuration();
        conf.set(ConfigOptions.CLIENT_WRITER_BUFFER_MEMORY_SIZE, new MemorySize(1024 * 1024));
        conf.set(ConfigOptions.CLIENT_WRITER_BATCH_SIZE, new MemorySize(16 * 1024));
        conf.set(ConfigOptions.CLIENT_WRITER_BUFFER_PAGE_SIZE, new MemorySize(256));
        conf.set(ConfigOptions.CLIENT_WRITER_BATCH_TIMEOUT, Duration.ZERO);
        conf.set(ConfigOptions.CLIENT_WRITER_RETRY_BACKOFF, Duration.ZERO);
        conf.set(ConfigOptions.CLIENT_WRITER_RETRY_BACKOFF_MAX, Duration.ZERO);
        idempotenceManager =
                new IdempotenceManager(
                        idempotenceEnabled,
                        5,
                        metadataUpdater.newRandomTabletServerClient(),
                        metadataUpdater);
        TestingWriterMetricGroup writerMetricGroup = TestingWriterMetricGroup.newInstance();
        accumulator =
                new RecordAccumulator(
                        conf, idempotenceManager, writerMetricGroup, SystemClock.getInstance());
        sender =
                new Sender(
                        accumulator,
                        5000,
                        1024 * 1024,
                        (short) -1,
                        Integer.MAX_VALUE,
                        metadataUpdater,
                        idempotenceManager,
                        writerMetricGroup,
                        partitionCreator);
    }

    private DynamicPartitionCreator partitionCreator(
            @Nullable Admin admin, boolean dynamicPartitionEnabled) {
        return new DynamicPartitionCreator(
                metadataUpdater,
                admin,
                dynamicPartitionEnabled,
                t -> sender.recordFatalError(t),
                (path, tableId, rejection) ->
                        sender.onPartitionCreationCompleted(path, tableId, rejection));
    }

    private DynamicPartitionCreator recordingPartitionCreator() {
        return new DynamicPartitionCreator(
                metadataUpdater, null, true, t -> {}, (path, tableId, rejection) -> {}) {
            @Override
            public void createMissingPartitionAsync(
                    PhysicalTablePath physicalTablePath, TableInfo tableInfo) {
                assertThat(tableInfo).isSameAs(TABLE_INFO);
                createdPartitions.add(physicalTablePath);
            }
        };
    }

    private void servePartition(PhysicalTablePath path, long partitionId, int numBuckets) {
        serverPartitions.put(
                path,
                IntStream.range(0, numBuckets)
                        .mapToObj(bucket -> new TableBucket(DATA1_TABLE_ID_PK, partitionId, bucket))
                        .collect(Collectors.toList()));
    }

    private CompletableFuture<Exception> upsertToDroppedPartition(int numBuckets) throws Exception {
        servePartition(MISSING_PATH, MISSING_BUCKET.getPartitionId(), numBuckets);
        metadataUpdater.checkAndUpdatePartitionMetadata(MISSING_PATH);
        CompletableFuture<Exception> future = upsert(MISSING_PATH, 1);
        serverPartitions.remove(MISSING_PATH);
        metadataUpdater.invalidPhysicalTableBucketMeta(Collections.singleton(MISSING_PATH));
        return future;
    }

    private CompletableFuture<Exception> upsert(PhysicalTablePath path, int id) throws Exception {
        return upsert(TABLE_INFO, path, id);
    }

    private CompletableFuture<Exception> upsert(TableInfo tableInfo, PhysicalTablePath path, int id)
            throws Exception {
        BinaryRow row =
                compactedRow(tableInfo.getRowType(), new Object[] {id, path.getPartitionName()});
        byte[] key =
                new CompactedKeyEncoder(
                                tableInfo.getRowType(),
                                tableInfo.getSchema().getPrimaryKeyIndexes())
                        .encodeKey(row);
        CompletableFuture<Exception> future = new CompletableFuture<>();
        accumulator.append(
                WriteRecord.forUpsert(
                        tableInfo, path, row, key, key, WriteFormat.COMPACTED_KV, null),
                (tableBucket, logEndOffset, error) -> future.complete(error),
                metadataUpdater.getCluster());
        return future;
    }

    private TestTabletServerGateway gateway(ServerNode node) {
        return (TestTabletServerGateway) metadataUpdater.newTabletServerClientForNode(node.id());
    }

    private static PutKvResponse putKvResponse(TableBucket bucket, long endOffset) {
        return makePutKvResponse(
                Collections.singletonList(new PutKvResultForBucket(bucket, endOffset)));
    }

    private static PutKvResponse putKvResponse(TableBucket bucket, Errors error) {
        return makePutKvResponse(
                Collections.singletonList(new PutKvResultForBucket(bucket, error.toApiError())));
    }

    private static TableInfo tableInfo(long tableId) {
        return TableInfo.of(
                DATA1_TABLE_PATH_PK,
                tableId,
                1,
                TableDescriptor.builder()
                        .schema(
                                Schema.newBuilder()
                                        .column("id", DataTypes.INT())
                                        .column("dt", DataTypes.STRING())
                                        .primaryKey("id", "dt")
                                        .build())
                        .partitionedBy("dt")
                        .distributedBy(1)
                        .property(ConfigOptions.TABLE_AUTO_PARTITION_ENABLED, true)
                        .property(
                                ConfigOptions.TABLE_AUTO_PARTITION_TIME_UNIT,
                                AutoPartitionTimeUnit.DAY)
                        .property(ConfigOptions.TABLE_AUTO_PARTITION_NUM_RETENTION, 7)
                        .property(ConfigOptions.TABLE_AUTO_PARTITION_TIMEZONE, "UTC")
                        .build(),
                DEFAULT_REMOTE_DATA_DIR,
                1L,
                1L);
    }

    private static Map<TablePath, TableInfo> tableInfos() {
        Map<TablePath, TableInfo> tableInfos = new HashMap<>();
        tableInfos.put(TABLE_INFO.getTablePath(), TABLE_INFO);
        tableInfos.put(DATA1_TABLE_INFO.getTablePath(), DATA1_TABLE_INFO);
        return tableInfos;
    }

    /** How the coordinator answers a request to create a partition. */
    private enum CreationOutcome {
        CREATED,
        INVALID,
        FAILED
    }

    /** Records requests to create partitions, which complete with {@link #creation}. */
    private static class PartitionCreatingAdmin extends FlussAdmin {

        private final List<PartitionSpec> createdPartitions = new CopyOnWriteArrayList<>();
        private final CompletableFuture<Void> creation = new CompletableFuture<>();

        private PartitionCreatingAdmin(MetadataUpdater metadataUpdater) {
            super(null, metadataUpdater);
        }

        @Override
        public CompletableFuture<Void> createPartition(
                TablePath tablePath, PartitionSpec partitionSpec, boolean ignoreIfExists) {
            assertThat(tablePath).isEqualTo(TABLE_INFO.getTablePath());
            createdPartitions.add(partitionSpec);
            return creation;
        }
    }

    private class ServerPartitionsMetadataUpdater extends TestingMetadataUpdater {

        private ServerPartitionsMetadataUpdater() {
            super(tableInfos());
        }

        @Override
        public void updatePhysicalTableMetadata(Set<PhysicalTablePath> physicalTablePaths) {
            for (PhysicalTablePath path : physicalTablePaths) {
                if (path.getPartitionName() != null && !serverPartitions.containsKey(path)) {
                    throw new PartitionNotExistException("Partition " + path + " does not exist.");
                }
            }
            for (PhysicalTablePath path : physicalTablePaths) {
                if (path.getPartitionName() == null) {
                    learn(path, DATA1_TABLE_BUCKETS, NODE2);
                } else {
                    learn(path, serverPartitions.get(path), NODE1);
                }
            }
        }

        @Override
        public boolean checkAndUpdatePartitionMetadata(PhysicalTablePath physicalTablePath) {
            if (!getCluster().getPartitionId(physicalTablePath).isPresent()) {
                updatePhysicalTableMetadata(Collections.singleton(physicalTablePath));
            }
            return getCluster().getPartitionId(physicalTablePath).isPresent();
        }

        private void learn(PhysicalTablePath path, List<TableBucket> buckets, ServerNode leader) {
            List<BucketLocation> locations = new ArrayList<>();
            for (TableBucket bucket : buckets) {
                locations.add(
                        new BucketLocation(path, bucket, leader.id(), new int[] {leader.id()}));
            }
            Long partitionId = buckets.get(0).getPartitionId();
            Cluster cluster = getCluster();
            Map<PhysicalTablePath, List<BucketLocation>> bucketLocations =
                    new HashMap<>(cluster.getBucketLocationsByPath());
            bucketLocations.put(path, locations);
            Map<PhysicalTablePath, Long> partitionIds =
                    new HashMap<>(cluster.getPartitionIdByPath());
            if (partitionId != null) {
                partitionIds.put(path, partitionId);
            }
            Map<TableOrPartition, Integer> bucketCounts =
                    new HashMap<>(cluster.getBucketCountByTableOrPartition());
            bucketCounts.put(
                    TableOrPartition.of(buckets.get(0).getTableId(), partitionId), buckets.size());
            updateCluster(
                    new Cluster(
                            cluster.getAliveTabletServers(),
                            cluster.getCoordinatorServer(),
                            bucketLocations,
                            cluster.getTableIdByPath(),
                            partitionIds,
                            bucketCounts));
        }
    }
}
