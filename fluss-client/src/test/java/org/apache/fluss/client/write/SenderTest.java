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

package org.apache.fluss.client.write;

import org.apache.fluss.client.metadata.TestingMetadataUpdater;
import org.apache.fluss.client.metrics.TestingWriterMetricGroup;
import org.apache.fluss.cluster.BucketLocation;
import org.apache.fluss.cluster.Cluster;
import org.apache.fluss.cluster.ServerNode;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.MemorySize;
import org.apache.fluss.exception.TableNotExistException;
import org.apache.fluss.exception.TimeoutException;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.MemoryLogRecords;
import org.apache.fluss.row.BinaryRow;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.encode.CompactedKeyEncoder;
import org.apache.fluss.rpc.entity.ProduceLogResultForBucket;
import org.apache.fluss.rpc.entity.PutKvResultForBucket;
import org.apache.fluss.rpc.messages.ApiMessage;
import org.apache.fluss.rpc.messages.ProduceLogRequest;
import org.apache.fluss.rpc.messages.ProduceLogResponse;
import org.apache.fluss.rpc.messages.PutKvResponse;
import org.apache.fluss.rpc.protocol.Errors;
import org.apache.fluss.server.tablet.TestTabletServerGateway;
import org.apache.fluss.utils.clock.SystemClock;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static org.apache.fluss.record.LogRecordBatchFormat.NO_WRITER_ID;
import static org.apache.fluss.record.TestData.DATA1_PHYSICAL_TABLE_PATH;
import static org.apache.fluss.record.TestData.DATA1_ROW_TYPE;
import static org.apache.fluss.record.TestData.DATA1_SCHEMA_PK;
import static org.apache.fluss.record.TestData.DATA1_TABLE_DESCRIPTOR;
import static org.apache.fluss.record.TestData.DATA1_TABLE_ID;
import static org.apache.fluss.record.TestData.DATA1_TABLE_ID_PK;
import static org.apache.fluss.record.TestData.DATA1_TABLE_INFO;
import static org.apache.fluss.record.TestData.DATA1_TABLE_INFO_PK;
import static org.apache.fluss.record.TestData.DATA1_TABLE_PATH;
import static org.apache.fluss.record.TestData.DATA1_TABLE_PATH_PK;
import static org.apache.fluss.record.TestData.DATA2_TABLE_ID;
import static org.apache.fluss.record.TestData.DEFAULT_REMOTE_DATA_DIR;
import static org.apache.fluss.rpc.protocol.Errors.SCHEMA_NOT_EXIST;
import static org.apache.fluss.server.utils.ServerRpcMessageUtils.getProduceLogData;
import static org.apache.fluss.server.utils.ServerRpcMessageUtils.makeProduceLogResponse;
import static org.apache.fluss.server.utils.ServerRpcMessageUtils.makePutKvResponse;
import static org.apache.fluss.testutils.DataTestUtils.compactedRow;
import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.apache.fluss.testutils.common.CommonTestUtils.retry;
import static org.assertj.core.api.Assertions.assertThat;

/** ITCase for {@link Sender}. */
final class SenderTest {
    private static final int TOTAL_MEMORY_SIZE = 1024 * 1024;
    private static final int MAX_REQUEST_SIZE = 1024 * 1024;
    private static final int BATCH_SIZE = 16 * 1024;
    private static final int PAGE_SIZE = 256;
    private static final int REQUEST_TIMEOUT = 5000;
    private static final short ACKS_ALL = -1;
    private static final int MAX_INFLIGHT_REQUEST_PER_BUCKET = 5;

    private final TableBucket tb1 = new TableBucket(DATA1_TABLE_ID, 0);
    private TestingMetadataUpdater metadataUpdater;
    private RecordAccumulator accumulator = null;
    private Sender sender = null;
    private TestingWriterMetricGroup writerMetricGroup;

    // TODO add more tests as kafka SenderTest.

    @BeforeEach
    public void setup() {
        metadataUpdater = initializeMetadataUpdater();
        writerMetricGroup = TestingWriterMetricGroup.newInstance();
        sender = setupWithIdempotenceState();
    }

    @AfterEach
    public void teardown() throws Exception {
        sender.destroyResources();
    }

    @Test
    void testSimple() throws Exception {
        long offset = 0;
        CompletableFuture<Exception> future = new CompletableFuture<>();
        appendToAccumulator(tb1, row(1, "a"), (tb, leo, e) -> future.complete(e));
        sender.runOnce();
        assertThat(sender.numOfInFlightBatches(tb1)).isEqualTo(1);
        finishRequest(tb1, 0, createProduceLogResponse(tb1, offset, 1));

        sender.runOnce();
        assertThat(sender.numOfInFlightBatches(tb1)).isEqualTo(0);
        assertThat(future.get()).isNull();
    }

    @Test
    void testRetries() throws Exception {
        // create a sender with retries = 1.
        int maxRetries = 1;
        Sender sender1 =
                setupWithIdempotenceState(
                        new IdempotenceManager(
                                false,
                                MAX_INFLIGHT_REQUEST_PER_BUCKET,
                                metadataUpdater.newRandomTabletServerClient(),
                                metadataUpdater),
                        maxRetries,
                        0);
        // do a successful retry.
        CompletableFuture<Exception> future = new CompletableFuture<>();
        appendToAccumulator(tb1, row(1, "a"), (tb, leo, e) -> future.complete(e));
        sender1.runOnce();
        assertThat(sender1.numOfInFlightBatches(tb1)).isEqualTo(1);
        long offset = 0;
        finishRequest(tb1, 0, createProduceLogResponse(tb1, offset, 1));

        sender1.runOnce();
        assertThat(sender1.numOfInFlightBatches(tb1)).isEqualTo(0);
        assertThat(future.get()).isNull();

        // do an unsuccessful retry.
        CompletableFuture<Exception> future2 = new CompletableFuture<>();
        appendToAccumulator(tb1, row(1, "a"), (tb, leo, e) -> future2.complete(e));
        sender1.runOnce();
        assertThat(sender1.numOfInFlightBatches(tb1)).isEqualTo(1);

        // timeout error can retry send.
        finishRequest(tb1, 0, createProduceLogResponse(tb1, Errors.REQUEST_TIME_OUT));
        sender1.runOnce();
        assertThat(sender1.numOfInFlightBatches(tb1)).isEqualTo(1);

        // Even if timeout error can retry send, but the retry number > maxRetries, which will
        // return error.
        finishRequest(tb1, 0, createProduceLogResponse(tb1, Errors.REQUEST_TIME_OUT));
        sender1.runOnce();
        assertThat(sender1.numOfInFlightBatches(tb1)).isEqualTo(0);
        assertThat(future2.get())
                .isInstanceOf(TimeoutException.class)
                .hasMessageContaining(Errors.REQUEST_TIME_OUT.message());
    }

    @Test
    void testInitWriterIdRequest() throws Exception {
        IdempotenceManager idempotenceManager = createIdempotenceManager(true);
        Sender sender1 = setupWithIdempotenceState(idempotenceManager);
        sender1.runOnce();
        assertThat(idempotenceManager.isWriterIdValid()).isTrue();
        assertThat(idempotenceManager.hasWriterId(0L)).isTrue();
        assertThat(idempotenceManager.writerId()).isEqualTo(0L);
    }

    /**
     * Verifies the two-phase close prevents the shutdown race condition. Previously,
     * initiateClose() destroyed the Arrow BufferAllocator while the sender's drain loop was still
     * running, causing "Accounted size went negative". With the fix, initiateClose() only rejects
     * new appends; resource destruction is deferred to destroyResources().
     */
    @Test
    void testCloseSenderBeforeAccumulatorDrain() throws Exception {
        // Append a record so there is an undrained batch.
        appendToAccumulator(tb1, row(1, "a"), (tb, leo, e) -> {});

        // Sender begins to prepare write data.
        Cluster clusterSnapshot = metadataUpdater.getCluster();
        RecordAccumulator.ReadyCheckResult readyCheckResult = accumulator.ready(clusterSnapshot);
        assertThat(readyCheckResult.readyNodes).isNotEmpty();

        // Close the accumulator before it drains — this is the race window.
        sender.initiateClose();

        // Drain after the sender is closed. Before the fix this threw "Accounted size went
        // negative" because the Arrow
        // allocator was closed while the drain was still running.
        Map<Integer, List<ReadyWriteBatch>> drained =
                accumulator.drain(clusterSnapshot, readyCheckResult.readyNodes, MAX_REQUEST_SIZE);
        assertThat(drained).isNotEmpty();
        sender.runOnce();
        assertThat(accumulator.hasUnDrained()).isFalse();

        // even double destroy is still safe.
        sender.destroyResources();
        sender.destroyResources();
    }

    @Test
    void testCanRetryWithoutIdempotence() throws Exception {
        // do a successful retry.
        CompletableFuture<Exception> future = new CompletableFuture<>();
        appendToAccumulator(tb1, row(1, "a"), (tb, leo, e) -> future.complete(e));
        sender.runOnce();
        assertThat(sender.numOfInFlightBatches(tb1)).isEqualTo(1);
        assertThat(future.isDone()).isFalse();

        ApiMessage firstRequest = getRequest(tb1, 0);
        assertThat(firstRequest).isInstanceOf(ProduceLogRequest.class);
        assertThat(hasIdempotentRecords(tb1, (ProduceLogRequest) firstRequest)).isFalse();
        // first complete with retriable error.
        finishRequest(tb1, 0, createProduceLogResponse(tb1, Errors.REQUEST_TIME_OUT));
        sender.runOnce();
        assertThat(future.isDone()).isFalse();

        // second retry complete.
        finishRequest(tb1, 0, createProduceLogResponse(tb1, 0L, 1L));
        sender.runOnce();
        assertThat(future.isDone()).isTrue();
        assertThat(future.get()).isNull();
    }

    @Test
    void testIdempotenceWithMultipleInflightBatch() throws Exception {
        IdempotenceManager idempotenceManager = createIdempotenceManager(true);
        Sender sender1 = setupWithIdempotenceState(idempotenceManager);
        sender1.runOnce();
        assertThat(idempotenceManager.isWriterIdValid()).isTrue();
        assertThat(idempotenceManager.nextSequence(tb1)).isEqualTo(0);

        // Send first ProduceLogRequest.
        CompletableFuture<Exception> future1 = new CompletableFuture<>();
        appendToAccumulator(tb1, row(1, "a"), (tb, leo, e) -> future1.complete(e));
        sender1.runOnce();
        assertThat(idempotenceManager.nextSequence(tb1)).isEqualTo(1);
        assertThat(idempotenceManager.lastAckedBatchSequence(tb1)).isNotPresent();

        // Send second ProduceLogRequest.
        CompletableFuture<Exception> future2 = new CompletableFuture<>();
        appendToAccumulator(tb1, row(1, "a"), (tb, leo, e) -> future2.complete(e));
        sender1.runOnce();
        assertThat(idempotenceManager.nextSequence(tb1)).isEqualTo(2);
        assertThat(idempotenceManager.lastAckedBatchSequence(tb1)).isNotPresent();

        assertThat(future1.isDone()).isFalse();
        assertThat(future2.isDone()).isFalse();

        finishIdempotentProduceLogRequest(0, tb1, 0, createProduceLogResponse(tb1, 0L, 1L));
        sender1.runOnce(); // receive response 0.
        assertThat(idempotenceManager.lastAckedBatchSequence(tb1)).isEqualTo(Optional.of(0));
        assertThat(future1.isDone()).isTrue();
        assertThat(future1.get()).isNull();
        assertThat(future2.isDone()).isFalse();

        finishIdempotentProduceLogRequest(1, tb1, 0, createProduceLogResponse(tb1, 1L, 2L));
        sender1.runOnce(); // receive response 1.
        assertThat(sender1.numOfInFlightBatches(tb1)).isEqualTo(0);
        assertThat(idempotenceManager.lastAckedBatchSequence(tb1)).isEqualTo(Optional.of(1));
        assertThat(future2.isDone()).isTrue();
        assertThat(future2.get()).isNull();
    }

    @Test
    void testIdempotenceWithMaxInflightBatch() throws Exception {
        IdempotenceManager idempotenceManager = createIdempotenceManager(true);
        Sender sender1 = setupWithIdempotenceState(idempotenceManager);
        sender1.runOnce();
        assertThat(idempotenceManager.isWriterIdValid()).isTrue();
        assertThat(idempotenceManager.nextSequence(tb1)).isEqualTo(0);

        for (int i = 0; i < MAX_INFLIGHT_REQUEST_PER_BUCKET - 1; i++) {
            CompletableFuture<Exception> future = new CompletableFuture<>();
            appendToAccumulator(tb1, row(1, "a"), (tb, leo, e) -> future.complete(e));
            sender1.runOnce();
            assertThat(idempotenceManager.inflightBatchSize(tb1)).isEqualTo(i + 1);
            assertThat(idempotenceManager.canSendMoreRequests(tb1)).isTrue();
        }

        // add one batch to make the inflight request size equal to max.
        CompletableFuture<Exception> future = new CompletableFuture<>();
        appendToAccumulator(tb1, row(1, "a"), (tb, leo, e) -> future.complete(e));
        sender1.runOnce();
        assertThat(idempotenceManager.inflightBatchSize(tb1))
                .isEqualTo(MAX_INFLIGHT_REQUEST_PER_BUCKET);
        assertThat(idempotenceManager.canSendMoreRequests(tb1)).isFalse();

        // add one more batch, it will not be drained from accumulator.
        CompletableFuture<Exception> future1 = new CompletableFuture<>();
        appendToAccumulator(tb1, row(1, "a"), (tb, leo, e) -> future1.complete(e));
        sender1.runOnce();
        assertThat(idempotenceManager.inflightBatchSize(tb1))
                .isEqualTo(MAX_INFLIGHT_REQUEST_PER_BUCKET);
        assertThat(accumulator.ready(metadataUpdater.getCluster()).readyNodes.size()).isEqualTo(1);

        // finish the first batch, the latest batch will be drained from the accumulator.
        finishIdempotentProduceLogRequest(0, tb1, 0, createProduceLogResponse(tb1, 0L, 1L));
        sender1.runOnce(); // receive response 0.
        assertThat(idempotenceManager.lastAckedBatchSequence(tb1)).isEqualTo(Optional.of(0));
        assertThat(idempotenceManager.inflightBatchSize(tb1))
                .isEqualTo(MAX_INFLIGHT_REQUEST_PER_BUCKET);
        assertThat(accumulator.ready(metadataUpdater.getCluster()).readyNodes.size()).isEqualTo(0);
    }

    @Test
    void testIdempotenceWithInflightBatchesExceedMaxInflightBatch() throws Exception {
        // When more than 5 batches (MAX_INFLIGHT_REQUEST_PER_BUCKET) of data are incorrectly
        // returned with retriable error, we can still continue to send requests, but only for the
        // one with the smallest batchSequence (first batchSequence) among the failed requests.
        IdempotenceManager idempotenceManager = createIdempotenceManager(true);
        Sender sender = setupWithIdempotenceState(idempotenceManager);
        sender.runOnce();
        assertThat(idempotenceManager.nextSequence(tb1)).isEqualTo(0);

        // 1. send five batches first to full MAX_INFLIGHT_REQUEST_PER_BUCKET.
        for (int i = 0; i < MAX_INFLIGHT_REQUEST_PER_BUCKET; i++) {
            CompletableFuture<Exception> future = new CompletableFuture<>();
            appendToAccumulator(tb1, row(1, "a"), (tb, leo, e) -> future.complete(e));
            assertThat(idempotenceManager.canSendMoreRequests(tb1)).isTrue();
            sender.runOnce(); // runOnce to send request.
        }
        assertThat(idempotenceManager.inflightBatchSize(tb1)).isEqualTo(5);
        assertThat(idempotenceManager.canSendMoreRequests(tb1)).isFalse();

        // 2. try to append more data into accumulator, it will not be drained from accumulator.
        for (int i = 0; i < 1000; i++) {
            CompletableFuture<Exception> future = new CompletableFuture<>();
            appendToAccumulator(tb1, row(1, "a"), (tb, leo, e) -> future.complete(e));
        }
        // No batches can be drained from accumulator as the inflight request size is max in
        // IdempotenceManager.
        Set<Integer> readyNodes = accumulator.ready(metadataUpdater.getCluster()).readyNodes;
        assertThat(readyNodes.isEmpty()).isFalse();
        Map<Integer, List<ReadyWriteBatch>> drained =
                accumulator.drain(metadataUpdater.getCluster(), readyNodes, Integer.MAX_VALUE);
        assertThat(drained.isEmpty()).isTrue();

        // try to send, no request will send.
        assertThat(pendingRequestSize(tb1)).isEqualTo(5);
        sender.runOnce();
        assertThat(pendingRequestSize(tb1)).isEqualTo(5);

        // 3. try to finish already send requests with retriable error.
        for (int i = 0; i < MAX_INFLIGHT_REQUEST_PER_BUCKET; i++) {
            finishIdempotentProduceLogRequest(
                    i, tb1, 0, createProduceLogResponse(tb1, Errors.STORAGE_EXCEPTION));
        }
        assertThat(pendingRequestSize(tb1)).isEqualTo(0);

        // 4. try to re-send many iterators.
        for (int i = 0; i < 20; i++) {
            // add more data into accumulator to make sure accumulator.ready() not return
            // empty.
            for (int j = 0; j < 50; j++) {
                CompletableFuture<Exception> future = new CompletableFuture<>();
                appendToAccumulator(tb1, row(1, "a"), (tb, leo, e) -> future.complete(e));
            }

            // already have five batches in idempotenceManager inflight batches.
            assertThat(idempotenceManager.canSendMoreRequests(tb1)).isFalse();
            readyNodes = accumulator.ready(metadataUpdater.getCluster()).readyNodes;
            assertThat(readyNodes.isEmpty()).isFalse();
            drained =
                    accumulator.drain(metadataUpdater.getCluster(), readyNodes, Integer.MAX_VALUE);
            if (i == 0) {
                // for first batch (retried first batch), we can send.
                assertThat(drained.isEmpty()).isFalse();
                List<ReadyWriteBatch> writeBatches = new ArrayList<>(drained.values()).get(0);
                assertThat(writeBatches.size()).isEqualTo(1);
                assertThat(writeBatches.get(0).writeBatch().batchSequence()).isEqualTo(0);
            } else {
                // for other batches, we will wait the result of the first batch and cannot be send.
                assertThat(drained.isEmpty()).isTrue();
            }
        }
    }

    @Test
    void testIdempotenceWithMultipleInflightBatchesRetriedInOrder() throws Exception {
        // Send multiple in flight requests, retry them all one at a time, in the correct order.
        IdempotenceManager idempotenceManager = createIdempotenceManager(true);
        Sender sender1 = setupWithIdempotenceState(idempotenceManager);
        sender1.runOnce();
        assertThat(idempotenceManager.isWriterIdValid()).isTrue();
        assertThat(idempotenceManager.nextSequence(tb1)).isEqualTo(0);

        // Send first ProduceLogRequest.
        CompletableFuture<Exception> future1 = new CompletableFuture<>();
        appendToAccumulator(tb1, row(1, "a"), (tb, leo, e) -> future1.complete(e));
        sender1.runOnce();
        assertThat(idempotenceManager.nextSequence(tb1)).isEqualTo(1);
        assertThat(idempotenceManager.lastAckedBatchSequence(tb1)).isNotPresent();

        // Send second ProduceLogRequest.
        CompletableFuture<Exception> future2 = new CompletableFuture<>();
        appendToAccumulator(tb1, row(1, "a"), (tb, leo, e) -> future2.complete(e));
        sender1.runOnce();

        // Send third ProduceLogRequest.
        CompletableFuture<Exception> future3 = new CompletableFuture<>();
        appendToAccumulator(tb1, row(1, "a"), (tb, leo, e) -> future3.complete(e));
        sender1.runOnce();

        // finish batch one with retriable error.
        finishIdempotentProduceLogRequest(
                0, tb1, 0, createProduceLogResponse(tb1, Errors.REQUEST_TIME_OUT));
        sender1.runOnce(); // receive response 0

        // Queue the forth request, it shouldn't sent until the first 3 complete.
        CompletableFuture<Exception> future4 = new CompletableFuture<>();
        appendToAccumulator(tb1, row(1, "a"), (tb, leo, e) -> future4.complete(e));
        assertThat(idempotenceManager.lastAckedBatchSequence(tb1)).isNotPresent();

        finishIdempotentProduceLogRequest(
                1, tb1, 0, createProduceLogResponse(tb1, Errors.OUT_OF_ORDER_SEQUENCE_EXCEPTION));
        sender1.runOnce(); // re send request 1, receive response 2

        finishIdempotentProduceLogRequest(
                2, tb1, 0, createProduceLogResponse(tb1, Errors.OUT_OF_ORDER_SEQUENCE_EXCEPTION));
        sender1.runOnce(); // receive response 3

        assertThat(idempotenceManager.lastAckedBatchSequence(tb1)).isNotPresent();
        sender1.runOnce(); // Do nothing, we are reduced to one in flight request during retries.

        // the batch for request 4 shouldn't have been drained, and hence the batch sequence should
        // not have been incremented.
        assertThat(idempotenceManager.nextSequence(tb1)).isEqualTo(3);
        assertThat(idempotenceManager.lastAckedBatchSequence(tb1)).isNotPresent();

        finishIdempotentProduceLogRequest(0, tb1, 0, createProduceLogResponse(tb1, 0L, 1L));
        assertThat(sender1.numOfInFlightBatches(tb1)).isEqualTo(0);
        sender1.runOnce(); // receive response 1
        assertThat(idempotenceManager.lastAckedBatchSequence(tb1)).isEqualTo(Optional.of(0));
        assertThat(future1.isDone()).isTrue();
        assertThat(future1.get()).isNull();

        sender1.runOnce(); // send request 2
        assertThat(sender1.numOfInFlightBatches(tb1)).isEqualTo(1);

        finishIdempotentProduceLogRequest(1, tb1, 0, createProduceLogResponse(tb1, 1L, 2L));
        assertThat(sender1.numOfInFlightBatches(tb1)).isEqualTo(0);
        sender1.runOnce(); // receive response 2
        assertThat(idempotenceManager.lastAckedBatchSequence(tb1)).isEqualTo(Optional.of(1));
        assertThat(future2.isDone()).isTrue();
        assertThat(future2.get()).isNull();

        sender1.runOnce(); // send request 3
        assertThat(sender1.numOfInFlightBatches(tb1)).isEqualTo(2);

        finishIdempotentProduceLogRequest(2, tb1, 0, createProduceLogResponse(tb1, 2L, 3L));
        assertThat(sender1.numOfInFlightBatches(tb1)).isEqualTo(1);
        sender1.runOnce(); // receive response 3
        assertThat(idempotenceManager.lastAckedBatchSequence(tb1)).isEqualTo(Optional.of(2));
        assertThat(future3.isDone()).isTrue();
        assertThat(future3.get()).isNull();

        finishIdempotentProduceLogRequest(3, tb1, 0, createProduceLogResponse(tb1, 3L, 4L));
        sender1.runOnce(); // receive response 4
        assertThat(idempotenceManager.lastAckedBatchSequence(tb1)).isEqualTo(Optional.of(3));
        assertThat(future4.isDone()).isTrue();
        assertThat(future4.get()).isNull();
        assertThat(sender1.numOfInFlightBatches(tb1)).isEqualTo(0);
    }

    @Test
    void testRetryAfterResettingInFlightBatchSequence() throws Exception {
        IdempotenceManager idempotenceManager = createIdempotenceManager(true);
        Sender sender1 = setupWithIdempotenceState(idempotenceManager);
        sender1.runOnce();
        assertThat(idempotenceManager.isWriterIdValid()).isTrue();
        assertThat(idempotenceManager.nextSequence(tb1)).isEqualTo(0);

        // Send the first ProduceLogRequest.
        CompletableFuture<Exception> future1 = new CompletableFuture<>();
        appendToAccumulator(tb1, row(1, "a"), (tb, leo, e) -> future1.complete(e));
        sender1.runOnce();
        // Send the second ProduceLogRequest.
        CompletableFuture<Exception> future2 = new CompletableFuture<>();
        appendToAccumulator(tb1, row(1, "a"), (tb, leo, e) -> future2.complete(e));
        sender1.runOnce();

        // response 0 with retrievable error which will reEnqueue the batch.
        finishIdempotentProduceLogRequest(
                0, tb1, 0, createProduceLogResponse(tb1, Errors.REQUEST_TIME_OUT));
        // response 1 with UnknownWriterIdException which will reset writer
        finishIdempotentProduceLogRequest(
                1, tb1, 0, createProduceLogResponse(tb1, Errors.UNKNOWN_WRITER_ID_EXCEPTION));
        retry(
                Duration.ofMinutes(1),
                () -> { // after response 1 is received, the writer will be reset
                    assertThat(idempotenceManager.hasInflightBatches(tb1)).isFalse();
                    assertThat(
                                    accumulator.getReadyDeque(
                                            DATA1_PHYSICAL_TABLE_PATH, tb1.getBucket()))
                            .hasSize(1);
                    assertThat(
                                    accumulator
                                            .getReadyDeque(
                                                    DATA1_PHYSICAL_TABLE_PATH, tb1.getBucket())
                                            .peek()
                                            .batchSequence())
                            .isEqualTo(0);
                });
        assertThat(future1.isDone()).isFalse();
        assertThat(future2.isDone()).isTrue();

        // resend the first ProduceLogRequest.
        sender1.runOnce();
        finishIdempotentProduceLogRequest(0, tb1, 0, createProduceLogResponse(tb1, 0L, 1L));
        sender1.runOnce();
        assertThat(sender1.numOfInFlightBatches(tb1)).isEqualTo(0);
        assertThat(future1.isDone()).isTrue();
    }

    @Test
    void testCorrectHandlingOfOutOfOrderResponses() throws Exception {
        IdempotenceManager idempotenceManager = createIdempotenceManager(true);
        Sender sender1 = setupWithIdempotenceState(idempotenceManager);
        sender1.runOnce();
        assertThat(idempotenceManager.isWriterIdValid()).isTrue();
        assertThat(idempotenceManager.nextSequence(tb1)).isEqualTo(0);

        // Send first ProduceLogRequest.
        CompletableFuture<Exception> future1 = new CompletableFuture<>();
        appendToAccumulator(tb1, row(1, "a"), (tb, leo, e) -> future1.complete(e));
        sender1.runOnce();
        assertThat(idempotenceManager.nextSequence(tb1)).isEqualTo(1);
        assertThat(idempotenceManager.lastAckedBatchSequence(tb1)).isNotPresent();

        // Send second ProduceLogRequest.
        CompletableFuture<Exception> future2 = new CompletableFuture<>();
        appendToAccumulator(tb1, row(1, "a"), (tb, leo, e) -> future2.complete(e));
        sender1.runOnce();
        assertThat(idempotenceManager.nextSequence(tb1)).isEqualTo(2);
        assertThat(idempotenceManager.lastAckedBatchSequence(tb1)).isNotPresent();
        assertThat(future1.isDone()).isFalse();
        assertThat(future2.isDone()).isFalse();

        // first finish second ProduceLogRequest with the out-of-order exception.
        finishIdempotentProduceLogRequest(
                1, tb1, 1, createProduceLogResponse(tb1, Errors.OUT_OF_ORDER_SEQUENCE_EXCEPTION));

        sender1.runOnce(); // receive response 1.
        Deque<WriteBatch> queuedBatches =
                accumulator.getReadyDeque(DATA1_PHYSICAL_TABLE_PATH, tb1.getBucket());

        // Make sure that we are queueing the second batch first.
        assertThat(queuedBatches.size()).isEqualTo(1);
        assertThat(queuedBatches.peek().batchSequence()).isEqualTo(1);
        assertThat(idempotenceManager.lastAckedBatchSequence(tb1)).isNotPresent();

        // receive response 0
        finishIdempotentProduceLogRequest(
                0, tb1, 0, createProduceLogResponse(tb1, Errors.REQUEST_TIME_OUT));
        // Make sure we requeued both batches in the correct order.
        assertThat(queuedBatches.size()).isEqualTo(2);
        assertThat(queuedBatches.peek().batchSequence()).isEqualTo(0);
        assertThat(queuedBatches.peekLast().batchSequence()).isEqualTo(1);
        assertThat(idempotenceManager.lastAckedBatchSequence(tb1)).isNotPresent();
        assertThat(future1.isDone()).isFalse();
        assertThat(future2.isDone()).isFalse();
        sender1.runOnce(); // send request 0
        sender1.runOnce(); // don't do anything, only one inflight allowed once we are retrying.
        assertThat(idempotenceManager.lastAckedBatchSequence(tb1)).isNotPresent();

        // Make sure that the requests are sent in order, even though the previous responses were
        // not in order.
        finishIdempotentProduceLogRequest(0, tb1, 0, createProduceLogResponse(tb1, 0L, 1L));
        sender1.runOnce(); // receive response 0.
        assertThat(idempotenceManager.lastAckedBatchSequence(tb1)).isEqualTo(Optional.of(0));
        assertThat(future1.isDone()).isTrue();
        assertThat(future1.get()).isNull();

        // send request 1.
        finishIdempotentProduceLogRequest(1, tb1, 0, createProduceLogResponse(tb1, 1L, 2L));
        sender1.runOnce(); // receive response 1.
        assertThat(idempotenceManager.lastAckedBatchSequence(tb1)).isEqualTo(Optional.of(1));
        assertThat(future2.isDone()).isTrue();
        assertThat(future2.get()).isNull();
    }

    @Test
    void testCorrectHandlingOfOutOfOrderResponsesWhenSecondSucceeds() throws Exception {
        IdempotenceManager idempotenceManager = createIdempotenceManager(true);
        Sender sender1 = setupWithIdempotenceState(idempotenceManager);
        sender1.runOnce();
        assertThat(idempotenceManager.isWriterIdValid()).isTrue();
        assertThat(idempotenceManager.nextSequence(tb1)).isEqualTo(0);

        assertThat(idempotenceManager.nextSequence(tb1)).isEqualTo(0);

        // Send first ProduceLogRequest.
        CompletableFuture<Exception> future1 = new CompletableFuture<>();
        appendToAccumulator(tb1, row(1, "a"), (tb, leo, e) -> future1.complete(e));
        sender1.runOnce();
        assertThat(idempotenceManager.nextSequence(tb1)).isEqualTo(1);
        assertThat(idempotenceManager.lastAckedBatchSequence(tb1)).isNotPresent();

        // Send second ProduceLogRequest.
        CompletableFuture<Exception> future2 = new CompletableFuture<>();
        appendToAccumulator(tb1, row(1, "a"), (tb, leo, e) -> future2.complete(e));
        sender1.runOnce();
        assertThat(idempotenceManager.nextSequence(tb1)).isEqualTo(2);
        assertThat(idempotenceManager.lastAckedBatchSequence(tb1)).isNotPresent();
        assertThat(future1.isDone()).isFalse();
        assertThat(future2.isDone()).isFalse();

        // first finish second ProduceLogRequest with success.
        finishIdempotentProduceLogRequest(1, tb1, 1, createProduceLogResponse(tb1, 1L, 2L));
        sender1.runOnce(); // receive response 1
        assertThat(future2.isDone()).isTrue();
        assertThat(future2.get()).isNull();
        assertThat(future1.isDone()).isFalse();
        Deque<WriteBatch> queuedBatches =
                accumulator.getReadyDeque(DATA1_PHYSICAL_TABLE_PATH, tb1.getBucket());

        assertThat(queuedBatches.size()).isEqualTo(0);
        assertThat(idempotenceManager.lastAckedBatchSequence(tb1)).isEqualTo(Optional.of(1));

        // then finish second ProduceLogRequest with error.
        finishIdempotentProduceLogRequest(
                0, tb1, 0, createProduceLogResponse(tb1, Errors.REQUEST_TIME_OUT));
        // Make sure we requeued both batches in the correct order.
        assertThat(queuedBatches.size()).isEqualTo(1);
        assertThat(queuedBatches.peek().batchSequence()).isEqualTo(0);
        assertThat(idempotenceManager.lastAckedBatchSequence(tb1)).isEqualTo(Optional.of(1));
        sender1.runOnce(); // resend request 1.
        assertThat(idempotenceManager.lastAckedBatchSequence(tb1)).isEqualTo(Optional.of(1));

        // Make sure we handle the out of order successful responses correctly.
        finishIdempotentProduceLogRequest(0, tb1, 0, createProduceLogResponse(tb1, 0L, 1L));
        sender1.runOnce(); // receive response 0.
        assertThat(queuedBatches.size()).isEqualTo(0);
        assertThat(idempotenceManager.lastAckedBatchSequence(tb1)).isEqualTo(Optional.of(1));
        assertThat(future1.isDone()).isTrue();
        assertThat(future1.get()).isNull();
    }

    /**
     * Tests that when a batch's response is lost (e.g., due to request timeout) but the batch was
     * successfully written on the server, and subsequent batches with higher sequence numbers are
     * acknowledged, the client should treat the retried batch as already committed instead of
     * entering an infinite retry loop with {@link
     * org.apache.fluss.exception.OutOfOrderSequenceException}.
     *
     * <p>Detailed scenario:
     *
     * <ol>
     *   <li>Send batch1(seq=0) ~ batch5(seq=4). All 5 batches are successfully written on the
     *       server (server {@code lastBatchSeq=4}).
     *   <li>batch2~5 (seq=1~4) responses return normally. Client {@code lastAckedBatchSequence=4}.
     *   <li>Send batch6(seq=5) and ack successfully. Server {@code lastBatchSeq=5}.
     *   <li>batch1(seq=0) response is lost due to {@code REQUEST_TIME_OUT}. batch1 is re-enqueued
     *       for retry.
     *   <li>Client retries batch1(seq=0). Since server {@code lastBatchSeq=5} and {@code 0 != 5+1},
     *       server returns {@code OUT_OF_ORDER_SEQUENCE_EXCEPTION}.
     *   <li>Client detects {@code batch1.seq(0) <= lastAckedBatchSequence(5)}: batch1 is already
     *       committed. Client completes batch1 successfully without further retries.
     * </ol>
     */
    @Test
    void testCorrectHandlingOfOutOfOrderResponsesWhenResponseLostButSubsequentBatchesSucceeded()
            throws Exception {
        IdempotenceManager idempotenceManager = createIdempotenceManager(true);
        Sender sender1 = setupWithIdempotenceState(idempotenceManager);
        sender1.runOnce();
        assertThat(idempotenceManager.isWriterIdValid()).isTrue();
        assertThat(idempotenceManager.nextSequence(tb1)).isEqualTo(0);

        // Send batch1 (seq=0): its response will be lost later.
        CompletableFuture<Exception> future1 = new CompletableFuture<>();
        appendToAccumulator(tb1, row(1, "a"), (tb, leo, e) -> future1.complete(e));
        sender1.runOnce();
        assertThat(future1.isDone()).isFalse();
        assertThat(idempotenceManager.nextSequence(tb1)).isEqualTo(1);
        assertThat(idempotenceManager.lastAckedBatchSequence(tb1)).isNotPresent();

        // Send batch2~5 (seq=1~4) and collect their futures.
        int numFollowingBatches = 4;
        List<CompletableFuture<Exception>> followingFutures = new ArrayList<>();
        for (int i = 0; i < numFollowingBatches; i++) {
            CompletableFuture<Exception> future = new CompletableFuture<>();
            followingFutures.add(future);
            appendToAccumulator(tb1, row(i + 2, "b"), (tb, leo, e) -> future.complete(e));
            sender1.runOnce();
            assertThat(future.isDone()).isFalse();
        }
        assertThat(idempotenceManager.nextSequence(tb1)).isEqualTo(5);

        // batch2~5 (seq=1~4) responses return normally.
        for (int seq = 1; seq <= numFollowingBatches; seq++) {
            finishIdempotentProduceLogRequest(
                    seq, tb1, 1, createProduceLogResponse(tb1, seq, seq + 1L));
            assertThat(idempotenceManager.lastAckedBatchSequence(tb1)).isEqualTo(Optional.of(seq));
            assertThat(followingFutures.get(seq - 1).isDone()).isTrue();
            assertThat(followingFutures.get(seq - 1).get()).isNull();
        }

        // Send batch6 (seq=5) and ack successfully.
        // Now server lastBatchSeq=5. batch1 (seq=0) is still waiting response.
        CompletableFuture<Exception> future6 = new CompletableFuture<>();
        appendToAccumulator(tb1, row(6, "f"), (tb, leo, e) -> future6.complete(e));
        sender1.runOnce(); // drain and send batch6 (seq=5)
        finishIdempotentProduceLogRequest(5, tb1, 1, createProduceLogResponse(tb1, 5L, 6L));
        assertThat(idempotenceManager.lastAckedBatchSequence(tb1)).isEqualTo(Optional.of(5));
        assertThat(future6.isDone()).isTrue();
        assertThat(future6.get()).isNull();

        // All 6 batches are written successfully on the server (server lastBatchSeq=5).
        // batch1 (seq=0) response is lost, simulated by REQUEST_TIME_OUT.
        finishIdempotentProduceLogRequest(
                0, tb1, 0, createProduceLogResponse(tb1, Errors.REQUEST_TIME_OUT));
        assertThat(future1.isDone()).isFalse();

        // Now retry batch1 (seq=0). Server lastBatchSeq=5, so 0 != 5+1,
        // server returns OUT_OF_ORDER_SEQUENCE_EXCEPTION.
        sender1.runOnce(); // send retried batch1
        finishIdempotentProduceLogRequest(
                0, tb1, 0, createProduceLogResponse(tb1, Errors.OUT_OF_ORDER_SEQUENCE_EXCEPTION));

        // The client should detect that batch1.seq(0) <= lastAckedBatchSequence(5),
        // meaning batch1 was already committed on the server (its response was just lost).
        // It should complete batch1 successfully instead of entering an infinite retry loop.
        assertThat(future1.isDone()).isTrue();
        assertThat(future1.get()).isNull();
        // lastAckedBatchSequence should remain at 5 (not changed by completing already-committed
        // batch1)
        assertThat(idempotenceManager.lastAckedBatchSequence(tb1)).isEqualTo(Optional.of(5));
        // No more inflight batches
        assertThat(sender1.numOfInFlightBatches(tb1)).isEqualTo(0);
    }

    @Test
    void testCorrectHandlingOfDuplicateSequenceError() throws Exception {
        IdempotenceManager idempotenceManager = createIdempotenceManager(true);
        Sender sender1 = setupWithIdempotenceState(idempotenceManager);
        sender1.runOnce();
        assertThat(idempotenceManager.isWriterIdValid()).isTrue();
        assertThat(idempotenceManager.nextSequence(tb1)).isEqualTo(0);

        // first finish second ProduceLogRequest with success.
        CompletableFuture<Exception> future1 = new CompletableFuture<>();
        appendToAccumulator(tb1, row(1, "a"), (tb, leo, e) -> future1.complete(e));
        sender1.runOnce();
        assertThat(idempotenceManager.nextSequence(tb1)).isEqualTo(1);
        assertThat(idempotenceManager.lastAckedBatchSequence(tb1)).isNotPresent();

        // Send second ProduceLogRequest.
        CompletableFuture<Exception> future2 = new CompletableFuture<>();
        appendToAccumulator(tb1, row(1, "a"), (tb, leo, e) -> future2.complete(e));
        sender1.runOnce();
        assertThat(idempotenceManager.nextSequence(tb1)).isEqualTo(2);
        assertThat(idempotenceManager.lastAckedBatchSequence(tb1)).isNotPresent();
        assertThat(future1.isDone()).isFalse();
        assertThat(future2.isDone()).isFalse();

        // first finish second ProduceLogRequest with success.
        finishIdempotentProduceLogRequest(1, tb1, 1, createProduceLogResponse(tb1, 1000L, 1001L));
        sender.runOnce(); // receive response 1
        assertThat(idempotenceManager.lastAckedBatchSequence(tb1)).isEqualTo(Optional.of(1));

        // then finish second ProduceLogRequest with  duplicate batch sequence error.
        finishIdempotentProduceLogRequest(
                0, tb1, 0, createProduceLogResponse(tb1, Errors.DUPLICATE_SEQUENCE_EXCEPTION));
        sender.runOnce(); // receive response 0.

        // Make sure that the last ack'd sequence doesn't change.
        assertThat(idempotenceManager.lastAckedBatchSequence(tb1)).isEqualTo(Optional.of(1));
    }

    @Test
    void testSequenceNumberIncrement() throws Exception {
        int maxRetries = 10;
        IdempotenceManager idempotenceManager = createIdempotenceManager(true);
        Sender sender1 = setupWithIdempotenceState(idempotenceManager, maxRetries, 0);
        sender1.runOnce();
        assertThat(idempotenceManager.isWriterIdValid()).isTrue();
        assertThat(idempotenceManager.nextSequence(tb1)).isEqualTo(0);

        CompletableFuture<Exception> future1 = new CompletableFuture<>();
        appendToAccumulator(tb1, row(1, "a"), (tb, leo, e) -> future1.complete(e));
        sender1.runOnce();
        finishIdempotentProduceLogRequest(0, tb1, 0, createProduceLogResponse(tb1, 0L, 1L));
        sender1.runOnce();
        assertThat(idempotenceManager.nextSequence(tb1)).isEqualTo(1);
        assertThat(idempotenceManager.lastAckedBatchSequence(tb1)).isEqualTo(Optional.of(0));
        assertThat(future1.isDone()).isTrue();
        assertThat(future1.get()).isNull();
    }

    /**
     * Tests that Sender.runOnce() cleans up stale physical table path entries from the
     * RecordAccumulator after a metadata update reveals that a partition no longer exists in the
     * cluster. This prevents unbounded growth of writeBatches for long-running partition-write jobs
     * where partitions are continuously created and dropped.
     */
    @Test
    void testSenderCleansUpStaleWriteBatchesAfterMetadataUpdate() throws Exception {
        // Use a non-partitioned stale path so that ready() / drain() work without needing
        // a partition ID in the cluster's partitionsIdByPath map.
        PhysicalTablePath stalePath = PhysicalTablePath.of(TablePath.of("test_db", "stale_table"));
        long staleTableId = 99901L;
        ServerNode leader = TestingMetadataUpdater.NODE1;
        int[] replicas = new int[] {leader.id()};
        BucketLocation staleBucket =
                new BucketLocation(stalePath, staleTableId, 0, leader.id(), replicas);

        // Build a cluster that includes the normal path AND the soon-to-be-dropped stale path.
        Cluster clusterWithStale =
                new Cluster(
                        metadataUpdater.getCluster().getAliveTabletServers(),
                        metadataUpdater.getCluster().getCoordinatorServer(),
                        addExtraPath(
                                metadataUpdater.getCluster().getBucketLocationsByPath(),
                                stalePath,
                                staleBucket),
                        addExtraTableId(
                                metadataUpdater.getCluster().getTableIdByPath(),
                                stalePath.getTablePath(),
                                staleTableId),
                        metadataUpdater.getCluster().getPartitionIdByPath());
        metadataUpdater.updateCluster(clusterWithStale);

        // Create a minimal TableInfo for the stale table so we can append to it.
        TableInfo staleTableInfo =
                TableInfo.of(
                        stalePath.getTablePath(),
                        staleTableId,
                        1,
                        DATA1_TABLE_INFO.toTableDescriptor(),
                        null,
                        System.currentTimeMillis(),
                        System.currentTimeMillis());

        // Append a record to the stale path — this registers it in writeBatches.
        accumulator.append(
                WriteRecord.forArrowAppend(staleTableInfo, stalePath, row(1, "a"), null),
                (tb, leo, e) -> {},
                clusterWithStale,
                0,
                false);

        assertThat(accumulator.getPhysicalTablePathsInBatches()).contains(stalePath);

        // Drain and deallocate to empty the stale deque. batchTimeoutMs=0 so the batch is
        // immediately ready and drain() will poll it out.
        RecordAccumulator.ReadyCheckResult ready = accumulator.ready(clusterWithStale);
        Map<Integer, List<ReadyWriteBatch>> batches =
                accumulator.drain(clusterWithStale, ready.readyNodes, MAX_REQUEST_SIZE);
        for (List<ReadyWriteBatch> batchList : batches.values()) {
            for (ReadyWriteBatch b : batchList) {
                accumulator.deallocate(b.writeBatch());
            }
        }

        // Simulate partition drop: update the cluster without the stale path.
        Map<PhysicalTablePath, List<BucketLocation>> bucketsWithoutStale =
                new HashMap<>(metadataUpdater.getCluster().getBucketLocationsByPath());
        bucketsWithoutStale.remove(stalePath);
        Map<TablePath, Long> tableIdsWithoutStale =
                new HashMap<>(metadataUpdater.getCluster().getTableIdByPath());
        tableIdsWithoutStale.remove(stalePath.getTablePath());
        Cluster clusterWithoutStale =
                new Cluster(
                        metadataUpdater.getCluster().getAliveTabletServers(),
                        metadataUpdater.getCluster().getCoordinatorServer(),
                        bucketsWithoutStale,
                        tableIdsWithoutStale,
                        metadataUpdater.getCluster().getPartitionIdByPath());
        metadataUpdater.updateCluster(clusterWithoutStale);

        // runOnce() calls cleanupStaleWriteBatches: marks the path stale, then removes it
        // because the deque is already empty.
        sender.runOnce();

        assertThat(accumulator.getPhysicalTablePathsInBatches()).doesNotContain(stalePath);
    }

    /**
     * Verifies that a path is NOT marked stale when bucket leaders become unavailable (server down)
     * but the table/partition ID is still present in the cluster metadata. This guards against the
     * false-positive where transient leader election empties bucketLocationsByPath and triggers
     * premature stale marking.
     */
    @Test
    void testSenderDoesNotMarkPathStaleOnLeaderUnavailability() throws Exception {
        PhysicalTablePath path = PhysicalTablePath.of(TablePath.of("test_db", "live_table"));
        long tableId = 99902L;
        ServerNode leader = TestingMetadataUpdater.NODE1;
        int[] replicas = new int[] {leader.id()};
        BucketLocation bucket = new BucketLocation(path, tableId, 0, leader.id(), replicas);

        // Build a cluster that includes the table path with a live leader.
        Cluster clusterWithLeader =
                new Cluster(
                        metadataUpdater.getCluster().getAliveTabletServers(),
                        metadataUpdater.getCluster().getCoordinatorServer(),
                        addExtraPath(
                                metadataUpdater.getCluster().getBucketLocationsByPath(),
                                path,
                                bucket),
                        addExtraTableId(
                                metadataUpdater.getCluster().getTableIdByPath(),
                                path.getTablePath(),
                                tableId),
                        metadataUpdater.getCluster().getPartitionIdByPath());
        metadataUpdater.updateCluster(clusterWithLeader);

        TableInfo tableInfo =
                TableInfo.of(
                        path.getTablePath(),
                        tableId,
                        1,
                        DATA1_TABLE_INFO.toTableDescriptor(),
                        null,
                        System.currentTimeMillis(),
                        System.currentTimeMillis());

        accumulator.append(
                WriteRecord.forArrowAppend(tableInfo, path, row(1, "a"), null),
                (tb, leo, e) -> {},
                clusterWithLeader,
                0,
                false);

        assertThat(accumulator.getPhysicalTablePathsInBatches()).contains(path);

        // Simulate leader failure: remove path from bucketLocationsByPath but keep tableIdByPath.
        Map<PhysicalTablePath, List<BucketLocation>> bucketsWithoutLeader =
                new HashMap<>(clusterWithLeader.getBucketLocationsByPath());
        bucketsWithoutLeader.remove(path);
        Cluster clusterWithoutLeader =
                new Cluster(
                        metadataUpdater.getCluster().getAliveTabletServers(),
                        metadataUpdater.getCluster().getCoordinatorServer(),
                        bucketsWithoutLeader,
                        addExtraTableId(
                                metadataUpdater.getCluster().getTableIdByPath(),
                                path.getTablePath(),
                                tableId),
                        metadataUpdater.getCluster().getPartitionIdByPath());
        metadataUpdater.updateCluster(clusterWithoutLeader);

        // runOnce() must NOT remove the path because the table still exists in metadata.
        sender.runOnce();

        assertThat(accumulator.getPhysicalTablePathsInBatches()).contains(path);

        // Drain and deallocate to avoid a memory-leak in teardown.
        RecordAccumulator.ReadyCheckResult ready = accumulator.ready(clusterWithLeader);
        Map<Integer, List<ReadyWriteBatch>> batches =
                accumulator.drain(clusterWithLeader, ready.readyNodes, MAX_REQUEST_SIZE);
        for (List<ReadyWriteBatch> batchList : batches.values()) {
            for (ReadyWriteBatch b : batchList) {
                accumulator.deallocate(b.writeBatch());
            }
        }
    }

    /**
     * Tests that a partitioned-table path is cleaned up from writeBatches after the partition is
     * dropped and partitionsIdByPath no longer contains it. This is the partitioned-table
     * counterpart to testSenderCleansUpStaleWriteBatchesAfterMetadataUpdate.
     */
    @Test
    void testSenderCleansUpStalePartitionedPathAfterPartitionDrop() throws Exception {
        TablePath tablePath = TablePath.of("test_db", "partitioned_table");
        long tableId = 99903L;
        long partitionId = 88801L;
        String partitionName = "p_2024";
        PhysicalTablePath partitionPath = PhysicalTablePath.of(tablePath, partitionName);
        ServerNode leader = TestingMetadataUpdater.NODE1;
        int[] replicas = new int[] {leader.id()};
        // Include partitionId in TableBucket so Cluster.leaderFor() can match it correctly.
        BucketLocation bucket =
                new BucketLocation(
                        partitionPath,
                        new TableBucket(tableId, partitionId, 0),
                        leader.id(),
                        replicas);

        // Build cluster with the partition present in both bucketLocationsByPath and
        // partitionsIdByPath.
        Map<PhysicalTablePath, Long> partitionIdByPathWithPartition =
                new HashMap<>(metadataUpdater.getCluster().getPartitionIdByPath());
        partitionIdByPathWithPartition.put(partitionPath, partitionId);
        Cluster clusterWithPartition =
                new Cluster(
                        metadataUpdater.getCluster().getAliveTabletServers(),
                        metadataUpdater.getCluster().getCoordinatorServer(),
                        addExtraPath(
                                metadataUpdater.getCluster().getBucketLocationsByPath(),
                                partitionPath,
                                bucket),
                        addExtraTableId(
                                metadataUpdater.getCluster().getTableIdByPath(),
                                tablePath,
                                tableId),
                        partitionIdByPathWithPartition);
        metadataUpdater.updateCluster(clusterWithPartition);

        TableInfo partitionedTableInfo =
                TableInfo.of(
                        tablePath,
                        tableId,
                        1,
                        DATA1_TABLE_INFO.toTableDescriptor(),
                        null,
                        System.currentTimeMillis(),
                        System.currentTimeMillis());

        accumulator.append(
                WriteRecord.forArrowAppend(partitionedTableInfo, partitionPath, row(1, "a"), null),
                (tb, leo, e) -> {},
                clusterWithPartition,
                0,
                false);

        assertThat(accumulator.getPhysicalTablePathsInBatches()).contains(partitionPath);

        // Drain and deallocate so the deque is empty before cleanup.
        RecordAccumulator.ReadyCheckResult ready = accumulator.ready(clusterWithPartition);
        Map<Integer, List<ReadyWriteBatch>> batches =
                accumulator.drain(clusterWithPartition, ready.readyNodes, MAX_REQUEST_SIZE);
        for (List<ReadyWriteBatch> batchList : batches.values()) {
            for (ReadyWriteBatch b : batchList) {
                accumulator.deallocate(b.writeBatch());
            }
        }

        // Simulate partition drop: remove partition from both bucketLocationsByPath and
        // partitionsIdByPath (as MetadataUtils does after a metadata refresh).
        Map<PhysicalTablePath, List<BucketLocation>> bucketsWithoutPartition =
                new HashMap<>(clusterWithPartition.getBucketLocationsByPath());
        bucketsWithoutPartition.remove(partitionPath);
        Map<PhysicalTablePath, Long> partitionIdByPathWithoutPartition =
                new HashMap<>(partitionIdByPathWithPartition);
        partitionIdByPathWithoutPartition.remove(partitionPath);
        Cluster clusterWithoutPartition =
                new Cluster(
                        metadataUpdater.getCluster().getAliveTabletServers(),
                        metadataUpdater.getCluster().getCoordinatorServer(),
                        bucketsWithoutPartition,
                        addExtraTableId(
                                metadataUpdater.getCluster().getTableIdByPath(),
                                tablePath,
                                tableId),
                        partitionIdByPathWithoutPartition);
        metadataUpdater.updateCluster(clusterWithoutPartition);

        sender.runOnce();

        assertThat(accumulator.getPhysicalTablePathsInBatches()).doesNotContain(partitionPath);
    }

    /**
     * Tests that a partitioned-table path is NOT marked stale when bucket leaders become
     * unavailable (server down / leader election) but partitionsIdByPath still contains the
     * partition. This is the partitioned-table counterpart to
     * testSenderDoesNotMarkPathStaleOnLeaderUnavailability.
     */
    @Test
    void testSenderDoesNotMarkPartitionedPathStaleOnLeaderUnavailability() throws Exception {
        TablePath tablePath = TablePath.of("test_db", "partitioned_table2");
        long tableId = 99904L;
        long partitionId = 88802L;
        String partitionName = "p_2025";
        PhysicalTablePath partitionPath = PhysicalTablePath.of(tablePath, partitionName);
        ServerNode leader = TestingMetadataUpdater.NODE1;
        int[] replicas = new int[] {leader.id()};
        // Include partitionId in TableBucket so Cluster.leaderFor() can match it correctly.
        BucketLocation bucket =
                new BucketLocation(
                        partitionPath,
                        new TableBucket(tableId, partitionId, 0),
                        leader.id(),
                        replicas);

        // Cluster with partition fully present.
        Map<PhysicalTablePath, Long> partitionIdByPathWithPartition =
                new HashMap<>(metadataUpdater.getCluster().getPartitionIdByPath());
        partitionIdByPathWithPartition.put(partitionPath, partitionId);
        Cluster clusterWithPartition =
                new Cluster(
                        metadataUpdater.getCluster().getAliveTabletServers(),
                        metadataUpdater.getCluster().getCoordinatorServer(),
                        addExtraPath(
                                metadataUpdater.getCluster().getBucketLocationsByPath(),
                                partitionPath,
                                bucket),
                        addExtraTableId(
                                metadataUpdater.getCluster().getTableIdByPath(),
                                tablePath,
                                tableId),
                        partitionIdByPathWithPartition);
        metadataUpdater.updateCluster(clusterWithPartition);

        TableInfo partitionedTableInfo =
                TableInfo.of(
                        tablePath,
                        tableId,
                        1,
                        DATA1_TABLE_INFO.toTableDescriptor(),
                        null,
                        System.currentTimeMillis(),
                        System.currentTimeMillis());

        accumulator.append(
                WriteRecord.forArrowAppend(partitionedTableInfo, partitionPath, row(1, "a"), null),
                (tb, leo, e) -> {},
                clusterWithPartition,
                0,
                false);

        assertThat(accumulator.getPhysicalTablePathsInBatches()).contains(partitionPath);

        // Drain and deallocate BEFORE simulating leader failure so that the deque is empty.
        // This prevents ready() from adding the path to unknownLeaderTables (which would trigger
        // a metadata update in TestingMetadataUpdater and cause an unexpected exception).
        RecordAccumulator.ReadyCheckResult readyBefore = accumulator.ready(clusterWithPartition);
        Map<Integer, List<ReadyWriteBatch>> batchesBefore =
                accumulator.drain(clusterWithPartition, readyBefore.readyNodes, MAX_REQUEST_SIZE);
        for (List<ReadyWriteBatch> batchList : batchesBefore.values()) {
            for (ReadyWriteBatch b : batchList) {
                accumulator.deallocate(b.writeBatch());
            }
        }

        // Simulate leader failure: remove partition from bucketLocationsByPath only.
        // partitionsIdByPath still contains the partition — it has not been dropped.
        Map<PhysicalTablePath, List<BucketLocation>> bucketsWithoutLeader =
                new HashMap<>(clusterWithPartition.getBucketLocationsByPath());
        bucketsWithoutLeader.remove(partitionPath);
        Cluster clusterWithoutLeader =
                new Cluster(
                        metadataUpdater.getCluster().getAliveTabletServers(),
                        metadataUpdater.getCluster().getCoordinatorServer(),
                        bucketsWithoutLeader,
                        addExtraTableId(
                                metadataUpdater.getCluster().getTableIdByPath(),
                                tablePath,
                                tableId),
                        partitionIdByPathWithPartition);
        metadataUpdater.updateCluster(clusterWithoutLeader);

        // runOnce() must NOT remove the partition path because it still exists in metadata.
        sender.runOnce();

        assertThat(accumulator.getPhysicalTablePathsInBatches()).contains(partitionPath);
    }

    private static Map<PhysicalTablePath, List<BucketLocation>> addExtraPath(
            Map<PhysicalTablePath, List<BucketLocation>> original,
            PhysicalTablePath extraPath,
            BucketLocation extraBucket) {
        Map<PhysicalTablePath, List<BucketLocation>> result = new HashMap<>(original);
        result.put(extraPath, Collections.singletonList(extraBucket));
        return result;
    }

    private static Map<TablePath, Long> addExtraTableId(
            Map<TablePath, Long> original, TablePath tablePath, long tableId) {
        Map<TablePath, Long> result = new HashMap<>(original);
        result.put(tablePath, tableId);
        return result;
    }

    @Test
    void testSendWhenDestinationIsNullInMetadata() throws Exception {
        long offset = 0;
        CompletableFuture<Exception> future = new CompletableFuture<>();
        appendToAccumulator(tb1, row(1, "a"), (tb, leo, e) -> future.complete(e));

        int leaderNode = metadataUpdater.leaderFor(DATA1_TABLE_PATH, tb1);
        // now, remove leader node ,so that send destination
        // server node is null
        Cluster oldCluster = metadataUpdater.getCluster();
        Map<Integer, ServerNode> aliveTabletServersById =
                new HashMap<>(oldCluster.getAliveTabletServers());
        aliveTabletServersById.remove(leaderNode);
        Cluster newCluster =
                new Cluster(
                        aliveTabletServersById,
                        oldCluster.getCoordinatorServer(),
                        oldCluster.getBucketLocationsByPath(),
                        oldCluster.getTableIdByPath(),
                        oldCluster.getPartitionIdByPath());

        metadataUpdater.updateCluster(newCluster);

        sender.runOnce();
        // should be no inflight batches
        assertThat(sender.numOfInFlightBatches(tb1)).isEqualTo(0);

        // the bucket location should be empty for the bucket since we'll invalid it
        // when send to a null destination
        assertThat(metadataUpdater.getCluster().getBucketLocation(tb1)).isEmpty();

        // update with old cluster to mock a metadata update
        metadataUpdater.updateCluster(oldCluster);

        // send again, should send successfully
        sender.runOnce();
        assertThat(sender.numOfInFlightBatches(tb1)).isEqualTo(1);

        finishRequest(tb1, 0, createProduceLogResponse(tb1, offset, 1));

        // send again, should send nothing since no batch in queue
        sender.runOnce();
        assertThat(sender.numOfInFlightBatches(tb1)).isEqualTo(0);
        assertThat(future.get()).isNull();
    }

    @Test
    void testRetryPutKeyWithSchemaNotExistException() throws Exception {
        TableBucket tableBucket = new TableBucket(DATA1_TABLE_ID_PK, 0);

        BinaryRow row = compactedRow(DATA1_ROW_TYPE, new Object[] {1, "a"});
        int[] pkIndex = DATA1_SCHEMA_PK.getPrimaryKeyIndexes();
        byte[] key = new CompactedKeyEncoder(DATA1_ROW_TYPE, pkIndex).encodeKey(row);
        CompletableFuture<Exception> future = new CompletableFuture<>();
        accumulator.append(
                WriteRecord.forUpsert(
                        DATA1_TABLE_INFO_PK,
                        PhysicalTablePath.of(DATA1_TABLE_PATH_PK),
                        row,
                        key,
                        key,
                        WriteFormat.COMPACTED_KV,
                        null),
                (tb, leo, e) -> future.complete(e),
                metadataUpdater.getCluster(),
                0,
                false);
        sender.runOnce();
        finishRequest(tableBucket, 0, createPutKvResponse(tableBucket, SCHEMA_NOT_EXIST));
        assertThat(sender.numOfInFlightBatches(tableBucket)).isEqualTo(0);

        // retry to put kv request again
        sender.runOnce();
        assertThat(sender.numOfInFlightBatches(tableBucket)).isEqualTo(1);
        finishRequest(tableBucket, 0, createPutKvResponse(tableBucket, 1));
        assertThat(sender.numOfInFlightBatches(tableBucket)).isEqualTo(0);
        assertThat(future.get()).isNull();
    }

    @Test
    void testSendWhenTableIdChanges() throws Exception {
        CompletableFuture<Exception> future1 = new CompletableFuture<>();
        appendToAccumulator(tb1, row(1, "a"), (tb, leo, e) -> future1.complete(e));
        TableInfo newTableInfo =
                TableInfo.of(
                        DATA1_TABLE_PATH,
                        DATA2_TABLE_ID,
                        1,
                        DATA1_TABLE_DESCRIPTOR,
                        DEFAULT_REMOTE_DATA_DIR,
                        System.currentTimeMillis(),
                        System.currentTimeMillis());
        TableBucket newTableBucket = new TableBucket(newTableInfo.getTableId(), tb1.getBucket());

        metadataUpdater.updateTableInfos(Collections.singletonMap(DATA1_TABLE_PATH, newTableInfo));
        sender.runOnce();
        Exception exception = future1.get();
        assertThat(exception).isNotNull();
        assertThat(exception).isExactlyInstanceOf(TableNotExistException.class);
        assertThat(exception.getMessage())
                .contains(
                        String.format(
                                "Table '%s' has been dropped and re-created with a new table ID (old: %s, new: %s)",
                                DATA1_TABLE_PATH, DATA1_TABLE_ID, newTableInfo.getTableId()));

        CompletableFuture<Exception> future2 = new CompletableFuture<>();
        appendToAccumulator(
                newTableInfo, newTableBucket, row(1, "a"), (tb, leo, e) -> future2.complete(e));
        sender.runOnce();
        finishRequest(newTableBucket, 0, createProduceLogResponse(newTableBucket, 0, 1));
        assertThat(future2.get()).isNull();
    }

    private TestingMetadataUpdater initializeMetadataUpdater() {
        Map<TablePath, TableInfo> tableInfos = new HashMap<>();
        tableInfos.put(DATA1_TABLE_PATH, DATA1_TABLE_INFO);
        tableInfos.put(DATA1_TABLE_PATH_PK, DATA1_TABLE_INFO_PK);
        return new TestingMetadataUpdater(tableInfos);
    }

    private void appendToAccumulator(TableBucket tb, GenericRow row, WriteCallback writeCallback)
            throws Exception {
        appendToAccumulator(DATA1_TABLE_INFO, tb, row, writeCallback);
    }

    private void appendToAccumulator(
            TableInfo tableInfo, TableBucket tb, GenericRow row, WriteCallback writeCallback)
            throws Exception {
        accumulator.append(
                WriteRecord.forArrowAppend(tableInfo, DATA1_PHYSICAL_TABLE_PATH, row, null),
                writeCallback,
                metadataUpdater.getCluster(),
                tb.getBucket(),
                false);
    }

    private ApiMessage getRequest(TableBucket tb, int index) {
        TestTabletServerGateway gateway =
                (TestTabletServerGateway)
                        metadataUpdater.newTabletServerClientForNode(
                                metadataUpdater.leaderFor(DATA1_TABLE_PATH, tb));
        return gateway.getRequest(index);
    }

    private void finishRequest(TableBucket tb, int index, ApiMessage response) {
        TestTabletServerGateway gateway =
                (TestTabletServerGateway)
                        metadataUpdater.newTabletServerClientForNode(
                                metadataUpdater.leaderFor(DATA1_TABLE_PATH, tb));
        gateway.response(index, response);
    }

    private int pendingRequestSize(TableBucket tb) {
        TestTabletServerGateway gateway =
                (TestTabletServerGateway)
                        metadataUpdater.newTabletServerClientForNode(
                                metadataUpdater.leaderFor(DATA1_TABLE_PATH, tb));
        return gateway.pendingRequestSize();
    }

    private void finishIdempotentProduceLogRequest(
            int batchSequence, TableBucket tb, int index, ProduceLogResponse response) {
        TestTabletServerGateway gateway =
                (TestTabletServerGateway)
                        metadataUpdater.newTabletServerClientForNode(
                                metadataUpdater.leaderFor(DATA1_TABLE_PATH, tb));
        ApiMessage request = getRequest(tb1, index);
        assertThat(request).isInstanceOf(ProduceLogRequest.class);
        assertThat(hasIdempotentRecords(tb1, (ProduceLogRequest) request)).isTrue();
        assertBatchSequenceEquals(tb1, (ProduceLogRequest) request, batchSequence);
        gateway.response(index, response);
    }

    private ProduceLogResponse createProduceLogResponse(
            TableBucket tb, long baseOffset, long endOffset) {
        return makeProduceLogResponse(
                Collections.singletonList(
                        new ProduceLogResultForBucket(tb, baseOffset, endOffset)));
    }

    private ProduceLogResponse createProduceLogResponse(TableBucket tb, Errors error) {
        return makeProduceLogResponse(
                Collections.singletonList(new ProduceLogResultForBucket(tb, error.toApiError())));
    }

    private PutKvResponse createPutKvResponse(TableBucket tb, long endOffset) {
        return makePutKvResponse(
                Collections.singletonList(new PutKvResultForBucket(tb, endOffset)));
    }

    private PutKvResponse createPutKvResponse(TableBucket tb, Errors error) {
        return makePutKvResponse(
                Collections.singletonList(new PutKvResultForBucket(tb, error.toApiError())));
    }

    private Sender setupWithIdempotenceState() {
        return setupWithIdempotenceState(createIdempotenceManager(false));
    }

    private Sender setupWithIdempotenceState(IdempotenceManager idempotenceManager) {
        return setupWithIdempotenceState(idempotenceManager, Integer.MAX_VALUE, 0);
    }

    private Sender setupWithIdempotenceState(
            IdempotenceManager idempotenceManager, int reties, int batchTimeoutMs) {
        Configuration conf = new Configuration();
        conf.set(ConfigOptions.CLIENT_WRITER_BUFFER_MEMORY_SIZE, new MemorySize(TOTAL_MEMORY_SIZE));
        conf.set(ConfigOptions.CLIENT_WRITER_BATCH_SIZE, new MemorySize(BATCH_SIZE));
        conf.set(ConfigOptions.CLIENT_WRITER_BUFFER_PAGE_SIZE, new MemorySize(PAGE_SIZE));
        conf.set(ConfigOptions.CLIENT_WRITER_BATCH_TIMEOUT, Duration.ofMillis(batchTimeoutMs));
        accumulator =
                new RecordAccumulator(
                        conf, idempotenceManager, writerMetricGroup, SystemClock.getInstance());
        return new Sender(
                accumulator,
                REQUEST_TIMEOUT,
                MAX_REQUEST_SIZE,
                ACKS_ALL,
                reties,
                metadataUpdater,
                idempotenceManager,
                writerMetricGroup);
    }

    private IdempotenceManager createIdempotenceManager(boolean idempotenceEnabled) {
        return new IdempotenceManager(
                idempotenceEnabled,
                MAX_INFLIGHT_REQUEST_PER_BUCKET,
                metadataUpdater.newRandomTabletServerClient(),
                metadataUpdater);
    }

    private static boolean hasIdempotentRecords(TableBucket tb, ProduceLogRequest request) {
        MemoryLogRecords memoryLogRecords = getProduceLogData(request).get(tb);
        return memoryLogRecords.batchIterator().next().writerId() != NO_WRITER_ID;
    }

    private static void assertBatchSequenceEquals(
            TableBucket tb, ProduceLogRequest request, int expectedBatchSequence) {
        MemoryLogRecords memoryLogRecords = getProduceLogData(request).get(tb);
        assertThat(memoryLogRecords.batchIterator().next().batchSequence())
                .isEqualTo(expectedBatchSequence);
    }
}
