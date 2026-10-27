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

package org.apache.fluss.client.table.scanner.batch;

import org.apache.fluss.client.metadata.MetadataUpdater;
import org.apache.fluss.cluster.BucketLocation;
import org.apache.fluss.cluster.Cluster;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.InvalidScanRequestException;
import org.apache.fluss.exception.NetworkException;
import org.apache.fluss.exception.ScannerExpiredException;
import org.apache.fluss.exception.TooManyScannersException;
import org.apache.fluss.exception.UnknownScannerIdException;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.SchemaGetter;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableOrPartition;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.DefaultValueRecordBatch;
import org.apache.fluss.record.TestingSchemaGetter;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.rpc.TestingTabletGatewayService;
import org.apache.fluss.rpc.gateway.TabletServerGateway;
import org.apache.fluss.rpc.messages.ScanKvRequest;
import org.apache.fluss.rpc.messages.ScanKvResponse;
import org.apache.fluss.rpc.protocol.Errors;
import org.apache.fluss.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.fluss.shaded.netty4.io.netty.buffer.Unpooled;
import org.apache.fluss.utils.CloseableIterator;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nullable;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static org.apache.fluss.record.TestData.DATA1_ROW_TYPE;
import static org.apache.fluss.record.TestData.DATA1_SCHEMA_PK;
import static org.apache.fluss.record.TestData.DATA1_TABLE_ID_PK;
import static org.apache.fluss.record.TestData.DATA1_TABLE_INFO_PK;
import static org.apache.fluss.record.TestData.DEFAULT_SCHEMA_ID;
import static org.apache.fluss.testutils.DataTestUtils.compactedRow;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/** Protocol-level unit tests for {@link KvBatchScanner} against a recording fake gateway. */
class KvBatchScannerTest {

    private static final TableBucket BUCKET_0 = new TableBucket(DATA1_TABLE_ID_PK, 0);
    private static final byte[] SCANNER_ID = new byte[] {1, 2, 3, 4};
    private static final Duration POLL_TIMEOUT = Duration.ofSeconds(5);
    private static final SchemaGetter SCHEMA_GETTER =
            new TestingSchemaGetter((short) 1, DATA1_SCHEMA_PK);

    private final List<ByteBuf> parsedBuffers = new ArrayList<>();

    @AfterEach
    void releaseParsedBuffers() {
        for (ByteBuf parsedBuffer : parsedBuffers) {
            releaseIfNeeded(parsedBuffer);
        }
    }

    @Test
    void firstPollOpensScannerWithCallSeqIdZero() throws Exception {
        RecordingGateway gateway = new RecordingGateway();
        gateway.enqueue(emptyTerminalResponse(SCANNER_ID));

        try (KvBatchScanner scanner = newScanner(gateway)) {
            assertThat(scanner.pollBatch(POLL_TIMEOUT)).isNull();

            ScanKvRequest open = gateway.requests.get(0);
            assertThat(open.hasBucketScanReq()).isTrue();
            assertThat(open.hasScannerId()).isFalse();
            assertThat(open.hasCallSeqId()).isTrue(); // open request also carries callSeqId
            assertThat(open.getCallSeqId()).isEqualTo(0); // open request with 0 seq_id
            assertThat(open.getBucketScanReq().getTableId()).isEqualTo(DATA1_TABLE_ID_PK);
            assertThat(open.getBucketScanReq().getBucketId()).isEqualTo(0);
        }
    }

    @Test
    void continuationsUsePreIncrementedCallSeqIdStartingAtOne() throws Exception {
        RecordingGateway gateway = new RecordingGateway();
        gateway.enqueue(emptyContinuationResponse(SCANNER_ID));
        gateway.enqueue(emptyContinuationResponse(SCANNER_ID));
        gateway.enqueue(emptyContinuationResponse(SCANNER_ID));
        gateway.enqueue(emptyTerminalResponse(SCANNER_ID));

        try (KvBatchScanner scanner = newScanner(gateway)) {
            for (int i = 0; i < 4; i++) {
                scanner.pollBatch(POLL_TIMEOUT);
            }
        }

        assertThat(gateway.requests).hasSize(4);
        assertThat(gateway.requests.get(0).getCallSeqId()).isEqualTo(0);
        assertThat(gateway.requests.get(1).getCallSeqId()).isEqualTo(1);
        assertThat(gateway.requests.get(2).getCallSeqId()).isEqualTo(2);
        assertThat(gateway.requests.get(3).getCallSeqId()).isEqualTo(3);
    }

    @Test
    void continuationsCarryScannerIdFromFirstResponse() throws Exception {
        RecordingGateway gateway = new RecordingGateway();
        gateway.enqueue(emptyContinuationResponse(SCANNER_ID));
        gateway.enqueue(emptyTerminalResponse(SCANNER_ID));

        try (KvBatchScanner scanner = newScanner(gateway)) {
            scanner.pollBatch(POLL_TIMEOUT);
            scanner.pollBatch(POLL_TIMEOUT);
        }

        assertThat(gateway.requests.get(1).hasScannerId()).isTrue();
        assertThat(gateway.requests.get(1).getScannerId()).isEqualTo(SCANNER_ID);
    }

    @Test
    void emptyBucketReturnsNullOnFirstPoll() throws Exception {
        RecordingGateway gateway = new RecordingGateway();
        gateway.enqueue(emptyTerminalResponse(SCANNER_ID));

        try (KvBatchScanner scanner = newScanner(gateway)) {
            assertThat(scanner.pollBatch(POLL_TIMEOUT)).isNull();
            assertThat(scanner.pollBatch(POLL_TIMEOUT)).isNull();
        }

        assertThat(gateway.requests).hasSize(1);
    }

    @Test
    void pipelinesNextRequestImmediatelyAfterResponse() throws Exception {
        RecordingGateway gateway = new RecordingGateway();
        gateway.enqueue(emptyContinuationResponse(SCANNER_ID));
        gateway.enqueue(emptyTerminalResponse(SCANNER_ID));

        try (KvBatchScanner scanner = newScanner(gateway)) {
            scanner.pollBatch(POLL_TIMEOUT);
            assertThat(gateway.requests).hasSize(2);
        }
    }

    // -------------------------------------------------------------------------
    // close() discipline
    // -------------------------------------------------------------------------

    @Test
    void closeIsIdempotent() throws Exception {
        RecordingGateway gateway = new RecordingGateway();
        gateway.enqueue(emptyContinuationResponse(SCANNER_ID));
        gateway.enqueue(emptyTerminalResponse(SCANNER_ID));

        KvBatchScanner scanner = newScanner(gateway);
        scanner.pollBatch(POLL_TIMEOUT);
        scanner.close();
        int after1 = gateway.requests.size();
        scanner.close();
        scanner.close();
        assertThat(gateway.requests).hasSize(after1);
        assertThat(scanner.isClosed()).isTrue();
    }

    @Test
    void closeAfterDrainedDoesNotSendCloseScanner() throws Exception {
        RecordingGateway gateway = new RecordingGateway();
        gateway.enqueue(emptyTerminalResponse(SCANNER_ID));

        try (KvBatchScanner scanner = newScanner(gateway)) {
            assertThat(scanner.pollBatch(POLL_TIMEOUT)).isNull();
            assertThat(scanner.isDrained()).isTrue();
        }

        assertThat(gateway.requests).hasSize(1);
        assertThat(gateway.requests.stream().anyMatch(KvBatchScannerTest::isCloseRequest))
                .isFalse();
    }

    @Test
    void closeMidStreamSendsCloseScannerWithScannerId() throws Exception {
        RecordingGateway gateway = new RecordingGateway();
        gateway.enqueue(emptyContinuationResponse(SCANNER_ID));
        gateway.enqueue(emptyContinuationResponse(SCANNER_ID));

        TestMetadataUpdater meta = new TestMetadataUpdater(gateway);
        KvBatchScanner scanner = newScanner(meta, BUCKET_0);
        scanner.pollBatch(POLL_TIMEOUT);
        scanner.close();

        // open + pipelined continuation + close_scanner
        assertThat(gateway.requests).hasSize(3);
        ScanKvRequest closeReq = gateway.requests.get(2);
        assertThat(isCloseRequest(closeReq)).isTrue();
        assertThat(closeReq.getScannerId()).isEqualTo(SCANNER_ID);
        assertThat(meta.invalidations.get()).isZero();
        assertThat(meta.getCluster().leaderFor(BUCKET_0)).isZero();
    }

    // -------------------------------------------------------------------------
    // TOO_MANY_SCANNERS retry
    // -------------------------------------------------------------------------

    @Test
    void tooManyScannersOnOpenRetriesUpToLimitThenSucceeds() throws Exception {
        RecordingGateway gateway = new RecordingGateway();
        gateway.enqueue(errorResponse(Errors.TOO_MANY_SCANNERS));
        gateway.enqueue(errorResponse(Errors.TOO_MANY_SCANNERS));
        gateway.enqueue(errorResponse(Errors.TOO_MANY_SCANNERS));
        gateway.enqueue(emptyTerminalResponse(SCANNER_ID));

        try (KvBatchScanner scanner = newScanner(gateway)) {
            assertThat(scanner.pollBatch(POLL_TIMEOUT))
                    .isNotNull()
                    .satisfies(it -> assertThat(it.hasNext()).isFalse());
            assertThat(scanner.pollBatch(POLL_TIMEOUT))
                    .isNotNull()
                    .satisfies(it -> assertThat(it.hasNext()).isFalse());
            assertThat(scanner.pollBatch(POLL_TIMEOUT))
                    .isNotNull()
                    .satisfies(it -> assertThat(it.hasNext()).isFalse());
            assertThat(scanner.pollBatch(POLL_TIMEOUT)).isNull();
            assertThat(scanner.openRetries()).isEqualTo(3);
        }
    }

    @Test
    void tooManyScannersOnOpenExhaustsRetriesAndFails() throws Exception {
        RecordingGateway gateway = new RecordingGateway();
        gateway.enqueue(errorResponse(Errors.TOO_MANY_SCANNERS));
        gateway.enqueue(errorResponse(Errors.TOO_MANY_SCANNERS));
        gateway.enqueue(errorResponse(Errors.TOO_MANY_SCANNERS));
        gateway.enqueue(errorResponse(Errors.TOO_MANY_SCANNERS));

        KvBatchScanner scanner = newScanner(gateway);
        scanner.pollBatch(POLL_TIMEOUT);
        scanner.pollBatch(POLL_TIMEOUT);
        scanner.pollBatch(POLL_TIMEOUT);

        assertThatThrownBy(() -> scanner.pollBatch(POLL_TIMEOUT))
                .isInstanceOf(IOException.class)
                .hasCauseInstanceOf(TooManyScannersException.class);
        assertThat(scanner.isClosed()).isTrue();
    }

    // -------------------------------------------------------------------------
    // ByteBuf ownership: every ScanKvResponse handed to KvBatchScanner carries a real,
    // ref-counted, lazily-parsed ByteBuf (mirrors NettyClientHandler#channelRead); the scanner
    // must release it exactly once on every exit path.
    // -------------------------------------------------------------------------

    @Test
    void emptyEofResponseBufIsReleasedAfterDrainAndClose() throws Exception {
        RecordingGateway gateway = new RecordingGateway();
        ScanKvResponse response = parseFromWire(emptyTerminalResponse(SCANNER_ID));
        ByteBuf buf = response.getParsedByteBuf();
        assertThat(buf.refCnt()).isEqualTo(1);
        gateway.enqueue(response);

        KvBatchScanner scanner = newScanner(gateway);
        assertThat(scanner.pollBatch(POLL_TIMEOUT)).isNull();
        assertThat(buf.refCnt()).isEqualTo(0);

        scanner.close();
        assertThat(buf.refCnt()).isEqualTo(0);
    }

    @Test
    void unknownScannerIdErrorResponseBufIsReleased() throws Exception {
        RecordingGateway gateway = new RecordingGateway();
        gateway.enqueue(emptyContinuationResponse(SCANNER_ID));
        ScanKvResponse errorResponse = parseFromWire(errorResponse(Errors.UNKNOWN_SCANNER_ID));
        ByteBuf buf = errorResponse.getParsedByteBuf();
        assertThat(buf.refCnt()).isEqualTo(1);
        gateway.enqueue(errorResponse);

        KvBatchScanner scanner = newScanner(gateway);
        scanner.pollBatch(POLL_TIMEOUT);
        assertThatThrownBy(() -> scanner.pollBatch(POLL_TIMEOUT)).isInstanceOf(IOException.class);

        assertThat(buf.refCnt()).isEqualTo(0);
    }

    @Test
    void completedInFlightResponseBufIsReleasedOnClose() throws Exception {
        RecordingGateway gateway = new RecordingGateway();
        gateway.enqueue(parseFromWire(emptyContinuationResponse(SCANNER_ID)));
        ScanKvResponse prefetched = parseFromWire(emptyTerminalResponse(SCANNER_ID));
        ByteBuf prefetchedBuf = prefetched.getParsedByteBuf();
        gateway.enqueue(prefetched);

        KvBatchScanner scanner = newScanner(gateway);
        // Consumes the open response and pipelines the continuation request;
        // RecordingGateway resolves it synchronously so `prefetched` is already completed
        // but unconsumed.
        scanner.pollBatch(POLL_TIMEOUT);
        assertThat(prefetchedBuf.refCnt()).isEqualTo(1);

        scanner.close();
        assertThat(prefetchedBuf.refCnt()).isEqualTo(0);
    }

    @Test
    void closeScannerAckResponseBufIsReleased() throws Exception {
        RecordingGateway gateway = new RecordingGateway();
        gateway.enqueue(parseFromWire(emptyContinuationResponse(SCANNER_ID)));

        KvBatchScanner scanner = newScanner(gateway);
        scanner.pollBatch(POLL_TIMEOUT);
        scanner.close();

        assertThat(gateway.closeAckResponses).hasSize(1);
        assertThat(gateway.closeAckResponses.get(0).getParsedByteBuf().refCnt()).isEqualTo(0);
    }

    @Test
    void recordsAndIntermediateEmptyResponsesReleaseBufsAfterRowsAreMaterialized()
            throws Exception {
        RecordingGateway gateway = new RecordingGateway();
        ScanKvResponse first = recordsResponse(true, 1);
        ScanKvResponse empty = parseFromWire(emptyContinuationResponse(SCANNER_ID));
        ScanKvResponse last = recordsResponse(false, 2);
        gateway.enqueue(first);
        gateway.enqueue(empty);
        gateway.enqueue(last);

        try (KvBatchScanner scanner = newScanner(gateway)) {
            CloseableIterator<InternalRow> firstRows = scanner.pollBatch(POLL_TIMEOUT);
            assertThat(first.getParsedByteBuf().refCnt()).isEqualTo(0);
            assertThat(firstRows.hasNext()).isTrue();
            InternalRow firstRow = firstRows.next();
            assertThat(firstRow.getInt(0)).isEqualTo(1);
            assertThat(firstRow.getString(1).toString()).isEqualTo("value-1");

            CloseableIterator<InternalRow> intermediate = scanner.pollBatch(POLL_TIMEOUT);
            assertThat(intermediate).isNotNull();
            assertThat(intermediate.hasNext()).isFalse();
            assertThat(empty.getParsedByteBuf().refCnt()).isEqualTo(0);

            CloseableIterator<InternalRow> lastRows = scanner.pollBatch(POLL_TIMEOUT);
            assertThat(last.getParsedByteBuf().refCnt()).isEqualTo(0);
            assertThat(lastRows.hasNext()).isTrue();
            InternalRow lastRow = lastRows.next();
            assertThat(lastRow.getInt(0)).isEqualTo(2);
            assertThat(lastRow.getString(1).toString()).isEqualTo("value-2");
        }
    }

    @Test
    void tooManyScannersRetryResponseBufIsReleased() throws Exception {
        RecordingGateway gateway = new RecordingGateway();
        ScanKvResponse tooMany = parseFromWire(errorResponse(Errors.TOO_MANY_SCANNERS));
        ScanKvResponse terminal = parseFromWire(emptyTerminalResponse(SCANNER_ID));
        gateway.enqueue(tooMany);
        gateway.enqueue(terminal);

        try (KvBatchScanner scanner = newScanner(gateway)) {
            CloseableIterator<InternalRow> retried = scanner.pollBatch(POLL_TIMEOUT);
            assertThat(retried).isNotNull();
            assertThat(retried.hasNext()).isFalse();
            assertThat(tooMany.getParsedByteBuf().refCnt()).isEqualTo(0);
            assertThat(scanner.pollBatch(POLL_TIMEOUT)).isNull();
            assertThat(terminal.getParsedByteBuf().refCnt()).isEqualTo(0);
        }
    }

    @Test
    void closeReleasesContinuationResponseCompletedAfterClose() throws Exception {
        RecordingGateway gateway = new RecordingGateway();
        gateway.enqueue(parseFromWire(emptyContinuationResponse(SCANNER_ID)));
        CompletableFuture<ScanKvResponse> continuation = new CompletableFuture<>();
        gateway.enqueue(continuation);
        ScanKvResponse lateResponse = parseFromWire(emptyTerminalResponse(SCANNER_ID));

        KvBatchScanner scanner = newScanner(gateway);
        scanner.pollBatch(POLL_TIMEOUT);
        scanner.close();
        assertThat(continuation.isCancelled()).isFalse();

        continuation.complete(lateResponse);
        assertThat(lateResponse.getParsedByteBuf().refCnt()).isEqualTo(0);
    }

    @Test
    void closeReleasesOpenResponseCompletedBeforeClose() throws Exception {
        RecordingGateway gateway = new RecordingGateway();
        CompletableFuture<ScanKvResponse> open = neverCompleting();
        gateway.enqueue(open);
        ScanKvResponse completed = parseFromWire(emptyTerminalResponse(SCANNER_ID));

        KvBatchScanner scanner = newScanner(gateway);
        assertThat(scanner.pollBatch(Duration.ZERO)).isNotNull();
        open.complete(completed);
        scanner.close();

        assertThat(completed.getParsedByteBuf().refCnt()).isEqualTo(0);
    }

    @Test
    void closeReleasesOpenResponseCompletedAfterCloseExactlyOnce() throws Exception {
        RecordingGateway gateway = new RecordingGateway();
        CompletableFuture<ScanKvResponse> open = neverCompleting();
        gateway.enqueue(open);
        ScanKvResponse lateResponse = parseFromWire(emptyTerminalResponse(SCANNER_ID), true);
        ByteBuf lateBuffer = lateResponse.getParsedByteBuf();

        KvBatchScanner scanner = newScanner(gateway);
        assertThat(scanner.pollBatch(Duration.ZERO)).isNotNull();
        scanner.close();
        scanner.close();
        open.complete(lateResponse);

        assertThat(lateBuffer.refCnt()).isEqualTo(0);
        verify(lateBuffer, times(1)).release();
    }

    @Test
    void closeScannerBusinessErrorResponseBufIsReleased() throws Exception {
        RecordingGateway gateway = new RecordingGateway();
        gateway.enqueue(parseFromWire(emptyContinuationResponse(SCANNER_ID)));
        ScanKvResponse closeAck = parseFromWire(errorResponse(Errors.INVALID_SCAN_REQUEST));
        gateway.enqueueClose(closeAck);

        KvBatchScanner scanner = newScanner(gateway);
        scanner.pollBatch(POLL_TIMEOUT);
        scanner.close();

        assertThat(closeAck.getParsedByteBuf().refCnt()).isEqualTo(0);
    }

    @Test
    void closeReleasesCloseAckCompletedAfterClose() throws Exception {
        RecordingGateway gateway = new RecordingGateway();
        gateway.enqueue(parseFromWire(emptyContinuationResponse(SCANNER_ID)));
        gateway.enqueue(neverCompleting());
        CompletableFuture<ScanKvResponse> closeAck = new CompletableFuture<>();
        gateway.enqueueClose(closeAck);

        KvBatchScanner scanner = newScanner(gateway);
        scanner.pollBatch(POLL_TIMEOUT);
        scanner.close();

        ScanKvResponse lateCloseAck = parseFromWire(new ScanKvResponse().setHasMoreResults(false));
        closeAck.complete(lateCloseAck);
        assertThat(lateCloseAck.getParsedByteBuf().refCnt()).isEqualTo(0);
    }

    @Test
    void parseFailureReleasesResponseAndCloseReleasesPrefetchedResponse() throws Exception {
        RecordingGateway gateway = new RecordingGateway();
        ScanKvResponse invalid =
                parseFromWire(
                        new ScanKvResponse()
                                .setScannerId(SCANNER_ID)
                                .setHasMoreResults(true)
                                .setRecords(new byte[1]));
        ScanKvResponse prefetched = parseFromWire(emptyTerminalResponse(SCANNER_ID));
        gateway.enqueue(invalid);
        gateway.enqueue(prefetched);

        KvBatchScanner scanner = newScanner(gateway);
        assertThatThrownBy(() -> scanner.pollBatch(POLL_TIMEOUT))
                .isInstanceOf(RuntimeException.class);
        assertThat(invalid.getParsedByteBuf().refCnt()).isEqualTo(0);
        assertThat(prefetched.getParsedByteBuf().refCnt()).isEqualTo(1);

        scanner.close();
        assertThat(prefetched.getParsedByteBuf().refCnt()).isEqualTo(0);
    }

    @Test
    void exceptionalOpenFutureWithoutResponseClosesScanner() throws Exception {
        RecordingGateway gateway = new RecordingGateway();
        CompletableFuture<ScanKvResponse> failed = new CompletableFuture<>();
        failed.completeExceptionally(new IOException("expected"));
        gateway.enqueue(failed);

        KvBatchScanner scanner = newScanner(gateway);
        assertThatThrownBy(() -> scanner.pollBatch(POLL_TIMEOUT)).isInstanceOf(IOException.class);
        assertThat(scanner.isClosed()).isTrue();
        assertThat(gateway.requests).hasSize(1);
    }

    private ScanKvResponse recordsResponse(boolean hasMore, int id) throws Exception {
        DefaultValueRecordBatch.Builder builder = DefaultValueRecordBatch.builder();
        builder.append(
                DEFAULT_SCHEMA_ID, compactedRow(DATA1_ROW_TYPE, new Object[] {id, "value-" + id}));
        DefaultValueRecordBatch batch = builder.build();
        byte[] records = new byte[batch.sizeInBytes()];
        batch.getSegment().get(batch.getPosition(), records);
        return parseFromWire(
                new ScanKvResponse()
                        .setScannerId(SCANNER_ID)
                        .setHasMoreResults(hasMore)
                        .setRecords(records));
    }

    private ScanKvResponse parseFromWire(ScanKvResponse toSerialize) {
        return parseFromWire(toSerialize, false);
    }

    private ScanKvResponse parseFromWire(ScanKvResponse toSerialize, boolean trackRelease) {
        byte[] wireBytes = toSerialize.toByteArray();
        ByteBuf buf = Unpooled.wrappedBuffer(wireBytes);
        if (trackRelease) {
            buf = spy(buf);
        }
        ScanKvResponse parsed = new ScanKvResponse();
        parsed.parseFrom(buf, buf.readableBytes());
        assertThat(buf.refCnt()).isEqualTo(1);
        assertThat(parsed.getParsedByteBuf()).isSameAs(buf);
        parsedBuffers.add(buf);
        return parsed;
    }

    /**
     * Drains any refcount left on {@code buf} so a failed assertion above does not leak a real
     * buffer into later tests; called after assertions, never before.
     */
    private static void releaseIfNeeded(ByteBuf buf) {
        while (buf.refCnt() > 0) {
            buf.release();
        }
    }

    // -------------------------------------------------------------------------
    // Other terminal errors
    // -------------------------------------------------------------------------

    @ParameterizedTest
    @MethodSource("routingErrors")
    void routingErrorInvalidatesMetadataAndFailsWithoutReopening(Errors error, boolean continuation)
            throws Exception {
        RecordingGateway oldLeader = new RecordingGateway();
        if (continuation) {
            oldLeader.enqueue(emptyContinuationResponse(SCANNER_ID));
        }
        ScanKvResponse failed = parseFromWire(errorResponse(error));
        oldLeader.enqueue(failed);
        RecordingGateway newLeader = new RecordingGateway();
        newLeader.enqueue(emptyTerminalResponse(SCANNER_ID));
        TestMetadataUpdater meta = new TestMetadataUpdater(oldLeader, newLeader);
        try (KvBatchScanner scanner = newScanner(meta, BUCKET_0)) {
            if (continuation) {
                scanner.pollBatch(POLL_TIMEOUT);
            }
            assertThatThrownBy(() -> scanner.pollBatch(POLL_TIMEOUT))
                    .isInstanceOf(IOException.class)
                    .hasCauseInstanceOf(error.exception().getClass());
            assertThat(scanner.isClosed()).isTrue();
            assertThat(scanner.pollBatch(POLL_TIMEOUT)).isNull();
            assertThat(meta.getCluster().leaderFor(BUCKET_0)).isNull();
            assertThat(meta.invalidations.get()).isEqualTo(1);
            assertThat(failed.getParsedByteBuf().refCnt()).isZero();
            assertThat(newLeader.requests).isEmpty();
            assertThat(oldLeader.requests.stream().filter(r -> r.hasBucketScanReq()).count())
                    .isEqualTo(1);
            if (error == Errors.SCANNER_EXPIRED || error == Errors.UNKNOWN_SCANNER_ID) {
                assertThat(oldLeader.requests.stream().anyMatch(KvBatchScannerTest::isCloseRequest))
                        .isFalse();
            }
        }
        try (KvBatchScanner rebuilt = newScanner(meta, BUCKET_0)) {
            assertThat(rebuilt.pollBatch(POLL_TIMEOUT)).isNull();
            assertThat(newLeader.requests).hasSize(1);
        }
    }

    private static Stream<Arguments> routingErrors() {
        return errorStages(
                Errors.NOT_LEADER_OR_FOLLOWER,
                Errors.LEADER_NOT_AVAILABLE_EXCEPTION,
                Errors.UNKNOWN_TABLE_OR_BUCKET_EXCEPTION,
                Errors.NETWORK_EXCEPTION,
                Errors.SCANNER_EXPIRED,
                Errors.UNKNOWN_SCANNER_ID);
    }

    @ParameterizedTest
    @MethodSource("dispatchFailures")
    void networkFailureInvalidatesAndFailsForOpenAndContinuation(
            boolean synchronous, boolean continuation) throws Exception {
        RecordingGateway gateway = new RecordingGateway();
        if (continuation) {
            gateway.enqueue(emptyContinuationResponse(SCANNER_ID));
        }
        NetworkException failure = new NetworkException("offline", new IOException("transport"));
        if (synchronous) {
            gateway.enqueueFailure(failure);
        } else {
            CompletableFuture<ScanKvResponse> failed = new CompletableFuture<>();
            failed.completeExceptionally(new CompletionException(failure));
            gateway.enqueue(failed);
        }
        TestMetadataUpdater meta = new TestMetadataUpdater(gateway);
        try (KvBatchScanner scanner = newScanner(meta, BUCKET_0)) {
            if (continuation && !synchronous) {
                scanner.pollBatch(POLL_TIMEOUT);
            }
            assertThatThrownBy(() -> scanner.pollBatch(POLL_TIMEOUT))
                    .isInstanceOf(IOException.class)
                    .hasCause(failure);
            assertThat(scanner.isClosed()).isTrue();
            assertThat(scanner.openRetries()).isZero();
            assertThat(meta.getCluster().leaderFor(BUCKET_0)).isNull();
            assertThat(meta.invalidations.get()).isEqualTo(1);
            assertThat(scanner.pollBatch(POLL_TIMEOUT)).isNull();
            assertThat(gateway.requests.stream().filter(r -> r.hasBucketScanReq()).count())
                    .isEqualTo(1);
        }
    }

    private static Stream<Arguments> dispatchFailures() {
        return Stream.of(
                Arguments.of(false, false),
                Arguments.of(false, true),
                Arguments.of(true, false),
                Arguments.of(true, true));
    }

    @ParameterizedTest
    @MethodSource("ordinaryErrors")
    void ordinaryFailureKeepsMetadata(Errors error, boolean continuation) throws Exception {
        RecordingGateway gateway = new RecordingGateway();
        if (continuation) {
            gateway.enqueue(emptyContinuationResponse(SCANNER_ID));
        }
        gateway.enqueue(errorResponse(error));
        TestMetadataUpdater meta = new TestMetadataUpdater(gateway);
        try (KvBatchScanner scanner = newScanner(meta, BUCKET_0)) {
            if (continuation) {
                scanner.pollBatch(POLL_TIMEOUT);
            }
            assertThatThrownBy(() -> scanner.pollBatch(POLL_TIMEOUT))
                    .isInstanceOf(IOException.class)
                    .hasCauseInstanceOf(error.exception().getClass());
            assertThat(scanner.isClosed()).isTrue();
            assertThat(meta.getCluster().leaderFor(BUCKET_0)).isZero();
            assertThat(meta.invalidations.get()).isZero();
        }
    }

    private static Stream<Arguments> ordinaryErrors() {
        return errorStages(
                Errors.INVALID_SCAN_REQUEST,
                Errors.UNKNOWN_SERVER_ERROR,
                Errors.REQUEST_TIME_OUT,
                Errors.INVALID_BUCKET_ROUTING);
    }

    private static Stream<Arguments> errorStages(Errors... errors) {
        return Stream.of(errors)
                .flatMap(error -> Stream.of(Arguments.of(error, false), Arguments.of(error, true)));
    }

    @Test
    void synchronousFailureDuringTooManyScannersRetryInvalidatesMetadata() throws Exception {
        RecordingGateway gateway = new RecordingGateway();
        gateway.enqueue(errorResponse(Errors.TOO_MANY_SCANNERS));
        NetworkException failure = new NetworkException("offline");
        gateway.enqueueFailure(failure);
        TestMetadataUpdater meta = new TestMetadataUpdater(gateway);
        try (KvBatchScanner scanner = newScanner(meta, BUCKET_0)) {
            assertThatThrownBy(() -> scanner.pollBatch(POLL_TIMEOUT))
                    .isInstanceOf(IOException.class)
                    .hasCause(failure);
            assertThat(scanner.isClosed()).isTrue();
            assertThat(scanner.openRetries()).isEqualTo(1);
            assertThat(gateway.requests).hasSize(2);
            assertThat(meta.getCluster().leaderFor(BUCKET_0)).isNull();
            assertThat(meta.invalidations.get()).isEqualTo(1);
        }
    }

    @Test
    void partitionFailurePreservesMappingAndOtherPartitionRoutes() throws Exception {
        TableBucket first = new TableBucket(DATA1_TABLE_ID_PK, 11L, 0);
        TableBucket second = new TableBucket(DATA1_TABLE_ID_PK, 12L, 0);
        RecordingGateway oldLeader = new RecordingGateway();
        oldLeader.enqueue(errorResponse(Errors.NOT_LEADER_OR_FOLLOWER));
        RecordingGateway newLeader = new RecordingGateway();
        newLeader.enqueue(emptyTerminalResponse(SCANNER_ID));
        TestMetadataUpdater meta = new TestMetadataUpdater(oldLeader, newLeader, first, second);
        try (KvBatchScanner scanner = newScanner(meta, first)) {
            assertThatThrownBy(() -> scanner.pollBatch(POLL_TIMEOUT))
                    .isInstanceOf(IOException.class);
            assertThat(meta.getCluster().leaderFor(first)).isNull();
            assertThat(meta.getCluster().leaderFor(second)).isZero();
            assertThat(meta.getCluster().getPartitionId(physicalPath(first))).hasValue(11L);
            assertThat(meta.getCluster().getBucketCount(TableOrPartition.ofPartition(11L)))
                    .isEmpty();
            assertThat(meta.getCluster().getBucketCount(TableOrPartition.ofPartition(12L)))
                    .hasValue(1);
        }
        try (KvBatchScanner rebuilt = newScanner(meta, first)) {
            assertThat(rebuilt.pollBatch(POLL_TIMEOUT)).isNull();
            assertThat(newLeader.requests.get(0).getBucketScanReq().getPartitionId())
                    .isEqualTo(11L);
            assertThat(newLeader.requests.get(0).getBucketScanReq().getRoutingBucketCount())
                    .isEqualTo(1);
        }
    }

    @Test
    void cleanupFailureDoesNotReplaceScanFailure() throws Exception {
        RecordingGateway gateway = new RecordingGateway();
        ScanKvResponse open = recordsResponse(true, 1);
        gateway.enqueue(open);
        NetworkException failure = new NetworkException("offline");
        gateway.enqueueFailure(failure);
        gateway.closeFailure = new IllegalStateException("close dispatch failed");
        TestMetadataUpdater meta = new TestMetadataUpdater(gateway);
        meta.invalidationFailure = new IllegalStateException("invalidation failed");
        try (KvBatchScanner scanner = newScanner(meta, BUCKET_0)) {
            assertThatThrownBy(() -> scanner.pollBatch(POLL_TIMEOUT))
                    .isInstanceOf(IOException.class)
                    .hasCause(failure);
            assertThat(open.getParsedByteBuf().refCnt()).isZero();
            assertThat(scanner.isClosed()).isTrue();
            assertThat(meta.invalidations.get()).isEqualTo(1);
        }
    }

    @Test
    void scannerExpiredIsTerminalAndDoesNotSendCloseScanner() throws Exception {
        RecordingGateway gateway = new RecordingGateway();
        gateway.enqueue(emptyContinuationResponse(SCANNER_ID));
        gateway.enqueue(errorResponse(Errors.SCANNER_EXPIRED));

        try (KvBatchScanner scanner = newScanner(gateway)) {
            scanner.pollBatch(POLL_TIMEOUT);
            assertThatThrownBy(() -> scanner.pollBatch(POLL_TIMEOUT))
                    .isInstanceOf(IOException.class)
                    .hasCauseInstanceOf(ScannerExpiredException.class);
        }

        assertThat(gateway.requests.stream().anyMatch(KvBatchScannerTest::isCloseRequest))
                .isFalse();
    }

    @Test
    void unknownScannerIdIsTerminalAndDoesNotSendCloseScanner() throws Exception {
        RecordingGateway gateway = new RecordingGateway();
        gateway.enqueue(emptyContinuationResponse(SCANNER_ID));
        gateway.enqueue(errorResponse(Errors.UNKNOWN_SCANNER_ID));

        try (KvBatchScanner scanner = newScanner(gateway)) {
            scanner.pollBatch(POLL_TIMEOUT);
            assertThatThrownBy(() -> scanner.pollBatch(POLL_TIMEOUT))
                    .isInstanceOf(IOException.class)
                    .hasCauseInstanceOf(UnknownScannerIdException.class);
        }

        assertThat(gateway.requests.stream().anyMatch(KvBatchScannerTest::isCloseRequest))
                .isFalse();
    }

    @Test
    void invalidScanRequestIsTerminalAndSendsCloseScanner() throws Exception {
        RecordingGateway gateway = new RecordingGateway();
        gateway.enqueue(emptyContinuationResponse(SCANNER_ID));
        gateway.enqueue(errorResponse(Errors.INVALID_SCAN_REQUEST));

        try (KvBatchScanner scanner = newScanner(gateway)) {
            scanner.pollBatch(POLL_TIMEOUT);
            assertThatThrownBy(() -> scanner.pollBatch(POLL_TIMEOUT))
                    .isInstanceOf(IOException.class)
                    .hasCauseInstanceOf(InvalidScanRequestException.class);
        }

        assertThat(gateway.requests.stream().anyMatch(KvBatchScannerTest::isCloseRequest)).isTrue();
    }

    // -------------------------------------------------------------------------
    // Timeout
    // -------------------------------------------------------------------------

    @Test
    void timeoutReturnsEmptyIteratorAndKeepsFutureInFlight() throws Exception {
        RecordingGateway gateway = new RecordingGateway();
        CompletableFuture<ScanKvResponse> responseFuture = neverCompleting();
        gateway.enqueue(responseFuture);

        TestMetadataUpdater meta = new TestMetadataUpdater(gateway);
        KvBatchScanner scanner = newScanner(meta, BUCKET_0);
        CloseableIterator<InternalRow> first = scanner.pollBatch(Duration.ofMillis(50));
        assertThat(first).isNotNull();
        assertThat(first.hasNext()).isFalse();

        assertThat(gateway.requests).hasSize(1);
        ScanKvResponse response = parseFromWire(emptyTerminalResponse(SCANNER_ID));
        responseFuture.complete(response);
        assertThat(scanner.pollBatch(POLL_TIMEOUT)).isNull();
        assertThat(response.getParsedByteBuf().refCnt()).isEqualTo(0);
        scanner.close();
        assertThat(meta.invalidations.get()).isZero();
        assertThat(meta.getCluster().leaderFor(BUCKET_0)).isZero();
    }

    // -------------------------------------------------------------------------
    // Test helpers
    // -------------------------------------------------------------------------

    private KvBatchScanner newScanner(RecordingGateway gateway) {
        return newScanner(new TestMetadataUpdater(gateway), BUCKET_0);
    }

    private KvBatchScanner newScanner(TestMetadataUpdater metadata, TableBucket bucket) {
        return new KvBatchScanner(DATA1_TABLE_INFO_PK, bucket, SCHEMA_GETTER, metadata, 4096, null);
    }

    private static ScanKvResponse emptyContinuationResponse(byte[] scannerId) {
        return new ScanKvResponse().setScannerId(scannerId).setHasMoreResults(true);
    }

    private static ScanKvResponse emptyTerminalResponse(byte[] scannerId) {
        return new ScanKvResponse()
                .setScannerId(scannerId)
                .setHasMoreResults(false)
                .setLogOffset(0L);
    }

    private static ScanKvResponse errorResponse(Errors error) {
        return new ScanKvResponse()
                .setErrorCode(error.code())
                .setErrorMessage(error.exception().getMessage());
    }

    private static CompletableFuture<ScanKvResponse> neverCompleting() {
        return new CompletableFuture<>();
    }

    private static boolean isCloseRequest(ScanKvRequest req) {
        return req.hasCloseScanner() && req.isCloseScanner();
    }

    private final class RecordingGateway extends TestingTabletGatewayService {
        final List<ScanKvRequest> requests = new ArrayList<>();
        final List<ScanKvResponse> closeAckResponses = new ArrayList<>();
        @Nullable RuntimeException closeFailure;
        private final Queue<Supplier<CompletableFuture<ScanKvResponse>>> queued =
                new LinkedList<>();
        private final Queue<CompletableFuture<ScanKvResponse>> closeQueued = new LinkedList<>();

        void enqueue(ScanKvResponse response) {
            queued.add(() -> CompletableFuture.completedFuture(response));
        }

        void enqueue(CompletableFuture<ScanKvResponse> future) {
            queued.add(() -> future);
        }

        void enqueueFailure(RuntimeException failure) {
            queued.add(
                    () -> {
                        throw failure;
                    });
        }

        void enqueueClose(ScanKvResponse response) {
            closeQueued.add(CompletableFuture.completedFuture(response));
        }

        void enqueueClose(CompletableFuture<ScanKvResponse> response) {
            closeQueued.add(response);
        }

        @Override
        public CompletableFuture<ScanKvResponse> scanKv(ScanKvRequest request) {
            requests.add(request);
            if (request.hasCloseScanner() && request.isCloseScanner()) {
                if (closeFailure != null) {
                    throw closeFailure;
                }
                CompletableFuture<ScanKvResponse> closeResponse = closeQueued.poll();
                if (closeResponse == null) {
                    closeResponse =
                            CompletableFuture.completedFuture(
                                    parseFromWire(new ScanKvResponse().setHasMoreResults(false)));
                }
                closeResponse.thenAccept(closeAckResponses::add);
                return closeResponse;
            }
            Supplier<CompletableFuture<ScanKvResponse>> next = queued.poll();
            if (next == null) {
                CompletableFuture<ScanKvResponse> failed = new CompletableFuture<>();
                failed.completeExceptionally(
                        new AssertionError(
                                "RecordingGateway received an unexpected request (no response queued): "
                                        + request));
                return failed;
            }
            return next.get();
        }
    }

    private static PhysicalTablePath physicalPath(TableBucket bucket) {
        TablePath path = DATA1_TABLE_INFO_PK.getTablePath();
        return bucket.getPartitionId() == null
                ? PhysicalTablePath.of(path)
                : PhysicalTablePath.of(path, "p" + bucket.getPartitionId());
    }

    private static Cluster cluster(int leader, TableBucket... buckets) {
        Map<PhysicalTablePath, List<BucketLocation>> locations = new HashMap<>();
        Map<PhysicalTablePath, Long> partitions = new HashMap<>();
        Map<TableOrPartition, Integer> counts = new HashMap<>();
        for (TableBucket bucket : buckets) {
            PhysicalTablePath path = physicalPath(bucket);
            locations.put(
                    path,
                    Collections.singletonList(
                            new BucketLocation(path, bucket, leader, new int[] {0, 1})));
            if (bucket.getPartitionId() != null) {
                partitions.put(path, bucket.getPartitionId());
            }
            counts.put(TableOrPartition.of(bucket.getTableId(), bucket.getPartitionId()), 1);
        }
        return new Cluster(
                Collections.emptyMap(),
                null,
                locations,
                Collections.singletonMap(DATA1_TABLE_INFO_PK.getTablePath(), DATA1_TABLE_ID_PK),
                partitions,
                counts);
    }

    private static final class TestMetadataUpdater extends MetadataUpdater {
        private final TabletServerGateway oldGateway;
        private final TabletServerGateway newGateway;
        private final TableBucket[] buckets;
        final AtomicInteger invalidations = new AtomicInteger();
        @Nullable RuntimeException invalidationFailure;

        TestMetadataUpdater(TabletServerGateway gateway) {
            this(gateway, gateway);
        }

        TestMetadataUpdater(TabletServerGateway oldGateway, TabletServerGateway newGateway) {
            this(oldGateway, newGateway, BUCKET_0);
        }

        TestMetadataUpdater(
                TabletServerGateway oldGateway,
                TabletServerGateway newGateway,
                TableBucket... buckets) {
            super(null, new Configuration(), cluster(0, buckets));
            this.oldGateway = oldGateway;
            this.newGateway = newGateway;
            this.buckets = buckets;
        }

        @Override
        public void invalidPhysicalTableBucketMeta(Set<PhysicalTablePath> physicalTables) {
            invalidations.incrementAndGet();
            super.invalidPhysicalTableBucketMeta(physicalTables);
            if (invalidationFailure != null) {
                throw invalidationFailure;
            }
        }

        @Override
        public void updateMetadata(
                @Nullable Set<TablePath> tablePaths,
                @Nullable Collection<PhysicalTablePath> partitionNames,
                @Nullable Collection<Long> partitionIds) {
            cluster = cluster(1, buckets);
        }

        @Override
        public @Nullable TabletServerGateway newTabletServerClientForNode(int serverId) {
            return serverId == 0 ? oldGateway : newGateway;
        }
    }
}
