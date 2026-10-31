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

package org.apache.fluss.client.table.scanner.log;

import org.apache.fluss.client.metadata.TestingMetadataUpdater;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.FetchException;
import org.apache.fluss.exception.WakeupException;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.record.LogRecordReadContext;
import org.apache.fluss.remote.RemoteLogSegment;
import org.apache.fluss.rpc.entity.FetchLogResultForBucket;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.fluss.record.TestData.DATA1;
import static org.apache.fluss.record.TestData.DATA1_ROW_TYPE;
import static org.apache.fluss.record.TestData.DATA1_TABLE_INFO;
import static org.apache.fluss.record.TestData.DATA1_TABLE_PATH;
import static org.apache.fluss.record.TestData.DEFAULT_SCHEMA_ID;
import static org.apache.fluss.record.TestData.TEST_SCHEMA_GETTER;
import static org.apache.fluss.testutils.DataTestUtils.genMemoryLogRecordsByObject;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link LogFetchBuffer}. */
public class LogFetchBufferTest {

    private final TableBucket tableBucket1 = new TableBucket(1, 0);
    private final TableBucket tableBucket2 = new TableBucket(1, 1);
    private final TableBucket tableBucket3 = new TableBucket(1, 2);
    private LogScannerStatus logScannerStatus;
    private LogRecordReadContext readContext;

    @BeforeEach
    void setup() {
        Map<TableBucket, Long> scanBuckets = new HashMap<>();
        scanBuckets.put(tableBucket1, 0L);
        scanBuckets.put(tableBucket2, 0L);
        scanBuckets.put(tableBucket3, 0L);
        logScannerStatus = new LogScannerStatus();
        logScannerStatus.assignScanBuckets(scanBuckets);
        readContext =
                LogRecordReadContext.createArrowReadContext(
                        DATA1_ROW_TYPE, DEFAULT_SCHEMA_ID, TEST_SCHEMA_GETTER);
    }

    @AfterEach
    void afterEach() {
        if (readContext != null) {
            readContext.close();
            readContext = null;
        }
    }

    @Test
    void testBasicPeekAndPoll() throws Exception {
        try (LogFetchBuffer logFetchBuffer = new LogFetchBuffer()) {
            CompletedFetch completedFetch = makeCompletedFetch(tableBucket1);
            assertThat(logFetchBuffer.isEmpty()).isTrue();
            logFetchBuffer.add(completedFetch);
            assertThat(logFetchBuffer.isEmpty()).isFalse();
            assertThat(logFetchBuffer.peek()).isNotNull();
            assertThat(logFetchBuffer.peek()).isEqualTo(completedFetch);
            assertThat(logFetchBuffer.poll()).isEqualTo(completedFetch);
            assertThat(logFetchBuffer.peek()).isNull();
        }
    }

    @Test
    void testCloseClearsData() throws Exception {
        LogFetchBuffer logFetchBuffer = null;
        try {
            logFetchBuffer = new LogFetchBuffer();
            assertThat(logFetchBuffer.nextInLineFetch()).isNull();
            assertThat(logFetchBuffer.isEmpty()).isTrue();

            logFetchBuffer.add(makeCompletedFetch(tableBucket1));
            assertThat(logFetchBuffer.isEmpty()).isFalse();

            logFetchBuffer.setNextInLineFetch(makeCompletedFetch(tableBucket1));
            assertThat(logFetchBuffer.nextInLineFetch()).isNotNull();
        } finally {
            if (logFetchBuffer != null) {
                logFetchBuffer.close();
            }
        }

        assertThat(logFetchBuffer.nextInLineFetch()).isNull();
        assertThat(logFetchBuffer.isEmpty()).isTrue();
    }

    @Test
    void testBufferedBuckets() throws Exception {
        try (LogFetchBuffer logFetchBuffer = new LogFetchBuffer()) {
            logFetchBuffer.setNextInLineFetch(makeCompletedFetch(tableBucket1));
            logFetchBuffer.add(makeCompletedFetch(tableBucket2));
            logFetchBuffer.add(makeCompletedFetch(tableBucket3));
            assertThat(logFetchBuffer.bufferedBuckets())
                    .containsExactlyInAnyOrder(tableBucket1, tableBucket2, tableBucket3);

            logFetchBuffer.setNextInLineFetch(null);
            assertThat(logFetchBuffer.bufferedBuckets())
                    .containsExactlyInAnyOrder(tableBucket2, tableBucket3);

            logFetchBuffer.poll();
            assertThat(logFetchBuffer.bufferedBuckets()).containsExactlyInAnyOrder(tableBucket3);

            logFetchBuffer.poll();
            assertThat(logFetchBuffer.bufferedBuckets()).isEmpty();
        }
    }

    @Test
    void testAddAllAndRetainAll() throws Exception {
        try (LogFetchBuffer logFetchBuffer = new LogFetchBuffer()) {
            logFetchBuffer.setNextInLineFetch(makeCompletedFetch(tableBucket1));
            logFetchBuffer.addAll(
                    Arrays.asList(
                            makeCompletedFetch(tableBucket2), makeCompletedFetch(tableBucket3)));
            logFetchBuffer.pend(makePendingFetch(tableBucket1));
            logFetchBuffer.pend(makePendingFetch(tableBucket2));
            logFetchBuffer.pend(makePendingFetch(tableBucket3));

            assertThat(logFetchBuffer.bufferedBuckets())
                    .containsExactlyInAnyOrder(tableBucket1, tableBucket2, tableBucket3);
            assertThat(logFetchBuffer.pendedBuckets())
                    .containsExactlyInAnyOrder(tableBucket1, tableBucket2, tableBucket3);

            logFetchBuffer.retainAll(new HashSet<>(Arrays.asList(tableBucket2, tableBucket3)));
            assertThat(logFetchBuffer.bufferedBuckets())
                    .containsExactlyInAnyOrder(tableBucket2, tableBucket3);
            assertThat(logFetchBuffer.pendedBuckets())
                    .containsExactlyInAnyOrder(tableBucket2, tableBucket3);

            logFetchBuffer.retainAll(Collections.singleton(tableBucket3));
            assertThat(logFetchBuffer.bufferedBuckets()).containsExactlyInAnyOrder(tableBucket3);
            assertThat(logFetchBuffer.pendedBuckets()).containsExactlyInAnyOrder(tableBucket3);

            logFetchBuffer.retainAll(Collections.emptySet());
            assertThat(logFetchBuffer.bufferedBuckets()).isEmpty();
            assertThat(logFetchBuffer.pendedBuckets()).isEmpty();
        }
    }

    @Test
    void testRetainAllDiscardsRemovedPendingFetches() throws Exception {
        AtomicBoolean discarded = new AtomicBoolean(false);
        PendingFetch pendingFetch =
                new PendingFetch() {
                    @Override
                    public TableBucket tableBucket() {
                        return tableBucket1;
                    }

                    @Override
                    public boolean isCompleted() {
                        return false;
                    }

                    @Override
                    public CompletedFetch toCompletedFetch() {
                        throw new AssertionError("Pending fetch should be discarded.");
                    }

                    @Override
                    public void discard() {
                        discarded.set(true);
                    }
                };

        try (LogFetchBuffer logFetchBuffer = new LogFetchBuffer()) {
            logFetchBuffer.pend(pendingFetch);
            logFetchBuffer.retainAll(Collections.emptySet());

            assertThat(discarded).isTrue();
            assertThat(logFetchBuffer.pendedBuckets()).isEmpty();
        }
    }

    @Test
    void testWakeup() throws Exception {
        try (LogFetchBuffer logFetchBuffer = new LogFetchBuffer()) {
            AtomicReference<Exception> exception = new AtomicReference<>();
            final Thread waitingThread =
                    new Thread(
                            () -> {
                                try {
                                    logFetchBuffer.awaitNotEmpty(
                                            System.nanoTime() + Duration.ofMinutes(1).toNanos());
                                } catch (Exception e) {
                                    exception.set(e);
                                }
                            });
            waitingThread.start();
            logFetchBuffer.wakeup();
            waitingThread.join(Duration.ofSeconds(30).toMillis());
            assertThat(waitingThread.isAlive()).isFalse();
            assertThat(exception.get()).isInstanceOf(WakeupException.class);
        }
    }

    @Test
    void testPendFetches() throws Exception {
        ExecutorService service = Executors.newSingleThreadExecutor();

        try (LogFetchBuffer logFetchBuffer = new LogFetchBuffer()) {
            assertThat(logFetchBuffer.isEmpty()).isTrue();
            AtomicBoolean completed1 = new AtomicBoolean(false);
            PendingFetch pending1 = makePendingFetch(tableBucket1, completed1);
            logFetchBuffer.pend(pending1);
            // pending fetches are not counted as completed fetches.
            assertThat(logFetchBuffer.isEmpty()).isTrue();

            // tableBucket3 completed fetch will not be pended
            logFetchBuffer.add(makeCompletedFetch(tableBucket3));
            assertThat(logFetchBuffer.isEmpty()).isFalse();
            assertThat(logFetchBuffer.poll().tableBucket).isEqualTo(tableBucket3);

            // tableBucket1 completed fetches will be pended
            logFetchBuffer.add(makeCompletedFetch(tableBucket1));
            assertThat(logFetchBuffer.isEmpty()).isTrue();

            AtomicBoolean completed2 = new AtomicBoolean(false);
            PendingFetch pending2 = makePendingFetch(tableBucket2, completed2);
            PendingFetch pending3 = makePendingFetch(tableBucket3);
            PendingFetch pending4 = makePendingFetch(tableBucket3);
            logFetchBuffer.pend(pending2);

            Future<Boolean> signal =
                    service.submit(() -> await(logFetchBuffer, Duration.ofSeconds(1)));
            logFetchBuffer.discardObsolete();

            // nothing happens, as pending1 is not completed
            assertThat(logFetchBuffer.isEmpty()).isTrue();
            // no condition signal
            assertThat(signal.get(5, SECONDS)).isFalse();

            signal = service.submit(() -> await(logFetchBuffer, Duration.ofMinutes(1)));
            completed1.set(true);
            logFetchBuffer.discardObsolete();
            assertThat(signal.get(5, SECONDS)).isTrue();
            assertThat(logFetchBuffer.isEmpty()).isFalse();
            assertThat(logFetchBuffer.poll().tableBucket).isEqualTo(tableBucket1);
            // the buffered completed fetch will be available now
            assertThat(logFetchBuffer.poll().tableBucket).isEqualTo(tableBucket1);
            assertThat(logFetchBuffer.isEmpty()).isTrue();

            signal = service.submit(() -> await(logFetchBuffer, Duration.ofMinutes(1)));
            completed2.set(true);
            logFetchBuffer.discardObsolete();

            assertThat(signal.get(5, SECONDS)).isTrue();
            assertThat(logFetchBuffer.isEmpty()).isFalse();
            assertThat(logFetchBuffer.poll().tableBucket).isEqualTo(tableBucket2);

            // discardObsolete() processes all buckets, so enqueue these completed
            // pending fetches after verifying tableBucket2.
            logFetchBuffer.pend(pending3);
            logFetchBuffer.pend(pending4);
            logFetchBuffer.discardObsolete();

            assertThat(logFetchBuffer.poll().tableBucket).isEqualTo(tableBucket3);
            assertThat(logFetchBuffer.poll().tableBucket).isEqualTo(tableBucket3);
            assertThat(logFetchBuffer.isEmpty()).isTrue();
        } finally {
            service.shutdownNow();
        }
    }

    @Test
    void testFetchException() throws Exception {
        ExecutorService service = Executors.newSingleThreadExecutor();
        try (LogFetchBuffer logFetchBuffer = new LogFetchBuffer()) {
            CompletableFuture<File> download = new CompletableFuture<>();
            RemotePendingFetch pendingFetch =
                    makeRemotePendingFetch(tableBucket1, download, new AtomicBoolean(false));

            logFetchBuffer.tryComplete(pendingFetch, null);
            assertThat(logFetchBuffer.isEmpty()).isTrue();
            logFetchBuffer.pend(pendingFetch);
            assertThat(logFetchBuffer.isEmpty()).isTrue();

            Future<Boolean> signal =
                    service.submit(() -> await(logFetchBuffer, Duration.ofMinutes(1)));

            IOException failure = new IOException("Test fetch exception");
            download.completeExceptionally(failure);
            logFetchBuffer.tryComplete(pendingFetch, failure);

            assertThat(signal.get(5, SECONDS)).isTrue();
            assertThat(logFetchBuffer.isEmpty()).isFalse();
            assertThatThrownBy(logFetchBuffer::poll)
                    .isExactlyInstanceOf(FetchException.class)
                    .hasMessageContaining("Test fetch exception");
            assertThatThrownBy(logFetchBuffer::peek)
                    .isExactlyInstanceOf(FetchException.class)
                    .hasMessageContaining("Test fetch exception");
        } finally {
            service.shutdownNow();
        }
    }

    @Test
    void testObsoleteBoundedRemoteFailureDoesNotPolluteBuffer() throws Exception {
        logScannerStatus.assignBoundedScanBucket(tableBucket1, 0L, 10L);
        AtomicBoolean discarded = new AtomicBoolean();
        RemotePendingFetch pending =
                makeRemotePendingFetch(tableBucket1, new CompletableFuture<>(), discarded);

        try (LogFetchBuffer buffer = new LogFetchBuffer()) {
            buffer.pend(pending);
            // Finish the original bounded subscription.
            assertThat(logScannerStatus.updateOffsetIfCurrent(tableBucket1, 0L, 10L, 10L)).isTrue();
            // Replace it with another bounded range.
            logScannerStatus.assignBoundedScanBucket(tableBucket1, 20L, 30L);
            // The old remote download fails after resubscription.
            buffer.tryComplete(pending, new IOException("obsolete download failure"));
            // No stale failure may become a global buffer error or a completed fetch for the new
            // subscription.
            assertThat(buffer.peek()).isNull();
            assertThat(buffer.isEmpty()).isTrue();
            assertThat(discarded).isTrue();

            assertThat(logScannerStatus.getBucketOffset(tableBucket1)).isEqualTo(20L);
            assertThat(logScannerStatus.allSubscribedBucketsFinished()).isFalse();
        }
    }

    @Test
    void testActiveBoundedRemoteFailureIsDelivered() throws Exception {
        logScannerStatus.assignBoundedScanBucket(tableBucket1, 0L, 10L);
        AtomicBoolean discarded = new AtomicBoolean();
        RemotePendingFetch pending =
                makeRemotePendingFetch(tableBucket1, new CompletableFuture<>(), discarded);

        try (LogFetchBuffer buffer = new LogFetchBuffer()) {
            buffer.pend(pending);
            buffer.tryComplete(pending, new IOException("active remote download failure"));
            CompletedFetch completed = buffer.peek();

            assertThat(completed).isNotNull();
            assertThat(discarded).isTrue();
            // Verify the real Collector error path, not only the failure field stored in
            // CompletedFetch.
            LogFetchCollector collector =
                    new LogFetchCollector(
                            logScannerStatus,
                            new Configuration(),
                            new TestingMetadataUpdater(
                                    Collections.singletonMap(DATA1_TABLE_PATH, DATA1_TABLE_INFO)));

            assertThatThrownBy(() -> collector.collectFetch(buffer))
                    .isInstanceOf(FetchException.class)
                    .hasRootCauseMessage("active remote download failure");
            // Failure must not advance the bounded range.
            assertThat(logScannerStatus.getBucketOffset(tableBucket1)).isZero();
            assertThat(logScannerStatus.drainFinishedBuckets()).isEmpty();
        }
    }

    @Test
    void testUnboundedRemoteFileOpenFailureIsDelivered() throws Exception {
        // The default setup already subscribes tableBucket1 as unbounded.
        AtomicBoolean discarded = new AtomicBoolean();
        File missingFile =
                new File(
                        System.getProperty("java.io.tmpdir"), "fluss-missing-" + UUID.randomUUID());
        CompletableFuture<File> download = CompletableFuture.completedFuture(missingFile);
        RemotePendingFetch pending = makeRemotePendingFetch(tableBucket1, download, discarded);

        try (LogFetchBuffer buffer = new LogFetchBuffer()) {
            buffer.pend(pending);
            // The download succeeded, but opening its local file will fail.
            buffer.tryComplete(pending, null);

            assertThat(discarded).isTrue();
            assertThat(buffer.pendedBuckets()).doesNotContain(tableBucket1);
            assertThat(buffer.peek()).isNotNull();

            LogFetchCollector collector =
                    new LogFetchCollector(
                            logScannerStatus,
                            new Configuration(),
                            new TestingMetadataUpdater(
                                    Collections.singletonMap(DATA1_TABLE_PATH, DATA1_TABLE_INFO)));

            assertThatThrownBy(() -> collector.collectFetch(buffer))
                    .isInstanceOf(FetchException.class)
                    .hasRootCauseInstanceOf(IOException.class);

            assertThat(logScannerStatus.getBucketOffset(tableBucket1)).isZero();
            assertThat(logScannerStatus.drainFinishedBuckets()).isEmpty();
        }
    }

    @Test
    void testRemoteFailureBetweenPeekAndPollDrainsFetch() throws Exception {
        AtomicBoolean drained = new AtomicBoolean();
        AtomicBoolean failureInjected = new AtomicBoolean();
        // Simulate an active unbounded remote download.
        RemotePendingFetch remotePending =
                makeRemotePendingFetch(
                        tableBucket2, new CompletableFuture<>(), new AtomicBoolean());
        // Track whether the initialized fetch is drained after the error.
        DefaultCompletedFetch completedFetch =
                new DefaultCompletedFetch(
                        tableBucket1,
                        DATA1_TABLE_PATH,
                        FetchLogResultForBucket.records(
                                tableBucket1, genMemoryLogRecordsByObject(DATA1), 10L, -1L, -1L),
                        readContext,
                        logScannerStatus,
                        true,
                        0L,
                        null) {
                    @Override
                    void drain() {
                        super.drain();
                        drained.set(true);
                    }
                };
        LogFetchCollector collector =
                new LogFetchCollector(
                        logScannerStatus,
                        new Configuration(),
                        new TestingMetadataUpdater(
                                Collections.singletonMap(DATA1_TABLE_PATH, DATA1_TABLE_INFO)));
        try (LogFetchBuffer buffer =
                new LogFetchBuffer() {
                    @Override
                    CompletedFetch poll() {
                        // The collector has already peeked and initialized the
                        // completed fetch. Inject a remote failure immediately
                        // before the buffer checks for errors in poll().
                        if (failureInjected.compareAndSet(false, true)) {
                            tryComplete(
                                    remotePending,
                                    new IOException("Remote failure between peek and poll"));
                        }
                        return super.poll();
                    }
                }) {
            buffer.add(completedFetch);
            buffer.pend(remotePending);
            assertThatThrownBy(() -> collector.collectFetch(buffer))
                    .isInstanceOf(FetchException.class)
                    .hasRootCauseMessage("Remote failure between peek and poll");
            assertThat(failureInjected).isTrue();
            assertThat(completedFetch.isInitialized()).isTrue();
            // Resource cleanup must happen when the collector fails,
            // not only when the entire scanner/buffer is later closed.
            assertThat(drained).isTrue();
            assertThat(completedFetch.isConsumed()).isTrue();
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testReplacedBoundedSubscriptionDiscardsOldRemoteFetch(boolean downloadSucceeded)
            throws Exception {
        logScannerStatus.assignBoundedScanBucket(tableBucket1, 0L, 10L);
        AtomicBoolean discarded = new AtomicBoolean();
        RemotePendingFetch oldPending =
                makeRemotePendingFetch(tableBucket1, new CompletableFuture<>(), discarded);

        try (LogFetchBuffer buffer = new LogFetchBuffer()) {
            buffer.pend(oldPending);
            assertThat(buffer.bufferedBuckets()).contains(tableBucket1);
            // Same stop, different start.
            logScannerStatus.assignBoundedScanBucket(tableBucket1, 5L, 10L);
            buffer.discardBucket(tableBucket1);

            assertThat(discarded).isTrue();
            assertThat(buffer.bufferedBuckets()).doesNotContain(tableBucket1);

            Throwable result =
                    downloadSucceeded ? null : new IOException("obsolete download failure");
            buffer.tryComplete(oldPending, result);

            assertThat(buffer.peek()).isNull();
            assertThat(buffer.isEmpty()).isTrue();
            assertThat(buffer.pendedBuckets()).doesNotContain(tableBucket1);

            assertThat(logScannerStatus.getBucketOffset(tableBucket1)).isEqualTo(5L);
            assertThat(logScannerStatus.drainFinishedBuckets()).isEmpty();
        }
    }

    @Test
    void testRemoteFutureFailureBeforeCallback() throws Exception {
        logScannerStatus.assignBoundedScanBucket(tableBucket1, 0L, 10L);
        CompletableFuture<File> download = new CompletableFuture<>();
        AtomicBoolean discarded = new AtomicBoolean();
        RemotePendingFetch pending = makeRemotePendingFetch(tableBucket1, download, discarded);
        try (LogFetchBuffer buffer = new LogFetchBuffer()) {
            buffer.pend(pending);
            IOException failure = new IOException("download failed");
            download.completeExceptionally(failure);
            // The Future is complete, but the callback has not yet updated this PendingFetch.
            assertThat(pending.isCompleted()).isFalse();
            assertThat(buffer.pendedBuckets()).contains(tableBucket1);
            assertThat(buffer.peek()).isNull();
            // Process the actual callback.
            buffer.tryComplete(pending, failure);
            assertThat(pending.isCompleted()).isTrue();
            assertThat(buffer.pendedBuckets()).doesNotContain(tableBucket1);
            assertThat(buffer.peek()).isNotNull();
            assertThat(discarded).isTrue();
            LogFetchCollector collector =
                    new LogFetchCollector(
                            logScannerStatus,
                            new Configuration(),
                            new TestingMetadataUpdater(
                                    Collections.singletonMap(DATA1_TABLE_PATH, DATA1_TABLE_INFO)));
            assertThatThrownBy(() -> collector.collectFetch(buffer))
                    .isInstanceOf(FetchException.class)
                    .hasRootCauseMessage("download failed");
        }
    }

    @Test
    void testRemoteCompletionUnblocksQueuedCompletedFetch() throws Exception {
        CompletedFetch remoteResult = makeCompletedFetch(tableBucket1);
        CompletedFetch localResult = makeCompletedFetch(tableBucket1);
        RemotePendingFetch remotePending =
                makeOrderedRemotePendingFetch(tableBucket1, remoteResult);
        try (LogFetchBuffer buffer = new LogFetchBuffer()) {
            buffer.pend(remotePending);
            // This creates a CompletedPendingFetch behind the remote download.
            buffer.add(localResult);
            assertThat(buffer.isEmpty()).isTrue();
            // The remote callback must release both fetches in FIFO order.
            buffer.tryComplete(remotePending, null);

            assertThat(buffer.poll()).isSameAs(remoteResult);
            assertThat(buffer.poll()).isSameAs(localResult);

            assertThat(buffer.isEmpty()).isTrue();
            assertThat(buffer.pendedBuckets()).isEmpty();
        }
    }

    @Test
    void testFetchRetrySignalBeforeAwaitIsNotLost() throws Exception {
        try (LogFetchBuffer buffer = new LogFetchBuffer()) {
            // The callback signals before the scanner starts waiting.
            buffer.requestFetchRetry();
            assertThat(buffer.awaitNotEmpty(System.nanoTime() + SECONDS.toNanos(1))).isTrue();
            // The retry signal must be consumed exactly once.
            assertThat(buffer.awaitNotEmpty(System.nanoTime())).isFalse();
        }
    }

    private boolean await(LogFetchBuffer buffer, Duration waitTime) throws InterruptedException {
        return buffer.awaitNotEmpty(System.nanoTime() + waitTime.toNanos());
    }

    private DefaultCompletedFetch makeCompletedFetch(TableBucket tableBucket) throws Exception {
        return new DefaultCompletedFetch(
                tableBucket,
                DATA1_TABLE_PATH,
                FetchLogResultForBucket.records(
                        tableBucket, genMemoryLogRecordsByObject(DATA1), 10L, -1L, -1L),
                readContext,
                logScannerStatus,
                true,
                0L,
                null);
    }

    private PendingFetch makePendingFetch(TableBucket tableBucket) throws Exception {
        return new CompletedPendingFetch(makeCompletedFetch(tableBucket));
    }

    private PendingFetch makePendingFetch(TableBucket tableBucket, AtomicBoolean completed)
            throws Exception {
        DefaultCompletedFetch completedFetch = makeCompletedFetch(tableBucket);
        return new PendingFetch() {
            @Override
            public TableBucket tableBucket() {
                return tableBucket;
            }

            @Override
            public boolean isCompleted() {
                return completed.get();
            }

            @Override
            public CompletedFetch toCompletedFetch() {
                return completedFetch;
            }
        };
    }

    private RemotePendingFetch makeRemotePendingFetch(
            TableBucket bucket, CompletableFuture<File> fileFuture, AtomicBoolean discarded) {

        RemoteLogDownloadFuture future =
                new RemoteLogDownloadFuture(fileFuture, () -> {}, () -> discarded.set(true));

        return new RemotePendingFetch(
                makeRemoteLogSegment(bucket),
                future,
                DATA1_TABLE_PATH,
                0,
                0L,
                10L,
                readContext,
                logScannerStatus,
                true,
                logScannerStatus.subscriptionSnapshot(bucket));
    }

    private RemoteLogSegment makeRemoteLogSegment(TableBucket bucket) {
        return RemoteLogSegment.Builder.builder()
                .tableBucket(bucket)
                .physicalTablePath(PhysicalTablePath.of(DATA1_TABLE_PATH))
                .remoteLogSegmentId(UUID.randomUUID())
                .remoteLogStartOffset(0L)
                .remoteLogEndOffset(10L)
                .maxTimestamp(0L)
                .segmentSizeInBytes(1024)
                .build();
    }

    private RemotePendingFetch makeOrderedRemotePendingFetch(
            TableBucket bucket, CompletedFetch completedFetch) {

        RemoteLogDownloadFuture future =
                new RemoteLogDownloadFuture(new CompletableFuture<>(), () -> {}, () -> {});

        return new RemotePendingFetch(
                makeRemoteLogSegment(bucket),
                future,
                DATA1_TABLE_PATH,
                0,
                0L,
                10L,
                readContext,
                logScannerStatus,
                true,
                logScannerStatus.subscriptionSnapshot(bucket)) {
            @Override
            public CompletedFetch toCompletedFetch() {
                return completedFetch;
            }
        };
    }
}
