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

package org.apache.fluss.server.kv;

import org.apache.fluss.exception.RemoteStorageException;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.record.LogRecordBatch;
import org.apache.fluss.remote.RemoteLogSegment;
import org.apache.fluss.server.log.LogTablet;
import org.apache.fluss.server.log.remote.RemoteLogTestBase;
import org.apache.fluss.server.replica.Replica;

import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.apache.fluss.record.TestData.DATA1_TABLE_ID;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link RemoteLogFetcher}. */
class RemoteLogFetcherTest extends RemoteLogTestBase {

    /** Default knobs for the tests: prefetch depth 2, 2 download threads. */
    private static final int DEFAULT_TEST_PREFETCH_NUM = 2;

    private static final int DEFAULT_TEST_DOWNLOAD_THREADS = 2;

    private RemoteLogFetcher newFetcher(TableBucket tb, File logTabletDir) {
        return new RemoteLogFetcher(
                remoteLogManager,
                tb,
                logTabletDir,
                DEFAULT_TEST_PREFETCH_NUM,
                DEFAULT_TEST_DOWNLOAD_THREADS);
    }

    private RemoteLogFetcher newFetcher(
            TableBucket tb, File logTabletDir, int prefetchNum, int downloadThreads) {
        return new RemoteLogFetcher(
                remoteLogManager, tb, logTabletDir, prefetchNum, downloadThreads);
    }

    @Test
    void testBasicFetch() throws Exception {
        TableBucket tb = new TableBucket(DATA1_TABLE_ID, 0);
        // Make leader and add 5 segments (each with 10 batches).
        makeLogTableAsLeader(tb, false);
        Replica replica = replicaManager.getReplicaOrException(tb);
        LogTablet logTablet = replica.getLogTablet();
        addMultiSegmentsToLogTablet(logTablet, 5);

        // Trigger remote log tasks to upload segments to remote storage.
        remoteLogTaskScheduler.triggerPeriodicScheduledTasks();

        List<RemoteLogSegment> segments = remoteLogManager.relevantRemoteLogSegments(tb, 0L);
        assertThat(segments).isNotEmpty();

        // Get the end offset of all remote segments to use as localLogStartOffset.
        long remoteEndOffset = segments.get(segments.size() - 1).remoteLogEndOffset();

        File logTabletDir = logTablet.getLogDir();
        try (RemoteLogFetcher fetcher = newFetcher(tb, logTabletDir)) {
            // Fetch all remote log batches from offset 0 to remoteEndOffset.
            Iterable<LogRecordBatch> batches = fetcher.fetch(0, remoteEndOffset);
            List<LogRecordBatch> batchList = new ArrayList<>();
            for (LogRecordBatch batch : batches) {
                batchList.add(batch);
            }

            // We should have received multiple batches.
            assertThat(batchList).isNotEmpty();
            // Verify ordering: each batch's baseLogOffset should be monotonically increasing.
            long prevOffset = -1;
            for (LogRecordBatch batch : batchList) {
                assertThat(batch.baseLogOffset()).isGreaterThan(prevOffset);
                prevOffset = batch.baseLogOffset();
            }
        }
    }

    @Test
    void testFetchWithStartOffsetInMiddleOfSegment() throws Exception {
        TableBucket tb = new TableBucket(DATA1_TABLE_ID, 0);
        makeLogTableAsLeader(tb, false);
        Replica replica = replicaManager.getReplicaOrException(tb);
        LogTablet logTablet = replica.getLogTablet();
        addMultiSegmentsToLogTablet(logTablet, 5);

        remoteLogTaskScheduler.triggerPeriodicScheduledTasks();

        List<RemoteLogSegment> segments = remoteLogManager.relevantRemoteLogSegments(tb, 0L);
        assertThat(segments).hasSizeGreaterThanOrEqualTo(2);

        // Start from the middle of the first segment (offset = 5, assuming each segment has 10
        // batches starting from 0).
        long startOffset = 5;
        long remoteEndOffset = segments.get(segments.size() - 1).remoteLogEndOffset();

        File logTabletDir = logTablet.getLogDir();
        try (RemoteLogFetcher fetcher = newFetcher(tb, logTabletDir)) {
            Iterable<LogRecordBatch> batches = fetcher.fetch(startOffset, remoteEndOffset);
            List<LogRecordBatch> batchList = new ArrayList<>();
            for (LogRecordBatch batch : batches) {
                batchList.add(batch);
            }

            assertThat(batchList).isNotEmpty();
            // All returned batches should have nextLogOffset > startOffset.
            for (LogRecordBatch batch : batchList) {
                assertThat(batch.nextLogOffset()).isGreaterThan(startOffset);
            }
        }
    }

    @Test
    void testFetchStopsAtLocalLogStartOffset() throws Exception {
        TableBucket tb = new TableBucket(DATA1_TABLE_ID, 0);
        makeLogTableAsLeader(tb, false);
        Replica replica = replicaManager.getReplicaOrException(tb);
        LogTablet logTablet = replica.getLogTablet();
        addMultiSegmentsToLogTablet(logTablet, 5);

        remoteLogTaskScheduler.triggerPeriodicScheduledTasks();

        List<RemoteLogSegment> segments = remoteLogManager.relevantRemoteLogSegments(tb, 0L);
        assertThat(segments).hasSizeGreaterThanOrEqualTo(2);

        // Use the start offset of the second segment as localLogStartOffset.
        long localLogStartOffset = segments.get(1).remoteLogStartOffset();

        File logTabletDir = logTablet.getLogDir();
        try (RemoteLogFetcher fetcher = newFetcher(tb, logTabletDir)) {
            Iterable<LogRecordBatch> batches = fetcher.fetch(0, localLogStartOffset);
            List<LogRecordBatch> batchList = new ArrayList<>();
            for (LogRecordBatch batch : batches) {
                batchList.add(batch);
            }

            assertThat(batchList).isNotEmpty();
            // All returned batches should have baseLogOffset < localLogStartOffset.
            for (LogRecordBatch batch : batchList) {
                assertThat(batch.baseLogOffset()).isLessThan(localLogStartOffset);
            }
        }
    }

    @Test
    void testFetchNoRemoteSegmentsThrowsException() throws Exception {
        // Use a table bucket that has no remote segments.
        TableBucket tb = new TableBucket(DATA1_TABLE_ID, 0);
        makeLogTableAsLeader(tb, false);
        // Don't upload any segments to remote.

        Replica replica = replicaManager.getReplicaOrException(tb);
        File logTabletDir = replica.getLogTablet().getLogDir();
        try (RemoteLogFetcher fetcher = newFetcher(tb, logTabletDir)) {
            assertThatThrownBy(() -> fetcher.fetch(0, 100))
                    .isInstanceOf(RemoteStorageException.class)
                    .hasMessageContaining("No remote log segments found");
        }
    }

    @Test
    void testStaleDirectoriesFromUncleanShutdownAreCleanedUp() throws Exception {
        TableBucket tb = new TableBucket(DATA1_TABLE_ID, 0);
        makeLogTableAsLeader(tb, false);
        Replica replica = replicaManager.getReplicaOrException(tb);
        File logTabletDir = replica.getLogTablet().getLogDir();

        Path tmpDir = logTabletDir.toPath().resolve("tmp");
        Files.createDirectories(tmpDir);

        // Simulate stale directories left behind by a previous unclean shutdown.
        Path staleDir1 = Files.createDirectories(tmpDir.resolve("remote-log-recovery-stale-1"));
        Path staleDir2 = Files.createDirectories(tmpDir.resolve("remote-log-recovery-stale-2"));
        // Create a file inside one stale dir to simulate a partially downloaded segment.
        Files.createFile(staleDir1.resolve("00000000000000000000.log"));

        assertThat(Files.exists(staleDir1)).isTrue();
        assertThat(Files.exists(staleDir2)).isTrue();

        // Simulate next startup: creating and closing a new RemoteLogFetcher should
        // clean up the entire tmp directory including all stale recovery directories.
        RemoteLogFetcher fetcher = newFetcher(tb, logTabletDir);
        fetcher.close();

        assertThat(Files.exists(staleDir1)).isFalse();
        assertThat(Files.exists(staleDir2)).isFalse();
        assertThat(Files.exists(tmpDir)).isFalse();
    }

    @Test
    void testTempDirectoryCreatedLazilyOnFetchAndCleanedUp() throws Exception {
        TableBucket tb = new TableBucket(DATA1_TABLE_ID, 0);
        makeLogTableAsLeader(tb, false);
        Replica replica = replicaManager.getReplicaOrException(tb);
        LogTablet logTablet = replica.getLogTablet();
        addMultiSegmentsToLogTablet(logTablet, 5);

        remoteLogTaskScheduler.triggerPeriodicScheduledTasks();

        List<RemoteLogSegment> segments = remoteLogManager.relevantRemoteLogSegments(tb, 0L);
        assertThat(segments).isNotEmpty();
        long remoteEndOffset = segments.get(segments.size() - 1).remoteLogEndOffset();

        File logTabletDir = logTablet.getLogDir();
        RemoteLogFetcher fetcher = newFetcher(tb, logTabletDir);
        Path tempDir = fetcher.getTempDir();

        // Not yet created before fetch.
        assertThat(Files.exists(tempDir)).isFalse();

        // Fetch triggers lazy directory creation.
        for (LogRecordBatch batch : fetcher.fetch(0, remoteEndOffset)) {
            assertThat(Files.exists(tempDir)).isTrue();
        }
        assertThat(Files.exists(tempDir)).isTrue();

        // Close and verify cleanup.
        fetcher.close();
        assertThat(Files.exists(tempDir)).isFalse();
        // The parent "tmp" directory should also be removed when empty.
        assertThat(Files.exists(tempDir.getParent())).isFalse();
    }

    @Test
    void testFetchMultipleSegmentsInOrder() throws Exception {
        TableBucket tb = new TableBucket(DATA1_TABLE_ID, 0);
        makeLogTableAsLeader(tb, false);
        Replica replica = replicaManager.getReplicaOrException(tb);
        LogTablet logTablet = replica.getLogTablet();
        // Add more segments to test multi-segment iteration.
        addMultiSegmentsToLogTablet(logTablet, 8);

        remoteLogTaskScheduler.triggerPeriodicScheduledTasks();

        List<RemoteLogSegment> segments = remoteLogManager.relevantRemoteLogSegments(tb, 0L);
        // We expect at least 7 remote segments (8 total segments, last one is active).
        assertThat(segments).hasSizeGreaterThanOrEqualTo(4);

        long remoteEndOffset = segments.get(segments.size() - 1).remoteLogEndOffset();

        File logTabletDir = logTablet.getLogDir();
        try (RemoteLogFetcher fetcher = newFetcher(tb, logTabletDir)) {
            Iterable<LogRecordBatch> batches = fetcher.fetch(0, remoteEndOffset);

            long prevNextOffset = 0;
            int batchCount = 0;
            for (LogRecordBatch batch : batches) {
                // Verify strict monotonic ordering of offsets.
                assertThat(batch.baseLogOffset()).isGreaterThanOrEqualTo(prevNextOffset);
                prevNextOffset = batch.nextLogOffset();
                batchCount++;
            }
            // Verify we got a reasonable number of batches across all segments.
            assertThat(batchCount).isGreaterThan(10);
        }
    }

    @Test
    void testFetchWithEmptyRange() throws Exception {
        TableBucket tb = new TableBucket(DATA1_TABLE_ID, 0);
        makeLogTableAsLeader(tb, false);
        Replica replica = replicaManager.getReplicaOrException(tb);
        LogTablet logTablet = replica.getLogTablet();
        addMultiSegmentsToLogTablet(logTablet, 5);

        remoteLogTaskScheduler.triggerPeriodicScheduledTasks();

        List<RemoteLogSegment> segments = remoteLogManager.relevantRemoteLogSegments(tb, 0L);
        assertThat(segments).isNotEmpty();

        // Use the same offset for both startOffset and localLogStartOffset,
        // creating an empty range [x, x). No batches should be returned.
        // This guards against the >= comparison in advance() being accidentally
        // changed to >, which would leak batches and cause duplicate records
        // during KV recovery.
        long sameOffset = segments.get(0).remoteLogStartOffset();

        File logTabletDir = logTablet.getLogDir();
        try (RemoteLogFetcher fetcher = newFetcher(tb, logTabletDir)) {
            Iterable<LogRecordBatch> batches = fetcher.fetch(sameOffset, sameOffset);
            List<LogRecordBatch> batchList = new ArrayList<>();
            for (LogRecordBatch batch : batches) {
                batchList.add(batch);
            }
            assertThat(batchList).isEmpty();
        }
    }

    @Test
    void testFetchDownloadsFilesToTempDir() throws Exception {
        TableBucket tb = new TableBucket(DATA1_TABLE_ID, 0);
        makeLogTableAsLeader(tb, false);
        Replica replica = replicaManager.getReplicaOrException(tb);
        LogTablet logTablet = replica.getLogTablet();
        addMultiSegmentsToLogTablet(logTablet, 5);

        remoteLogTaskScheduler.triggerPeriodicScheduledTasks();

        List<RemoteLogSegment> segments = remoteLogManager.relevantRemoteLogSegments(tb, 0L);
        assertThat(segments).isNotEmpty();
        long remoteEndOffset = segments.get(segments.size() - 1).remoteLogEndOffset();

        File logTabletDir = logTablet.getLogDir();
        try (RemoteLogFetcher fetcher = newFetcher(tb, logTabletDir)) {
            Path tempDir = fetcher.getTempDir();

            // Consume all batches to trigger downloading.
            for (LogRecordBatch batch : fetcher.fetch(0, remoteEndOffset)) {
                // Just iterate through.
            }

            // Temp dir should contain downloaded log files.
            File[] downloadedFiles = tempDir.toFile().listFiles();
            assertThat(downloadedFiles).isNotNull();
            assertThat(downloadedFiles.length).isGreaterThan(0);
            // All files should end with .log.
            for (File file : downloadedFiles) {
                assertThat(file.getName()).endsWith(".log");
            }
        }
    }

    @Test
    void testPrefetchFailureFallsBackToSyncDownload() throws Exception {
        TableBucket tb = new TableBucket(DATA1_TABLE_ID, 0);
        makeLogTableAsLeader(tb, false);
        Replica replica = replicaManager.getReplicaOrException(tb);
        LogTablet logTablet = replica.getLogTablet();
        addMultiSegmentsToLogTablet(logTablet, 5);

        remoteLogTaskScheduler.triggerPeriodicScheduledTasks();

        List<RemoteLogSegment> segments = remoteLogManager.relevantRemoteLogSegments(tb, 0L);
        assertThat(segments).hasSizeGreaterThanOrEqualTo(2);
        long remoteEndOffset = segments.get(segments.size() - 1).remoteLogEndOffset();

        File logTabletDir = logTablet.getLogDir();
        try (RemoteLogFetcher fetcher = newFetcher(tb, logTabletDir)) {
            Iterator<LogRecordBatch> iterator = fetcher.fetch(0, remoteEndOffset).iterator();
            assertThat(iterator.hasNext()).isTrue();
            iterator.next();

            // Inject a failed prefetch for the very next segment to prove the iterator
            // transparently falls back to a synchronous download.
            CompletableFuture<File> failedFuture = new CompletableFuture<>();
            failedFuture.completeExceptionally(new RuntimeException("injected prefetch failure"));
            fetcher.injectPrefetchEntryForTest(segments.get(1), failedFuture);

            int count = 1;
            while (iterator.hasNext()) {
                iterator.next();
                count++;
            }
            assertThat(count).isGreaterThan(1);
        }
    }

    @Test
    void testCloseCancelsPendingPrefetchFuture() throws Exception {
        TableBucket tb = new TableBucket(DATA1_TABLE_ID, 0);
        makeLogTableAsLeader(tb, false);
        Replica replica = replicaManager.getReplicaOrException(tb);
        LogTablet logTablet = replica.getLogTablet();
        addMultiSegmentsToLogTablet(logTablet, 5);

        remoteLogTaskScheduler.triggerPeriodicScheduledTasks();

        List<RemoteLogSegment> segments = remoteLogManager.relevantRemoteLogSegments(tb, 0L);
        assertThat(segments).isNotEmpty();
        long remoteEndOffset = segments.get(segments.size() - 1).remoteLogEndOffset();

        File logTabletDir = logTablet.getLogDir();
        RemoteLogFetcher fetcher = newFetcher(tb, logTabletDir);
        try {
            Iterator<LogRecordBatch> iterator = fetcher.fetch(0, remoteEndOffset).iterator();
            assertThat(iterator.hasNext()).isTrue();
            iterator.next();

            CompletableFuture<File> pendingFuture = new CompletableFuture<>();
            fetcher.injectPrefetchEntryForTest(segments.get(0), pendingFuture);

            fetcher.close();
            assertThat(pendingFuture.isCancelled()).isTrue();
        } finally {
            fetcher.close();
        }
    }

    @Test
    void testSecondFetchClosesPreviousIteratorAndOldIteratorStops() throws Exception {
        TableBucket tb = new TableBucket(DATA1_TABLE_ID, 0);
        makeLogTableAsLeader(tb, false);
        Replica replica = replicaManager.getReplicaOrException(tb);
        LogTablet logTablet = replica.getLogTablet();
        addMultiSegmentsToLogTablet(logTablet, 5);

        remoteLogTaskScheduler.triggerPeriodicScheduledTasks();

        List<RemoteLogSegment> segments = remoteLogManager.relevantRemoteLogSegments(tb, 0L);
        assertThat(segments).hasSizeGreaterThanOrEqualTo(2);
        long remoteEndOffset = segments.get(segments.size() - 1).remoteLogEndOffset();

        File logTabletDir = logTablet.getLogDir();
        try (RemoteLogFetcher fetcher = newFetcher(tb, logTabletDir)) {
            Iterable<LogRecordBatch> firstIterable = fetcher.fetch(0, remoteEndOffset);
            Iterator<LogRecordBatch> firstIterator = firstIterable.iterator();

            assertThat(firstIterator.hasNext()).isTrue();
            firstIterator.next();

            // Trigger a second fetch. This should close the previous active iterator.
            Iterable<LogRecordBatch> secondIterable = fetcher.fetch(0, remoteEndOffset);
            Iterator<LogRecordBatch> secondIterator = secondIterable.iterator();

            assertThat(firstIterator.hasNext()).isFalse();
            assertThat(secondIterator.hasNext()).isTrue();
        }
    }

    @Test
    void testSecondFetchAfterRealAsyncPrefetchDoesNotLetOldIteratorContinue() throws Exception {
        TableBucket tb = new TableBucket(DATA1_TABLE_ID, 0);
        makeLogTableAsLeader(tb, false);
        Replica replica = replicaManager.getReplicaOrException(tb);
        LogTablet logTablet = replica.getLogTablet();
        addMultiSegmentsToLogTablet(logTablet, 8);

        remoteLogTaskScheduler.triggerPeriodicScheduledTasks();

        List<RemoteLogSegment> segments = remoteLogManager.relevantRemoteLogSegments(tb, 0L);
        assertThat(segments).hasSizeGreaterThanOrEqualTo(2);
        long remoteEndOffset = segments.get(segments.size() - 1).remoteLogEndOffset();

        File logTabletDir = logTablet.getLogDir();
        try (RemoteLogFetcher fetcher = newFetcher(tb, logTabletDir)) {
            Path tempDir = fetcher.getTempDir();

            Iterator<LogRecordBatch> firstIterator = fetcher.fetch(0, remoteEndOffset).iterator();

            assertThat(firstIterator.hasNext()).isTrue();
            firstIterator.next();

            waitUntilLogFileCountAtLeast(tempDir, 2);

            long fileCountBeforeSecondFetch = countLogFiles(tempDir);
            assertThat(fileCountBeforeSecondFetch).isGreaterThanOrEqualTo(2);

            Iterator<LogRecordBatch> secondIterator = fetcher.fetch(0, remoteEndOffset).iterator();

            assertThat(firstIterator.hasNext()).isFalse();
            assertThat(secondIterator.hasNext()).isTrue();
            assertThat(Files.exists(tempDir)).isTrue();
        }
    }

    // ======================================================================================
    // New tests specific to the configurable prefetch window / multi-threaded download
    // ======================================================================================

    @Test
    void testPrefetchDepthGreaterThanOnePrefetchesMultipleSegments() throws Exception {
        TableBucket tb = new TableBucket(DATA1_TABLE_ID, 0);
        makeLogTableAsLeader(tb, false);
        Replica replica = replicaManager.getReplicaOrException(tb);
        LogTablet logTablet = replica.getLogTablet();
        addMultiSegmentsToLogTablet(logTablet, 8);

        remoteLogTaskScheduler.triggerPeriodicScheduledTasks();

        List<RemoteLogSegment> segments = remoteLogManager.relevantRemoteLogSegments(tb, 0L);
        assertThat(segments).hasSizeGreaterThanOrEqualTo(4);
        long remoteEndOffset = segments.get(segments.size() - 1).remoteLogEndOffset();

        File logTabletDir = logTablet.getLogDir();
        // prefetchNum=3, downloadThreads=3 — should populate 3 concurrent downloads.
        try (RemoteLogFetcher fetcher = newFetcher(tb, logTabletDir, 3, 3)) {
            Path tempDir = fetcher.getTempDir();
            Iterator<LogRecordBatch> iterator = fetcher.fetch(0, remoteEndOffset).iterator();
            assertThat(iterator.hasNext()).isTrue();
            iterator.next();

            // After consuming one batch the first segment is opened and the window refills.
            // We expect >= 3 .log files to appear on disk before we iterate further.
            waitUntilLogFileCountAtLeast(tempDir, 3);
            assertThat(countLogFiles(tempDir)).isGreaterThanOrEqualTo(3);
        }
    }

    @Test
    void testPrefetchWindowRefillsAfterSegmentConsumed() throws Exception {
        TableBucket tb = new TableBucket(DATA1_TABLE_ID, 0);
        makeLogTableAsLeader(tb, false);
        Replica replica = replicaManager.getReplicaOrException(tb);
        LogTablet logTablet = replica.getLogTablet();
        addMultiSegmentsToLogTablet(logTablet, 8);

        remoteLogTaskScheduler.triggerPeriodicScheduledTasks();

        List<RemoteLogSegment> segments = remoteLogManager.relevantRemoteLogSegments(tb, 0L);
        assertThat(segments).hasSizeGreaterThanOrEqualTo(4);
        long remoteEndOffset = segments.get(segments.size() - 1).remoteLogEndOffset();

        File logTabletDir = logTablet.getLogDir();
        // Small window (2) forces the refill path to trigger repeatedly.
        try (RemoteLogFetcher fetcher = newFetcher(tb, logTabletDir, 2, 2)) {
            Iterable<LogRecordBatch> batches = fetcher.fetch(0, remoteEndOffset);
            int batchCount = 0;
            long prevNextOffset = 0;
            for (LogRecordBatch batch : batches) {
                assertThat(batch.baseLogOffset()).isGreaterThanOrEqualTo(prevNextOffset);
                prevNextOffset = batch.nextLogOffset();
                batchCount++;
            }
            // With 4+ segments and 10 batches each we expect clearly more than 2*windowSize.
            assertThat(batchCount).isGreaterThan(10);
        }
    }

    @Test
    void testPrefetchDepthOfOnePreservesLegacyBehavior() throws Exception {
        TableBucket tb = new TableBucket(DATA1_TABLE_ID, 0);
        makeLogTableAsLeader(tb, false);
        Replica replica = replicaManager.getReplicaOrException(tb);
        LogTablet logTablet = replica.getLogTablet();
        addMultiSegmentsToLogTablet(logTablet, 5);

        remoteLogTaskScheduler.triggerPeriodicScheduledTasks();

        List<RemoteLogSegment> segments = remoteLogManager.relevantRemoteLogSegments(tb, 0L);
        assertThat(segments).isNotEmpty();
        long remoteEndOffset = segments.get(segments.size() - 1).remoteLogEndOffset();

        File logTabletDir = logTablet.getLogDir();
        try (RemoteLogFetcher fetcher = newFetcher(tb, logTabletDir, 1, 1)) {
            assertThat(fetcher.getPrefetchNum()).isEqualTo(1);

            Iterable<LogRecordBatch> batches = fetcher.fetch(0, remoteEndOffset);
            int batchCount = 0;
            long prevNextOffset = 0;
            for (LogRecordBatch batch : batches) {
                assertThat(batch.baseLogOffset()).isGreaterThanOrEqualTo(prevNextOffset);
                prevNextOffset = batch.nextLogOffset();
                batchCount++;
            }
            assertThat(batchCount).isGreaterThan(0);
        }
    }

    @Test
    void testCloseCancelsAllPendingPrefetchFutures() throws Exception {
        TableBucket tb = new TableBucket(DATA1_TABLE_ID, 0);
        makeLogTableAsLeader(tb, false);
        Replica replica = replicaManager.getReplicaOrException(tb);
        LogTablet logTablet = replica.getLogTablet();
        addMultiSegmentsToLogTablet(logTablet, 5);

        remoteLogTaskScheduler.triggerPeriodicScheduledTasks();

        List<RemoteLogSegment> segments = remoteLogManager.relevantRemoteLogSegments(tb, 0L);
        assertThat(segments).isNotEmpty();
        long remoteEndOffset = segments.get(segments.size() - 1).remoteLogEndOffset();

        File logTabletDir = logTablet.getLogDir();
        RemoteLogFetcher fetcher = newFetcher(tb, logTabletDir, 2, 2);
        try {
            Iterator<LogRecordBatch> iterator = fetcher.fetch(0, remoteEndOffset).iterator();
            assertThat(iterator.hasNext()).isTrue();
            iterator.next();

            CompletableFuture<File> pendingFuture = new CompletableFuture<>();
            fetcher.injectPrefetchEntryForTest(segments.get(0), pendingFuture);

            List<Future<File>> futures = fetcher.snapshotPrefetchFuturesForTest();
            assertThat(futures).containsExactly(pendingFuture);

            fetcher.close();

            assertThat(pendingFuture.isCancelled()).isTrue();
        } finally {
            fetcher.close();
        }
    }

    // ======================================================================================
    // Integration tests: exercise the real download path with a slow / failing remote storage.
    // These tests piggy-back on the per-test TestingRemoteLogStorage that ReplicaTestBase wires
    // into the RemoteLogManager, and toggle injection knobs *after* segments have been uploaded
    // so only the consumer-side fetchLogData() calls are affected.
    // ======================================================================================

    @Test
    void testSlowDownloadOverlappedWithConsumption() throws Exception {
        TableBucket tb = new TableBucket(DATA1_TABLE_ID, 0);
        makeLogTableAsLeader(tb, false);
        Replica replica = replicaManager.getReplicaOrException(tb);
        LogTablet logTablet = replica.getLogTablet();
        addMultiSegmentsToLogTablet(logTablet, 8);

        remoteLogTaskScheduler.triggerPeriodicScheduledTasks();

        List<RemoteLogSegment> segments = remoteLogManager.relevantRemoteLogSegments(tb, 0L);
        assertThat(segments).hasSizeGreaterThanOrEqualTo(4);
        long remoteEndOffset = segments.get(segments.size() - 1).remoteLogEndOffset();

        // After uploads are done, slow every fetchLogData by 200ms so we can observe overlap.
        remoteLogStorage.fetchLogDataDelayMs.set(200L);

        File logTabletDir = logTablet.getLogDir();
        // prefetchNum=3, downloadThreads=3 — three concurrent slow downloads should overlap with
        // the consumer reading the first segment.
        try (RemoteLogFetcher fetcher = newFetcher(tb, logTabletDir, 3, 3)) {
            Path tempDir = fetcher.getTempDir();
            long startNanos = System.nanoTime();

            Iterator<LogRecordBatch> iterator = fetcher.fetch(0, remoteEndOffset).iterator();
            assertThat(iterator.hasNext()).isTrue();
            iterator.next();

            long firstBatchElapsedMs =
                    java.util.concurrent.TimeUnit.NANOSECONDS.toMillis(
                            System.nanoTime() - startNanos);

            // Within ~5s the prefetch window should hold at least 3 concurrently downloaded
            // .log files on disk, proving downloads run in parallel rather than serially.
            waitUntilLogFileCountAtLeast(tempDir, 3);
            assertThat(countLogFiles(tempDir)).isGreaterThanOrEqualTo(3);

            int batchCount = 1;
            while (iterator.hasNext()) {
                iterator.next();
                batchCount++;
            }

            // Sanity: with 4+ segments * 10 batches we expect plenty of records.
            assertThat(batchCount).isGreaterThan(10);

            // If downloads were serial we'd need at least 4 * 200ms before the FIRST batch can
            // be returned (head segment must be downloaded first). The first-batch wait must
            // therefore be close to a single 200ms slot, not multiplied by the segment count.
            // We give a generous upper bound to keep the test stable on slow CI.
            assertThat(firstBatchElapsedMs).isLessThan(2_000L);
        } finally {
            remoteLogStorage.fetchLogDataDelayMs.set(0L);
        }
    }

    @Test
    void testPersistentDownloadFailureTriggersRetryAndPropagates() throws Exception {
        TableBucket tb = new TableBucket(DATA1_TABLE_ID, 0);
        makeLogTableAsLeader(tb, false);
        Replica replica = replicaManager.getReplicaOrException(tb);
        LogTablet logTablet = replica.getLogTablet();
        addMultiSegmentsToLogTablet(logTablet, 5);

        remoteLogTaskScheduler.triggerPeriodicScheduledTasks();

        List<RemoteLogSegment> segments = remoteLogManager.relevantRemoteLogSegments(tb, 0L);
        assertThat(segments).isNotEmpty();
        long remoteEndOffset = segments.get(segments.size() - 1).remoteLogEndOffset();

        // Force every fetchLogData to fail once uploads are done.
        remoteLogStorage.fetchLogDataAlwaysFail.set(true);

        File logTabletDir = logTablet.getLogDir();
        try (RemoteLogFetcher fetcher = newFetcher(tb, logTabletDir, 1, 1)) {
            int countBefore = remoteLogStorage.fetchLogDataInvocationCount();

            // The very first hasNext() will trigger downloadSegmentWithRetry on the head
            // segment, which exhausts MAX_RETRIES (5) + initial attempt = 6 calls, then
            // surfaces an IOException wrapped as RuntimeException by advance().
            assertThatThrownBy(() -> fetcher.fetch(0, remoteEndOffset).iterator().hasNext())
                    .isInstanceOf(RuntimeException.class)
                    .hasMessageContaining("Failed to fetch remote log segment");

            int countAfter = remoteLogStorage.fetchLogDataInvocationCount();
            // 1 initial attempt + 5 retries = 6 invocations expected. We assert >= 6 to also
            // tolerate the eager prefetch path performing one extra invocation before the
            // sync fallback begins.
            assertThat(countAfter - countBefore).isGreaterThanOrEqualTo(6);
        } finally {
            remoteLogStorage.fetchLogDataAlwaysFail.set(false);
        }
    }

    @Test
    void testTransientDownloadFailureRecoversOnRetry() throws Exception {
        TableBucket tb = new TableBucket(DATA1_TABLE_ID, 0);
        makeLogTableAsLeader(tb, false);
        Replica replica = replicaManager.getReplicaOrException(tb);
        LogTablet logTablet = replica.getLogTablet();
        addMultiSegmentsToLogTablet(logTablet, 5);

        remoteLogTaskScheduler.triggerPeriodicScheduledTasks();

        List<RemoteLogSegment> segments = remoteLogManager.relevantRemoteLogSegments(tb, 0L);
        assertThat(segments).isNotEmpty();
        long remoteEndOffset = segments.get(segments.size() - 1).remoteLogEndOffset();

        // The first 2 fetchLogData invocations fail; from the 3rd onward they succeed. With
        // prefetchNum=1 and a single segment to consume, the retry loop must recover and the
        // iterator must produce all batches without leaking prefetch slots.
        remoteLogStorage.fetchLogDataFailFirstN.set(2);

        File logTabletDir = logTablet.getLogDir();
        try (RemoteLogFetcher fetcher = newFetcher(tb, logTabletDir, 1, 1)) {
            int batchCount = 0;
            long prevNextOffset = 0;
            for (LogRecordBatch batch : fetcher.fetch(0, remoteEndOffset)) {
                assertThat(batch.baseLogOffset()).isGreaterThanOrEqualTo(prevNextOffset);
                prevNextOffset = batch.nextLogOffset();
                batchCount++;
            }

            assertThat(batchCount).isGreaterThan(0);
            // We expect at least 2 (failed) + 1 (succeeded) = 3 invocations on the head segment
            // alone, plus normal downloads for the remaining segments.
            assertThat(remoteLogStorage.fetchLogDataInvocationCount()).isGreaterThanOrEqualTo(3);
        }
    }

    @Test
    void testCloseInterruptsOngoingDownload() throws Exception {
        TableBucket tb = new TableBucket(DATA1_TABLE_ID, 0);
        makeLogTableAsLeader(tb, false);
        Replica replica = replicaManager.getReplicaOrException(tb);
        LogTablet logTablet = replica.getLogTablet();
        addMultiSegmentsToLogTablet(logTablet, 5);

        remoteLogTaskScheduler.triggerPeriodicScheduledTasks();

        List<RemoteLogSegment> segments = remoteLogManager.relevantRemoteLogSegments(tb, 0L);
        assertThat(segments).isNotEmpty();
        long remoteEndOffset = segments.get(segments.size() - 1).remoteLogEndOffset();

        // prefetch=2 + 2 download threads → up to 2 workers may enter fetchLogData
        // concurrently. The release latch is never counted down by the test — the ONLY way
        // out for a stuck worker is Thread.interrupt() arriving at `release.await()`, which
        // is the exact behavior we need to assert. The `finished` latch is counted down in
        // a finally block inside the barrier so it fires whether await() returned normally
        // or was interrupted — but since `release` is never counted down, any observed
        // countDown on `finished` is definitive evidence that the worker was interrupted.
        int expectedInFlight = 2;
        CountDownLatch entered = new CountDownLatch(expectedInFlight);
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch finished = new CountDownLatch(expectedInFlight);

        remoteLogStorage.fetchLogDataBarrier =
                () -> {
                    entered.countDown();
                    try {
                        release.await();
                    } finally {
                        // Single countDown point — fires on both the normal-release path and
                        // the interrupted path, so the barrier contract is always honored.
                        finished.countDown();
                    }
                };

        File logTabletDir = logTablet.getLogDir();
        RemoteLogFetcher fetcher = newFetcher(tb, logTabletDir, 2, 2);
        try {
            // fetch() is lazy — kicks off prefetch workers that will block on `release`.
            fetcher.fetch(0, remoteEndOffset);

            // (1) Real-progress evidence: prefetch workers actually entered fetchLogData.
            assertThat(entered.await(30, TimeUnit.SECONDS))
                    .as("prefetch workers should have entered fetchLogData")
                    .isTrue();

            // Snapshot the in-flight futures BEFORE close so we can assert on them afterwards.
            List<Future<File>> inflight = fetcher.snapshotPrefetchFuturesForTest();
            assertThat(inflight).isNotEmpty();

            long closeStart = System.nanoTime();
            fetcher.close();
            long closeMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - closeStart);

            // (2) Real-interruption evidence: because `release` is never counted down by the
            // test, the only way `finished` can reach zero is if Thread.interrupt() unblocked
            // `release.await()` inside the barrier. This is the assertion fresh-borzoni asked
            // for — f.isCancelled() alone can't prove this, but finished.await() can.
            assertThat(finished.await(10, TimeUnit.SECONDS))
                    .as(
                            "close() should have interrupted the blocked prefetch workers,"
                                    + " causing them to exit fetchLogData")
                    .isTrue();

            // (3) close() must not block waiting for `release` — it should shut the executor
            // down promptly (well under 5s on any healthy machine).
            assertThat(closeMs).isLessThan(5_000L);

            // (4) API-level state: every captured in-flight future must end up done and
            // cancelled or completed exceptionally (an interrupted await surfaces as an
            // InterruptedException wrapped in RemoteStorageException).
            for (Future<File> f : inflight) {
                assertThat(f.isDone()).isTrue();
                boolean exceptional = false;
                try {
                    f.get();
                } catch (CancellationException | ExecutionException expected) {
                    exceptional = true;
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                }
                assertThat(f.isCancelled() || exceptional).isTrue();
            }
        } finally {
            remoteLogStorage.fetchLogDataBarrier = null;
            // Belt-and-braces: release any straggler worker that might still be blocked
            // (e.g. if close() didn't fully propagate the interrupt for some reason).
            release.countDown();
            fetcher.close();
        }
    }

    private static void waitUntilLogFileCountAtLeast(Path dir, int expectedCount) throws Exception {
        long deadline = System.currentTimeMillis() + 5_000L;
        while (System.currentTimeMillis() < deadline) {
            if (Files.exists(dir) && countLogFiles(dir) >= expectedCount) {
                return;
            }
            Thread.sleep(50L);
        }

        assertThat(countLogFiles(dir)).isGreaterThanOrEqualTo(expectedCount);
    }

    private static long countLogFiles(Path dir) throws IOException {
        if (!Files.exists(dir)) {
            return 0L;
        }

        try (java.util.stream.Stream<Path> paths = Files.list(dir)) {
            return paths.filter(path -> path.getFileName().toString().endsWith(".log")).count();
        }
    }
}
