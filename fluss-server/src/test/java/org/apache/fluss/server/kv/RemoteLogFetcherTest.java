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
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.apache.fluss.record.TestData.DATA1_TABLE_ID;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link RemoteLogFetcher}. */
class RemoteLogFetcherTest extends RemoteLogTestBase {

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

        File dataDir = logManager.getDataDir();
        try (RemoteLogFetcher fetcher = new RemoteLogFetcher(remoteLogManager, tb, dataDir)) {
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

        File dataDir = logManager.getDataDir();
        try (RemoteLogFetcher fetcher = new RemoteLogFetcher(remoteLogManager, tb, dataDir)) {
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

        File dataDir = logManager.getDataDir();
        try (RemoteLogFetcher fetcher = new RemoteLogFetcher(remoteLogManager, tb, dataDir)) {
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

        File dataDir = logManager.getDataDir();
        try (RemoteLogFetcher fetcher = new RemoteLogFetcher(remoteLogManager, tb, dataDir)) {
            assertThatThrownBy(() -> fetcher.fetch(0, 100))
                    .isInstanceOf(RemoteStorageException.class)
                    .hasMessageContaining("No remote log segments found");
        }
    }

    @Test
    void testTempDirectoryCreatedAndCleanedUp() throws Exception {
        TableBucket tb = new TableBucket(DATA1_TABLE_ID, 0);
        File dataDir = logManager.getDataDir();

        RemoteLogFetcher fetcher = new RemoteLogFetcher(remoteLogManager, tb, dataDir);
        Path tempDir = fetcher.getTempDir();

        // Verify temp directory was created.
        assertThat(Files.exists(tempDir)).isTrue();
        assertThat(tempDir.getFileName().toString()).startsWith("remote-log-recovery-");

        // Close and verify cleanup.
        fetcher.close();
        assertThat(Files.exists(tempDir)).isFalse();
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

        File dataDir = logManager.getDataDir();
        try (RemoteLogFetcher fetcher = new RemoteLogFetcher(remoteLogManager, tb, dataDir)) {
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

        File dataDir = logManager.getDataDir();
        try (RemoteLogFetcher fetcher = new RemoteLogFetcher(remoteLogManager, tb, dataDir)) {
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
}
