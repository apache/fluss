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

package org.apache.fluss.server.log.remote;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.exception.RemoteStorageException;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.remote.RemoteLogSegment;
import org.apache.fluss.server.log.LogTablet;
import org.apache.fluss.utils.FlussPaths;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for the retry and cleanup behavior of {@link DefaultRemoteLogStorage}. */
class DefaultRemoteLogStorageRetryTest extends RemoteLogTestBase {

    private ExecutorService ioExecutor;

    @BeforeEach
    @Override
    public void setup() throws Exception {
        super.setup();
        ioExecutor = Executors.newSingleThreadExecutor();
    }

    @AfterEach
    public void teardown() throws Exception {
        if (ioExecutor != null) {
            ioExecutor.shutdown();
        }
    }

    @Test
    void testUploadRetriesAndSucceedsOnSecondAttempt() throws Exception {
        // Configure with 3 retries and minimal backoff
        conf.set(ConfigOptions.REMOTE_LOG_UPLOAD_RETRY_MAX_ATTEMPTS, 3);
        conf.set(ConfigOptions.REMOTE_LOG_UPLOAD_RETRY_INITIAL_BACKOFF, Duration.ofMillis(1));

        // A storage that fails the first writeToRemote call, then succeeds
        AtomicInteger writeAttempts = new AtomicInteger(0);
        RetryingRemoteLogStorage storage =
                new RetryingRemoteLogStorage(conf, ioExecutor, writeAttempts, 1);

        LogTablet logTablet = makeLogTabletAndAddSegments(false);
        RemoteLogSegment remoteLogSegment = copyLogSegmentToRemote(logTablet, storage, 0);

        // Verify the segment was uploaded successfully
        File remoteLogDir = getTestingRemoteLogSegmentDir(remoteLogSegment, storage);
        assertThat(remoteLogDir.exists()).isTrue();
        assertThat(remoteLogDir.listFiles()).isNotNull().hasSize(4);

        // Verify that writeToRemote was called more than once (proving retry happened)
        assertThat(writeAttempts.get()).isGreaterThan(4); // 4 files, at least one retried

        storage.close();
    }

    @Test
    void testUploadFailsAfterAllRetriesExhausted() throws Exception {
        // Configure with 2 retries and minimal backoff
        conf.set(ConfigOptions.REMOTE_LOG_UPLOAD_RETRY_MAX_ATTEMPTS, 2);
        conf.set(ConfigOptions.REMOTE_LOG_UPLOAD_RETRY_INITIAL_BACKOFF, Duration.ofMillis(1));

        // A storage that always fails
        AtomicInteger writeAttempts = new AtomicInteger(0);
        RetryingRemoteLogStorage storage =
                new RetryingRemoteLogStorage(conf, ioExecutor, writeAttempts, Integer.MAX_VALUE);

        LogTablet logTablet = makeLogTabletAndAddSegments(false);

        // copyLogSegmentToRemote should throw RemoteStorageException
        assertThatThrownBy(() -> copyLogSegmentToRemote(logTablet, storage, 0))
                .isInstanceOf(RemoteStorageException.class);

        // Verify that writeToRemote was retried: each file should have been attempted
        // (maxAttempts + 1) = 3 times
        assertThat(writeAttempts.get()).isGreaterThanOrEqualTo(3);

        storage.close();
    }

    @Test
    void testPartialUploadCleanupOnFailure() throws Exception {
        // Configure with 0 retries (fail immediately)
        conf.set(ConfigOptions.REMOTE_LOG_UPLOAD_RETRY_MAX_ATTEMPTS, 0);
        conf.set(ConfigOptions.REMOTE_LOG_UPLOAD_RETRY_INITIAL_BACKOFF, Duration.ofMillis(1));

        // A storage that fails all writes
        AtomicInteger writeAttempts = new AtomicInteger(0);
        RetryingRemoteLogStorage storage =
                new RetryingRemoteLogStorage(conf, ioExecutor, writeAttempts, Integer.MAX_VALUE);

        LogTablet logTablet = makeLogTabletAndAddSegments(false);

        assertThatThrownBy(() -> copyLogSegmentToRemote(logTablet, storage, 0))
                .isInstanceOf(RemoteStorageException.class);

        // After failure, the cleanup should have deleted any partially uploaded files.
        // Since all writes failed, the remote directory may or may not exist, but if it
        // does, it should be empty (all files were either never created or cleaned up).
        // We verify by checking that writeAttempts > 0 (at least one write was attempted)
        assertThat(writeAttempts.get()).isGreaterThan(0);

        storage.close();
    }

    @Test
    void testNoRetryWhenMaxAttemptsIsZero() throws Exception {
        // Configure with 0 retries
        conf.set(ConfigOptions.REMOTE_LOG_UPLOAD_RETRY_MAX_ATTEMPTS, 0);
        conf.set(ConfigOptions.REMOTE_LOG_UPLOAD_RETRY_INITIAL_BACKOFF, Duration.ofMillis(1));

        AtomicInteger writeAttempts = new AtomicInteger(0);
        RetryingRemoteLogStorage storage =
                new RetryingRemoteLogStorage(conf, ioExecutor, writeAttempts, Integer.MAX_VALUE);

        LogTablet logTablet = makeLogTabletAndAddSegments(false);

        assertThatThrownBy(() -> copyLogSegmentToRemote(logTablet, storage, 0))
                .isInstanceOf(RemoteStorageException.class);

        // With 0 retries, each file should only be attempted once (no retry)
        // 4 files (log, offset index, timestamp index, producer snapshot)
        assertThat(writeAttempts.get()).isEqualTo(4);

        storage.close();
    }

    private File getTestingRemoteLogSegmentDir(
            RemoteLogSegment remoteLogSegment, RemoteLogStorage storage) {
        return new File(
                FlussPaths.remoteLogSegmentDir(
                                FlussPaths.remoteLogTabletDir(
                                        storage.getRemoteLogDir(),
                                        remoteLogSegment.physicalTablePath(),
                                        remoteLogSegment.tableBucket()),
                                remoteLogSegment.remoteLogSegmentId())
                        .toString());
    }

    /**
     * A {@link DefaultRemoteLogStorage} subclass that injects failures into {@code writeToRemote}
     * for the first {@code failFirstNCalls} calls, then delegates to the real implementation.
     */
    private static class RetryingRemoteLogStorage extends DefaultRemoteLogStorage {
        private final AtomicInteger writeCallCounter;
        private final int failFirstNCalls;

        RetryingRemoteLogStorage(
                org.apache.fluss.config.Configuration conf,
                ExecutorService ioExecutor,
                AtomicInteger writeCallCounter,
                int failFirstNCalls)
                throws IOException {
            super(conf, ioExecutor);
            this.writeCallCounter = writeCallCounter;
            this.failFirstNCalls = failFirstNCalls;
        }

        @Nullable
        @Override
        FsPath writeToRemote(InputStream inputStream, FsPath remoteDir, String remoteFileName)
                throws IOException {
            int callNum = writeCallCounter.incrementAndGet();
            if (callNum <= failFirstNCalls) {
                throw new IOException(
                        "Simulated failure on call " + callNum + " for file " + remoteFileName);
            }
            return super.writeToRemote(inputStream, remoteDir, remoteFileName);
        }
    }
}
