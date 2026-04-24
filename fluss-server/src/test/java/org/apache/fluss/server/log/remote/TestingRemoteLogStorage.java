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

import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.RemoteStorageException;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.remote.RemoteLogSegment;

import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A testing implementation of {@link org.apache.fluss.server.log.remote.RemoteLogStorage} which can
 * be used to simulate failures.
 */
public class TestingRemoteLogStorage extends DefaultRemoteLogStorage {

    public final AtomicBoolean writeManifestFail = new AtomicBoolean(false);

    /**
     * When set to a non-negative value N, the first N calls to {@link #copyLogSegmentFiles} will
     * succeed and the (N+1)th call will throw a {@link RemoteStorageException}. A negative value
     * (default) disables this failure injection.
     */
    public final AtomicInteger copySegmentFailAfterNCopies = new AtomicInteger(-1);

    private final AtomicInteger copySegmentCount = new AtomicInteger(0);

    /**
     * When set to a positive value, each call to {@link #fetchLogData} will sleep for the given
     * number of milliseconds before delegating to the real implementation. Used by integration
     * tests that need to simulate a slow remote storage backend.
     */
    public final AtomicLong fetchLogDataDelayMs = new AtomicLong(0L);

    /**
     * When set to a positive value N, the first N {@link #fetchLogData} calls (across <b>all</b>
     * segments) will throw a {@link RemoteStorageException}. Subsequent calls behave normally.
     * Useful to exercise the exponential-backoff retry loop inside {@code
     * RemoteLogFetcher#downloadSegmentWithRetry}.
     */
    public final AtomicInteger fetchLogDataFailFirstN = new AtomicInteger(0);

    /**
     * When set to {@code true}, every {@link #fetchLogData} call throws a {@link
     * RemoteStorageException}. Used to validate that retries are exhausted and the failure is
     * correctly propagated.
     */
    public final AtomicBoolean fetchLogDataAlwaysFail = new AtomicBoolean(false);

    private final AtomicInteger fetchLogDataCount = new AtomicInteger(0);

    public TestingRemoteLogStorage(Configuration conf, ExecutorService ioExecutor)
            throws IOException {
        super(conf, ioExecutor);
    }

    @Override
    public void copyLogSegmentFiles(
            RemoteLogSegment remoteLogSegment, LogSegmentFiles logSegmentFiles)
            throws RemoteStorageException {
        int failAfter = copySegmentFailAfterNCopies.get();
        if (failAfter >= 0 && copySegmentCount.get() >= failAfter) {
            throw new RemoteStorageException(
                    "Simulated copy failure after " + failAfter + " successful copies");
        }
        super.copyLogSegmentFiles(remoteLogSegment, logSegmentFiles);
        copySegmentCount.incrementAndGet();
    }

    @Override
    public FsPath writeRemoteLogManifestSnapshot(RemoteLogManifest manifest)
            throws RemoteStorageException {
        if (writeManifestFail.get()) {
            throw new RuntimeException("failed to upload remote log manifest snapshot");
        }
        return super.writeRemoteLogManifestSnapshot(manifest);
    }

    @Override
    public InputStream fetchLogData(RemoteLogSegment remoteLogSegment)
            throws RemoteStorageException {
        int idx = fetchLogDataCount.getAndIncrement();
        if (fetchLogDataAlwaysFail.get()) {
            throw new RemoteStorageException(
                    "Simulated persistent fetchLogData failure for segment "
                            + remoteLogSegment.remoteLogSegmentId());
        }
        int failFirstN = fetchLogDataFailFirstN.get();
        if (idx < failFirstN) {
            throw new RemoteStorageException(
                    "Simulated transient fetchLogData failure, attempt "
                            + (idx + 1)
                            + "/"
                            + failFirstN);
        }
        long delayMs = fetchLogDataDelayMs.get();
        if (delayMs > 0L) {
            try {
                Thread.sleep(delayMs);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RemoteStorageException("Interrupted while simulating slow download", e);
            }
        }
        return super.fetchLogData(remoteLogSegment);
    }

    /** Returns how many times {@link #fetchLogData} has been invoked since construction. */
    public int fetchLogDataInvocationCount() {
        return fetchLogDataCount.get();
    }
}
