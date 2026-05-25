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

import org.apache.fluss.record.LogRecords;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

/**
 * Represents the future of a single chunk read from a remote log segment. Each chunk is delivered
 * as a {@link LogRecords} via a {@link CompletableFuture}.
 */
public class RemoteLogDownloadFuture {

    private static final Logger LOG = LoggerFactory.getLogger(RemoteLogDownloadFuture.class);

    private final CompletableFuture<LogRecords> chunkFuture;
    private final Runnable recycleCallback;
    private Consumer<RemoteLogDownloadFuture> nextChunkCallback;

    public RemoteLogDownloadFuture(
            CompletableFuture<LogRecords> chunkFuture, Runnable recycleCallback) {
        this.chunkFuture = chunkFuture;
        this.recycleCallback = recycleCallback;
    }

    public boolean isDone() {
        return chunkFuture.isDone();
    }

    /** Returns the chunk data. Blocks until the chunk is ready. */
    public LogRecords getLogRecords() {
        return chunkFuture.join();
    }

    public Runnable getRecycleCallback() {
        return recycleCallback;
    }

    public void onComplete(Runnable callback) {
        // Use whenComplete (instead of thenRun) so the callback fires regardless of whether
        // the chunkFuture completed successfully or exceptionally. This is critical: if a chunk
        // read fails, the LogFetchBuffer still needs tryComplete() to be called so the failed
        // PendingFetch can be drained (otherwise the bucket would be stuck permanently).
        chunkFuture.whenComplete(
                (result, throwable) -> {
                    try {
                        callback.run();
                    } catch (Throwable t) {
                        LOG.error("Exception in chunk completion callback", t);
                    }
                });
    }

    /**
     * Sets a callback that will be invoked when the next chunk's future is created. This allows the
     * {@link LogFetcher} to register a new {@link RemotePendingFetch} for each subsequent chunk.
     */
    public void setNextChunkCallback(Consumer<RemoteLogDownloadFuture> callback) {
        this.nextChunkCallback = callback;
    }

    public Consumer<RemoteLogDownloadFuture> getNextChunkCallback() {
        return nextChunkCallback;
    }
}
