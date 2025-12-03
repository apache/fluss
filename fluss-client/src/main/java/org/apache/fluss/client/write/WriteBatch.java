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

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.memory.MemorySegment;
import org.apache.fluss.memory.MemorySegmentPool;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.record.bytesview.BytesView;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.fluss.record.LogRecordBatchFormat.NO_BATCH_SEQUENCE;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** The abstract write batch contains write callback object to wait write request feedback. */
@Internal
public abstract class WriteBatch {
    private static final Logger LOG = LoggerFactory.getLogger(WriteBatch.class);

    private final long createdMs;
    private final PhysicalTablePath physicalTablePath;
    private final RequestFuture requestFuture;
    private final int bucketId;

    protected final List<WriteCallback> callbacks = new ArrayList<>();
    private final AtomicReference<FinalState> finalState = new AtomicReference<>(null);
    private final AtomicInteger attempts = new AtomicInteger(0);
    protected boolean reopened;
    protected int recordCount;
    private long drainedMs;

    public WriteBatch(int bucketId, PhysicalTablePath physicalTablePath, long createdMs) {
        this.physicalTablePath = physicalTablePath;
        this.createdMs = createdMs;
        this.bucketId = bucketId;
        this.requestFuture = new RequestFuture();
        this.recordCount = 0;
    }

    /**
     * Check if the batch is log batch, e.g., ArrowLogBatch or IndexedLogBatch, and should use
     * ProduceLog request. Otherwise, it is a kv batch, and should use PutKv request.
     *
     * @return true if log batch, false if kv batch
     */
    public abstract boolean isLogBatch();

    /**
     * try to append one write record to the record batch.
     *
     * @param writeRecord the record to write
     * @param callback the callback to send back to writer
     * @return true if append success, false if the batch is full.
     */
    public abstract boolean tryAppend(WriteRecord writeRecord, WriteCallback callback)
            throws Exception;

    /**
     * Gets the memory segment bytes view of the batch. This includes the latest updated {@link
     * #setWriterState(long, int)} in the bytes view.
     */
    public abstract BytesView build();

    /** close the batch to not append new records. */
    public abstract void close() throws Exception;

    /**
     * check if the batch is closed.
     *
     * @return true if closed, false otherwise
     */
    public abstract boolean isClosed();

    /**
     * Get an estimate of the number of bytes written to the underlying buffer. The returned value
     * is exactly correct if the record set is not compressed or if the batch has been {@link
     * #build()}.
     */
    public abstract int estimatedSizeInBytes();

    /**
     * get pooled memory segments to de-allocate. After produceLog/PutKv acks, the {@link
     * WriteBatch} need to de-allocate the allocated pooled {@link MemorySegment}s back to {@link
     * MemorySegmentPool} for reusing.
     *
     * @return the pooled memory segment this batch allocated
     */
    public abstract List<MemorySegment> pooledMemorySegments();

    public abstract void setWriterState(long writerId, int batchSequence);

    public abstract long writerId();

    public abstract int batchSequence();

    public abstract void abortRecordAppends();

    public boolean hasBatchSequence() {
        return batchSequence() != NO_BATCH_SEQUENCE;
    }

    public void resetWriterState(long writerId, int batchSequence) {
        LOG.info(
                "Resetting batch sequence of batch with current batch sequence {} for table path {} to {}",
                batchSequence(),
                physicalTablePath,
                batchSequence);
        reopened = true;
    }

    /** Abort the batch and complete the future and callbacks. */
    public void abort(Exception exception) {
        if (!finalState.compareAndSet(null, FinalState.ABORTED)) {
            throw new IllegalStateException(
                    "Batch has already been completed in final stata " + finalState.get());
        }

        LOG.trace(
                "Abort batch for table path {} with bucket_id {}",
                physicalTablePath,
                bucketId,
                exception);
        completeFutureAndFireCallbacks(exception);
    }

    public boolean sequenceHasBeenReset() {
        return reopened;
    }

    public int bucketId() {
        return bucketId;
    }

    public PhysicalTablePath physicalTablePath() {
        return physicalTablePath;
    }

    public RequestFuture getRequestFuture() {
        return requestFuture;
    }

    public long waitedTimeMs(long nowMs) {
        return Math.max(0, nowMs - createdMs);
    }

    public int getRecordCount() {
        return recordCount;
    }

    public long getQueueTimeMs() {
        return drainedMs - createdMs;
    }

    /** Complete the batch successfully. */
    public boolean complete() {
        return done(null);
    }

    /**
     * Complete the batch exceptionally. The provided exception will be used for each record future
     * contained in the batch.
     */
    public boolean completeExceptionally(Exception exception) {
        checkNotNull(exception);
        return done(exception);
    }

    private void completeFutureAndFireCallbacks(@Nullable Exception exception) {
        // execute callbacks.
        callbacks.forEach(
                callback -> {
                    try {
                        callback.onCompletion(exception);
                    } catch (Exception e) {
                        LOG.error(
                                "Error executing user-provided callback on message for table path '{}'",
                                physicalTablePath,
                                e);
                    }
                });
        requestFuture.done();
    }

    int attempts() {
        return attempts.get();
    }

    void reEnqueued() {
        attempts.getAndIncrement();
    }

    void drained(long nowMs) {
        this.drainedMs = Math.max(drainedMs, nowMs);
    }

    /**
     * Check if the batch has been completed (either successfully or exceptionally).
     *
     * @return `true` if the batch has been completed, `false` otherwise.
     */
    boolean isDone() {
        return finalState.get() != null;
    }

    /**
     * Finalize the state of a batch. Final state, once set, is immutable. This function may be
     * called twice when an inflight batch expires before a response from the tablet server is
     * received. The batch's final state is set to FAILED. But it could succeed on the tablet server
     * and second time around batch.done() may try to set SUCCEEDED final state.
     */
    private boolean done(@Nullable Exception batchException) {
        final FinalState tryFinalState =
                (batchException == null) ? FinalState.SUCCEEDED : FinalState.FAILED;
        if (tryFinalState == FinalState.SUCCEEDED) {
            LOG.trace("Successfully produced messages to {}.", physicalTablePath);
        } else {
            LOG.trace("Failed to produce messages to {}.", physicalTablePath, batchException);
        }

        if (finalState.compareAndSet(null, tryFinalState)) {
            completeFutureAndFireCallbacks(batchException);
            return true;
        }

        if (this.finalState.get() != FinalState.SUCCEEDED) {
            if (tryFinalState == FinalState.SUCCEEDED) {
                // Log if a previously unsuccessful batch succeeded later on.
                LOG.debug(
                        "ProduceLogResponse returned {} for {} after batch has already been {}.",
                        tryFinalState,
                        physicalTablePath,
                        finalState.get());
            } else {
                // FAILED --> FAILED transitions are ignored.
                LOG.debug(
                        "Ignore state transition {} -> {} for batch.",
                        this.finalState.get(),
                        tryFinalState);
            }
        } else {
            // A SUCCESSFUL batch must not attempt another state change.
            throw new IllegalStateException(
                    "A "
                            + this.finalState.get()
                            + " batch must not attempt another state change to "
                            + tryFinalState);
        }
        return false;
    }

    private enum FinalState {
        FAILED,
        SUCCEEDED,
        ABORTED
    }

    /** The future for this batch. */
    public static class RequestFuture {
        private final CountDownLatch latch = new CountDownLatch(1);

        /** Mark this request as complete and unblock any threads waiting on its completion. */
        public void done() {
            latch.countDown();
        }

        /** Await the completion of this request. */
        public void await() throws InterruptedException {
            latch.await();
        }
    }
}
