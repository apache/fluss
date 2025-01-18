/*
 * Copyright (c) 2024 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.client.write;

import com.alibaba.fluss.annotation.Internal;
import com.alibaba.fluss.memory.MemorySegment;
import com.alibaba.fluss.memory.MemorySegmentPool;
import com.alibaba.fluss.metadata.PhysicalTablePath;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.record.LogRecordBatch;
import com.alibaba.fluss.record.bytesview.BytesView;
import com.alibaba.fluss.utils.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/** The abstract write batch contains write callback object to wait write request feedback. */
@Internal
public abstract class WriteBatch {
    private static final Logger LOG = LoggerFactory.getLogger(WriteBatch.class);

    private final long createdMs;
    private final PhysicalTablePath physicalTablePath;
    private final TableBucket tableBucket;
    private final RequestFuture requestFuture;

    protected final List<WriteCallback> callbacks = new ArrayList<>();
    private final AtomicReference<FinalState> finalState = new AtomicReference<>(null);
    private final AtomicInteger attempts = new AtomicInteger(0);
    protected boolean reopened;
    protected int recordCount;
    private long drainedMs;

    public WriteBatch(TableBucket tableBucket, PhysicalTablePath physicalTablePath) {
        this.physicalTablePath = physicalTablePath;
        this.createdMs = System.currentTimeMillis();
        this.tableBucket = tableBucket;
        this.requestFuture = new RequestFuture();
        this.recordCount = 0;
    }

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

    public boolean hasBatchSequence() {
        return batchSequence() != LogRecordBatch.NO_BATCH_SEQUENCE;
    }

    public void resetWriterState(long writerId, int batchSequence) {
        LOG.info(
                "Resetting batch sequence of batch with current batch sequence {} for table bucket {} to {}",
                batchSequence(),
                tableBucket,
                batchSequence);
        reopened = true;
    }

    public boolean sequenceHasBeenReset() {
        return reopened;
    }

    public TableBucket tableBucket() {
        return tableBucket;
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
        Preconditions.checkNotNull(exception);
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
                                "Error executing user-provided callback on message for table-bucket '{}'",
                                tableBucket,
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
            LOG.trace("Successfully produced messages to {}.", tableBucket);
        } else {
            LOG.trace("Failed to produce messages to {}.", tableBucket, batchException);
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
                        tableBucket,
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
        SUCCEEDED
    }

    /** The type of write batch. */
    public enum WriteBatchType {
        ARROW_LOG,
        INDEXED_LOG,
        KV
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
