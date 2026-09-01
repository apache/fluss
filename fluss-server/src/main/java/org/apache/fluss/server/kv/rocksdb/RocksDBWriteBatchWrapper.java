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

package org.apache.fluss.server.kv.rocksdb;

import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.exception.StorageBackpressureException;
import org.apache.fluss.metrics.Counter;
import org.apache.fluss.metrics.Histogram;
import org.apache.fluss.server.kv.KvBatchWriter;
import org.apache.fluss.utils.IOUtils;

import io.github.fluss_contrib.rocksdb.RocksDB;
import io.github.fluss_contrib.rocksdb.RocksDBException;
import io.github.fluss_contrib.rocksdb.Status;
import io.github.fluss_contrib.rocksdb.WriteBatch;
import io.github.fluss_contrib.rocksdb.WriteOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.NotThreadSafe;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.fluss.utils.Preconditions.checkArgument;

/* This file is based on source code of Apache Flink Project (https://flink.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/** It's a wrapper class around RocksDB's {@link WriteBatch} for writing in bulk. */
@NotThreadSafe
public class RocksDBWriteBatchWrapper implements KvBatchWriter {

    private static final Logger LOG = LoggerFactory.getLogger(RocksDBWriteBatchWrapper.class);

    // set max try times to 10;
    private static final int MAX_TRY_TIMES = 10;

    // the parameter is from Flink, we just keep it same as Flink currently.
    private static final int PER_RECORD_BYTES = 100;

    private final RocksDB db;
    private final WriteBatch batch;

    @VisibleForTesting final WriteOptions options;
    // we hard code it to 500 just like Flink,
    // and according to the doc of rocksdb, it's best practice to set it to hundreds of keys
    private final int capacity = 500;

    @Nonnegative private final long batchSize;

    /** List of all objects that we need to close in close(). */
    private final List<AutoCloseable> toClose;

    /** Metrics for flush operations. */
    private final Counter flushCount;

    private final Histogram flushLatencyHistogram;

    private final boolean noSlowdown;

    /**
     * When {@code false}, {@link #put} and {@link #delete} never trigger an implicit flush; the
     * caller owns all native write boundaries via explicit {@link #flush()} calls. Used by the
     * asynchronous KV flush so that exactly the entries of one prepared segment form one atomic
     * native write.
     */
    private final boolean autoFlush;

    private final Runnable backpressureRejectionCallback;

    private boolean flushFailed;

    public RocksDBWriteBatchWrapper(
            @Nonnull RocksDB rocksDB,
            long batchSize,
            Counter flushCount,
            Histogram flushLatencyHistogram) {
        this(rocksDB, batchSize, flushCount, flushLatencyHistogram, false, true, () -> {});
    }

    RocksDBWriteBatchWrapper(
            @Nonnull RocksDB rocksDB,
            long batchSize,
            Counter flushCount,
            Histogram flushLatencyHistogram,
            boolean noSlowdown,
            boolean autoFlush,
            Runnable backpressureRejectionCallback) {
        checkArgument(batchSize >= 0, "Max batch size have to be no negative.");
        this.db = rocksDB;
        this.batchSize = batchSize;
        this.flushCount = flushCount;
        this.flushLatencyHistogram = flushLatencyHistogram;
        this.noSlowdown = noSlowdown;
        this.autoFlush = autoFlush;
        this.backpressureRejectionCallback = backpressureRejectionCallback;
        this.toClose = new ArrayList<>(2);
        if (this.batchSize > 0) {
            this.batch =
                    new WriteBatch(
                            (int) Math.min(this.batchSize, this.capacity * PER_RECORD_BYTES));
        } else {
            this.batch = new WriteBatch(this.capacity * PER_RECORD_BYTES);
        }
        this.toClose.add(this.batch);
        this.options = new WriteOptions().setDisableWAL(true).setNoSlowdown(noSlowdown);
        // We own this object, so we must ensure that we close it.
        this.toClose.add(this.options);
    }

    public void put(@Nonnull byte[] key, @Nonnull byte[] value) throws IOException {
        try {
            batch.put(key, value);
            flushIfNeeded();
        } catch (RocksDBException e) {
            // Mark the batch as failed so close() does not flush the residual batch of an
            // aborted write attempt behind the caller's back.
            flushFailed = true;
            throw new IOException("Failed to put key-value pair to RocksDB.", e);
        }
    }

    public void delete(@Nonnull byte[] key) throws IOException {
        try {
            batch.delete(key);
            flushIfNeeded();
        } catch (RocksDBException e) {
            // Mark the batch as failed so close() does not flush the residual batch of an
            // aborted write attempt behind the caller's back.
            flushFailed = true;
            throw new IOException("Failed to remove key from RocksDB.", e);
        }
    }

    public void flush() throws IOException {
        long start = System.nanoTime();
        Exception lastException = null;
        for (int tryTime = 0; tryTime < MAX_TRY_TIMES; tryTime++) {
            try {
                db.write(options, batch);
                batch.clear();
                flushCount.inc();
                flushLatencyHistogram.update((System.nanoTime() - start) / 1_000_000);
                return;
            } catch (RocksDBException e) {
                if (noSlowdown && isNoSlowdownRejection(e)) {
                    flushFailed = true;
                    backpressureRejectionCallback.run();
                    throw new StorageBackpressureException(
                            "RocksDB rejected a KV flush because writes are delayed or stalled.");
                }
                lastException = e;
                // retry
                LOG.warn("Failed to flush RocksDB, try time is {}, retrying.", tryTime, e);
            }
        }
        flushFailed = true;
        throw new IOException(
                "Failed to flush to RocksDB after retrying " + MAX_TRY_TIMES + " times.",
                lastException);
    }

    private boolean isNoSlowdownRejection(RocksDBException e) {
        Status status = e.getStatus();
        if (status == null) {
            return false;
        }
        Status.Code code = status.getCode();
        return code == Status.Code.Incomplete
                || code == Status.Code.Busy
                || code == Status.Code.TryAgain;
    }

    private void flushIfNeeded() throws IOException {
        if (!autoFlush) {
            return;
        }
        boolean needFlush =
                batch.count() == capacity || (batchSize > 0 && batch.getDataSize() >= batchSize);
        if (needFlush) {
            flush();
        }
    }

    @Override
    public void close() throws IOException {
        try {
            // Skipping the final flush after a failed flush is safe: flush() has already thrown
            // (flushFailed == true), so the caller sees the original exception, discards this
            // writer and re-processes the whole write batch from its own source (e.g. the KV
            // pre-write buffer) with a new writer. Attempting to flush the residual batch here
            // would likely fail again and only mask the original exception, since close() runs
            // during the exception unwind of try-with-resources.
            //
            // In non-autoFlush mode the caller owns all native write boundaries, so a residual
            // batch means the caller aborted; it must not be written behind the caller's back.
            if (autoFlush && !flushFailed && batch.count() != 0) {
                flush();
            }
        } finally {
            IOUtils.closeAllQuietly(toClose);
        }
    }
}
