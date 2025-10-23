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

import org.apache.fluss.server.kv.KvBatchWriter;
import org.apache.fluss.utils.IOUtils;

import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;
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
    private final WriteOptions options;
    // we hard code it to 500 just like Flink,
    // and according to the doc of rocksdb, it's best practice to set it to hundreds of keys
    private final int capacity = 500;

    @Nonnegative private final long batchSize;

    /** List of all objects that we need to close in close(). */
    private final List<AutoCloseable> toClose;

    public RocksDBWriteBatchWrapper(@Nonnull RocksDB rocksDB, long batchSize) {
        checkArgument(batchSize >= 0, "Max batch size have to be no negative.");
        this.db = rocksDB;
        this.batchSize = batchSize;
        this.toClose = new ArrayList<>(2);
        if (this.batchSize > 0) {
            this.batch =
                    new WriteBatch(
                            (int) Math.min(this.batchSize, this.capacity * PER_RECORD_BYTES));
        } else {
            this.batch = new WriteBatch(this.capacity * PER_RECORD_BYTES);
        }
        this.toClose.add(this.batch);
        // Use default write options with disabled WAL
        this.options = new WriteOptions().setDisableWAL(true);
        // We own this object, so we must ensure that we close it.
        this.toClose.add(this.options);
    }

    public void put(@Nonnull byte[] key, @Nonnull byte[] value) throws IOException {
        try {
            batch.put(key, value);
            flushIfNeeded();
        } catch (RocksDBException e) {
            throw new IOException("Failed to put key-value pair to RocksDB.", e);
        }
    }

    public void delete(@Nonnull byte[] key) throws IOException {
        try {
            batch.delete(key);
            flushIfNeeded();
        } catch (RocksDBException e) {
            throw new IOException("Failed to remove key from RocksDB.", e);
        }
    }

    public void flush() throws IOException {
        Exception lastException = null;
        for (int tryTime = 0; tryTime < MAX_TRY_TIMES; tryTime++) {
            try {
                db.write(options, batch);
                batch.clear();
                return;
            } catch (RocksDBException e) {
                lastException = e;
                // retry
                LOG.warn("Failed to flush RocksDB, try time is {}, retrying.", tryTime, e);
            }
        }
        throw new IOException(
                "Failed to flush to RocksDB after retrying " + MAX_TRY_TIMES + " times.",
                lastException);
    }

    private void flushIfNeeded() throws IOException {
        boolean needFlush =
                batch.count() == capacity || (batchSize > 0 && batch.getDataSize() >= batchSize);
        if (needFlush) {
            flush();
        }
    }

    @Override
    public void close() throws IOException {
        try {
            if (batch.count() != 0) {
                flush();
            }
        } finally {
            IOUtils.closeAllQuietly(toClose);
        }
    }
}
