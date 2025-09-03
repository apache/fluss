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

package org.apache.fluss.lake.lance.tiering;

import org.apache.fluss.record.LogRecord;
import org.apache.fluss.types.RowType;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.IOException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** A custom arrow reader that supports writes Fluss internal rows while reading data in batches. */
public class LanceArrowWriter extends ArrowReader {
    private final Schema schema;
    private final RowType rowType;
    private final int batchSize;

    private volatile boolean finished;

    private final AtomicLong totalBytesRead = new AtomicLong();
    private ArrowWriter arrowWriter = null;
    private final AtomicInteger count = new AtomicInteger(0);
    private final Semaphore writeToken;
    private final Semaphore loadToken;

    public LanceArrowWriter(
            BufferAllocator allocator, Schema schema, int batchSize, RowType rowType) {
        super(allocator);
        checkNotNull(schema);
        checkArgument(batchSize > 0);
        this.schema = schema;
        this.rowType = rowType;
        this.batchSize = batchSize;
        this.writeToken = new Semaphore(0);
        this.loadToken = new Semaphore(0);
    }

    void write(LogRecord row) {
        checkNotNull(row);
        try {
            // wait util prepareLoadNextBatch to release write token,
            writeToken.acquire();
            arrowWriter.writeRow(row.getRow());
            if (count.incrementAndGet() == batchSize) {
                // notify loadNextBatch to take the batch
                loadToken.release();
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    void setFinished() {
        loadToken.release();
        finished = true;
    }

    @Override
    public void prepareLoadNextBatch() throws IOException {
        super.prepareLoadNextBatch();
        arrowWriter = ArrowWriter.create(this.getVectorSchemaRoot(), rowType);
        // release batch size token for write
        writeToken.release(batchSize);
    }

    @Override
    public boolean loadNextBatch() throws IOException {
        prepareLoadNextBatch();
        try {
            if (finished && count.get() == 0) {
                return false;
            }
            // wait util batch if full or finished
            loadToken.acquire();
            arrowWriter.finish();
            if (!finished) {
                count.set(0);
                return true;
            } else {
                // true if it has some rows and return false if there is no record
                if (count.get() > 0) {
                    count.set(0);
                    return true;
                } else {
                    return false;
                }
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public long bytesRead() {
        throw new UnsupportedOperationException();
    }

    @Override
    protected synchronized void closeReadSource() throws IOException {
        // Implement if needed
    }

    @Override
    protected Schema readSchema() {
        return this.schema;
    }
}
