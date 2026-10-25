/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.kafka.transcode;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.record.MemoryLogRecords;
import org.apache.fluss.server.kv.wal.ArrowWalBuilder;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.fluss.utils.Preconditions.checkState;

/**
 * Encoded log records and the pooled memory retained until their append completes.
 *
 * <p>The caller must invoke {@link #deallocate()} after the log append completes or fails. For a
 * synchronous append, use try-with-resources; for an asynchronous append, release in its completion
 * callback. Release is idempotent and may run on another thread, but must not race with reads or
 * writes of the records. The shared memory segment pool must remain open until all results have
 * been released.
 */
@Internal
public final class EncodedRecords implements AutoCloseable {

    private final MemoryLogRecords records;
    private final ArrowWalBuilder builder;
    private final AtomicBoolean deallocated = new AtomicBoolean();

    EncodedRecords(MemoryLogRecords records, ArrowWalBuilder builder) {
        this.records = records;
        this.builder = builder;
    }

    /** Returns the records to append; their storage remains valid until {@link #deallocate()}. */
    public MemoryLogRecords records() {
        checkState(!deallocated.get(), "Encoded records have been deallocated.");
        return records;
    }

    /** Returns pooled memory after the append completes, at most once. */
    public void deallocate() {
        if (deallocated.compareAndSet(false, true)) {
            builder.deallocate();
        }
    }

    @Override
    public void close() {
        deallocate();
    }
}
