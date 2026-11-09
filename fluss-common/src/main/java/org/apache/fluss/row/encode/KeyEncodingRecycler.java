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

package org.apache.fluss.row.encode;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.row.InternalRow;

import javax.annotation.concurrent.ThreadSafe;

import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.function.ToIntFunction;

import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

/**
 * Reuses mutable key encoding writers with a fixed number of retained instances.
 *
 * <p>A borrowed writer is owned by one {@link KeyEncoder#encodeKey(InternalRow)} call until it is
 * recycled. Borrowing and recycling do not wait: borrowing creates a new writer when no retained
 * writer is available, and recycling drops the writer when the retained slots are full.
 *
 * <p>Writers are reset before being retained for reuse and must not be accessed after they are
 * recycled.
 *
 * <p>Concurrency: {@link #borrow()} and {@link #recycle(Object)} may be called from any thread, and
 * a borrowed writer is exclusively owned by its borrower until recycled. That is what allows a
 * single {@link KeyEncoder} instance to be shared by concurrent callers; replacing this pool with
 * one reusable writer per encoder would silently reintroduce cross-thread corruption of that shared
 * writer.
 */
@Internal
@ThreadSafe
public final class KeyEncodingRecycler<W> {

    private static final int DEFAULT_RETAINED_WRITERS = 4;
    private static final int DEFAULT_MAX_RETAINED_SIZE_IN_BYTES = 64 * 1024;

    private final AtomicReferenceArray<W> writers;
    private final Supplier<W> writerFactory;
    private final Consumer<W> writerResetter;
    private final ToIntFunction<W> retainedSizeGetter;
    private final int maxRetainedSizeInBytes;

    /**
     * Creates a key encoding writer recycler with default retention limits.
     *
     * @param writerFactory factory used when no retained writer is available
     * @param writerResetter reset action before retaining a recycled writer
     * @param retainedSizeGetter returns the retained size of a writer in bytes
     */
    public KeyEncodingRecycler(
            Supplier<W> writerFactory,
            Consumer<W> writerResetter,
            ToIntFunction<W> retainedSizeGetter) {
        this(
                writerFactory,
                writerResetter,
                retainedSizeGetter,
                DEFAULT_RETAINED_WRITERS,
                DEFAULT_MAX_RETAINED_SIZE_IN_BYTES);
    }

    @VisibleForTesting
    KeyEncodingRecycler(
            Supplier<W> writerFactory,
            Consumer<W> writerResetter,
            ToIntFunction<W> retainedSizeGetter,
            int retainedWriters,
            int maxRetainedSizeInBytes) {
        checkArgument(retainedWriters > 0, "Retained writers must be positive.");
        checkArgument(
                maxRetainedSizeInBytes >= 0, "Max retained size in bytes must be non-negative.");
        this.writerFactory = checkNotNull(writerFactory, "Writer factory must not be null.");
        this.writerResetter = checkNotNull(writerResetter, "Writer resetter must not be null.");
        this.retainedSizeGetter =
                checkNotNull(retainedSizeGetter, "Retained size getter must not be null.");
        this.writers = new AtomicReferenceArray<>(retainedWriters);
        this.maxRetainedSizeInBytes = maxRetainedSizeInBytes;
    }

    /**
     * Borrows a writer for one key encoding call.
     *
     * @return a writer that is not retained by this recycler until it is recycled
     */
    public W borrow() {
        for (int i = 0; i < writers.length(); i++) {
            W writer = writers.getAndSet(i, null);
            if (writer != null) {
                return writer;
            }
        }
        return writerFactory.get();
    }

    /**
     * Recycles a writer after a key encoding call.
     *
     * @param writer the writer to recycle
     */
    public void recycle(W writer) {
        W nonNullWriter = checkNotNull(writer, "Writer to recycle must not be null.");
        if (retainedSizeGetter.applyAsInt(nonNullWriter) > maxRetainedSizeInBytes) {
            return;
        }

        boolean reset = false;
        for (int i = 0; i < writers.length(); i++) {
            if (writers.get(i) != null) {
                continue;
            }
            if (!reset) {
                writerResetter.accept(nonNullWriter);
                reset = true;
            }
            if (writers.compareAndSet(i, null, nonNullWriter)) {
                return;
            }
        }
    }
}
