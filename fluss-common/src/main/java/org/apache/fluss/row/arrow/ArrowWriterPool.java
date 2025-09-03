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

package org.apache.fluss.row.arrow;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.compression.ArrowCompressionInfo;
import org.apache.fluss.compression.ArrowCompressionRatioEstimator;
import org.apache.fluss.exception.FlussRuntimeException;
import org.apache.fluss.shaded.arrow.org.apache.arrow.memory.BufferAllocator;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.fluss.types.RowType;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

import static org.apache.fluss.utils.concurrent.LockUtils.inLock;

/**
 * A pool that pools {@link ArrowWriter}. See Javadoc of {@link VectorSchemaRoot} for more
 * information about pooling.
 */
@ThreadSafe
@Internal
public class ArrowWriterPool implements ArrowWriterProvider {

    private final BufferAllocator allocator;

    @GuardedBy("lock")
    private final Map<String, Deque<ArrowWriter>> freeWriters;

    @GuardedBy("lock")
    private boolean closed = false;

    @GuardedBy("lock")
    private final Map<String, ArrowCompressionRatioEstimator> compressionRatioEstimators;

    private final ReentrantLock lock = new ReentrantLock();

    public ArrowWriterPool(BufferAllocator allocator) {
        this.allocator = allocator;
        this.freeWriters = new HashMap<>();
        this.compressionRatioEstimators = new HashMap<>();
    }

    @Override
    public void recycleWriter(ArrowWriter writer) {
        inLock(
                lock,
                () -> {
                    if (closed) {
                        // close the vector schema root in place
                        writer.root.close();
                    } else {
                        Deque<ArrowWriter> roots =
                                freeWriters.computeIfAbsent(
                                        writer.writerKey, k -> new ArrayDeque<>());
                        writer.increaseEpoch();
                        roots.add(writer);
                    }
                });
    }

    @Override
    public ArrowWriter getOrCreateWriter(
            long tableId,
            int schemaId,
            int bufferSizeInBytes,
            RowType schema,
            ArrowCompressionInfo compressionInfo) {
        final String writerKey = tableId + "-" + schemaId + "-" + compressionInfo.toString();
        return inLock(
                lock,
                () -> {
                    if (closed) {
                        throw new FlussRuntimeException(
                                "Arrow VectorSchemaRoot pool closed while getting/creating root.");
                    }
                    Deque<ArrowWriter> writers = freeWriters.get(writerKey);
                    ArrowCompressionRatioEstimator compressionRatioEstimator =
                            compressionRatioEstimators.computeIfAbsent(
                                    writerKey, k -> new ArrowCompressionRatioEstimator());
                    if (writers != null && !writers.isEmpty()) {
                        return initialize(writers.pollFirst(), bufferSizeInBytes);
                    } else {
                        return initialize(
                                new ArrowWriter(
                                        writerKey,
                                        bufferSizeInBytes,
                                        schema,
                                        allocator,
                                        this,
                                        compressionInfo,
                                        compressionRatioEstimator),
                                bufferSizeInBytes);
                    }
                });
    }

    private ArrowWriter initialize(ArrowWriter writer, int bufferSizeInBytes) {
        writer.reset(bufferSizeInBytes);
        return writer;
    }

    @Override
    public void close() {
        lock.lock();
        try {
            for (Deque<ArrowWriter> writers : freeWriters.values()) {
                for (ArrowWriter writer : writers) {
                    writer.root.close();
                }
            }
            freeWriters.clear();
            compressionRatioEstimators.clear();
            closed = true;
        } finally {
            lock.unlock();
        }
    }

    @VisibleForTesting
    public Map<String, Deque<ArrowWriter>> freeWriters() {
        return freeWriters;
    }
}
