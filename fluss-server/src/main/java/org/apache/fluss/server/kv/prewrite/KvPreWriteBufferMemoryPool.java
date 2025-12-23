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

package org.apache.fluss.server.kv.prewrite;

import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.PreWriteBufferFullException;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.utils.MapUtils;

import javax.annotation.concurrent.GuardedBy;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static org.apache.fluss.utils.concurrent.LockUtils.inLock;

/** a shared off-heap memory pool for {@link KvPreWriteBuffer} of different tableBucket. */
public class KvPreWriteBufferMemoryPool {

    /** The maximum memory size for this memory pool. */
    private final long maxMemorySize;

    /** The maximum memory size for this memory pool per bucket. */
    private final long maxMemorySizePerBucket;

    private final Lock lock = new ReentrantLock();

    /** The total used memory size. */
    @GuardedBy("lock")
    private final AtomicLong totalUsedBytes = new AtomicLong(0);

    /** Memory allocated per bucket. */
    @GuardedBy("lock")
    private final Map<TableBucket, Long> allocatedBufferForBucket = MapUtils.newConcurrentHashMap();

    public KvPreWriteBufferMemoryPool(Configuration conf) {
        this(
                conf.get(ConfigOptions.KV_PRE_WRITE_BUFFER_MEMORY_POOL_SIZE).getBytes(),
                conf.get(ConfigOptions.KV_PRE_WRITE_BUFFER_MEMORY_POOL_SIZ_PER_BUCKET).getBytes());
    }

    @VisibleForTesting
    public KvPreWriteBufferMemoryPool(long maxMemorySize, long maxMemorySizePerBucket) {
        this.maxMemorySize = maxMemorySize;
        this.maxMemorySizePerBucket = maxMemorySizePerBucket;
    }

    public long getTotalUsed() {
        return totalUsedBytes.get();
    }

    public long getPerBucketUsage(TableBucket bucket) {
        return allocatedBufferForBucket.getOrDefault(bucket, 0L);
    }

    public long getMaxMemorySize() {
        return maxMemorySize;
    }

    public long getMaxMemorySizePerBucket() {
        return maxMemorySizePerBucket;
    }

    /**
     * Allocate a buffer and copy the input value into the new buffer.
     *
     * @param bucket the bucket of the table.
     * @param value the value to copy.
     * @return the new buffer.
     */
    ByteBuffer allocate(TableBucket bucket, ByteBuffer value) {
        if (value == null) {
            throw new NullPointerException("The input buffer buffer cannot be null");
        }
        int size = value.remaining();
        ByteBuffer newBuffer = allocate(bucket, size);
        newBuffer.put(value.duplicate());
        newBuffer.flip();
        return newBuffer;
    }

    /**
     * Allocate a buffer and copy the input value into the new buffer.
     *
     * @param bucket the bucket of the table.
     * @param value the value to copy.
     * @return the new buffer.
     */
    public ByteBuffer allocate(TableBucket bucket, byte[] value) {
        if (value == null) {
            throw new NullPointerException("The input buffer buffer cannot be null");
        }
        return allocate(bucket, ByteBuffer.wrap(value));
    }

    /**
     * Allocate a buffer with the given size.
     *
     * @param bucket the bucket of the table.
     * @param size the size of the buffer.
     * @return the new buffer.
     */
    private ByteBuffer allocate(TableBucket bucket, int size) {
        if (size <= 0) {
            throw new IllegalArgumentException("Allocation size must be > 0");
        }

        inLock(
                lock,
                () -> {
                    long currentBucketUsage = allocatedBufferForBucket.getOrDefault(bucket, 0L);

                    // check global memory limit.
                    long currentTotal = totalUsedBytes.get();
                    long newTotal = currentTotal + size;

                    if (newTotal > maxMemorySize) {
                        throw new PreWriteBufferFullException(
                                "Global PreWriteBuffer memory pool exhausted. Requested: "
                                        + size
                                        + " bytes, Total used: "
                                        + currentTotal
                                        + ", Limit: "
                                        + maxMemorySize);
                    }

                    // check per bucket memory limit.
                    long newBucketUsage = currentBucketUsage + size;
                    if (newBucketUsage > maxMemorySizePerBucket) {
                        throw new PreWriteBufferFullException(
                                "PreWriteBuffer memory limit exceeded for table bucket: "
                                        + bucket
                                        + ", Current: "
                                        + currentBucketUsage
                                        + ", Request: "
                                        + size
                                        + ", Limit: "
                                        + maxMemorySizePerBucket);
                    }

                    allocatedBufferForBucket.put(bucket, newBucketUsage);
                    totalUsedBytes.addAndGet(size);
                });

        // TODO Maybe we can cache small buffers to avoid frequent allocation and release.
        // Allocate a new buffer lazily.
        return ByteBuffer.allocateDirect(size);
    }

    /**
     * Release the buffer.
     *
     * @param bucket the bucket of the table.
     * @param buffer the buffer to release.
     */
    public void release(TableBucket bucket, ByteBuffer buffer) {
        if (buffer == null || !buffer.isDirect()) {
            return;
        }

        int size = buffer.capacity();
        inLock(
                lock,
                () -> {
                    Long currentBucketUsage = allocatedBufferForBucket.get(bucket);
                    if (currentBucketUsage == null || currentBucketUsage < size) {
                        throw new IllegalStateException(
                                "Release more than allocated for bucket: " + bucket);
                    }

                    long newUsage = currentBucketUsage - size;
                    if (newUsage <= 0) {
                        allocatedBufferForBucket.remove(bucket);
                    } else {
                        allocatedBufferForBucket.put(bucket, newUsage);
                    }

                    totalUsedBytes.addAndGet(-size);
                });
    }

    public void close() {
        // do nothing now.
    }
}
