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
import com.alibaba.fluss.annotation.VisibleForTesting;
import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.exception.BufferExhaustedException;
import com.alibaba.fluss.exception.FlussRuntimeException;
import com.alibaba.fluss.memory.MemorySegment;
import com.alibaba.fluss.memory.MemorySegmentPool;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import static com.alibaba.fluss.utils.concurrent.LockUtils.inLock;

/* This file is based on source code of Apache Kafka Project (https://kafka.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/**
 * A pool of {@link MemorySegment} kept under a given memory limit. This class is fairly specific to
 * the needs of the {@link WriterClient}. In particular, it has the following properties:
 *
 * <ol>
 *   <li>There is a special "page size" and segments of this size are kept in freePages and
 *       recycled. The page size is configure by conf {@link
 *       ConfigOptions#CLIENT_WRITER_LEGACY_BATCH_SIZE}.
 *   <li>It is fair. That is all memory is given to the longest waiting thread until it has
 *       sufficient memory. This prevents starvation or deadlock when a thread asks for a large
 *       chunk of memory and needs to block until multiple segments are deallocated. The total pool
 *       managed memory is configured by conf {@link
 *       ConfigOptions#CLIENT_WRITER_BUFFER_MEMORY_SIZE}.
 * </ol>
 *
 * <p>Note: This class will be removed, after we uniform this class with {@link MemorySegmentPool}.
 */
@Internal
@ThreadSafe
@Deprecated
public class WriterMemoryBuffer {

    /** The maximum amount of memory that this memory segment pool can allocate. */
    private final long totalMemory;

    /** The page size of each page to cache in the freePages rather than deallocating. */
    private final int pageSize;

    /** The lock to guard the memory pool. */
    private final ReentrantLock lock;

    /** The memory segment with page size kept in the freePages rather than deallocating. */
    @GuardedBy("lock")
    private final Deque<MemorySegment> freePages;

    @GuardedBy("lock")
    private final Deque<Condition> waiters;

    /**
     * Total available memory = nonPooledAvailableMemory + (the number of byte buffers in freePages
     * * pageSize).
     */
    @GuardedBy("lock")
    private long nonPooledAvailableMemory;

    @GuardedBy("lock")
    private boolean closed;

    private volatile long waitTimeNs;

    public WriterMemoryBuffer(Configuration conf) {
        this(
                conf.get(ConfigOptions.CLIENT_WRITER_BUFFER_MEMORY_SIZE).getBytes(),
                (int) conf.get(ConfigOptions.CLIENT_WRITER_LEGACY_BATCH_SIZE).getBytes());
    }

    @VisibleForTesting
    public WriterMemoryBuffer(long memory, int pageSize) {
        this.totalMemory = memory;
        this.pageSize = pageSize;
        this.lock = new ReentrantLock();
        this.freePages = new ArrayDeque<>();
        this.waiters = new ArrayDeque<>();
        this.nonPooledAvailableMemory = totalMemory;
        this.closed = false;
    }

    /** The total memory in bytes managed by this pool. */
    public long getTotalMemory() {
        return totalMemory;
    }

    /** The page size in bytes to cache in the freePage list rather than deallocating. */
    public int getPageSize() {
        return pageSize;
    }

    /** Get the unallocated memory in bytes (not in the freePages or in use). */
    public long getUnallocatedMemory() {
        return inLock(lock, () -> nonPooledAvailableMemory);
    }

    /** the total free memory in bytes both unallocated and in the freePages. */
    public long getAvailableMemory() {
        return inLock(lock, () -> nonPooledAvailableMemory + numOfFreePages() * (long) pageSize);
    }

    public long getWaitTimeMs() {
        return TimeUnit.NANOSECONDS.toMillis(waitTimeNs);
    }

    /**
     * Allocate a memory segment of the given size. This method blocks if there is not enough memory
     * and the memory segment pool is configured with blocking mode.
     *
     * @param size The buffer size to allocate in bytes.
     * @param maxTimeToBlockMs The maximum time in milliseconds to block for buffer memory to be
     *     available.
     * @return the allocated memory segment.
     */
    public MemorySegment allocate(int size, long maxTimeToBlockMs) throws InterruptedException {
        if (size > totalMemory) {
            throw new IllegalArgumentException(
                    "Attempt to allocate memory segment size "
                            + size
                            + " is larger than the memory total size of client: "
                            + totalMemory);
        }

        MemorySegment memorySegment = null;
        lock.lock();
        if (closed) {
            lock.unlock();
            throw new FlussRuntimeException("Memory segment pool closed while allocating memory");
        }

        try {
            // check if we have a free memory segment of the right size pooled.
            if (size == pageSize && !freePages.isEmpty()) {
                return freePages.pollFirst();
            }

            // now check if the request is immediately satisfiable with the memory on hand as
            // nonPooledAvailableMemory or if we need to block.
            int pooledAvailableMemory = numOfFreePages() * pageSize;
            if (nonPooledAvailableMemory + pooledAvailableMemory >= size) {
                // we have enough unallocated or pooled memory to immediately satisfy the request,
                // but need to allocate the segment.
                freeUp(size);
                nonPooledAvailableMemory -= size;
            } else {
                // Out of memory and will have to block.
                int accumulated = 0;
                Condition moreMemory = lock.newCondition();
                try {
                    long remainingTimeToBlockNs = TimeUnit.MILLISECONDS.toNanos(maxTimeToBlockMs);
                    this.waiters.addLast(moreMemory);
                    // loop over and over until we have a memory segment or have reserved enough
                    // memory to allocate one.
                    while (accumulated < size) {
                        long startWaitNs = System.nanoTime();
                        long timeNs;
                        boolean waitingTimeElapsed;
                        try {
                            waitingTimeElapsed =
                                    !moreMemory.await(remainingTimeToBlockNs, TimeUnit.NANOSECONDS);
                        } finally {
                            long endWaitNs = System.nanoTime();
                            timeNs = Math.max(0L, endWaitNs - startWaitNs);
                            waitTimeNs = timeNs;
                        }

                        if (closed) {
                            throw new FlussRuntimeException(
                                    "Memory segment pool closed while allocating memory");
                        }

                        if (waitingTimeElapsed) {
                            throw new BufferExhaustedException(
                                    "Failed to allocate "
                                            + size
                                            + " bytes within the configured max blocking time "
                                            + maxTimeToBlockMs
                                            + " ms. Total memory: "
                                            + getTotalMemory()
                                            + " bytes. Available memory: "
                                            + getAvailableMemory()
                                            + " bytes. page size: "
                                            + getPageSize()
                                            + " bytes");
                        }

                        remainingTimeToBlockNs -= timeNs;

                        // check if we can satisfy this request from the freePages, otherwise
                        // allocate memory.
                        if (accumulated == 0 && size == pageSize && !freePages.isEmpty()) {
                            // just grab a memory from the freePages.
                            memorySegment = freePages.pollFirst();
                            accumulated = size;
                        } else {
                            // we'll need to allocate memory, but we may only get part of what we
                            // need on this iteration.
                            freeUp(size - accumulated);
                            int got = (int) Math.min(size - accumulated, nonPooledAvailableMemory);
                            nonPooledAvailableMemory -= got;
                            accumulated += got;
                        }
                    }

                    // Don't reclaim memory on throwable since nothing was thrown.
                    accumulated = 0;
                } finally {
                    // When this loop was not able to successfully terminate don't lose available
                    // memory.
                    nonPooledAvailableMemory += accumulated;
                    waiters.remove(moreMemory);
                }
            }

        } finally {
            // signal any additional waiters if there is more memory left over for them
            try {
                if (!(nonPooledAvailableMemory == 0 && freePages.isEmpty()) && !waiters.isEmpty()) {
                    waiters.peekFirst().signal();
                }
            } finally {
                // Another finally... otherwise find bugs complains
                lock.unlock();
            }
        }

        if (memorySegment == null) {
            return safeAllocateMemorySegment(size);
        } else {
            return memorySegment;
        }
    }

    /**
     * Return segments to the memory segment pool. If they are of the page size add them to the
     * freePages, otherwise just mark the memory as free.
     */
    public void deallocate(MemorySegment memorySegment, int size) {
        inLock(
                lock,
                () -> {
                    if (size == pageSize && size == memorySegment.size()) {
                        // do nothing, return memory segment.
                        freePages.add(memorySegment);
                    } else {
                        // mark as free.
                        memorySegment.free();
                        nonPooledAvailableMemory += size;
                    }

                    Condition moreMem = waiters.peekFirst();
                    if (moreMem != null) {
                        moreMem.signal();
                    }
                });
    }

    void deallocate(MemorySegment memorySegment) {
        deallocate(memorySegment, memorySegment.size());
    }

    public int queued() {
        return inLock(lock, waiters::size);
    }

    @VisibleForTesting
    protected int numOfFreePages() {
        return freePages.size();
    }

    @VisibleForTesting
    protected MemorySegment allocateMemorySegment(int size) {
        return MemorySegment.allocateHeapMemory(size);
    }

    /**
     * Allocate a {@link MemorySegment}. If allocate fails (e.g. because of OOM) then return the
     * size count back to available memory and signal the next waiter if it exists.
     */
    private MemorySegment safeAllocateMemorySegment(int size) {
        boolean error = true;
        try {
            MemorySegment segment = allocateMemorySegment(size);
            error = false;
            return segment;
        } finally {
            if (error) {
                inLock(
                        lock,
                        () -> {
                            nonPooledAvailableMemory += size;
                            if (!waiters.isEmpty()) {
                                waiters.peekFirst().signal();
                            }
                        });
            }
        }
    }

    /**
     * Attempt to ensure we have at least the requested number of bytes of memory for allocation by
     * deallocating pooled memory segments (if needed).
     */
    private void freeUp(int size) {
        while (!freePages.isEmpty() && nonPooledAvailableMemory < size) {
            nonPooledAvailableMemory += freePages.pollLast().size();
        }
    }

    public void close() {
        inLock(
                lock,
                () -> {
                    closed = true;
                    for (Condition waiter : waiters) {
                        waiter.signal();
                    }
                });
    }

    @VisibleForTesting
    Deque<Condition> waiters() {
        return this.waiters;
    }
}
