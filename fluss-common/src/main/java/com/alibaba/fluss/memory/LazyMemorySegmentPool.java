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

package com.alibaba.fluss.memory;

import com.alibaba.fluss.annotation.Internal;
import com.alibaba.fluss.annotation.VisibleForTesting;
import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.exception.FlussRuntimeException;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import static com.alibaba.fluss.utils.Preconditions.checkArgument;
import static com.alibaba.fluss.utils.concurrent.LockUtils.inLock;

/** MemorySegment pool of a MemorySegment list. */
@Internal
@ThreadSafe
public class LazyMemorySegmentPool implements MemorySegmentPool, Closeable {

    /** The lock to guard the memory pool. */
    private final ReentrantLock lock = new ReentrantLock();

    @GuardedBy("lock")
    private final List<MemorySegment> cachePages;

    @VisibleForTesting
    @GuardedBy("lock")
    final Deque<Condition> waiters;

    private final int pageSize;
    private final int maxPages;
    private final int perRequestPages;
    private final long maxTimeToBlockMs;

    @GuardedBy("lock")
    private boolean closed;

    private int pageUsage;

    @VisibleForTesting
    LazyMemorySegmentPool(
            int maxPages, int pageSize, long maxTimeToBlockMs, long perRequestMemorySize) {
        checkArgument(maxPages > 0, "MaxPages for LazyMemorySegmentPool should be greater than 0.");
        checkArgument(
                pageSize >= 64,
                "Page size should be greater than 64 bytes to include the record batch header, but is "
                        + pageSize
                        + " bytes.");
        checkArgument(
                perRequestMemorySize >= pageSize,
                String.format(
                        "Page size should be less than or equal to per request memory size. Page size is:"
                                + " %s KB, per request memory size is %s KB.",
                        pageSize / 1024, perRequestMemorySize / 1024));
        this.cachePages = new ArrayList<>();
        this.pageUsage = 0;
        this.maxPages = maxPages;
        this.pageSize = pageSize;
        this.perRequestPages = Math.max(1, (int) (perRequestMemorySize / pageSize()));

        this.closed = false;
        this.waiters = new ArrayDeque<>();
        this.maxTimeToBlockMs = maxTimeToBlockMs;
    }

    public static LazyMemorySegmentPool createWriterBufferPool(Configuration conf) {
        long totalBytes = conf.get(ConfigOptions.CLIENT_WRITER_BUFFER_MEMORY_SIZE).getBytes();
        long batchSize = conf.get(ConfigOptions.CLIENT_WRITER_BATCH_SIZE).getBytes();
        checkArgument(
                totalBytes >= batchSize * 2,
                String.format(
                        "Buffer memory size '%s=%s' should be at least twice of batch size '%s=%s'.",
                        ConfigOptions.CLIENT_WRITER_BUFFER_MEMORY_SIZE.key(),
                        totalBytes,
                        ConfigOptions.CLIENT_WRITER_BATCH_SIZE.key(),
                        batchSize));
        int pageSize = (int) conf.get(ConfigOptions.CLIENT_WRITER_BUFFER_PAGE_SIZE).getBytes();
        long perRequestMemorySize =
                conf.get(ConfigOptions.CLIENT_WRITER_PER_REQUEST_MEMORY_SIZE).getBytes();
        int segmentCount = (int) (totalBytes / pageSize);
        long waitTimeout = conf.get(ConfigOptions.CLIENT_WRITER_BUFFER_WAIT_TIMEOUT).toMillis();
        return new LazyMemorySegmentPool(segmentCount, pageSize, waitTimeout, perRequestMemorySize);
    }

    public static LazyMemorySegmentPool createServerBufferPool(Configuration conf) {
        long totalBytes = conf.get(ConfigOptions.SERVER_BUFFER_MEMORY_SIZE).getBytes();
        int pageSize = (int) conf.get(ConfigOptions.SERVER_BUFFER_PAGE_SIZE).getBytes();
        long perRequestMemorySize =
                conf.get(ConfigOptions.SERVER_BUFFER_PER_REQUEST_MEMORY_SIZE).getBytes();
        int segmentCount = (int) (totalBytes / pageSize);
        long waitTimeout = conf.get(ConfigOptions.SERVER_BUFFER_POOL_WAIT_TIMEOUT).toMillis();
        return new LazyMemorySegmentPool(segmentCount, pageSize, waitTimeout, perRequestMemorySize);
    }

    @Override
    public MemorySegment nextSegment() throws IOException {
        return inLock(lock, () -> allocatePages(1).get(0));
    }

    @Override
    public List<MemorySegment> allocatePages(int requiredPages) throws IOException {
        if (maxPages < requiredPages) { // immediately fail if the request is impossible to satisfy
            throw new EOFException(
                    String.format(
                            "Allocation request cannot be satisfied because the number of maximum available pages is "
                                    + "exceeded. Total pages: %d. Requested pages: %d",
                            this.maxPages, requiredPages));
        }

        return inLock(
                lock,
                () -> {
                    checkClosed();

                    if (freePages() < requiredPages) {
                        waitForSegment(requiredPages);
                    }

                    lazilyAllocatePages(requiredPages);
                    return drain(requiredPages);
                });
    }

    private List<MemorySegment> drain(int numPages) {
        List<MemorySegment> pages = new ArrayList<>(numPages);
        for (int i = 0; i < numPages; i++) {
            pages.add(cachePages.remove(cachePages.size() - 1));
        }
        pageUsage += numPages;
        return pages;
    }

    @VisibleForTesting
    protected void lazilyAllocatePages(int required) {
        if (cachePages.size() < required) {
            int minAllocatePages = required - cachePages.size();
            int maxAllocatePages = freePages() - cachePages.size();
            // try to allocate more pages than minAllocatePages to have better CPU cache
            int numPages = Math.min(maxAllocatePages, Math.max(minAllocatePages, perRequestPages));

            for (int i = 0; i < numPages; i++) {
                cachePages.add(MemorySegment.allocateHeapMemory(pageSize));
            }
        }
    }

    private void waitForSegment(int requiredPages) throws EOFException {
        Condition moreMemory = lock.newCondition();
        waiters.addLast(moreMemory);
        try {
            while (freePages() < requiredPages) {
                boolean success = moreMemory.await(maxTimeToBlockMs, TimeUnit.MILLISECONDS);
                if (!success) {
                    throw new EOFException(
                            "Failed to allocate new segment within the configured max blocking time "
                                    + maxTimeToBlockMs
                                    + " ms. Total memory: "
                                    + totalSize()
                                    + " bytes. Page size: "
                                    + pageSize
                                    + " bytes. Available pages: "
                                    + freePages()
                                    + ". Requested pages: "
                                    + requiredPages);
                }
                checkClosed();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new FlussRuntimeException(e);
        } finally {
            waiters.remove(moreMemory);
        }
    }

    @Override
    public int pageSize() {
        return pageSize;
    }

    @Override
    public long totalSize() {
        return (long) maxPages * pageSize;
    }

    @Override
    public void returnPage(MemorySegment segment) {
        returnAll(Collections.singletonList(segment));
    }

    @Override
    public void returnAll(List<MemorySegment> memory) {
        if (memory.isEmpty()) {
            return;
        }
        inLock(
                lock,
                () -> {
                    final int newPageUsage = pageUsage - memory.size();
                    if (newPageUsage < 0) {
                        throw new RuntimeException("Return too more memories.");
                    }
                    pageUsage = newPageUsage;
                    cachePages.addAll(memory);
                    for (int i = 0; i < memory.size() && !waiters.isEmpty(); i++) {
                        waiters.peekFirst().signal();
                    }
                });
    }

    @Override
    public int freePages() {
        return inLock(lock, () -> this.maxPages - this.pageUsage);
    }

    @Override
    public long availableMemory() {
        return ((long) freePages()) * pageSize;
    }

    @Override
    public void close() {
        inLock(
                lock,
                () -> {
                    closed = true;
                    cachePages.clear();
                    waiters.forEach(Condition::signal);
                });
    }

    private void checkClosed() {
        if (closed) {
            throw new FlussRuntimeException("Memory segment pool closed while allocating memory");
        }
    }

    public int queued() {
        return inLock(lock, waiters::size);
    }

    @VisibleForTesting
    public List<MemorySegment> getAllCachePages() {
        return cachePages;
    }
}
