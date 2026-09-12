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

package org.apache.fluss.memory;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.FlussRuntimeException;
import org.apache.fluss.exception.RecordTooLargeException;
import org.apache.fluss.exception.TimeoutException;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.concurrent.LockUtils.inLock;

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

    private volatile int pageUsage;

    @GuardedBy("lock")
    private final Set<Allocation> allocations = new LinkedHashSet<>();

    private final Condition allocationChanged = lock.newCondition();

    @GuardedBy("lock")
    private int waitingAllocations;

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
                    allocationChanged.signalAll();
                    for (int i = 0; i < memory.size() && !waiters.isEmpty(); i++) {
                        waiters.peekFirst().signal();
                    }
                });
    }

    @Override
    public int freePages() {
        return inLock(lock, () -> this.maxPages - this.pageUsage);
    }

    /** Returns the number of pages currently allocated to callers. */
    public int usedPages() {
        return pageUsage;
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
                    allocationChanged.signalAll();
                });
    }

    private void checkClosed() {
        if (closed) {
            throw new FlussRuntimeException("Memory segment pool closed while allocating memory");
        }
    }

    public int queued() {
        return inLock(lock, () -> waiters.size() + waitingAllocations);
    }

    @VisibleForTesting
    public List<MemorySegment> getAllCachePages() {
        return cachePages;
    }

    @Override
    public MemoryAllocation newAllocation() {
        return inLock(
                lock,
                () -> {
                    checkClosed();
                    Allocation allocation = new Allocation();
                    allocations.add(allocation);
                    return allocation;
                });
    }

    /** Called only under memory pressure, with the pool lock held. */
    private void resolveAllocationDeadlock() {
        int heldPages = 0;
        Allocation victim = null;
        for (Allocation allocation : allocations) {
            int held = allocation.pages.size();
            heldPages += held;
            if (held > 0) {
                // An active owner can still finish, or an aborted owner is already unwinding.
                if (allocation.pendingPages == 0 || allocation.aborted) {
                    return;
                }
                victim = allocation;
            }
            if (allocation.pendingPages > 0 && allocation.pendingPages <= maxPages - pageUsage) {
                return;
            }
        }
        // Pages outside allocation scopes may be returned independently.
        if (heldPages == pageUsage && victim != null) {
            // Registration order keeps older operations alive when holders block each other.
            victim.aborted = true;
            allocationChanged.signalAll();
        }
    }

    private final class Allocation extends MemoryAllocation {
        private int pendingPages;
        private boolean aborted;

        private Allocation() {
            super(LazyMemorySegmentPool.this);
        }

        @Override
        public List<MemorySegment> allocatePages(int required) throws IOException {
            checkArgument(required > 0, "Requested pages must be positive.");
            lock.lock();
            try {
                checkAllocationOpen();
                if (required > maxPages - pages.size()) {
                    aborted = true;
                    throw new RecordTooLargeException(
                            "Memory allocation exceeds the memory pool capacity of "
                                    + totalSize()
                                    + " bytes: held pages="
                                    + pages.size()
                                    + ", requested pages="
                                    + required
                                    + ", page size="
                                    + pageSize);
                }
                if (required > maxPages - pageUsage) {
                    awaitPages(required);
                }
                lazilyAllocatePages(required);
                List<MemorySegment> allocated = drain(required);
                if (required == 1) {
                    pages.add(allocated.get(0));
                } else {
                    pages.addAll(allocated);
                }
                return allocated;
            } finally {
                lock.unlock();
            }
        }

        private void awaitPages(int required) {
            pendingPages = required;
            waitingAllocations++;
            long remaining = TimeUnit.MILLISECONDS.toNanos(maxTimeToBlockMs);
            try {
                while (required > maxPages - pageUsage) {
                    resolveAllocationDeadlock();
                    checkAllocationOpen();
                    if (remaining <= 0) {
                        throw new TimeoutException("Timed out waiting for memory allocation.");
                    }
                    remaining = allocationChanged.awaitNanos(remaining);
                    checkAllocationOpen();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new FlussRuntimeException(e);
            } finally {
                pendingPages = 0;
                waitingAllocations--;
            }
        }

        private void checkAllocationOpen() {
            checkClosed();
            if (closed) {
                throw new IllegalStateException("Memory allocation is closed.");
            }
            if (aborted) {
                // Use the existing retryable wire error so older clients can retry as well.
                throw new TimeoutException(
                        "Memory allocation aborted because blocked allocations cannot make progress. "
                                + "Release the allocation and retry the operation.");
            }
        }

        @Override
        public void returnAll(List<MemorySegment> memory) {
            inLock(lock, () -> super.returnAll(memory));
        }

        @Override
        public void close() {
            inLock(
                    lock,
                    () -> {
                        super.close();
                        allocations.remove(this);
                        allocationChanged.signalAll();
                    });
        }
    }
}
