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

/* This file is based on source code of Apache Flink Project (https://flink.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/** MemorySegment pool of a MemorySegment list. */
@Internal
@ThreadSafe
public class LazyMemorySegmentPool implements MemorySegmentPool, Closeable {

    private static final long PER_REQUEST_MEMORY_SIZE = 16 * 1024 * 1024;
    private static final long DEFAULT_WAIT_TIMEOUT_MS = 1000 * 60 * 5;

    /** The lock to guard the memory pool. */
    private final ReentrantLock lock = new ReentrantLock();

    @GuardedBy("lock")
    private final List<MemorySegment> cachePages;

    @GuardedBy("lock")
    private final Deque<Condition> waiters;

    private final int pageSize;
    private final int maxPages;
    private final int perRequestPages;
    private final long maxTimeToBlockMs;

    @GuardedBy("lock")
    private boolean closed;

    private int pageUsage;

    LazyMemorySegmentPool(int maxPages, int pageSize, long maxTimeToBlockMs) {
        checkArgument(
                maxPages > 0, "MaxPages for LazyMemorySegmentPool " + "should be greater than 0.");
        checkArgument(
                PER_REQUEST_MEMORY_SIZE > pageSize,
                String.format(
                        "Page size should be less than PER_REQUEST_MEMORY_SIZE. Page size is:"
                                + " %s KB, PER_REQUEST_MEMORY_SIZE is %s KB.",
                        pageSize / 1024, PER_REQUEST_MEMORY_SIZE / 1024));
        this.cachePages = new ArrayList<>();
        this.pageUsage = 0;
        this.maxPages = maxPages;
        this.pageSize = pageSize;
        this.perRequestPages = Math.max(1, (int) (PER_REQUEST_MEMORY_SIZE / pageSize()));

        this.closed = false;
        this.waiters = new ArrayDeque<>();
        this.maxTimeToBlockMs = maxTimeToBlockMs;
    }

    public static LazyMemorySegmentPool create(Configuration conf) {
        long totalBytes = conf.get(ConfigOptions.CLIENT_WRITER_BUFFER_MEMORY_SIZE).getBytes();
        long batchSize = conf.get(ConfigOptions.CLIENT_WRITER_BATCH_SIZE).getBytes();
        checkArgument(
                totalBytes >= batchSize * 2,
                String.format(
                        "Buffer memory size '%s' should be at least twice of batch size '%s'.",
                        ConfigOptions.CLIENT_WRITER_BUFFER_MEMORY_SIZE.key(),
                        ConfigOptions.CLIENT_WRITER_BATCH_SIZE.key()));

        int pageSize = (int) conf.get(ConfigOptions.CLIENT_WRITER_BUFFER_PAGE_SIZE).getBytes();
        int segmentCount = (int) (totalBytes / pageSize);
        return new LazyMemorySegmentPool(segmentCount, pageSize, DEFAULT_WAIT_TIMEOUT_MS);
    }

    @Override
    public MemorySegment nextSegment(boolean waiting) throws IOException {
        return inLock(
                lock,
                () -> {
                    checkClosed();
                    int freePages = freePages();
                    if (freePages == 0) {
                        if (waiting) {
                            return waitForSegment();
                        } else {
                            return null;
                        }
                    }

                    if (cachePages.isEmpty()) {
                        int numPages = Math.min(freePages, perRequestPages);
                        for (int i = 0; i < numPages; i++) {
                            cachePages.add(MemorySegment.allocateHeapMemory(pageSize));
                        }
                    }

                    this.pageUsage++;
                    return cachePages.remove(this.cachePages.size() - 1);
                });
    }

    private MemorySegment waitForSegment() throws EOFException {
        Condition moreMemory = lock.newCondition();
        waiters.addLast(moreMemory);
        try {
            while (cachePages.isEmpty()) {
                boolean success = moreMemory.await(maxTimeToBlockMs, TimeUnit.MILLISECONDS);
                if (!success) {
                    throw new EOFException(
                            "Failed to allocate new segment within the configured max blocking time "
                                    + maxTimeToBlockMs
                                    + " ms. Total memory: "
                                    + totalSize()
                                    + " bytes. Available memory: "
                                    + freePages() * pageSize
                                    + " bytes. page size: "
                                    + pageSize
                                    + " bytes");
                }
                checkClosed();
            }

            this.pageUsage++;
            return cachePages.remove(cachePages.size() - 1);
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

    public int totalSize() {
        return maxPages * pageSize;
    }

    @Override
    public void returnPage(MemorySegment segment) {
        returnAll(Collections.singletonList(segment));
    }

    @Override
    public void returnAll(List<MemorySegment> memory) {
        inLock(
                lock,
                () -> {
                    pageUsage -= memory.size();
                    if (this.pageUsage < 0) {
                        throw new RuntimeException("Return too more memories.");
                    }
                    cachePages.addAll(memory);
                    for (int i = 0; i < memory.size() && !waiters.isEmpty(); i++) {
                        waiters.pollFirst().signal();
                    }
                });
    }

    @Override
    public int freePages() {
        return this.maxPages - this.pageUsage;
    }

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
