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

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.MemorySize;
import org.apache.fluss.exception.FlussRuntimeException;

import org.apache.commons.lang3.RandomUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.EOFException;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;

import static org.apache.fluss.testutils.common.CommonTestUtils.retry;
import static org.apache.fluss.utils.function.ThrowingRunnable.unchecked;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link org.apache.fluss.memory.LazyMemorySegmentPool}. */
public class LazyMemorySegmentPoolTest {

    @Test
    void testSimpleAllocateAndReturnPages() throws Exception {
        // test the simple non-blocking allocation paths.
        int totalMemory = Long.valueOf(MemorySize.parse("64kb").getBytes()).intValue();
        int pageSize = (int) MemorySize.parse("1kb").getBytes();
        int totalSegmentCount = totalMemory / pageSize;

        LazyMemorySegmentPool pool =
                buildLazyMemorySegmentSource(totalSegmentCount, pageSize, 10, pageSize);

        // allocate one page and check the segment and the memory pool
        List<MemorySegment> maybeSegments = pool.allocatePages(1);
        assertThat(maybeSegments.size()).isEqualTo(1);
        MemorySegment segment = maybeSegments.get(0);
        assertThat(segment.size()).isEqualTo(pageSize);
        assertThat(pool.availableMemory()).isEqualTo(totalMemory - pageSize);
        segment.putInt(0, 1);
        assertThat(segment.getInt(0)).isEqualTo(1);

        // return the page to the memory pool and check the memory pool
        pool.returnPage(segment);
        assertThat(pool.availableMemory()).isEqualTo(totalMemory);

        // allocate another page and check the segment and the memory pool
        maybeSegments = pool.allocatePages(1);
        assertThat(maybeSegments.size()).isEqualTo(1);
        segment = maybeSegments.get(0);
        assertThat(segment.size()).isEqualTo(pageSize);
        assertThat(pool.availableMemory()).isEqualTo(totalMemory - pageSize);
        assertThat(pool.freePages()).isEqualTo(totalSegmentCount - 1);

        // return the page to the memory pool and check the memory pool
        pool.returnPage(segment);
        assertThat(pool.availableMemory()).isEqualTo(totalMemory);
        assertThat(pool.freePages()).isEqualTo(totalSegmentCount);

        // allocate another 2 pages and check the segments and the memory pool
        maybeSegments = pool.allocatePages(2);
        assertThat(maybeSegments.size()).isEqualTo(2);
        List<MemorySegment> segments = maybeSegments;
        assertThat(pool.availableMemory()).isEqualTo(totalMemory - (2L * pageSize));
        assertThat(pool.freePages()).isEqualTo(totalSegmentCount - 2);
        pool.returnAll(segments);
        assertThat(pool.availableMemory()).isEqualTo(totalMemory);
        assertThat(pool.freePages()).isEqualTo(totalSegmentCount);
    }

    @Test
    void testNewPagesAreAllocatedWhenCachedPagesDoNotSuffice() throws Exception {
        LazyMemorySegmentPool pool = buildLazyMemorySegmentSource(10, 512, Long.MAX_VALUE, 512);
        pool.returnAll(pool.allocatePages(4));
        List<MemorySegment> segments = pool.allocatePages(6);
        assertThat(segments.size()).isEqualTo(6);
        assertThat(pool.freePages()).isEqualTo(4);
        assertThat(pool.getAllCachePages().size()).isEqualTo(0);
    }

    @Test
    void testPerRequestMemorySizeAllocatesCorrectPageNumber() throws Exception {
        LazyMemorySegmentPool pool = buildLazyMemorySegmentSource(10, 512, Long.MAX_VALUE, 2048);
        List<MemorySegment> segments = new ArrayList<>();
        // should allocate 4 new pages (1 used, 3 cached)
        segments.addAll(pool.allocatePages(1));
        assertThat(pool.freePages()).isEqualTo(9);
        assertThat(pool.getAllCachePages().size()).isEqualTo(3);
        // should not allocate additional pages
        segments.addAll(pool.allocatePages(3));
        assertThat(pool.freePages()).isEqualTo(6);
        assertThat(pool.getAllCachePages().size()).isEqualTo(0);

        pool.returnAll(segments);
        segments.clear();

        // should allocate use the 4 old pages and allocate 4 new ones (all used)
        segments.addAll(pool.allocatePages(8));
        assertThat(pool.freePages()).isEqualTo(2);
        assertThat(pool.getAllCachePages().size()).isEqualTo(0);
    }

    @Test
    void testPerRequestMemorySizeRespectsTotalPageLimit() throws Exception {
        LazyMemorySegmentPool pool = buildLazyMemorySegmentSource(5, 512, Long.MAX_VALUE, 2048);
        pool.allocatePages(3);
        // should allocate 4 new pages (3 used, 1 cached, 2 more available)
        assertThat(pool.getAllCachePages().size()).isEqualTo(1);
        assertThat(pool.availableMemory()).isEqualTo(1024);
        assertThat(pool.freePages()).isEqualTo(2);
        // should trigger an allocation request; should use the existing cached page and just
        // allocate 1 additional one and not 2048/512=4
        pool.allocatePages(2);
        // check if page limit is respected
        assertThat(pool.getAllCachePages().size()).isEqualTo(0);
        // there should also be no further pages available
        assertThat(pool.availableMemory()).isEqualTo(0);
        assertThat(pool.freePages()).isEqualTo(0);
    }

    @Test
    void testCloseClearsCachedPages() throws Exception {
        LazyMemorySegmentPool pool = buildLazyMemorySegmentSource(1, 64, 100, 64);
        pool.returnPage(pool.nextSegment());
        assertThat(pool.getAllCachePages().size()).isGreaterThan(0);
        pool.close();
        assertThat(pool.getAllCachePages()).isEmpty();
    }

    @Test
    void testClosePreventsFurtherAllocationButAllowsReturningPages() throws Exception {
        LazyMemorySegmentPool pool = buildLazyMemorySegmentSource(1, 512, Long.MAX_VALUE, 512);
        List<MemorySegment> segment = pool.allocatePages(1);

        // Close the memory segment pool. This should prevent any further allocations.
        pool.close();

        assertThatThrownBy(() -> pool.allocatePages(1)).isInstanceOf(FlussRuntimeException.class);
        assertThatThrownBy(pool::nextSegment).isInstanceOf(FlussRuntimeException.class);

        // ensure de-allocation still works.
        pool.returnAll(segment);
    }

    @Test
    void testCannotAllocateMorePagesThanAvailable() throws Exception {
        LazyMemorySegmentPool pool = buildLazyMemorySegmentSource(2, 512, 10, 512);
        List<MemorySegment> segment = pool.allocatePages(2);
        assertThat(segment.size()).isEqualTo(2);
        assertThat(pool.availableMemory()).isEqualTo(0);
        assertThat(pool.freePages()).isEqualTo(0);
        assertThat(pool.getAllCachePages().size()).isEqualTo(0);

        pool.returnAll(segment);
        assertThat(pool.availableMemory()).isEqualTo(1024);
        assertThat(pool.freePages()).isEqualTo(2);
        assertThat(pool.getAllCachePages().size()).isEqualTo(2);

        assertThatThrownBy(() -> pool.allocatePages(3))
                .isInstanceOf(EOFException.class)
                .hasMessageContaining("Total pages: 2. Requested pages: 3");
    }

    @Test
    void testComplexDelayedAllocation() throws Exception {
        LazyMemorySegmentPool pool = buildLazyMemorySegmentSource(8, 1024, Long.MAX_VALUE, 1024);
        // allocate all segments
        List<MemorySegment> segments1 = pool.allocatePages(4);
        List<MemorySegment> segments2 = pool.allocatePages(4);

        CountDownLatch doDealloc1 = asyncReturnAll(pool, segments1);
        CountDownLatch doDealloc2 = asyncReturnAll(pool, segments2);

        // should block until all memory is returned
        CountDownLatch allocation = asyncAllocatePages(pool, 8);
        assertThat(allocation.getCount()).isEqualTo(1);
        // return a part of memory
        doDealloc1.countDown();

        // no enough memory allocate
        assertThat(allocation.await(1, TimeUnit.SECONDS)).isFalse();

        doDealloc2.countDown();
        // have enough memory to allocate
        assertThat(allocation.await(1, TimeUnit.SECONDS)).isTrue();
    }

    @Test
    void testDelayedAllocation() throws Exception {
        LazyMemorySegmentPool pool = buildLazyMemorySegmentSource(5, 1024, Long.MAX_VALUE, 1024);
        MemorySegment segment = pool.nextSegment();
        CountDownLatch doDealloc = asyncReturnAll(pool, Collections.singletonList(segment));
        // should block until memory is returned
        CountDownLatch allocation = asyncAllocatePages(pool, 5);
        assertThat(allocation.getCount()).isEqualTo(1);
        // return the memory
        doDealloc.countDown();
        // give pool enough time to return memory to make the test stable
        assertThat(allocation.await(2, TimeUnit.SECONDS)).isTrue();
        assertThat(pool.availableMemory()).isEqualTo(0);
    }

    @Test
    void testUnsatisfiableRequestDoesNotBlockAndThrowsException() {
        LazyMemorySegmentPool pool = buildLazyMemorySegmentSource(1, 1024, Long.MAX_VALUE, 1024);
        assertThatThrownBy(() -> pool.allocatePages(2))
                .isInstanceOf(EOFException.class)
                .hasMessageContaining("Total pages: 1. Requested pages: 2");
        assertThat(pool.queued()).isEqualTo(0);
        assertThat(pool.freePages()).isEqualTo(1);
    }

    @ParameterizedTest
    @ValueSource(ints = {10, 500, 1000, 2500})
    void testBlockTimeIsRespected(int maxTimeToBlockMs) throws Exception {
        LazyMemorySegmentPool pool = buildLazyMemorySegmentSource(1, 1024, maxTimeToBlockMs, 1024);
        pool.nextSegment();

        long start = System.currentTimeMillis();
        assertThatThrownBy(
                        () ->
                                // will block for at least maxTimeToBlockMs
                                pool.allocatePages(1))
                .isInstanceOf(EOFException.class)
                .hasMessageContaining("Available pages: 0. Requested pages: 1");
        assertThat(System.currentTimeMillis() - start).isGreaterThanOrEqualTo(maxTimeToBlockMs);

        assertThat(pool.queued()).isEqualTo(0);
        assertThat(pool.freePages()).isEqualTo(0);
    }

    @Test
    void testCleanupMemoryAvailabilityWaiterOnInterruption() throws Exception {
        LazyMemorySegmentPool pool = buildLazyMemorySegmentSource(2, 1024, 5000, 1024);
        pool.allocatePages(1);
        Thread t1 = new Thread(new MemorySegmentPoolAllocator(pool));
        Thread t2 = new Thread(new MemorySegmentPoolAllocator(pool));
        // start thread t1 which will try to allocate more memory on to the memory segment
        t1.start();
        // retry until condition variable c1 associated with pool.allocate() by thread t1 inserted
        // in the waiters queue.
        retry(Duration.ofSeconds(1), () -> assertThat(pool.queued()).isEqualTo(1));
        Deque<Condition> waiters = pool.waiters;
        // get the condition object associated with pool.allocate() by thread t1.
        Condition c1 = waiters.getFirst();
        // start thread t2 which will try to allocate more memory on to the memory segment pool.
        t2.start();
        // retry until condition variable c2 associated with pool.allocate() by thread t2 inserted
        // in the waiters queue. The waiters queue will have 2 entries c1 and c2.
        retry(Duration.ofSeconds(1), () -> assertThat(pool.queued()).isEqualTo(2));
        t1.interrupt();
        // retry until queue has only 1 entry
        retry(Duration.ofSeconds(1), () -> assertThat(pool.queued()).isEqualTo(1));
        // get the condition object associated with allocate() by thread t2
        Condition c2 = waiters.getLast();
        t2.interrupt();
        assertThat(c1).isNotEqualTo(c2);
        t1.join();
        t2.join();
        // both allocate() called by threads t1 and t2 should have been interrupted and the waiters
        // queue should be empty.
        assertThat(pool.queued()).isEqualTo(0);
    }

    @Test
    void testOutOfMemoryOnAllocation() {
        LazyMemorySegmentPool pool =
                new LazyMemorySegmentPool(1, 1024, 10, 1024) {
                    @Override
                    protected void lazilyAllocatePages(int required) {
                        throw new OutOfMemoryError();
                    }
                };

        assertThatThrownBy(() -> pool.allocatePages(1)).isInstanceOf(OutOfMemoryError.class);
        assertThatThrownBy(pool::nextSegment).isInstanceOf(OutOfMemoryError.class);

        assertThat(pool.availableMemory()).isEqualTo(1024);
    }

    @Test
    void testCloseNotifyWaiters() throws Exception {
        final int numWorkers = 2;

        LazyMemorySegmentPool pool = new LazyMemorySegmentPool(1, 64, Long.MAX_VALUE, 64);
        MemorySegment segment = pool.allocatePages(1).get(0);

        ExecutorService executor = Executors.newFixedThreadPool(numWorkers);
        Callable<Void> work =
                () -> {
                    assertThatThrownBy(() -> pool.allocatePages(1))
                            .isInstanceOf(FlussRuntimeException.class);
                    assertThatThrownBy(pool::nextSegment).isInstanceOf(FlussRuntimeException.class);
                    return null;
                };

        for (int i = 0; i < numWorkers; ++i) {
            executor.submit(work);
        }

        retry(Duration.ofSeconds(15), () -> assertThat(pool.queued()).isEqualTo(numWorkers));

        // Close the buffer pool. This should notify all waiters.
        pool.close();

        retry(Duration.ofSeconds(15), () -> assertThat(pool.queued()).isEqualTo(0));

        pool.returnPage(segment);
        assertThat(pool.availableMemory()).isEqualTo(64);
    }

    @Test
    void testLargeAvailablePageSize() throws InterruptedException, IOException {
        int maxPages = 10_000_000;
        final AtomicInteger freeSize = new AtomicInteger(maxPages);

        LazyMemorySegmentPool pool =
                new LazyMemorySegmentPool(maxPages, 2_000_000_000, 0, 2_000_000_000) {
                    @Override
                    protected void lazilyAllocatePages(int required) {
                        for (int i = 0; i < required; i++) {
                            // Ignore size to avoid OOM due to large buffers.
                            this.getAllCachePages().add(MemorySegment.allocateHeapMemory(0));
                            freeSize.decrementAndGet();
                        }
                    }

                    @Override
                    public int freePages() {
                        return freeSize.get();
                    }
                };

        pool.allocatePages(1_000_000);
        assertThat(pool.availableMemory()).isEqualTo(18_000_000_000_000_000L);
        pool.allocatePages(1_000_000);
        assertThat(pool.availableMemory()).isEqualTo(16_000_000_000_000_000L);

        // Emulate `deallocate` by increasing `freeSize`.
        freeSize.addAndGet(1_000_000);
        assertThat(pool.availableMemory()).isEqualTo(18_000_000_000_000_000L);
        freeSize.addAndGet(1_000_000);
        assertThat(pool.availableMemory()).isEqualTo(20_000_000_000_000_000L);
    }

    @Test
    void testNextSegmentWaiter() throws Exception {
        LazyMemorySegmentPool source = buildLazyMemorySegmentSource(10, 64, 100, 64);
        assertThat(source.pageSize()).isEqualTo(64);
        assertThat(source.availableMemory()).isEqualTo(64 * 10);
        assertThat(source.freePages()).isEqualTo(10);

        MemorySegment ms1 = source.nextSegment();
        assertThat(source.availableMemory()).isEqualTo(64 * 9);
        assertThat(source.freePages()).isEqualTo(9);

        MemorySegment ms2 = source.nextSegment();
        assertThat(source.availableMemory()).isEqualTo(64 * 8);
        assertThat(source.freePages()).isEqualTo(8);

        for (int i = 0; i < 8; i++) {
            source.nextSegment();
        }
        assertThat(source.availableMemory()).isEqualTo(0);
        assertThat(source.freePages()).isEqualTo(0);

        assertThatThrownBy(source::nextSegment)
                .isInstanceOf(EOFException.class)
                .hasMessage(
                        "Failed to allocate new segment within the configured max blocking time 100 ms. "
                                + "Total memory: 640 bytes. Page size: 64 bytes. Available pages: 0. Requested pages: 1");

        CountDownLatch returnAllLatch = asyncReturnAll(source, Arrays.asList(ms1, ms2));
        CountDownLatch getNextSegmentLatch = asyncGetNextSegment(source);
        assertThat(getNextSegmentLatch.getCount()).isEqualTo(1);
        returnAllLatch.countDown();
        assertThat(getNextSegmentLatch.await(Long.MAX_VALUE, TimeUnit.SECONDS)).isTrue();
    }

    @Test
    void testConstructorIllegalArgument() {
        assertThatThrownBy(() -> buildLazyMemorySegmentSource(0, 64, 100, 64))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("MaxPages for LazyMemorySegmentPool should be greater than 0.");
        assertThatThrownBy(
                        () ->
                                buildLazyMemorySegmentSource(
                                        10, 32 * 1024 * 1024, 100, (32 * 1024 * 1024) / 2))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(
                        "Page size should be less than or equal to per request memory size. "
                                + "Page size is: 32768 KB, per request memory size is 16384 KB.");
        assertThatThrownBy(() -> buildLazyMemorySegmentSource(10, 30, 100, 30))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(
                        "Page size should be greater than 64 bytes to include the record batch header, but is 30 bytes.");

        LazyMemorySegmentPool lazyMemorySegmentPool =
                buildLazyMemorySegmentSource(10, 100, 100, 100);
        assertThatThrownBy(
                        () ->
                                lazyMemorySegmentPool.returnAll(
                                        Arrays.asList(
                                                MemorySegment.allocateHeapMemory(100),
                                                MemorySegment.allocateHeapMemory(100))))
                .hasMessage("Return too more memories.");
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2, 4}) // >= 1
    void testStressfulSituation(int perRequestMemorySizeFactor) throws Exception {
        // This test creates lots of threads that hammer on the memory segment pool.
        int numThreads = 10;
        final int iterations = 50_000;
        final int pageSize = 64;
        final int maxPages = 512;
        LazyMemorySegmentPool pool =
                buildLazyMemorySegmentSource(
                        maxPages, pageSize, 20_000, perRequestMemorySizeFactor * pageSize);

        List<StressTestThread> threads = new ArrayList<>();
        for (int i = 0; i < numThreads; i++) {
            threads.add(new StressTestThread(pool, iterations, numThreads));
        }

        for (StressTestThread thread : threads) {
            thread.start();
        }

        for (StressTestThread thread : threads) {
            thread.join();
        }

        for (StressTestThread thread : threads) {
            // Thread should have completed all iterations successfully.
            assertThat(thread.success.get()).isTrue();
        }

        assertThat(pool.availableMemory()).isEqualTo(maxPages * pageSize);
    }

    @Nested
    class FactoryMethodTest {

        private Configuration conf;

        @BeforeEach
        void setUp() throws Exception {
            conf = new Configuration();
        }

        @Test
        void testCreateWriterBufferPoolWithDefaultConfig() {
            LazyMemorySegmentPool pool = LazyMemorySegmentPool.createWriterBufferPool(conf);
            assertThat(pool).isNotNull();
            assertThat(pool.totalSize())
                    .isEqualTo(conf.get(ConfigOptions.CLIENT_WRITER_BUFFER_MEMORY_SIZE).getBytes());
            assertThat(pool.pageSize())
                    .isEqualTo(conf.get(ConfigOptions.CLIENT_WRITER_BUFFER_PAGE_SIZE).getBytes());
            assertThat(pool.availableMemory())
                    .isEqualTo(conf.get(ConfigOptions.CLIENT_WRITER_BUFFER_MEMORY_SIZE).getBytes());
            assertThat(pool.freePages())
                    .isEqualTo(
                            conf.get(ConfigOptions.CLIENT_WRITER_BUFFER_MEMORY_SIZE).getBytes()
                                    / conf.get(ConfigOptions.CLIENT_WRITER_BUFFER_PAGE_SIZE)
                                            .getBytes());
            assertThat(pool.getAllCachePages().size()).isEqualTo(0);
        }

        @Test
        void testCreateReaderBufferPoolWithCustomConfig() throws IOException {
            conf.set(ConfigOptions.CLIENT_WRITER_BUFFER_MEMORY_SIZE, MemorySize.parse("128kb"));
            conf.set(ConfigOptions.CLIENT_WRITER_BATCH_SIZE, MemorySize.parse("64kb"));
            conf.set(ConfigOptions.CLIENT_WRITER_BUFFER_PAGE_SIZE, MemorySize.parse("2kb"));
            conf.set(ConfigOptions.CLIENT_WRITER_PER_REQUEST_MEMORY_SIZE, MemorySize.parse("2kb"));
            conf.set(ConfigOptions.CLIENT_WRITER_BUFFER_WAIT_TIMEOUT, Duration.ofMillis(1000L));

            LazyMemorySegmentPool pool = LazyMemorySegmentPool.createWriterBufferPool(conf);
            assertThat(pool).isNotNull();
            assertThat(pool.totalSize())
                    .isEqualTo(conf.get(ConfigOptions.CLIENT_WRITER_BUFFER_MEMORY_SIZE).getBytes());
            assertThat(pool.pageSize())
                    .isEqualTo(conf.get(ConfigOptions.CLIENT_WRITER_BUFFER_PAGE_SIZE).getBytes());
            assertThat(pool.availableMemory())
                    .isEqualTo(conf.get(ConfigOptions.CLIENT_WRITER_BUFFER_MEMORY_SIZE).getBytes());
            assertThat(pool.freePages())
                    .isEqualTo(
                            conf.get(ConfigOptions.CLIENT_WRITER_BUFFER_MEMORY_SIZE).getBytes()
                                    / conf.get(ConfigOptions.CLIENT_WRITER_BUFFER_PAGE_SIZE)
                                            .getBytes());
            assertThat(pool.getAllCachePages().size()).isEqualTo(0);

            pool.allocatePages(64);
            assertThatThrownBy(() -> pool.allocatePages(1))
                    .isInstanceOf(EOFException.class)
                    .hasMessageContaining("1000 ms");
        }

        @Test
        void testCreateWriterBufferPoolIllegalArguments() {
            conf.set(ConfigOptions.CLIENT_WRITER_BUFFER_MEMORY_SIZE, MemorySize.parse("96kb"));
            conf.set(ConfigOptions.CLIENT_WRITER_BATCH_SIZE, MemorySize.parse("64kb"));
            conf.set(ConfigOptions.CLIENT_WRITER_BUFFER_PAGE_SIZE, MemorySize.parse("4kb"));
            conf.set(ConfigOptions.CLIENT_WRITER_PER_REQUEST_MEMORY_SIZE, MemorySize.parse("8kb"));

            assertThatThrownBy(() -> LazyMemorySegmentPool.createWriterBufferPool(conf))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("should be at least twice of batch size");
        }

        @Test
        void testCreateServerBufferPoolWithDefaultConfig() {
            LazyMemorySegmentPool pool = LazyMemorySegmentPool.createServerBufferPool(conf);
            assertThat(pool).isNotNull();
            assertThat(pool.totalSize())
                    .isEqualTo(conf.get(ConfigOptions.SERVER_BUFFER_MEMORY_SIZE).getBytes());
            assertThat(pool.pageSize())
                    .isEqualTo(conf.get(ConfigOptions.SERVER_BUFFER_PAGE_SIZE).getBytes());
            assertThat(pool.availableMemory())
                    .isEqualTo(conf.get(ConfigOptions.SERVER_BUFFER_MEMORY_SIZE).getBytes());
            assertThat(pool.freePages())
                    .isEqualTo(
                            conf.get(ConfigOptions.SERVER_BUFFER_MEMORY_SIZE).getBytes()
                                    / conf.get(ConfigOptions.SERVER_BUFFER_PAGE_SIZE).getBytes());
            assertThat(pool.getAllCachePages().size()).isEqualTo(0);
        }

        @Test
        void testCreateServerBufferPoolWithCustomConfig() throws IOException {
            conf.set(ConfigOptions.SERVER_BUFFER_MEMORY_SIZE, MemorySize.parse("128kb"));
            conf.set(ConfigOptions.SERVER_BUFFER_PAGE_SIZE, MemorySize.parse("2kb"));
            conf.set(ConfigOptions.SERVER_BUFFER_PER_REQUEST_MEMORY_SIZE, MemorySize.parse("2kb"));
            conf.set(ConfigOptions.SERVER_BUFFER_POOL_WAIT_TIMEOUT, Duration.ofMillis(1000L));

            LazyMemorySegmentPool pool = LazyMemorySegmentPool.createServerBufferPool(conf);
            assertThat(pool).isNotNull();
            assertThat(pool.totalSize())
                    .isEqualTo(conf.get(ConfigOptions.SERVER_BUFFER_MEMORY_SIZE).getBytes());
            assertThat(pool.pageSize())
                    .isEqualTo(conf.get(ConfigOptions.SERVER_BUFFER_PAGE_SIZE).getBytes());
            assertThat(pool.availableMemory())
                    .isEqualTo(conf.get(ConfigOptions.SERVER_BUFFER_MEMORY_SIZE).getBytes());
            assertThat(pool.freePages())
                    .isEqualTo(
                            conf.get(ConfigOptions.SERVER_BUFFER_MEMORY_SIZE).getBytes()
                                    / conf.get(ConfigOptions.SERVER_BUFFER_PAGE_SIZE).getBytes());
            assertThat(pool.getAllCachePages().size()).isEqualTo(0);

            pool.allocatePages(64);
            assertThatThrownBy(() -> pool.allocatePages(1))
                    .isInstanceOf(EOFException.class)
                    .hasMessageContaining("1000 ms");
        }
    }

    private static LazyMemorySegmentPool buildLazyMemorySegmentSource(
            int maxPages, int pageSize, long maxTimeToBlockMs, int perRequestMemorySize) {
        return new LazyMemorySegmentPool(
                maxPages, pageSize, maxTimeToBlockMs, perRequestMemorySize);
    }

    private static class MemorySegmentPoolAllocator implements Runnable {
        LazyMemorySegmentPool pool;

        MemorySegmentPoolAllocator(LazyMemorySegmentPool pool) {
            this.pool = pool;
        }

        @Override
        public void run() {
            assertThatThrownBy(() -> pool.allocatePages(2))
                    .isInstanceOfAny(FlussRuntimeException.class, InterruptedException.class);
        }
    }

    private static CountDownLatch asyncReturnAll(
            LazyMemorySegmentPool source, List<MemorySegment> segments) {
        CountDownLatch latch = new CountDownLatch(1);
        Thread thread =
                new Thread(
                        unchecked(
                                () -> {
                                    latch.await();
                                    source.returnAll(segments);
                                }));
        thread.start();
        return latch;
    }

    private static CountDownLatch asyncGetNextSegment(LazyMemorySegmentPool source) {
        final CountDownLatch completed = new CountDownLatch(1);
        Thread thread =
                new Thread(
                        () -> {
                            try {
                                try {
                                    source.nextSegment();
                                } catch (IOException e) {
                                    throw new RuntimeException(e);
                                }
                            } finally {
                                completed.countDown();
                            }
                        });
        thread.start();
        return completed;
    }

    private static CountDownLatch asyncAllocatePages(
            LazyMemorySegmentPool source, int requiredPages) {
        final CountDownLatch completed = new CountDownLatch(1);
        Thread thread =
                new Thread(
                        () -> {
                            try {
                                try {
                                    source.allocatePages(requiredPages);
                                } catch (IOException e) {
                                    throw new RuntimeException(e);
                                }
                            } finally {
                                completed.countDown();
                            }
                        });
        thread.start();
        return completed;
    }

    private static class StressTestThread extends Thread {
        private final int iterations;
        private final LazyMemorySegmentPool pool;
        private final int numThreads;
        public final AtomicBoolean success = new AtomicBoolean(false);

        public StressTestThread(LazyMemorySegmentPool pool, int iterations, int numThreads) {
            this.iterations = iterations;
            this.pool = pool;
            this.numThreads = numThreads;
        }

        @Override
        public void run() {
            try {
                for (int i = 0; i < iterations; i++) {
                    int numPages;
                    if (RandomUtils.nextBoolean()) {
                        // allocate a single page
                        numPages = 1;
                    } else {
                        // allocate a random size, use discount factor to avoid lock contention that
                        // leads to timeout
                        numPages =
                                RandomUtils.nextInt(
                                        0,
                                        (int) ((pool.totalSize() / pool.pageSize()))
                                                        / (this.numThreads / 2)
                                                + 1);
                    }

                    List<MemorySegment> segments = pool.allocatePages(numPages);
                    pool.returnAll(segments);
                }
                success.set(true);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
