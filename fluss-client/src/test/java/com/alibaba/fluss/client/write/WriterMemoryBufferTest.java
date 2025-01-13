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

import com.alibaba.fluss.config.MemorySize;
import com.alibaba.fluss.exception.BufferExhaustedException;
import com.alibaba.fluss.exception.FlussRuntimeException;
import com.alibaba.fluss.memory.MemorySegment;

import org.apache.commons.lang3.RandomUtils;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
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

import static com.alibaba.fluss.testutils.common.CommonTestUtils.retry;
import static com.alibaba.fluss.utils.function.ThrowingRunnable.unchecked;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link WriterMemoryBuffer}. */
// TODO: Migrate tests of WriterMemoryBufferTest to LazyMemorySegmentPoolTest #323
public class WriterMemoryBufferTest {
    private final long maxBlockTimeMs = 10;

    @Test
    void testSimple() throws Exception {
        // test the simple non-blocking allocation paths.
        long totalMemory = MemorySize.parse("64kb").getBytes();
        int batchSize = (int) MemorySize.parse("1kb").getBytes();

        WriterMemoryBuffer writerMemoryBuffer = new WriterMemoryBuffer(totalMemory, batchSize);
        MemorySegment segment = writerMemoryBuffer.allocate(batchSize, maxBlockTimeMs);
        assertThat(segment.size()).isEqualTo(batchSize);
        assertThat(writerMemoryBuffer.getUnallocatedMemory()).isEqualTo(totalMemory - batchSize);
        assertThat(writerMemoryBuffer.getAvailableMemory()).isEqualTo(totalMemory - batchSize);
        segment.putInt(0, 1);
        writerMemoryBuffer.deallocate(segment);
        assertThat(writerMemoryBuffer.getAvailableMemory()).isEqualTo(totalMemory);
        assertThat(writerMemoryBuffer.getUnallocatedMemory()).isEqualTo(totalMemory - batchSize);

        segment = writerMemoryBuffer.allocate(batchSize, maxBlockTimeMs);
        writerMemoryBuffer.deallocate(segment);
        assertThat(writerMemoryBuffer.getAvailableMemory()).isEqualTo(totalMemory);
        assertThat(writerMemoryBuffer.getUnallocatedMemory()).isEqualTo(totalMemory - batchSize);

        segment = writerMemoryBuffer.allocate(2 * batchSize, maxBlockTimeMs);
        writerMemoryBuffer.deallocate(segment);
        assertThat(writerMemoryBuffer.getAvailableMemory()).isEqualTo(totalMemory);
        assertThat(writerMemoryBuffer.getUnallocatedMemory()).isEqualTo(totalMemory - batchSize);
    }

    @Test
    void testCanAllocateMoreMemoryThanHave() throws Exception {
        WriterMemoryBuffer pool = new WriterMemoryBuffer(1024, 512);
        MemorySegment segment = pool.allocate(1024, maxBlockTimeMs);
        assertThat(segment.size()).isEqualTo(1024);
        pool.deallocate(segment);
        assertThatThrownBy(() -> pool.allocate(1025, maxBlockTimeMs))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Attempt to allocate memory segment size");
    }

    @Test
    void testDelayedAllocation() throws Exception {
        WriterMemoryBuffer pool = new WriterMemoryBuffer(5 * 1024, 1024);
        MemorySegment segment = pool.allocate(1024, maxBlockTimeMs);
        CountDownLatch doDealloc = asyncDeallocate(pool, segment);
        CountDownLatch allocation = asyncAllocate(pool, 5 * 1024);
        assertThat(allocation.getCount()).isEqualTo(1);
        doDealloc.countDown(); // return the memory.
        assertThat(allocation.await(1, TimeUnit.SECONDS)).isTrue();
    }

    @Test
    void testBufferExhaustedExceptionIsThrown() throws Exception {
        // If there is not enough memory to allocate and the elapsed time is greater than the max
        // specified block time.
        WriterMemoryBuffer pool = new WriterMemoryBuffer(2, 1);
        pool.allocate(1, maxBlockTimeMs);
        assertThatThrownBy(() -> pool.allocate(2, maxBlockTimeMs))
                .isInstanceOf(BufferExhaustedException.class)
                .hasMessageContaining(
                        "Failed to allocate 2 bytes within the configured max blocking time 10 ms."
                                + " Total memory: 2 bytes. Available memory: 1 bytes. page size: 1 bytes");
        assertThat(pool.queued()).isEqualTo(0);
        assertThat(pool.getAvailableMemory()).isEqualTo(1);
    }

    @Test
    void testCleanupMemoryAvailabilityWaiterOnInterruption() throws Exception {
        WriterMemoryBuffer pool = new WriterMemoryBuffer(2, 1);
        long blockTime = 5000;
        pool.allocate(1, maxBlockTimeMs);

        Thread t1 = new Thread(new MemorySegmentPoolAllocator(pool, blockTime));
        Thread t2 = new Thread(new MemorySegmentPoolAllocator(pool, blockTime));
        // start thread t1 which will try to allocate more memory on to the memory segment pool.
        t1.start();
        // retry until condition variable c1 associated with pool.allocate() by thread t1 inserted
        // in the waiters queue.
        retry(Duration.ofSeconds(1), () -> assertThat(pool.queued()).isEqualTo(1));
        Deque<Condition> waiters = pool.waiters();
        // get the condition object associated with pool.allocate() by thread t1.
        Condition c1 = waiters.getFirst();
        // start thread t2 which will try to allocate more memory on to the memory segment pool.
        t2.start();
        // retry until condition variable c2 associated with pool.allocate() by thread t2  inserted
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
        // both allocate() called by threads t1 and t2 should have been interrupted and the
        // waiters queue should be empty.
        assertThat(pool.queued()).isEqualTo(0);
    }

    @Test
    void testStressfulSituation() throws Exception {
        // This test creates lots of threads that hammer on the memory segment pool.
        int numThreads = 10;
        final int iterations = 50000;
        final int batchSize = 1024;
        final long totalMemory = numThreads / 2 * batchSize;
        WriterMemoryBuffer pool = new WriterMemoryBuffer(totalMemory, batchSize);
        List<StressTestThread> threads = new ArrayList<>();
        for (int i = 0; i < numThreads; i++) {
            threads.add(new StressTestThread(pool, iterations));
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

        assertThat(pool.getAvailableMemory()).isEqualTo(totalMemory);
    }

    @Test
    void testLargeAvailableMemory() throws InterruptedException {
        long memory = 20_000_000_000L;
        int batchSize = 2_000_000_000;
        final AtomicInteger freeSize = new AtomicInteger(0);

        WriterMemoryBuffer pool =
                new WriterMemoryBuffer(memory, batchSize) {
                    @Override
                    protected MemorySegment allocateMemorySegment(int size) {
                        // Ignore size to avoid OOM due to large buffers.
                        return MemorySegment.allocateHeapMemory(0);
                    }

                    @Override
                    protected int numOfFreePages() {
                        return freeSize.get();
                    }
                };

        pool.allocate(batchSize, 0);
        assertThat(pool.getAvailableMemory()).isEqualTo(18_000_000_000L);
        pool.allocate(batchSize, 0);
        assertThat(pool.getAvailableMemory()).isEqualTo(16_000_000_000L);

        // Emulate `deallocate` by increasing `freeSize`.
        freeSize.incrementAndGet();
        assertThat(pool.getAvailableMemory()).isEqualTo(18_000_000_000L);
        freeSize.incrementAndGet();
        assertThat(pool.getAvailableMemory()).isEqualTo(20_000_000_000L);
    }

    @Test
    void testOutOfMemoryOnAllocation() {
        WriterMemoryBuffer pool =
                new WriterMemoryBuffer(1024, 1024) {
                    @Override
                    protected MemorySegment allocateMemorySegment(int size) {
                        throw new OutOfMemoryError();
                    }
                };

        assertThatThrownBy(() -> pool.allocate(1024, maxBlockTimeMs))
                .isInstanceOf(OutOfMemoryError.class);

        assertThat(pool.getAvailableMemory()).isEqualTo(1024);
    }

    @Test
    void testCloseAllocations() throws Exception {
        WriterMemoryBuffer pool = new WriterMemoryBuffer(10, 1);
        MemorySegment segment = pool.allocate(1, Long.MAX_VALUE);

        // Close the memory segment pool. This should prevent any further allocations.
        pool.close();

        assertThatThrownBy(() -> pool.allocate(1, maxBlockTimeMs))
                .isInstanceOf(FlussRuntimeException.class);

        // ensure de-allocation still works.
        pool.deallocate(segment);
    }

    @Test
    void testCloseNotifyWaiters() throws Exception {
        final int numWorkers = 2;

        WriterMemoryBuffer pool = new WriterMemoryBuffer(1, 1);
        MemorySegment segment = pool.allocate(1, Long.MAX_VALUE);

        ExecutorService executor = Executors.newFixedThreadPool(numWorkers);
        Callable<Void> work =
                () -> {
                    assertThatThrownBy(() -> pool.allocate(1, Long.MAX_VALUE))
                            .isInstanceOf(FlussRuntimeException.class);
                    return null;
                };

        for (int i = 0; i < numWorkers; ++i) {
            executor.submit(work);
        }

        retry(Duration.ofSeconds(15), () -> assertThat(pool.queued()).isEqualTo(numWorkers));

        // Close the buffer pool. This should notify all waiters.
        pool.close();

        retry(Duration.ofSeconds(15), () -> assertThat(pool.queued()).isEqualTo(0));

        pool.deallocate(segment);
    }

    private static class MemorySegmentPoolAllocator implements Runnable {
        WriterMemoryBuffer pool;
        long maxBlockTimeMs;

        MemorySegmentPoolAllocator(WriterMemoryBuffer pool, long maxBlockTimeMs) {
            this.pool = pool;
            this.maxBlockTimeMs = maxBlockTimeMs;
        }

        @Override
        public void run() {
            assertThatThrownBy(() -> pool.allocate(2, maxBlockTimeMs))
                    .isInstanceOfAny(BufferExhaustedException.class, InterruptedException.class);
        }
    }

    private static class StressTestThread extends Thread {
        private final int iterations;
        private final WriterMemoryBuffer writerMemoryBuffer;
        public final AtomicBoolean success = new AtomicBoolean(false);

        public StressTestThread(WriterMemoryBuffer writerMemoryBuffer, int iterations) {
            this.iterations = iterations;
            this.writerMemoryBuffer = writerMemoryBuffer;
        }

        @Override
        public void run() {
            try {
                for (int i = 0; i < iterations; i++) {
                    int size;
                    if (RandomUtils.nextBoolean()) {
                        // allocate batch size.
                        size = writerMemoryBuffer.getPageSize();
                    } else {
                        // allocate a random size.
                        size = RandomUtils.nextInt(0, (int) writerMemoryBuffer.getTotalMemory());
                    }

                    long maxBlockTimeMs = 20_000;
                    MemorySegment segment = writerMemoryBuffer.allocate(size, maxBlockTimeMs);
                    writerMemoryBuffer.deallocate(segment);
                }
                success.set(true);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private CountDownLatch asyncDeallocate(
            final WriterMemoryBuffer pool, final MemorySegment segment) {
        final CountDownLatch latch = new CountDownLatch(1);
        Thread thread =
                new Thread(
                        unchecked(
                                () -> {
                                    latch.await();
                                    pool.deallocate(segment);
                                }));
        thread.start();
        return latch;
    }

    private CountDownLatch asyncAllocate(final WriterMemoryBuffer pool, final int size) {
        final CountDownLatch completed = new CountDownLatch(1);
        Thread thread =
                new Thread(
                        () -> {
                            try {
                                pool.allocate(size, maxBlockTimeMs);
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            } finally {
                                completed.countDown();
                            }
                        });
        thread.start();
        return completed;
    }
}
