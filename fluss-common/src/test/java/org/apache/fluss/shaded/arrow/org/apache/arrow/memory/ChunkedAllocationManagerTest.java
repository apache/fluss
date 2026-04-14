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

package org.apache.fluss.shaded.arrow.org.apache.arrow.memory;

import org.apache.fluss.shaded.arrow.org.apache.arrow.memory.ChunkedAllocationManager.ChunkedFactory;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link ChunkedAllocationManager}. */
class ChunkedAllocationManagerTest {

    /** Use a small chunk size (1KB) for testing to make chunk transitions easy to trigger. */
    private static final long TEST_CHUNK_SIZE = 1024;

    private static final int TEST_MAX_FREE_CHUNKS = 2;

    private ChunkedFactory factory;
    private BufferAllocator allocator;

    @BeforeEach
    void setUp() {
        factory = new ChunkedFactory(TEST_CHUNK_SIZE, TEST_MAX_FREE_CHUNKS);
        allocator = BufferAllocatorUtil.createBufferAllocator(factory);
    }

    @AfterEach
    void tearDown() {
        allocator.close();
        factory.close();
    }

    @Test
    void testSmallAllocationPackedIntoSameChunk() {
        // Multiple small allocations should land in the same chunk (contiguous addresses).
        ArrowBuf buf1 = allocator.buffer(64);
        ArrowBuf buf2 = allocator.buffer(128);
        ArrowBuf buf3 = allocator.buffer(32);
        // The gap between buf1 and buf2 should be exactly the aligned size of buf1 (64 bytes,
        // already 8-byte aligned).
        assertThat(buf2.memoryAddress() - buf1.memoryAddress()).isEqualTo(64);
        // buf2 is 128 bytes, 8-byte aligned → gap of 128.
        assertThat(buf3.memoryAddress() - buf2.memoryAddress()).isEqualTo(128);

        buf1.close();
        buf2.close();
        buf3.close();
    }

    @Test
    void testAlignmentBytes() {
        // Allocate a non-8-byte-aligned size; the next allocation should still be 8-byte aligned.
        ArrowBuf buf1 = allocator.buffer(7); // 7 bytes → aligned to 8
        ArrowBuf buf2 = allocator.buffer(1);

        assertThat(buf2.memoryAddress() - buf1.memoryAddress()).isEqualTo(8);
        buf1.close();
        buf2.close();
    }

    @Test
    void testAllocateNewChunkWhenFull() {
        // Fill the first chunk (1KB) and verify a new chunk is allocated.
        // Each allocation is 256 bytes → 4 allocations fill the chunk.
        List<ArrowBuf> bufs = new ArrayList<>();
        for (int i = 0; i < 4; i++) {
            bufs.add(allocator.buffer(256));
        }

        // The first 4 buffers should be contiguous within one chunk.
        long chunkBaseAddress = bufs.get(0).memoryAddress();
        for (int i = 1; i < 4; i++) {
            assertThat(bufs.get(i).memoryAddress()).isEqualTo(chunkBaseAddress + (long) i * 256);
        }

        assertThat(factory.getActiveChunk().used).isEqualTo(TEST_CHUNK_SIZE);
        // The 5th allocation should land in a new chunk.
        ArrowBuf overflow = allocator.buffer(256);
        // The active chunk should now be a fresh chunk with only the overflow allocation,
        // proving a new chunk was created (rather than checking memory address which is
        // non-deterministic — the OS may allocate the new chunk right after the first one).
        assertThat(factory.getActiveChunk().used).isEqualTo(256);

        overflow.close();
        bufs.forEach(ArrowBuf::close);
    }

    @Test
    void testChunkRecycledAfterAllBuffersReleased() {
        // Allocate a buffer, release it (drains the chunk), then allocate again — should reuse.
        ArrowBuf buf1 = allocator.buffer(64);
        long firstAddress = buf1.memoryAddress();
        buf1.close();

        // The chunk was drained (subAllocCount=0) and should be recycled (reset bump pointer).
        // Next allocation should reuse the same chunk starting from offset 0.
        ArrowBuf buf2 = allocator.buffer(64);
        assertThat(buf2.memoryAddress()).isEqualTo(firstAddress);

        buf2.close();
    }

    @Test
    void testBatchAllocateAndReleaseCycle() {
        // Simulate the batch-friendly lifecycle: allocate many, release all, allocate again.
        int bufferCount = 10;
        List<ArrowBuf> batch1 = new ArrayList<>();
        for (int i = 0; i < bufferCount; i++) {
            batch1.add(allocator.buffer(64));
        }
        long batch1BaseAddress = batch1.get(0).memoryAddress();

        // Release all — chunk should be recycled.
        batch1.forEach(ArrowBuf::close);

        // Second batch should reuse the same chunk.
        List<ArrowBuf> batch2 = new ArrayList<>();
        for (int i = 0; i < bufferCount; i++) {
            batch2.add(allocator.buffer(64));
        }
        assertThat(batch2.get(0).memoryAddress()).isEqualTo(batch1BaseAddress);

        batch2.forEach(ArrowBuf::close);
    }

    @Test
    void testFreeListCapRespected() {
        // Create multiple chunks, release them all — only maxFreeChunks should be cached.
        // chunkSize=1024, allocate 512 per chunk to create distinct chunks.
        List<ArrowBuf> chunk1Bufs = new ArrayList<>();
        // Fill chunk 1 (1024 bytes)
        chunk1Bufs.add(allocator.buffer(512));
        chunk1Bufs.add(allocator.buffer(512));

        // Fill chunk 2 (triggers new chunk)
        List<ArrowBuf> chunk2Bufs = new ArrayList<>();
        chunk2Bufs.add(allocator.buffer(512));
        chunk2Bufs.add(allocator.buffer(512));

        // Fill chunk 3 (triggers new chunk)
        List<ArrowBuf> chunk3Bufs = new ArrayList<>();
        chunk3Bufs.add(allocator.buffer(512));
        chunk3Bufs.add(allocator.buffer(512));

        // Fill chunk 4 (triggers new chunk)
        List<ArrowBuf> chunk4Bufs = new ArrayList<>();
        chunk4Bufs.add(allocator.buffer(512));
        chunk4Bufs.add(allocator.buffer(512));

        // Release all chunks — free-list can hold only 2 (maxFreeChunks=2).
        // Active chunk gets reset, inactive ones go to free-list or are destroyed.
        chunk1Bufs.forEach(ArrowBuf::close);
        chunk2Bufs.forEach(ArrowBuf::close);
        chunk3Bufs.forEach(ArrowBuf::close);
        chunk4Bufs.forEach(ArrowBuf::close);

        assertThat(factory.getFreeChunks()).hasSize(TEST_MAX_FREE_CHUNKS);
        assertThat(factory.getActiveChunk()).isNotNull();
        assertThat(factory.getActiveChunk().used).isEqualTo(0L);
    }

    @Test
    void testEmptyBufferReturnedFromFactory() {
        ArrowBuf empty = factory.empty();
        assertThat(empty).isNotNull();
        assertThat(empty.capacity()).isEqualTo(0);
    }

    @Test
    void testMultipleSmallBatches() {
        // Verify stable memory across multiple batch cycles(smaller than batch size).
        for (int batch = 0; batch < 5; batch++) {
            List<ArrowBuf> bufs = new ArrayList<>();
            for (int i = 0; i < 50; i++) {
                bufs.add(allocator.buffer(16));
            }
            bufs.forEach(ArrowBuf::close);
        }

        assertThat(factory.getFreeChunks()).hasSize(0);
        assertThat(factory.getActiveChunk()).isNotNull();
        assertThat(factory.getActiveChunk().used).isEqualTo(0L);
    }

    @Test
    void testFactoryCloseReleasesResources() {
        ChunkedFactory localFactory = new ChunkedFactory(TEST_CHUNK_SIZE, TEST_MAX_FREE_CHUNKS);
        BufferAllocator localAllocator = BufferAllocatorUtil.createBufferAllocator(localFactory);

        ArrowBuf buf = localAllocator.buffer(64);
        buf.close();

        // close the allocator.
        localAllocator.close();
        localFactory.close();
        assertThat(localFactory.getFreeChunks()).isEmpty();
        assertThat(localFactory.getActiveChunk()).isNull();
    }

    @Test
    void testConcurrentAllocateAndRelease() throws Exception {
        // Stress test for the double-recycle race: multiple threads sharing one factory,
        // each doing allocate -> close in a tight loop. Before the drainGeneration fix,
        // onChunkDrained could be called twice for the same drain event, causing double-recycle
        // (duplicate entries in freeChunks) or use-after-free (native memory corruption).
        int threadCount = 8;
        int iterationsPerThread = 500;
        ChunkedFactory concurrentFactory =
                new ChunkedFactory(TEST_CHUNK_SIZE, TEST_MAX_FREE_CHUNKS);
        BufferAllocator concurrentAllocator =
                BufferAllocatorUtil.createBufferAllocator(concurrentFactory);

        CyclicBarrier barrier = new CyclicBarrier(threadCount);
        AtomicReference<Throwable> failure = new AtomicReference<>();

        Thread[] threads = new Thread[threadCount];
        for (int t = 0; t < threadCount; t++) {
            threads[t] =
                    new Thread(
                            () -> {
                                try {
                                    barrier.await();
                                    for (int i = 0; i < iterationsPerThread; i++) {
                                        ArrowBuf buf = concurrentAllocator.buffer(64);
                                        buf.close();
                                    }
                                } catch (Throwable ex) {
                                    failure.compareAndSet(null, ex);
                                }
                            });
            threads[t].start();
        }

        for (Thread thread : threads) {
            thread.join();
        }

        assertThat(failure.get()).isNull();

        // After all threads finish, the free-list must not exceed maxFreeChunks
        // (would indicate double-recycle).
        assertThat(concurrentFactory.getFreeChunks().size())
                .isLessThanOrEqualTo(TEST_MAX_FREE_CHUNKS);

        concurrentAllocator.close();
        concurrentFactory.close();
    }

    @Test
    void testCloseWhileArrowBufsAliveDoesNotLeakMemory() {
        // Verify that closing the factory while ArrowBufs are still alive does not leak memory.
        ChunkedFactory localFactory = new ChunkedFactory(TEST_CHUNK_SIZE, TEST_MAX_FREE_CHUNKS);
        BufferAllocator localAllocator = BufferAllocatorUtil.createBufferAllocator(localFactory);

        // Allocate some buffers so the active chunk has outstanding sub-allocations.
        List<ArrowBuf> bufs = new ArrayList<>();
        for (int i = 0; i < 4; i++) {
            bufs.add(localAllocator.buffer(64));
        }

        // Close factory while ArrowBufs are still alive.

        localFactory.close();

        // Active chunk was not destroyed (subAllocCount > 0), just nulled.
        assertThat(localFactory.getActiveChunk()).isNull();

        // Release all ArrowBufs — onChunkDrained should destroy the chunk, not recycle it.
        bufs.forEach(ArrowBuf::close);

        // The chunk must NOT have been added to freeChunks (would be a leak).
        assertThat(localFactory.getFreeChunks()).isEmpty();
        localAllocator.close();
    }
}
