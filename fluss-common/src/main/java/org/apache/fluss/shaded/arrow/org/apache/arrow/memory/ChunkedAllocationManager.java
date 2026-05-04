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

import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.shaded.arrow.org.apache.arrow.memory.util.MemoryUtil;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * An {@link AllocationManager} that packs small allocations into large pre-allocated chunks using a
 * bump-pointer strategy, specifically designed for Arrow column decompression in high-column-count
 * scenarios (e.g., 1000+ columns).
 *
 * <h3>Why not Netty's PooledByteBufAllocator?</h3>
 *
 * <p>Arrow column decompression produces a burst of ~3000 small, variable-sized allocations (1000
 * columns x ~3 buffers each: validity, offset, data). Netty's allocator is optimized for a
 * different pattern and causes problems here:
 *
 * <table border="1" cellpadding="4">
 *   <tr><th>Aspect</th><th>Netty PooledByteBufAllocator</th><th>ChunkedAllocationManager</th></tr>
 *   <tr>
 *     <td>Small allocation</td>
 *     <td>Routes through size-class subpages (tiny/small/normal). Different sizes land on
 *         different subpages within different pages of the <b>same or different chunks</b>,
 *         causing cross-chunk fragmentation.</td>
 *     <td>Sequential bump-pointer within the current chunk. All sizes packed contiguously into
 *         <b>one chunk</b> regardless of size class.</td>
 *   </tr>
 *   <tr>
 *     <td>Chunk selection</td>
 *     <td>Tries q050 &rarr; q025 &rarr; q000 &rarr; qInit &rarr; q075, preferring
 *         moderately-used chunks. May <b>spread allocations across multiple chunks</b>.</td>
 *     <td>Always uses the current active chunk. Only switches when <b>full</b>.</td>
 *   </tr>
 *   <tr>
 *     <td>Chunk reclamation</td>
 *     <td>A chunk is freed only when it is in {@code q000} and usage drops to 0%. But
 *         cross-size-class subpage pinning often prevents any single chunk from reaching 0%.
 *         The first chunk per arena (in {@code qInit}) is <b>never freed</b>.</td>
 *     <td>When all sub-allocations in a chunk are released ({@code subAllocCount} reaches 0),
 *         the chunk is <b>immediately recycled</b> to the free-list.</td>
 *   </tr>
 *   <tr>
 *     <td>Thread-local caching</td>
 *     <td>{@code PoolThreadCache} holds released buffers in thread-local caches, delaying their
 *         return to the chunk. Cache is trimmed only every 8192 allocations or on thread exit,
 *         making chunk usage appear higher than actual.</td>
 *     <td>No thread-local caching. Release decrements {@code subAllocCount} immediately.</td>
 *   </tr>
 *   <tr>
 *     <td>Typical result for 1000-column decompression</td>
 *     <td>3000 buffers spread across 2-5 chunks. After batch release, chunks remain partially
 *         used due to subpage pinning + thread cache. <b>Memory grows over successive batches
 *         &rarr; OOM.</b></td>
 *     <td>3000 buffers land in 1-2 chunks. After batch release, all chunks immediately
 *         recyclable. <b>Stable memory footprint across batches.</b></td>
 *   </tr>
 * </table>
 *
 * <h3>How it works</h3>
 *
 * <ul>
 *   <li>Allocates large chunks (default 4MB) from native memory via {@code Unsafe}.
 *   <li>For each small allocation request, bumps a pointer within the current active chunk.
 *   <li>Only switches to a new chunk when the current one has no room.
 *   <li>Reference-counts each chunk: when all sub-allocations are released, the chunk is recycled
 *       into a free-list for reuse instead of being freed to the OS.
 * </ul>
 *
 * <h3>Batch-friendly lifecycle</h3>
 *
 * <pre>
 *   Batch N:  allocator.buffer() x 3000  &rarr;  all land in chunk A (bump pointer)
 *             process batch...
 *             release all buffers          &rarr;  subAllocCount = 0 &rarr; chunk A recycled
 *
 *   Batch N+1: allocator.buffer() x 3000  &rarr;  reuse chunk A (zero malloc/free)
 * </pre>
 *
 * <h3>Usage</h3>
 *
 * <pre>{@code
 * BufferAllocator allocator = new RootAllocator(
 *         BaseAllocator.configBuilder()
 *                 .allocationManagerFactory(new ChunkedAllocationManager.ChunkedFactory())
 *                 .build());
 * }</pre>
 *
 * <p>For allocations >= chunkSize, a dedicated memory region is allocated directly (no bump
 * pointer), behaving identically to {@code UnsafeAllocationManager}.
 */
public class ChunkedAllocationManager extends AllocationManager {

    /** 8-byte alignment for all sub-allocations within a chunk. */
    private static final long ALIGNMENT = 8;

    /** Default chunk size: 4MB (matches Netty 4.1+ maxOrder=9). */
    private static final long DEFAULT_CHUNK_SIZE = 4L * 1024 * 1024;

    /** Default maximum number of empty chunks to keep in the free-list. */
    private static final int DEFAULT_MAX_FREE_CHUNKS = 3;

    private final long allocatedSize;

    // --- Fields for sub-allocation (small request carved from a shared chunk) ---
    private final Chunk chunk;
    private final long offsetInChunk;

    // --- Fields for direct allocation (large request, owns its own memory) ---
    private final long directAddress;

    /** Sub-allocation carved from a shared {@link Chunk}. */
    private ChunkedAllocationManager(
            BufferAllocator accountingAllocator, Chunk chunk, long offset, long size) {
        super(accountingAllocator);
        this.chunk = chunk;
        this.offsetInChunk = offset;
        this.allocatedSize = size;
        this.directAddress = 0;
    }

    /** Direct allocation for oversized requests (>= chunkSize). Owns its own memory region. */
    private ChunkedAllocationManager(BufferAllocator accountingAllocator, long address, long size) {
        super(accountingAllocator);
        this.chunk = null;
        this.offsetInChunk = 0;
        this.allocatedSize = size;
        this.directAddress = address;
    }

    @Override
    public long getSize() {
        return allocatedSize;
    }

    @Override
    protected long memoryAddress() {
        if (chunk != null) {
            return chunk.address + offsetInChunk;
        }
        return directAddress;
    }

    @Override
    protected void release0() {
        if (chunk != null) {
            chunk.releaseSubAllocation();
        } else {
            MemoryUtil.UNSAFE.freeMemory(directAddress);
        }
    }

    // -------------------------------------------------------------------------
    // Chunk — a contiguous memory region with bump-pointer sub-allocation
    // -------------------------------------------------------------------------

    /**
     * A contiguous native memory region that holds multiple small allocations via bump-pointer.
     * Reference-counted: when all sub-allocations are released (count reaches 0), the chunk is
     * recycled back to the factory's free-list.
     */
    static class Chunk {
        final long address;
        final long capacity;
        /** Bump pointer — only accessed under the factory's synchronized lock. */
        long used;
        /**
         * Number of active sub-allocations. Decremented from arbitrary threads when ArrowBuf
         * instances are released.
         */
        final AtomicInteger subAllocCount = new AtomicInteger(0);
        /**
         * Generation counter incremented each time this chunk is successfully drained. Used to
         * prevent ABA double-recycle: a caller snapshots the generation before the atomic
         * decrement; if the generation has changed by the time {@link
         * ChunkedFactory#onChunkDrained} runs, the drain is stale and must be skipped.
         */
        volatile int drainGeneration;
        /** Back-reference to the owning factory for recycling on drain. */
        final ChunkedFactory factory;

        Chunk(long capacity, ChunkedFactory factory) {
            this.address = MemoryUtil.UNSAFE.allocateMemory(capacity);
            this.capacity = capacity;
            this.used = 0;
            this.factory = factory;
        }

        /** Returns true if this chunk has room for an aligned allocation of the given size. */
        boolean hasRoom(long alignedSize) {
            return used + alignedSize <= capacity;
        }

        /**
         * Bump-allocates {@code alignedSize} bytes from this chunk. Must be called under the
         * factory lock.
         *
         * @return the offset within this chunk where the allocation starts.
         */
        long bumpAllocate(long alignedSize) {
            long offset = used;
            used += alignedSize;
            subAllocCount.incrementAndGet();
            return offset;
        }

        /**
         * Called when a sub-allocation's ArrowBuf is released. If the chunk is now empty (all
         * sub-allocations freed), notifies the factory for recycling.
         *
         * <h4>Why not always call {@code onChunkDrained}?</h4>
         *
         * <p>Unconditionally calling {@link ChunkedFactory#onChunkDrained} on every release would
         * be thread-safe (the method is {@code synchronized}), but prohibitively expensive: in a
         * high-column scenario (e.g., 1000 columns x 3 buffers = 3000 releases per batch), every
         * release would contend for the factory lock even though only the very last one actually
         * needs to recycle the chunk.
         *
         * <p>To avoid this, we first perform a lock-free check: {@code
         * subAllocCount.decrementAndGet() == 0}, and only enter the synchronized {@code
         * onChunkDrained} when the chunk is truly empty. This reduces lock contention from O(N) to
         * O(1) per drain cycle.
         *
         * <h4>ABA double-recycle problem</h4>
         *
         * <p>However, the gap between {@code subAllocCount.decrementAndGet() == 0} and the
         * subsequent {@code onChunkDrained} call is <b>not atomic</b>. During this window another
         * thread can complete a full allocate-release cycle on the same chunk, triggering a second
         * legitimate drain. Without protection, both callers would enter {@code onChunkDrained} and
         * recycle the same chunk twice (duplicate free-list entry or use-after-free).
         *
         * <p>The {@link Chunk#drainGeneration} snapshot solves this: we capture the generation
         * <i>before</i> the atomic decrement. Inside {@code onChunkDrained}, the generation is
         * compared and incremented atomically under the lock, so only the first caller succeeds;
         * the stale caller sees a mismatch and is skipped.
         */
        void releaseSubAllocation() {
            int gen = drainGeneration;
            if (subAllocCount.decrementAndGet() == 0) {
                factory.onChunkDrained(this, gen);
            }
        }

        /** Resets the bump pointer so the chunk can be reused for new allocations. */
        void resetBump() {
            used = 0;
            // subAllocCount is already 0 at this point.
        }

        /** Frees the underlying native memory. */
        void destroy() {
            MemoryUtil.UNSAFE.freeMemory(address);
        }
    }

    // -------------------------------------------------------------------------
    // Factory
    // -------------------------------------------------------------------------

    /**
     * Factory that creates {@link ChunkedAllocationManager} instances.
     *
     * <p>Small allocations (< chunkSize) are packed into the current active chunk via bump-pointer.
     * When the active chunk is full, a recycled or freshly-allocated chunk is used. Large
     * allocations (>= chunkSize) get their own dedicated memory region.
     *
     * <p>This factory is thread-safe: {@link #create} and {@link #onChunkDrained} are {@code
     * synchronized}.
     */
    public static class ChunkedFactory implements AllocationManager.Factory {

        private final long chunkSize;
        private final int maxFreeChunks;

        /** The chunk currently receiving bump allocations. May be null initially. */
        private Chunk activeChunk;

        /** Pool of empty chunks available for reuse. */
        private final Deque<Chunk> freeChunks = new ArrayDeque<>();

        /** Set to true when {@link #close()} is called. */
        private boolean closed;

        /** Creates a factory with default chunk size (4MB) and max 3 free chunks. */
        public ChunkedFactory() {
            this(DEFAULT_CHUNK_SIZE, DEFAULT_MAX_FREE_CHUNKS);
        }

        /**
         * Creates a factory with the given chunk size and free-chunk pool limit.
         *
         * @param chunkSize maximum size of each chunk (bytes). Allocations >= this go direct.
         * @param maxFreeChunks maximum number of empty chunks to keep cached for reuse.
         */
        public ChunkedFactory(long chunkSize, int maxFreeChunks) {
            this.chunkSize = chunkSize;
            this.maxFreeChunks = maxFreeChunks;
        }

        @Override
        public synchronized AllocationManager create(
                BufferAllocator accountingAllocator, long size) {
            if (size > chunkSize) {
                // Large allocation: give it its own memory region.
                long address = MemoryUtil.UNSAFE.allocateMemory(size);
                return new ChunkedAllocationManager(accountingAllocator, address, size);
            }

            // Align to 8 bytes for safe direct-memory access.
            long alignedSize = (size + ALIGNMENT - 1) & ~(ALIGNMENT - 1);

            if (activeChunk == null || !activeChunk.hasRoom(alignedSize)) {
                // Current chunk is full or doesn't exist — obtain a recycled or new chunk.
                activeChunk = obtainChunk();
            }

            long offset = activeChunk.bumpAllocate(alignedSize);
            return new ChunkedAllocationManager(accountingAllocator, activeChunk, offset, size);
        }

        @Override
        public ArrowBuf empty() {
            return NettyAllocationManager.EMPTY_BUFFER;
        }

        /**
         * Obtains a chunk for use as the new active chunk. Prefers recycled chunks from the
         * free-list; falls back to allocating a fresh one.
         */
        private synchronized Chunk obtainChunk() {
            Chunk recycled = freeChunks.pollFirst();
            if (recycled != null) {
                recycled.resetBump();
                return recycled;
            }
            return new Chunk(chunkSize, this);
        }

        /**
         * Called when a chunk's sub-allocation count reaches zero (all ArrowBufs released).
         * Recycles the chunk if possible, or frees its memory if the free-list is full.
         *
         * <p>Thread-safety: between {@code subAllocCount} reaching 0 and this method acquiring the
         * lock, another thread may complete a full allocate-release cycle on the same chunk (ABA
         * problem). The {@code expectedGeneration} parameter detects this: each successful drain
         * increments the generation, so a stale caller's snapshot will mismatch.
         *
         * @param chunk the chunk whose sub-allocation count just reached zero.
         * @param expectedGeneration the drain generation snapshot taken before the atomic
         *     decrement. If it no longer matches, another drain already processed this event.
         */
        synchronized void onChunkDrained(Chunk chunk, int expectedGeneration) {
            // ABA guard: if another thread already drained and recycled this chunk, skip.
            if (chunk.drainGeneration != expectedGeneration) {
                return;
            }
            // Simple re-allocation guard: new sub-allocations arrived after our decrement.
            if (chunk.subAllocCount.get() > 0) {
                return;
            }
            chunk.drainGeneration++;

            if (closed) {
                // Factory is closed — no one will ever reuse this chunk. Free it.
                chunk.destroy();
            } else if (chunk == activeChunk) {
                // Still the active chunk — just reset bump pointer for continued use.
                chunk.resetBump();
            } else if (freeChunks.size() < maxFreeChunks) {
                // Not active, pool has room — recycle.
                freeChunks.offerFirst(chunk);
            } else {
                // Pool is full — free native memory.
                chunk.destroy();
            }
        }

        /**
         * Closes this factory, freeing all cached and idle chunks. Active chunks with outstanding
         * sub-allocations will be freed when their last ArrowBuf is released (see {@link
         * #onChunkDrained}).
         */
        public synchronized void close() {
            closed = true;
            while (!freeChunks.isEmpty()) {
                Chunk poll = freeChunks.poll();
                poll.destroy();
            }
            if (activeChunk != null && activeChunk.subAllocCount.get() == 0) {
                activeChunk.destroy();
            }
            activeChunk = null;
        }

        @VisibleForTesting
        Deque<Chunk> getFreeChunks() {
            return freeChunks;
        }

        @VisibleForTesting
        Chunk getActiveChunk() {
            return activeChunk;
        }
    }
}
