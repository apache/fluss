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

package org.apache.fluss.server.replica;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.metadata.TableBucket;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Semaphore;

/**
 * Handles historical partition requests with flow control and per-bucket serial execution.
 *
 * <p>This handler provides two key mechanisms:
 *
 * <ul>
 *   <li><b>Flow control</b>: A bounded semaphore limits the total number of in-flight historical
 *       requests. Each request acquires one permit regardless of how many buckets it contains. When
 *       the limit is reached, new requests are rejected so the client can back off and retry.
 *   <li><b>Per-bucket serial execution</b>: Write tasks to the same bucket are executed in FIFO
 *       order to preserve write ordering. Different buckets can execute concurrently.
 * </ul>
 *
 * <p>Lookup tasks do not require ordering guarantees and are submitted directly to the executor for
 * concurrent execution.
 *
 * <p>Usage: call {@link #tryAcquire()} to acquire one request permit. If successful, submit tasks
 * via {@link #submitWrite} or {@link #submitLookup}. When the entire request completes (all tasks
 * done), call {@link #release()} to return the permit.
 */
@Internal
@ThreadSafe
public class HistoricalPartitionHandler {

    private static final Logger LOG = LoggerFactory.getLogger(HistoricalPartitionHandler.class);

    private final Semaphore requestPermits;
    private final ExecutorService ioExecutor;
    private final int capacity;

    private final Object bucketQueuesLock = new Object();
    private final Map<TableBucket, BucketTaskQueue> bucketQueues = new HashMap<>();

    public HistoricalPartitionHandler(ExecutorService ioExecutor, int capacity) {
        this.ioExecutor = ioExecutor;
        this.capacity = Math.max(1, capacity);
        this.requestPermits = new Semaphore(this.capacity);
    }

    /**
     * Tries to acquire one request permit. Returns {@code true} if the permit was acquired, {@code
     * false} if the request queue is full.
     *
     * <p>After a successful acquisition, the caller must eventually call {@link #release()} when
     * all tasks for the request have completed.
     */
    public boolean tryAcquire() {
        return requestPermits.tryAcquire();
    }

    /**
     * Releases one request permit. Must be called exactly once per successful {@link
     * #tryAcquire()}.
     */
    public void release() {
        requestPermits.release();
    }

    /**
     * Submits a write task for the given bucket. Write tasks to the same bucket are executed in
     * FIFO order (serial). Write tasks to different buckets can execute concurrently.
     *
     * <p>The caller must have already acquired a permit via {@link #tryAcquire()}.
     *
     * @param bucket the target bucket
     * @param task the write task to execute
     */
    public void submitWrite(TableBucket bucket, Runnable task) {
        enqueueWriteTask(bucket, task);
    }

    /**
     * Submits a lookup task for concurrent execution. Lookups do not need per-bucket ordering.
     *
     * <p>The caller must have already acquired a permit via {@link #tryAcquire()}.
     *
     * @param task the lookup task to execute
     */
    public void submitLookup(Runnable task) {
        ioExecutor.execute(task);
    }

    /** Returns the configured capacity of the request queue. */
    @VisibleForTesting
    int getCapacity() {
        return capacity;
    }

    /** Returns the number of currently available permits. */
    @VisibleForTesting
    int availablePermits() {
        return requestPermits.availablePermits();
    }

    private void enqueueWriteTask(TableBucket bucket, Runnable task) {
        boolean shouldSubmit;
        synchronized (bucketQueuesLock) {
            BucketTaskQueue queue =
                    bucketQueues.computeIfAbsent(bucket, k -> new BucketTaskQueue());
            queue.tasks.addLast(task);
            shouldSubmit = !queue.running;
            if (shouldSubmit) {
                queue.running = true;
            }
        }
        if (shouldSubmit) {
            ioExecutor.execute(() -> drainBucketQueue(bucket));
        }
    }

    private void drainBucketQueue(TableBucket bucket) {
        while (true) {
            Runnable task;
            synchronized (bucketQueuesLock) {
                BucketTaskQueue queue = bucketQueues.get(bucket);
                if (queue == null || queue.tasks.isEmpty()) {
                    if (queue != null) {
                        queue.running = false;
                        bucketQueues.remove(bucket);
                    }
                    return;
                }
                task = queue.tasks.pollFirst();
            }
            try {
                task.run();
            } catch (Exception e) {
                LOG.error(
                        "Error executing historical partition write task for bucket {}", bucket, e);
            }
        }
    }

    /** Internal queue for per-bucket task ordering. */
    private static final class BucketTaskQueue {
        final Deque<Runnable> tasks = new ArrayDeque<>();
        boolean running = false;
    }
}
