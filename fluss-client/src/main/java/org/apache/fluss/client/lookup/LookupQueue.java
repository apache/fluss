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

package org.apache.fluss.client.lookup;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;

import javax.annotation.concurrent.ThreadSafe;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * A queue that buffers the pending lookup operations and provides a list of {@link LookupQuery}
 * when call method {@link #drain()}.
 */
@ThreadSafe
@Internal
class LookupQueue {

    private volatile boolean closed;
    // buffering both the Lookup and PrefixLookup.
    private final ArrayBlockingQueue<AbstractLookupQuery<?>> lookupQueue;
    private final int maxBatchSize;
    private final long batchTimeoutNanos;

    LookupQueue(Configuration conf) {
        this.lookupQueue =
                new ArrayBlockingQueue<>(conf.get(ConfigOptions.CLIENT_LOOKUP_QUEUE_SIZE));
        this.maxBatchSize = conf.get(ConfigOptions.CLIENT_LOOKUP_MAX_BATCH_SIZE);
        this.batchTimeoutNanos = conf.get(ConfigOptions.CLIENT_LOOKUP_BATCH_TIMEOUT).toNanos();
        this.closed = false;
    }

    void appendLookup(AbstractLookupQuery<?> lookup) {
        if (closed) {
            throw new IllegalStateException(
                    "Can not append lookup operation since the LookupQueue is closed.");
        }

        try {
            lookupQueue.put(lookup);
        } catch (InterruptedException e) {
            lookup.future().completeExceptionally(e);
        }
    }

    boolean hasUnDrained() {
        return !lookupQueue.isEmpty();
    }

    /** Drain a batch of {@link LookupQuery}s from the lookup queue. */
    List<AbstractLookupQuery<?>> drain() throws Exception {
        final long startNanos = System.nanoTime();
        List<AbstractLookupQuery<?>> lookupOperations = new ArrayList<>(maxBatchSize);
        int count = 0;
        while (true) {
            long waitNanos = batchTimeoutNanos - (System.nanoTime() - startNanos);
            if (waitNanos <= 0) {
                break;
            }

            AbstractLookupQuery<?> lookup = lookupQueue.poll(waitNanos, TimeUnit.NANOSECONDS);
            if (lookup == null) {
                break;
            }
            lookupOperations.add(lookup);
            count++;
            int transferred = lookupQueue.drainTo(lookupOperations, maxBatchSize - count);
            count += transferred;
            if (count >= maxBatchSize) {
                break;
            }
        }
        return lookupOperations;
    }

    /** Drain all the {@link LookupQuery}s from the lookup queue. */
    List<AbstractLookupQuery<?>> drainAll() {
        List<AbstractLookupQuery<?>> lookupOperations = new ArrayList<>(lookupQueue.size());
        lookupQueue.drainTo(lookupOperations);
        return lookupOperations;
    }

    public void close() {
        closed = true;
    }
}
