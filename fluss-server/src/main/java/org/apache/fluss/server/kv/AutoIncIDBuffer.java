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

package org.apache.fluss.server.kv;

import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.server.zk.data.ZkData;
import org.apache.fluss.shaded.curator5.org.apache.curator.framework.CuratorFramework;
import org.apache.fluss.shaded.curator5.org.apache.curator.framework.recipes.atomic.AtomicValue;
import org.apache.fluss.shaded.curator5.org.apache.curator.framework.recipes.atomic.DistributedAtomicLong;
import org.apache.fluss.shaded.curator5.org.apache.curator.retry.BoundedExponentialBackoffRetry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;

import java.util.concurrent.atomic.AtomicLong;

import static org.apache.fluss.utils.Preconditions.checkArgument;

/**
 * A buffer for auto-increment IDs. It buffers a range of IDs from ZooKeeper to
 * reduce the overhead
 * of accessing ZooKeeper.
 */
@ThreadSafe
public class AutoIncIDBuffer {

    private static final Logger LOG = LoggerFactory.getLogger(AutoIncIDBuffer.class);

    private static final int RETRY_TIMES = 10;
    private static final int BASE_SLEEP_MS = 100;
    private static final int MAX_SLEEP_MS = 1000;

    private final DistributedAtomicLong distAtomicLong;
    private final long batchSize;

    // The current available range [localCurrent, localEnd)
    private final AtomicLong localCurrent;
    private final AtomicLong localEnd;

    public AutoIncIDBuffer(
            CuratorFramework zkClient, TablePath tablePath, int columnIdx, long batchSize) {
        this(
                new DistributedAtomicLong(
                        zkClient,
                        ZkData.AutoIncZNode.path(tablePath, columnIdx),
                        new BoundedExponentialBackoffRetry(
                                BASE_SLEEP_MS, MAX_SLEEP_MS, RETRY_TIMES)),
                batchSize);
    }

    @VisibleForTesting
    AutoIncIDBuffer(DistributedAtomicLong distAtomicLong, long batchSize) {
        checkArgument(batchSize > 0, "Batch size must be greater than 0.");
        this.distAtomicLong = distAtomicLong;
        this.batchSize = batchSize;
        this.localCurrent = new AtomicLong(0);
        this.localEnd = new AtomicLong(0);
    }

    /**
     * Get a unique ID. If the buffer is empty, it will fetch a new batch of IDs
     * from ZooKeeper.
     *
     * @return a unique ID.
     */
    public synchronized long getAndIncrement() throws Exception {
        if (localCurrent.get() >= localEnd.get()) {
            // buffer is empty, fetch new batch
            fetchBatch();
        }

        return localCurrent.getAndIncrement();
    }

    private void fetchBatch() throws Exception {
        AtomicValue<Long> value = distAtomicLong.add(batchSize);
        if (value.succeeded()) {
            // value.preValue() is the value BEFORE addition.
            // value.postValue() is the value AFTER addition.
            // Example:
            // Initial ZK value = 0.
            // add(100). pre=0, post=100.
            // We own range: 1 to 100 (inclusive).
            // So logic:
            // start = preValue + 1
            // end = postValue + 1 (exclusive) -> so valid is < end

            long preValue = value.preValue();
            long postValue = value.postValue();

            localCurrent.set(preValue + 1);
            localEnd.set(postValue + 1);

            LOG.info("Fetched new batch of IDs: [{}, {}).", localCurrent.get(), localEnd.get());
        } else {
            throw new Exception("Failed to allocate ID range from ZooKeeper.");
        }
    }
}
