/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
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

package com.alibaba.fluss.server.zk;

import com.alibaba.fluss.server.SequenceIDCounter;
import com.alibaba.fluss.shaded.curator5.org.apache.curator.framework.CuratorFramework;
import com.alibaba.fluss.shaded.curator5.org.apache.curator.framework.recipes.atomic.AtomicValue;
import com.alibaba.fluss.shaded.curator5.org.apache.curator.framework.recipes.atomic.DistributedAtomicLong;
import com.alibaba.fluss.shaded.curator5.org.apache.curator.retry.RetryNTimes;

import javax.annotation.concurrent.ThreadSafe;

/** An implementation of {@link SequenceIDCounter} with zookeeper. */
@ThreadSafe
public class ZkSequenceIDCounter implements SequenceIDCounter {

    // maybe make it as configurable
    private static final int RETRY_TIMES = 10;
    private static final int RETRY_INTERVAL_MS = 100;

    private final DistributedAtomicLong sequenceIdCounter;

    public ZkSequenceIDCounter(CuratorFramework curatorClient, String sequenceIDPath) {
        sequenceIdCounter =
                new DistributedAtomicLong(
                        curatorClient,
                        sequenceIDPath,
                        new RetryNTimes(RETRY_TIMES, RETRY_INTERVAL_MS));
    }

    /**
     * Atomically increments the current sequence ID.
     *
     * @return The previous sequence ID
     */
    @Override
    public long getAndIncrement() throws Exception {
        AtomicValue<Long> incrementValue = sequenceIdCounter.increment();
        if (incrementValue.succeeded()) {
            return incrementValue.preValue();
        } else {
            throw new Exception("Failed to increment sequence id counter.");
        }
    }
}
