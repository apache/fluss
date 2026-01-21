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

package org.apache.fluss.server.zk;

import org.apache.fluss.server.SequenceIDCounter;
import org.apache.fluss.shaded.curator5.org.apache.curator.framework.CuratorFramework;
import org.apache.fluss.shaded.curator5.org.apache.curator.framework.recipes.atomic.AtomicValue;
import org.apache.fluss.shaded.curator5.org.apache.curator.framework.recipes.atomic.DistributedAtomicLong;
import org.apache.fluss.shaded.curator5.org.apache.curator.retry.BoundedExponentialBackoffRetry;

import javax.annotation.concurrent.ThreadSafe;

/** An implementation of {@link SequenceIDCounter} with zookeeper. */
@ThreadSafe
public class ZkSequenceIDCounter implements SequenceIDCounter {

    // maybe make it as configurable
    private static final int RETRY_TIMES = 10;
    private static final int BASE_SLEEP_MS = 100;
    private static final int MAX_SLEEP_MS = 1000;

    private final String sequenceIDPath;
    private final DistributedAtomicLong sequenceIdCounter;

    public ZkSequenceIDCounter(CuratorFramework curatorClient, String sequenceIDPath) {
        this.sequenceIDPath = sequenceIDPath;
        sequenceIdCounter =
                new DistributedAtomicLong(
                        curatorClient,
                        sequenceIDPath,
                        new BoundedExponentialBackoffRetry(
                                BASE_SLEEP_MS, MAX_SLEEP_MS, RETRY_TIMES));
    }

    @Override
    public long getCurrent() throws Exception {
        return sequenceIdCounter.get().postValue();
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
            throw new Exception(
                    String.format(
                            "Failed to increment sequence id counter. ZooKeeper sequence ID path: %s.",
                            sequenceIDPath));
        }
    }

    /**
     * Atomically adds the given delta to the current sequence ID.
     *
     * @return The previous sequence ID
     */
    @Override
    public long getAndAdd(Long delta) throws Exception {
        AtomicValue<Long> incrementValue = sequenceIdCounter.add(delta);
        if (incrementValue.succeeded()) {
            return incrementValue.preValue();
        } else {
            throw new Exception(
                    String.format(
                            "Failed to increment sequence id counter. ZooKeeper sequence ID path: %s, Delta value: %d.",
                            sequenceIDPath, delta));
        }
    }
}
