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

import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.server.zk.ZooKeeperExtension;
import org.apache.fluss.shaded.curator5.org.apache.curator.framework.CuratorFramework;
import org.apache.fluss.server.zk.NOPErrorHandler;
import org.apache.fluss.testutils.common.AllCallbackWrapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import static org.assertj.core.api.Assertions.assertThat;

class AutoIncIDBufferTest {

    @RegisterExtension
    public static final AllCallbackWrapper<ZooKeeperExtension> ZOO_KEEPER_EXTENSION_WRAPPER = new AllCallbackWrapper<>(
            new ZooKeeperExtension());

    private CuratorFramework zkClient;

    @BeforeEach
    void setUp() {
        zkClient = ZOO_KEEPER_EXTENSION_WRAPPER.getCustomExtension().getZooKeeperClient(NOPErrorHandler.INSTANCE)
                .getCuratorClient();
    }

    @Test
    void testGetAndIncrement() throws Exception {
        TablePath tablePath = TablePath.of("db", "t1");
        int columnIdx = 0;
        int batchSize = 10;
        AutoIncIDBuffer buffer = new AutoIncIDBuffer(zkClient, tablePath, columnIdx, batchSize);

        // buffer is empty, should fetch from ZK
        // ZK initial value is 0.
        // First batch: [1, 10]
        assertThat(buffer.getAndIncrement()).isEqualTo(1L);
        assertThat(buffer.getAndIncrement()).isEqualTo(2L);

        // Consume all remaining
        for (int i = 0; i < 8; i++) {
            buffer.getAndIncrement();
        }
        // now buffer should be empty (current=11, end=11)

        // checking internal state via side effect or just call again
        // Next batch: [11, 20]
        assertThat(buffer.getAndIncrement()).isEqualTo(11L);
    }

    @Test
    void testMultipleBuffers() throws Exception {
        TablePath tablePath = TablePath.of("db", "t2");
        int columnIdx = 0;
        int batchSize = 10;
        AutoIncIDBuffer buffer1 = new AutoIncIDBuffer(zkClient, tablePath, columnIdx, batchSize);
        AutoIncIDBuffer buffer2 = new AutoIncIDBuffer(zkClient, tablePath, columnIdx, batchSize);

        // buffer1 takes [1, 10]
        assertThat(buffer1.getAndIncrement()).isEqualTo(1L);

        // buffer2 sees buffer1 took range, so it takes [11, 20]
        assertThat(buffer2.getAndIncrement()).isEqualTo(11L);

        // buffer1 continues
        assertThat(buffer1.getAndIncrement()).isEqualTo(2L);
    }

    @Test
    void testConcurrentFetch() throws Exception {
        TablePath tablePath = TablePath.of("db", "t3");
        int columnIdx = 0;
        int batchSize = 100;
        int numThreads = 5;
        int fetchesPerThread = 50;

        java.util.concurrent.ExecutorService executor = java.util.concurrent.Executors.newFixedThreadPool(numThreads);
        java.util.List<java.util.concurrent.Future<java.util.Set<Long>>> futures = new java.util.ArrayList<>();

        for (int i = 0; i < numThreads; i++) {
            futures.add(executor.submit(() -> {
                AutoIncIDBuffer buffer = new AutoIncIDBuffer(zkClient, tablePath, columnIdx, batchSize);
                java.util.Set<Long> ids = new java.util.HashSet<>();
                for (int j = 0; j < fetchesPerThread; j++) {
                    ids.add(buffer.getAndIncrement());
                }
                return ids;
            }));
        }

        java.util.Set<Long> allIds = new java.util.HashSet<>();
        for (java.util.concurrent.Future<java.util.Set<Long>> future : futures) {
            allIds.addAll(future.get());
        }

        // Check if we got expected number of unique IDs
        assertThat(allIds).hasSize(numThreads * fetchesPerThread);

        executor.shutdown();
    }
}
