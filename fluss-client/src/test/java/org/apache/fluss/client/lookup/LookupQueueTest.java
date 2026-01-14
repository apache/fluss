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

import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.TableBucket;

import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.apache.fluss.config.ConfigOptions.CLIENT_LOOKUP_BATCH_TIMEOUT;
import static org.apache.fluss.config.ConfigOptions.CLIENT_LOOKUP_MAX_BATCH_SIZE;
import static org.apache.fluss.config.ConfigOptions.CLIENT_LOOKUP_QUEUE_SIZE;
import static org.apache.fluss.record.TestData.DATA1_TABLE_PATH_PK;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link LookupQueue}. */
class LookupQueueTest {

    @Test
    void testDrainMaxBatchSize() throws Exception {
        Configuration conf = new Configuration();
        conf.set(CLIENT_LOOKUP_MAX_BATCH_SIZE, 10);
        conf.setString(CLIENT_LOOKUP_BATCH_TIMEOUT.key(), "1ms");
        LookupQueue queue = new LookupQueue(conf);

        // drain empty
        assertThat(queue.drain()).hasSize(0);

        appendLookups(queue, 1);
        assertThat(queue.drain()).hasSize(1);
        assertThat(queue.hasUnDrained()).isFalse();

        appendLookups(queue, 9);
        assertThat(queue.drain()).hasSize(9);
        assertThat(queue.hasUnDrained()).isFalse();

        appendLookups(queue, 10);
        assertThat(queue.drain()).hasSize(10);
        assertThat(queue.hasUnDrained()).isFalse();

        appendLookups(queue, 20);
        assertThat(queue.drain()).hasSize(10);
        assertThat(queue.hasUnDrained()).isTrue();
        assertThat(queue.drainAll()).hasSize(10);
        assertThat(queue.hasUnDrained()).isFalse();
    }

    @Test
    void testAppendLookupBlocksWhenQueueIsFull() throws Exception {
        Configuration conf = new Configuration();
        conf.set(CLIENT_LOOKUP_QUEUE_SIZE, 5);
        LookupQueue queue = new LookupQueue(conf);

        appendLookups(queue, 5);
        assertThat(queue.getLookupQueue()).hasSize(5);

        CompletableFuture<Void> future =
                CompletableFuture.runAsync(
                        () -> {
                            appendLookups(queue, 1); // will be blocked.
                        });

        // appendLookup should block and not complete immediately.
        assertThat(future.isDone()).isFalse();

        Thread.sleep(100);
        // Still blocked after 100ms.
        assertThat(future.isDone()).isFalse();

        queue.drain();
        future.get(1, TimeUnit.SECONDS);
        assertThat(future.isDone()).isTrue();
    }

    @Test
    void testReEnqueueNotBlock() throws Exception {
        Configuration conf = new Configuration();
        conf.set(CLIENT_LOOKUP_QUEUE_SIZE, 5);
        conf.set(CLIENT_LOOKUP_MAX_BATCH_SIZE, 5);
        LookupQueue queue = new LookupQueue(conf);

        appendLookups(queue, 5);
        assertThat(queue.getLookupQueue()).hasSize(5);
        assertThat(queue.getReEnqueuedLookupQueue()).hasSize(0);

        queue.reEnqueue(
                new LookupQuery(DATA1_TABLE_PATH_PK, new TableBucket(1, 1), new byte[] {0}));
        assertThat(queue.getLookupQueue()).hasSize(5);
        // This batch will be put into re-enqueued lookup queue.
        assertThat(queue.getReEnqueuedLookupQueue()).hasSize(1);
        assertThat(queue.hasUnDrained()).isTrue();

        assertThat(queue.drain()).hasSize(5);
        // drain re-enqueued lookup first.
        assertThat(queue.getReEnqueuedLookupQueue().isEmpty()).isTrue();
        assertThat(queue.getLookupQueue()).hasSize(1);
        assertThat(queue.hasUnDrained()).isTrue();

        assertThat(queue.drain()).hasSize(1);
        assertThat(queue.hasUnDrained()).isFalse();
    }

    private static void appendLookups(LookupQueue queue, int count) {
        for (int i = 0; i < count; i++) {
            queue.appendLookup(
                    new LookupQuery(DATA1_TABLE_PATH_PK, new TableBucket(1, 1), new byte[] {0}));
        }
    }
}
