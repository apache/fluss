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

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.apache.fluss.config.ConfigOptions.CLIENT_LOOKUP_BATCH_TIMEOUT;
import static org.apache.fluss.config.ConfigOptions.CLIENT_LOOKUP_MAX_BATCH_SIZE;
import static org.apache.fluss.config.ConfigOptions.CLIENT_LOOKUP_MAX_INFLIGHT_REQUESTS_PER_BUCKET;
import static org.apache.fluss.config.ConfigOptions.CLIENT_LOOKUP_QUEUE_SIZE;
import static org.apache.fluss.record.TestData.DATA1_TABLE_PATH_PK;
import static org.apache.fluss.testutils.common.CommonTestUtils.waitUntil;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link LookupQueue}. */
class LookupQueueTest {

    @Test
    void testDrainMaxBatchSize() throws Exception {
        Configuration conf = new Configuration();
        conf.set(CLIENT_LOOKUP_MAX_BATCH_SIZE, 10);
        conf.setString(CLIENT_LOOKUP_BATCH_TIMEOUT.key(), "1ms");
        LookupQueue queue = new LookupQueue(conf);

        assertThat(queue.drain()).isEmpty();

        appendLookups(queue, 1);
        assertThat(queue.drain()).hasSize(1);
        assertThat(queue.hasUnDrained()).isFalse();

        appendLookups(queue, 9);
        assertThat(queue.drain()).hasSize(9);
        assertThat(queue.hasUnDrained()).isFalse();

        appendLookups(queue, 10);
        assertThat(queue.drain()).hasSize(10);
        assertThat(queue.hasUnDrained()).isFalse();

        appendLookups(queue, 30);
        assertThat(queue.drain()).hasSize(10);
        assertThat(queue.hasUnDrained()).isTrue();
        assertThat(queue.drainAll()).hasSize(20);
        assertThat(queue.hasUnDrained()).isFalse();
    }

    @Test
    void testDrainByLookupQueueKeyAndRotateStartKey() throws Exception {
        Configuration conf = new Configuration();
        conf.set(CLIENT_LOOKUP_MAX_BATCH_SIZE, 4);
        conf.setString(CLIENT_LOOKUP_BATCH_TIMEOUT.key(), "1ms");
        LookupQueue queue = new LookupQueue(conf);
        TableBucket bucketA = new TableBucket(1, 0);
        TableBucket bucketB = new TableBucket(1, 1);
        TableBucket bucketC = new TableBucket(1, 2);

        appendLookup(queue, bucketA, "A0");
        appendLookup(queue, bucketB, "B0");
        appendLookup(queue, bucketA, "A1");
        appendLookup(queue, bucketC, "C0");
        appendLookup(queue, bucketA, "A2");
        appendLookup(queue, bucketB, "B1");
        appendLookup(queue, bucketA, "A3");
        appendLookup(queue, bucketC, "C1");
        appendLookup(queue, bucketA, "A4");
        appendLookup(queue, bucketB, "B2");
        appendLookup(queue, bucketA, "A5");
        appendLookup(queue, bucketC, "C2");

        assertThat(keys(queue.drain())).containsExactly("A0", "A1", "A2", "A3");
        assertThat(keys(queue.drain())).containsExactly("B0", "B1", "B2", "C0");
        assertThat(keys(queue.drain())).containsExactly("A4", "A5", "C1", "C2");
        assertThat(queue.hasUnDrained()).isFalse();
    }

    @Test
    void testRotateByLookupQueueKey() throws Exception {
        Configuration conf = new Configuration();
        conf.set(CLIENT_LOOKUP_MAX_BATCH_SIZE, 1);
        conf.setString(CLIENT_LOOKUP_BATCH_TIMEOUT.key(), "1ms");
        LookupQueue queue = new LookupQueue(conf);
        TableBucket bucketA = new TableBucket(1, 0);
        TableBucket bucketB = new TableBucket(1, 1);

        appendLookup(queue, bucketA, "A-N0");
        appendPrefixLookup(queue, bucketA, "A-P0");
        appendLookup(queue, bucketB, "B-N0");
        appendLookup(queue, bucketA, "A-N1");
        appendPrefixLookup(queue, bucketA, "A-P1");
        appendLookup(queue, bucketB, "B-N1");

        assertThat(keys(queue.drain())).containsExactly("A-N0");
        assertThat(keys(queue.drain())).containsExactly("A-P0");
        assertThat(keys(queue.drain())).containsExactly("B-N0");
        assertThat(keys(queue.drain())).containsExactly("A-N1");
        assertThat(keys(queue.drain())).containsExactly("A-P1");
        assertThat(keys(queue.drain())).containsExactly("B-N1");
    }

    @Test
    void testAppendSameBucketWhileDrainWaitsForBatchTimeout() throws Exception {
        Configuration conf = new Configuration();
        conf.set(CLIENT_LOOKUP_QUEUE_SIZE, 2);
        conf.set(CLIENT_LOOKUP_MAX_BATCH_SIZE, 4);
        conf.set(CLIENT_LOOKUP_MAX_INFLIGHT_REQUESTS_PER_BUCKET, 1);
        conf.set(CLIENT_LOOKUP_BATCH_TIMEOUT, Duration.ofSeconds(10));
        LookupQueue queue = new LookupQueue(conf);
        TableBucket bucketA = new TableBucket(1, 0);
        appendLookup(queue, bucketA, "A0");
        appendLookup(queue, bucketA, "A1");

        CompletableFuture<List<AbstractLookupQuery<?>>> drainFuture =
                CompletableFuture.supplyAsync(
                        () -> {
                            try {
                                return queue.drain();
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                                throw new CompletionException(e);
                            }
                        });
        waitUntil(
                () -> queue.queuedSize() == 0,
                Duration.ofSeconds(1),
                "the first two lookups to be moved into the sender batch");

        CompletableFuture<Void> appendFuture =
                CompletableFuture.runAsync(
                        () -> {
                            appendLookup(queue, bucketA, "A2");
                            appendLookup(queue, bucketA, "A3");
                        });

        appendFuture.get(1, TimeUnit.SECONDS);
        assertThat(keys(drainFuture.get(1, TimeUnit.SECONDS)))
                .containsExactly("A0", "A1", "A2", "A3");
    }

    @Test
    void testAppendLookupBlocksWhenQueueIsFull() throws Exception {
        Configuration conf = new Configuration();
        conf.set(CLIENT_LOOKUP_QUEUE_SIZE, 5);
        conf.set(CLIENT_LOOKUP_MAX_BATCH_SIZE, 5);
        LookupQueue queue = new LookupQueue(conf);

        appendLookups(queue, 5);
        assertThat(queue.queuedSize()).isEqualTo(5);

        CompletableFuture<Void> future = CompletableFuture.runAsync(() -> appendLookups(queue, 1));

        assertThat(future.isDone()).isFalse();
        Thread.sleep(100);
        assertThat(future.isDone()).isFalse();

        assertThat(queue.drain()).hasSize(5);
        future.get(1, TimeUnit.SECONDS);
        assertThat(queue.queuedSize()).isEqualTo(1);
    }

    @Test
    void testReEnqueueDoesNotBlock() throws Exception {
        Configuration conf = new Configuration();
        conf.set(CLIENT_LOOKUP_QUEUE_SIZE, 5);
        conf.set(CLIENT_LOOKUP_MAX_BATCH_SIZE, 5);
        LookupQueue queue = new LookupQueue(conf);

        appendLookups(queue, 5);
        assertThat(queue.queuedSize()).isEqualTo(5);
        assertThat(queue.reEnqueuedLookupCount()).isZero();

        queue.reEnqueue(lookup(new TableBucket(1, 1), "retry"));
        assertThat(queue.queuedSize()).isEqualTo(5);
        assertThat(queue.reEnqueuedLookupCount()).isEqualTo(1);

        assertThat(keys(queue.drain()))
                .containsExactly("retry", "lookup-0", "lookup-1", "lookup-2", "lookup-3");
        assertThat(queue.reEnqueuedLookupCount()).isZero();
        assertThat(queue.queuedSize()).isEqualTo(1);

        assertThat(keys(queue.drain())).containsExactly("lookup-4");
        assertThat(queue.hasUnDrained()).isFalse();
    }

    @Test
    void testDrainHonorsMaxInFlightBatchesPerLookupQueueKey() throws Exception {
        Configuration conf = new Configuration();
        conf.set(CLIENT_LOOKUP_MAX_BATCH_SIZE, 1);
        conf.set(CLIENT_LOOKUP_MAX_INFLIGHT_REQUESTS_PER_BUCKET, 2);
        conf.setString(CLIENT_LOOKUP_BATCH_TIMEOUT.key(), "1ms");
        LookupQueue queue = new LookupQueue(conf);
        TableBucket bucketA = new TableBucket(1, 0);
        LookupQueueKey normalKey = LookupQueueKey.of(bucketA, LookupType.LOOKUP, false);

        appendLookup(queue, bucketA, "A0");
        appendLookup(queue, bucketA, "A1");
        appendLookup(queue, bucketA, "A2");

        assertThat(keys(queue.drain())).containsExactly("A0");
        LookupSender.InFlightBatch batch0 = startInFlightBatch(queue, normalKey);
        assertThat(queue.inFlightRequestCount(normalKey)).isEqualTo(1);
        assertThat(keys(queue.drain())).containsExactly("A1");
        startInFlightBatch(queue, normalKey);
        assertThat(queue.inFlightRequestCount(normalKey)).isEqualTo(2);
        assertThat(queue.drain()).isEmpty();
        assertThat(queue.queuedSize()).isEqualTo(1);

        batch0.requestCompleted();
        assertThat(batch0.requestCompleted()).isFalse();
        assertThat(queue.inFlightRequestCount(normalKey)).isEqualTo(1);
        assertThat(keys(queue.drain())).containsExactly("A2");
        startInFlightBatch(queue, normalKey);
        assertThat(queue.inFlightRequestCount(normalKey)).isEqualTo(2);
    }

    @Test
    void testDrainSkipsReservedKeyAndContinuesNextKey() throws Exception {
        Configuration conf = new Configuration();
        conf.set(CLIENT_LOOKUP_MAX_BATCH_SIZE, 2);
        conf.set(CLIENT_LOOKUP_MAX_INFLIGHT_REQUESTS_PER_BUCKET, 1);
        conf.setString(CLIENT_LOOKUP_BATCH_TIMEOUT.key(), "1ms");
        LookupQueue queue = new LookupQueue(conf);
        TableBucket bucketA = new TableBucket(1, 0);
        TableBucket bucketB = new TableBucket(1, 1);
        LookupQueueKey bucketANormalKey = LookupQueueKey.of(bucketA, LookupType.LOOKUP, false);

        appendLookup(queue, bucketA, "A0");
        appendLookup(queue, bucketA, "A1");
        appendLookup(queue, bucketA, "A2");
        appendLookup(queue, bucketB, "B0");

        assertThat(keys(queue.drain())).containsExactly("A0", "A1");
        LookupSender.InFlightBatch bucketABatch = startInFlightBatch(queue, bucketANormalKey);
        assertThat(queue.inFlightRequestCount(bucketANormalKey)).isEqualTo(1);
        assertThat(keys(queue.drain())).containsExactly("B0");
        assertThat(queue.queuedSize()).isEqualTo(1);

        bucketABatch.requestCompleted();
        assertThat(keys(queue.drain())).containsExactly("A2");
    }

    @Test
    void testInFlightLimitIsIndependentForLookupQueueKeys() throws Exception {
        Configuration conf = new Configuration();
        conf.set(CLIENT_LOOKUP_MAX_BATCH_SIZE, 1);
        conf.set(CLIENT_LOOKUP_MAX_INFLIGHT_REQUESTS_PER_BUCKET, 1);
        conf.setString(CLIENT_LOOKUP_BATCH_TIMEOUT.key(), "1ms");
        LookupQueue queue = new LookupQueue(conf);
        TableBucket bucket = new TableBucket(1, 0);
        LookupQueueKey normalKey = LookupQueueKey.of(bucket, LookupType.LOOKUP, false);
        LookupQueueKey historicalKey = LookupQueueKey.of(bucket, LookupType.LOOKUP, true);
        LookupQueueKey prefixKey = LookupQueueKey.of(bucket, LookupType.PREFIX_LOOKUP, false);
        LookupQueueKey insertKey =
                LookupQueueKey.of(bucket, LookupType.LOOKUP_WITH_INSERT_IF_NOT_EXISTS, false);

        appendLookup(queue, bucket, "N0");
        appendLookup(queue, bucket, "N1");
        appendHistoricalLookup(queue, bucket, "H0", "dt=20200101");
        appendHistoricalLookup(queue, bucket, "H1", "dt=20200102");
        appendPrefixLookup(queue, bucket, "P0");
        appendInsertLookup(queue, bucket, "I0");

        assertThat(keys(queue.drain())).containsExactly("N0");
        LookupSender.InFlightBatch normalBatch = startInFlightBatch(queue, normalKey);
        assertThat(keys(queue.drain())).containsExactly("H0");
        LookupSender.InFlightBatch historicalBatch = startInFlightBatch(queue, historicalKey);
        assertThat(keys(queue.drain())).containsExactly("P0");
        startInFlightBatch(queue, prefixKey);
        assertThat(keys(queue.drain())).containsExactly("I0");
        startInFlightBatch(queue, insertKey);
        assertThat(queue.drain()).isEmpty();
        assertThat(queue.queuedSize()).isEqualTo(2);
        assertThat(queue.inFlightRequestCount(normalKey)).isEqualTo(1);
        assertThat(queue.inFlightRequestCount(historicalKey)).isEqualTo(1);
        assertThat(queue.inFlightRequestCount(prefixKey)).isEqualTo(1);
        assertThat(queue.inFlightRequestCount(insertKey)).isEqualTo(1);

        normalBatch.requestCompleted();
        assertThat(keys(queue.drain())).containsExactly("N1");
        startInFlightBatch(queue, normalKey);
        assertThat(queue.drain()).isEmpty();

        historicalBatch.requestCompleted();
        assertThat(keys(queue.drain())).containsExactly("H1");
    }

    @Test
    void testLookupQueueKeyClassification() {
        TableBucket bucket = new TableBucket(1, 0);
        LookupQueueKey normalKey = LookupQueueKey.fromLookup(lookup(bucket, "normal"));
        LookupQueueKey historicalKey1 =
                LookupQueueKey.fromLookup(historicalLookup(bucket, "historical-1", "dt=20200101"));
        LookupQueueKey historicalKey2 =
                LookupQueueKey.fromLookup(historicalLookup(bucket, "historical-2", "dt=20200102"));
        LookupQueueKey prefixKey = LookupQueueKey.fromLookup(prefixLookup(bucket, "prefix"));
        LookupQueueKey insertKey = LookupQueueKey.fromLookup(insertLookup(bucket, "insert"));

        assertThat(historicalKey1).isEqualTo(historicalKey2);
        assertThat(Arrays.asList(normalKey, historicalKey1, prefixKey, insertKey))
                .doesNotHaveDuplicates();
    }

    @Test
    void testForceCloseUnblocksDrainAllAtInFlightLimit() throws Exception {
        Configuration conf = new Configuration();
        conf.set(CLIENT_LOOKUP_MAX_BATCH_SIZE, 1);
        conf.set(CLIENT_LOOKUP_MAX_INFLIGHT_REQUESTS_PER_BUCKET, 1);
        LookupQueue queue = new LookupQueue(conf);
        TableBucket bucketA = new TableBucket(1, 0);

        appendLookup(queue, bucketA, "A0");
        appendLookup(queue, bucketA, "A1");
        assertThat(keys(queue.drain())).containsExactly("A0");
        startInFlightBatch(queue, LookupQueueKey.of(bucketA, LookupType.LOOKUP, false));

        CompletableFuture<List<AbstractLookupQuery<?>>> drainFuture =
                CompletableFuture.supplyAsync(
                        () -> {
                            try {
                                return queue.drainAll();
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                                throw new CompletionException(e);
                            }
                        });
        Thread.sleep(100);
        assertThat(drainFuture).isNotDone();

        queue.forceClose();
        assertThat(drainFuture.get(1, TimeUnit.SECONDS)).isEmpty();
        assertThat(queue.queuedSize()).isEqualTo(1);
    }

    private static LookupSender.InFlightBatch startInFlightBatch(
            LookupQueue queue, LookupQueueKey lookupQueueKey) {
        LookupSender.InFlightBatch inFlightBatch =
                new LookupSender.InFlightBatch(queue, Collections.singleton(lookupQueueKey));
        inFlightBatch.startInFlightRequests();
        return inFlightBatch;
    }

    private static void appendLookups(LookupQueue queue, int count) {
        TableBucket tableBucket = new TableBucket(1, 0);
        for (int i = 0; i < count; i++) {
            appendLookup(queue, tableBucket, "lookup-" + i);
        }
    }

    private static void appendLookup(LookupQueue queue, TableBucket tableBucket, String lookupKey) {
        queue.appendLookup(lookup(tableBucket, lookupKey));
    }

    private static void appendHistoricalLookup(
            LookupQueue queue,
            TableBucket tableBucket,
            String lookupKey,
            String originalPartitionName) {
        queue.appendLookup(historicalLookup(tableBucket, lookupKey, originalPartitionName));
    }

    private static void appendPrefixLookup(
            LookupQueue queue, TableBucket tableBucket, String lookupKey) {
        queue.appendLookup(prefixLookup(tableBucket, lookupKey));
    }

    private static void appendInsertLookup(
            LookupQueue queue, TableBucket tableBucket, String lookupKey) {
        queue.appendLookup(insertLookup(tableBucket, lookupKey));
    }

    private static LookupQuery lookup(TableBucket tableBucket, String lookupKey) {
        return new LookupQuery(
                DATA1_TABLE_PATH_PK, tableBucket, lookupKey.getBytes(StandardCharsets.UTF_8));
    }

    private static LookupQuery historicalLookup(
            TableBucket tableBucket, String lookupKey, String originalPartitionName) {
        return new LookupQuery(
                DATA1_TABLE_PATH_PK,
                tableBucket,
                lookupKey.getBytes(StandardCharsets.UTF_8),
                false,
                originalPartitionName);
    }

    private static PrefixLookupQuery prefixLookup(TableBucket tableBucket, String lookupKey) {
        return new PrefixLookupQuery(
                DATA1_TABLE_PATH_PK, tableBucket, lookupKey.getBytes(StandardCharsets.UTF_8));
    }

    private static LookupQuery insertLookup(TableBucket tableBucket, String lookupKey) {
        return new LookupQuery(
                DATA1_TABLE_PATH_PK,
                tableBucket,
                lookupKey.getBytes(StandardCharsets.UTF_8),
                true,
                null);
    }

    private static List<String> keys(List<AbstractLookupQuery<?>> lookups) {
        return lookups.stream()
                .map(lookup -> new String(lookup.key(), StandardCharsets.UTF_8))
                .collect(Collectors.toList());
    }
}
