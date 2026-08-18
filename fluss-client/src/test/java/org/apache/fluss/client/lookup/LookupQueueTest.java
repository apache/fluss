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

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.apache.fluss.config.ConfigOptions.CLIENT_LOOKUP_BATCH_TIMEOUT;
import static org.apache.fluss.config.ConfigOptions.CLIENT_LOOKUP_MAX_BATCH_SIZE;
import static org.apache.fluss.config.ConfigOptions.CLIENT_LOOKUP_MAX_INFLIGHT_REQUESTS_PER_BUCKET;
import static org.apache.fluss.config.ConfigOptions.CLIENT_LOOKUP_QUEUE_SIZE;
import static org.apache.fluss.record.TestData.DATA1_TABLE_PATH_PK;
import static org.apache.fluss.testutils.common.CommonTestUtils.waitUntil;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link LookupQueue}. */
class LookupQueueTest {

    @Test
    void testFullBatchDrainsImmediatelyAndReleasesCapacity() throws Exception {
        LookupQueue queue = createQueue(10, 5, "10s");

        // Creating this batch reserves five queue slots. Filling it makes it immediately drainable,
        // without waiting for the long batch timeout.
        appendLookups(queue, new TableBucket(1, 0), 5);

        List<LookupBatch> batches = queue.drain();
        assertThat(batches).singleElement().extracting(LookupBatch::size).isEqualTo(5);
        assertThat(queue.remainingQueueSize()).isEqualTo(5);
        assertThat(queue.hasUnDrained()).isFalse();

        // Queue capacity belongs to the batch until its RPC attempt finishes.
        queue.releaseBatchCapacity(batches.get(0));
        assertThat(queue.remainingQueueSize()).isEqualTo(10);
    }

    @Test
    void testUnderfilledBatchWaitsForItsTimeout() throws Exception {
        LookupQueue queue = createQueue(10, 5, "100ms");
        appendLookups(queue, new TableBucket(1, 0), 1);

        // The batch is not full, so drain() should wait for its batch timeout before returning it.
        long startNanos = System.nanoTime();
        List<LookupBatch> batches = queue.drain();
        long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);

        assertThat(elapsedMs).isGreaterThanOrEqualTo(50);
        assertThat(batches).singleElement().extracting(LookupBatch::size).isEqualTo(1);
        queue.releaseBatchCapacity(batches.get(0));
    }

    @Test
    void testReadyBucketDoesNotWaitForAnotherBucket() throws Exception {
        LookupQueue queue = createQueue(20, 2, "10s");
        TableBucket slowBucket = new TableBucket(1, 0);
        TableBucket readyBucket = new TableBucket(1, 1);

        appendLookups(queue, slowBucket, 1);
        appendLookups(queue, readyBucket, 2);

        // The ready bucket is full and must drain immediately; it must not wait for the underfilled
        // batch belonging to another bucket.
        List<LookupBatch> readyBatches = queue.drain();
        assertThat(readyBatches).singleElement();
        assertThat(readyBatches.get(0).tableBucket()).isEqualTo(readyBucket);
        assertThat(queue.hasUnDrained()).isTrue();
        queue.releaseBatchCapacity(readyBatches.get(0));

        // Force-drain the remaining underfilled batch to clean up the test.
        List<LookupBatch> remainingBatches = queue.drainAll();
        assertThat(remainingBatches).singleElement();
        assertThat(remainingBatches.get(0).tableBucket()).isEqualTo(slowBucket);
        queue.releaseBatchCapacity(remainingBatches.get(0));
    }

    @Test
    void testCreatingNewBatchBlocksUntilCapacityIsReleased() throws Exception {
        LookupQueue queue = createQueue(10, 5, "10s");
        // Capacity is reserved per batch. These two one-element batches reserve 5 slots each and
        // therefore consume all 10 queue slots.
        appendLookups(queue, new TableBucket(1, 0), 1);
        appendLookups(queue, new TableBucket(1, 1), 1);
        assertThat(queue.remainingQueueSize()).isZero();

        // A third bucket needs a new batch, so its appender must wait for five slots to be
        // released.
        CompletableFuture<Void> appendFuture =
                CompletableFuture.runAsync(() -> appendLookups(queue, new TableBucket(1, 2), 1));
        waitUntil(
                () -> queue.waitingAppenderCount() == 1,
                Duration.ofSeconds(5),
                "lookup appender waiting for batch capacity");
        assertThat(appendFuture).isNotDone();

        List<LookupBatch> pressuredBatches = queue.drain();
        assertThat(pressuredBatches).hasSize(2);

        // Releasing either drained batch returns five slots and unblocks the third appender.
        queue.releaseBatchCapacity(pressuredBatches.get(0));
        appendFuture.get(1, TimeUnit.SECONDS);
        assertThat(queue.queuedBatchCount()).isEqualTo(1);

        queue.releaseBatchCapacity(pressuredBatches.get(1));
        List<LookupBatch> lastBatch = queue.drainAll();
        assertThat(lastBatch).singleElement();
        queue.releaseBatchCapacity(lastBatch.get(0));
        assertThat(queue.remainingQueueSize()).isEqualTo(10);
    }

    @Test
    void testCompletingBatchWakesAllAppendersThatCanShareNewBatch() throws Exception {
        LookupQueue queue = createQueue(2, 2, "10s");
        TableBucket firstBucket = new TableBucket(1, 0);
        TableBucket sharedBucket = new TableBucket(1, 1);

        // The first full batch reserves all queue capacity until its RPC attempt completes.
        appendLookups(queue, firstBucket, 2);
        LookupBatch firstBatch = queue.drain().get(0);

        // Both appenders need a new batch for the same bucket, so both initially wait for capacity.
        CompletableFuture<Void> firstAppend =
                CompletableFuture.runAsync(() -> queue.appendLookup(newLookup(sharedBucket)));
        CompletableFuture<Void> secondAppend =
                CompletableFuture.runAsync(() -> queue.appendLookup(newLookup(sharedBucket)));
        waitUntil(
                () -> queue.waitingAppenderCount() == 2,
                Duration.ofSeconds(5),
                "two lookup appenders waiting for batch capacity");

        // One completion provides capacity for one new batch. Both appenders must wake: one creates
        // that batch and the other fills its remaining slot without reserving additional capacity.
        queue.releaseBatchCapacity(firstBatch);
        CompletableFuture.allOf(firstAppend, secondAppend).get(1, TimeUnit.SECONDS);

        List<LookupBatch> sharedBatches = queue.drain();
        assertThat(sharedBatches).singleElement();
        assertThat(sharedBatches.get(0).tableBucket()).isEqualTo(sharedBucket);
        assertThat(sharedBatches.get(0).size()).isEqualTo(2);
        queue.releaseBatchCapacity(sharedBatches.get(0));
    }

    @Test
    void testRetryDoesNotBlockCallbackAndReservesCapacityAgain() throws Exception {
        LookupQueue queue = createQueue(1, 1, "10s");
        LookupQuery lookup = newLookup(new TableBucket(1, 0));
        queue.appendLookup(lookup);
        LookupBatch firstAttempt = queue.drain().get(0);

        // The RPC callback only places the failed lookup in the retry deque. It returns immediately
        // even though the first attempt still owns the queue's only reserved slot.
        queue.reEnqueue(lookup);
        assertThat(queue.reEnqueuedLookupCount()).isEqualTo(1);
        assertThat(queue.remainingQueueSize()).isZero();

        // Once the first attempt releases its slot, the sender thread can reserve it for the retry.
        queue.releaseBatchCapacity(firstAttempt);
        LookupBatch retryAttempt = queue.drain().get(0);
        assertThat(retryAttempt.lookups()).containsExactly(lookup);
        assertThat(queue.remainingQueueSize()).isZero();

        queue.releaseBatchCapacity(retryAttempt);
        assertThat(queue.remainingQueueSize()).isEqualTo(1);
    }

    @Test
    void testSameBucketDifferentLookupKindsAreDrainedRoundRobin() throws Exception {
        Configuration conf = new Configuration();
        conf.set(CLIENT_LOOKUP_QUEUE_SIZE, 4);
        conf.set(CLIENT_LOOKUP_MAX_BATCH_SIZE, 1);
        conf.set(CLIENT_LOOKUP_MAX_INFLIGHT_REQUESTS_PER_BUCKET, 1);
        conf.setString(CLIENT_LOOKUP_BATCH_TIMEOUT.key(), "10s");
        LookupQueue queue = new LookupQueue(conf);
        TableBucket tableBucket = new TableBucket(1, 0);
        LookupQuery normalLookup1 = newLookup(tableBucket);
        LookupQuery normalLookup2 = newLookup(tableBucket);
        LookupQuery historicalLookup =
                new LookupQuery(
                        DATA1_TABLE_PATH_PK, tableBucket, new byte[] {1}, false, "dt=20200101");
        PrefixLookupQuery prefixLookup =
                new PrefixLookupQuery(DATA1_TABLE_PATH_PK, tableBucket, new byte[] {2});

        queue.appendLookup(normalLookup1);
        queue.appendLookup(normalLookup2);
        queue.appendLookup(historicalLookup);
        queue.appendLookup(prefixLookup);

        LookupBatch firstBatch = queue.drain().get(0);
        assertThat(firstBatch.lookups()).containsExactly(normalLookup1);
        queue.addInFlightRequests(Collections.singleton(tableBucket));
        queue.completeInFlightRequests(Collections.singleton(tableBucket));
        queue.releaseBatchCapacity(firstBatch);

        // The normal lookup key still has backlog, but it must not monopolize the bucket permit.
        // The next drain should move to the historical lookup key for the same table bucket.
        LookupBatch secondBatch = queue.drain().get(0);
        assertThat(secondBatch.lookups()).containsExactly(historicalLookup);
        queue.releaseBatchCapacity(secondBatch);

        LookupBatch thirdBatch = queue.drain().get(0);
        assertThat(thirdBatch.lookups()).containsExactly(prefixLookup);
        queue.releaseBatchCapacity(thirdBatch);

        LookupBatch fourthBatch = queue.drain().get(0);
        assertThat(fourthBatch.lookups()).containsExactly(normalLookup2);
        queue.releaseBatchCapacity(fourthBatch);
    }

    @Test
    void testReadyRetryReservesReleasedCapacityBeforeWaitingAppenders() throws Exception {
        LookupQueue queue = createQueue(1, 1, "10s");
        TableBucket retryBucket = new TableBucket(1, 0);
        TableBucket newLookupBucket = new TableBucket(1, 1);
        LookupQuery retryLookup = newLookup(retryBucket);
        queue.appendLookup(retryLookup);
        LookupBatch firstAttempt = queue.drain().get(0);

        // The retry is ready but cannot reserve capacity until the first attempt releases its slot.
        queue.reEnqueue(retryLookup);
        assertThat(queue.remainingQueueSize()).isZero();

        CompletableFuture<Void> appendFuture =
                CompletableFuture.runAsync(() -> queue.appendLookup(newLookup(newLookupBucket)));
        waitUntil(
                () -> queue.waitingAppenderCount() == 1,
                Duration.ofSeconds(5),
                "lookup appender waiting for batch capacity");

        // Releasing the first attempt must reserve the slot for the ready retry before waking the
        // waiting appender. Otherwise, sustained appends can repeatedly consume every released slot
        // and starve retries forever.
        queue.releaseBatchCapacity(firstAttempt);
        assertThat(appendFuture).isNotDone();
        LookupBatch retryAttempt = queue.drain().get(0);
        assertThat(retryAttempt.lookups()).containsExactly(retryLookup);
        assertThat(queue.remainingQueueSize()).isZero();

        queue.releaseBatchCapacity(retryAttempt);
        appendFuture.get(1, TimeUnit.SECONDS);
        LookupBatch newLookupAttempt = queue.drain().get(0);
        assertThat(newLookupAttempt.tableBucket()).isEqualTo(newLookupBucket);
        queue.releaseBatchCapacity(newLookupAttempt);
    }

    @Test
    void testPerBucketInFlightLimitSkipsOnlyLimitedBucket() throws Exception {
        Configuration conf = new Configuration();
        conf.set(CLIENT_LOOKUP_QUEUE_SIZE, 10);
        conf.set(CLIENT_LOOKUP_MAX_BATCH_SIZE, 1);
        conf.set(CLIENT_LOOKUP_MAX_INFLIGHT_REQUESTS_PER_BUCKET, 5);
        conf.setString(CLIENT_LOOKUP_BATCH_TIMEOUT.key(), "10s");
        LookupQueue queue = new LookupQueue(conf);
        TableBucket limitedBucket = new TableBucket(1, 0);
        TableBucket otherBucket = new TableBucket(1, 1);

        // Pretend the limited bucket already has five outstanding RPCs, exhausting its permit
        // count.
        for (int i = 0; i < 5; i++) {
            queue.addInFlightRequests(Collections.singleton(limitedBucket));
        }
        appendLookups(queue, limitedBucket, 2);
        appendLookups(queue, otherBucket, 1);

        // drain() skips only the saturated bucket and still returns work for the other bucket.
        List<LookupBatch> readyBatches = queue.drain();
        assertThat(readyBatches).singleElement();
        assertThat(readyBatches.get(0).tableBucket()).isEqualTo(otherBucket);
        queue.releaseBatchCapacity(readyBatches.get(0));

        // Releasing one permit makes one batch from the previously saturated bucket drainable.
        queue.completeInFlightRequests(Collections.singleton(limitedBucket));
        List<LookupBatch> newlyReady = queue.drain();
        assertThat(newlyReady).singleElement();
        assertThat(newlyReady.get(0).tableBucket()).isEqualTo(limitedBucket);
        queue.releaseBatchCapacity(newlyReady.get(0));
    }

    @Test
    void testLookupKindsAndHistoricalLookupsUseSeparateBatches() throws Exception {
        LookupQueue queue = createQueue(8, 2, "10s");
        TableBucket tableBucket = new TableBucket(1, 0);

        // All queries target the same physical bucket, but they form four batches: normal lookup,
        // historical lookup, lookup-with-insert, and prefix lookup.
        queue.appendLookup(newLookup(tableBucket));
        queue.appendLookup(newLookup(tableBucket));
        queue.appendLookup(
                new LookupQuery(
                        DATA1_TABLE_PATH_PK, tableBucket, new byte[] {1}, false, "dt=20200101"));
        queue.appendLookup(
                new LookupQuery(
                        DATA1_TABLE_PATH_PK, tableBucket, new byte[] {2}, false, "dt=20200102"));
        queue.appendLookup(
                new LookupQuery(DATA1_TABLE_PATH_PK, tableBucket, new byte[] {3}, true, null));
        queue.appendLookup(
                new LookupQuery(DATA1_TABLE_PATH_PK, tableBucket, new byte[] {4}, true, null));
        queue.appendLookup(new PrefixLookupQuery(DATA1_TABLE_PATH_PK, tableBucket, new byte[] {5}));
        queue.appendLookup(new PrefixLookupQuery(DATA1_TABLE_PATH_PK, tableBucket, new byte[] {6}));

        // Lookup type and historical mode are part of the batching key, so incompatible operations
        // must never be merged even when their table bucket is identical.
        List<LookupBatch> batches = queue.drain();
        assertThat(batches).hasSize(4);
        assertThat(batches)
                .extracting(LookupBatch::lookupType)
                .containsExactlyInAnyOrder(
                        LookupType.LOOKUP,
                        LookupType.LOOKUP,
                        LookupType.LOOKUP_WITH_INSERT_IF_NOT_EXISTS,
                        LookupType.PREFIX_LOOKUP);

        LookupBatch historicalBatch = null;
        for (LookupBatch batch : batches) {
            if (batch.historical()) {
                historicalBatch = batch;
                break;
            }
        }
        assertThat(historicalBatch).isNotNull();
        assertThat(historicalBatch.lookups())
                .extracting(AbstractLookupQuery::originalPartitionName)
                .containsExactly("dt=20200101", "dt=20200102");

        for (LookupBatch batch : batches) {
            queue.releaseBatchCapacity(batch);
        }
    }

    @Test
    void testQueueSizeMustFitOneBatch() {
        Configuration conf = new Configuration();
        conf.set(CLIENT_LOOKUP_QUEUE_SIZE, 4);
        conf.set(CLIENT_LOOKUP_MAX_BATCH_SIZE, 5);

        assertThatThrownBy(() -> new LookupQueue(conf))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must not be smaller");
    }

    private static LookupQueue createQueue(int queueSize, int batchSize, String timeout) {
        Configuration conf = new Configuration();
        conf.set(CLIENT_LOOKUP_QUEUE_SIZE, queueSize);
        conf.set(CLIENT_LOOKUP_MAX_BATCH_SIZE, batchSize);
        conf.setString(CLIENT_LOOKUP_BATCH_TIMEOUT.key(), timeout);
        return new LookupQueue(conf);
    }

    private static void appendLookups(LookupQueue queue, TableBucket tableBucket, int count) {
        for (int i = 0; i < count; i++) {
            queue.appendLookup(newLookup(tableBucket));
        }
    }

    private static LookupQuery newLookup(TableBucket tableBucket) {
        return new LookupQuery(DATA1_TABLE_PATH_PK, tableBucket, new byte[] {0});
    }
}
