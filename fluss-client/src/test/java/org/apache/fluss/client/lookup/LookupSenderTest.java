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

import org.apache.fluss.client.metadata.TestingMetadataUpdater;
import org.apache.fluss.cluster.BucketLocation;
import org.apache.fluss.cluster.Cluster;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.FlussRuntimeException;
import org.apache.fluss.exception.HistoricalPartitionThrottledException;
import org.apache.fluss.exception.InvalidBucketRoutingException;
import org.apache.fluss.exception.InvalidTableException;
import org.apache.fluss.exception.LookupLimitExceededException;
import org.apache.fluss.exception.NotLeaderOrFollowerException;
import org.apache.fluss.exception.PartitionNotExistException;
import org.apache.fluss.exception.TableNotExistException;
import org.apache.fluss.exception.TimeoutException;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.rpc.gateway.TabletServerGateway;
import org.apache.fluss.rpc.messages.LookupRequest;
import org.apache.fluss.rpc.messages.LookupResponse;
import org.apache.fluss.rpc.messages.PbLookupReqForBucket;
import org.apache.fluss.rpc.messages.PbLookupRespForBucket;
import org.apache.fluss.rpc.messages.PbPrefixLookupRespForBucket;
import org.apache.fluss.rpc.messages.PrefixLookupRequest;
import org.apache.fluss.rpc.messages.PrefixLookupResponse;
import org.apache.fluss.rpc.protocol.ApiError;
import org.apache.fluss.server.tablet.TestTabletServerGateway;
import org.apache.fluss.utils.ExponentialBackoff;
import org.apache.fluss.utils.clock.Clock;
import org.apache.fluss.utils.clock.ManualClock;
import org.apache.fluss.utils.clock.SystemClock;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.apache.fluss.record.TestData.DATA1_TABLE_ID_PK;
import static org.apache.fluss.record.TestData.DATA1_TABLE_INFO_PK;
import static org.apache.fluss.record.TestData.DATA1_TABLE_PATH_PK;
import static org.apache.fluss.testutils.common.CommonTestUtils.waitUntil;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link LookupSender}. */
public class LookupSenderTest {

    private final TableBucket tb1 = new TableBucket(DATA1_TABLE_ID_PK, 0);

    private TestingLeaderlessMetadataUpdater metadataUpdater;
    private LookupSender lookupSender;

    private static final int MAX_RETRIES = 3;
    private static final int MAX_INFLIGHT_REQUESTS = 10;
    private static final int MAX_BATCH_SIZE = 10;
    private static final TableBucket TABLE_BUCKET = new TableBucket(DATA1_TABLE_ID_PK, 0);
    private static final TableBucket OTHER_TABLE_BUCKET = new TableBucket(DATA1_TABLE_ID_PK, 1);
    private static final TableBucket THIRD_TABLE_BUCKET = new TableBucket(DATA1_TABLE_ID_PK, 2);

    private LookupQueue lookupQueue;
    private Thread senderThread;
    private ConfigurableTestTabletServerGateway gateway;

    @BeforeEach
    void setup() {
        // create a configurable gateway for testing
        gateway = new ConfigurableTestTabletServerGateway();

        Map<TablePath, TableInfo> tableInfos = new HashMap<>();
        tableInfos.put(DATA1_TABLE_PATH_PK, DATA1_TABLE_INFO_PK);
        metadataUpdater = new TestingLeaderlessMetadataUpdater(tableInfos, gateway);

        // Keep the backoff short so retry tests that don't assert on timing stay fast.
        startLookupSender(new ExponentialBackoff(10, 2, 100, 0.2));
    }

    @AfterEach
    void teardown() throws InterruptedException {
        if (lookupSender != null) {
            lookupSender.forceClose();
        }
        if (senderThread != null) {
            senderThread.join(5000);
        }
    }

    private void restartLookupSender(ExponentialBackoff retryBackoff) throws InterruptedException {
        teardown();
        startLookupSender(retryBackoff);
    }

    private void startLookupSender(ExponentialBackoff retryBackoff) {
        createLookupSender(
                new LookupQueue(lookupQueueConf(), SystemClock.getInstance()), retryBackoff);
        senderThread = new Thread(lookupSender);
        senderThread.start();
    }

    /**
     * Replaces the sender with one whose thread is not started, so that each test drives the passes
     * from the test thread and the queue is not drained concurrently.
     */
    private void restartLookupSenderWithoutThread(ExponentialBackoff retryBackoff, Clock clock)
            throws InterruptedException {
        teardown();
        createLookupSender(new LookupQueue(lookupQueueConf(), clock), retryBackoff);
    }

    private static Configuration lookupQueueConf() {
        Configuration conf = new Configuration();
        conf.set(ConfigOptions.CLIENT_LOOKUP_QUEUE_SIZE, 5);
        conf.set(ConfigOptions.CLIENT_LOOKUP_MAX_BATCH_SIZE, MAX_BATCH_SIZE);
        return conf;
    }

    private void createLookupSender(LookupQueue queue, ExponentialBackoff retryBackoff) {
        lookupQueue = queue;
        lookupSender =
                new LookupSender(
                        metadataUpdater,
                        lookupQueue,
                        MAX_INFLIGHT_REQUESTS,
                        MAX_RETRIES,
                        retryBackoff,
                        (short) -1,
                        1000);
        senderThread = null;
    }

    @Test
    void testHistoricalLookupsBatchDifferentPartitionsForSameBucket() throws Exception {
        List<LookupRequest> receivedRequests = Collections.synchronizedList(new ArrayList<>());
        gateway.setLookupHandler(
                request -> {
                    receivedRequests.add(request);
                    return createPartitionNameEchoResponse(request);
                });

        // These queries verify that response dispatch uses both the historical bucket and original
        // partition name, allowing different original partitions to share one RPC.
        LookupQuery sameBucketQuery1 =
                new LookupQuery(
                        DATA1_TABLE_PATH_PK, TABLE_BUCKET, bytes("key1"), false, "dt=20200101");
        LookupQuery sameBucketQuery2 =
                new LookupQuery(
                        DATA1_TABLE_PATH_PK, TABLE_BUCKET, bytes("key2"), false, "dt=20200102");

        LookupQuery differentBucketQuery =
                new LookupQuery(
                        DATA1_TABLE_PATH_PK,
                        new TableBucket(DATA1_TABLE_ID_PK, 1),
                        bytes("key3"),
                        false,
                        "dt=20200103");

        lookupSender.sendLookups(
                1,
                LookupType.LOOKUP,
                Arrays.asList(sameBucketQuery1, sameBucketQuery2, differentBucketQuery));

        assertThat(sameBucketQuery1.future().get(5, TimeUnit.SECONDS))
                .isEqualTo(responseValue("dt=20200101", "key1"));
        assertThat(sameBucketQuery2.future().get(5, TimeUnit.SECONDS))
                .isEqualTo(responseValue("dt=20200102", "key2"));
        assertThat(differentBucketQuery.future().get(5, TimeUnit.SECONDS))
                .isEqualTo(responseValue("dt=20200103", "key3"));

        assertThat(receivedRequests).hasSize(1);
        LookupRequest request = receivedRequests.get(0);
        assertThat(request.getBucketsReqsList())
                .extracting(PbLookupReqForBucket::getBucketId)
                .containsExactly(0, 0, 1);
        assertThat(request.getBucketsReqsList())
                .extracting(PbLookupReqForBucket::getOriginalPartitionName)
                .containsExactly("dt=20200101", "dt=20200102", "dt=20200103");
    }

    @Test
    void testNormalAndHistoricalLookupsSplitRequests() throws Exception {
        List<LookupRequest> receivedRequests = Collections.synchronizedList(new ArrayList<>());
        gateway.setLookupHandler(
                request -> {
                    receivedRequests.add(request);
                    return createPartitionNameEchoResponse(request);
                });

        // Normal lookup has no original partition name.
        LookupQuery normalQuery =
                new LookupQuery(DATA1_TABLE_PATH_PK, TABLE_BUCKET, bytes("normal-key"));
        // Historical lookup carries the original partition name, so it is sent in another RPC even
        // when it targets a different bucket.
        LookupQuery historicalQuery =
                new LookupQuery(
                        DATA1_TABLE_PATH_PK,
                        new TableBucket(DATA1_TABLE_ID_PK, 1),
                        bytes("historical-key"),
                        false,
                        "dt=20200101");

        lookupSender.sendLookups(1, LookupType.LOOKUP, Arrays.asList(normalQuery, historicalQuery));

        assertThat(normalQuery.future().get(5, TimeUnit.SECONDS))
                .isEqualTo(responseValue("", "normal-key"));
        assertThat(historicalQuery.future().get(5, TimeUnit.SECONDS))
                .isEqualTo(responseValue("dt=20200101", "historical-key"));
        assertThat(receivedRequests).hasSize(2);

        LookupRequest normalRequest = receivedRequests.get(0);
        assertThat(normalRequest.getBucketsReqsCount()).isEqualTo(1);
        assertThat(normalRequest.getBucketsReqAt(0).getBucketId()).isEqualTo(0);
        assertThat(normalRequest.getBucketsReqAt(0).hasOriginalPartitionName()).isFalse();

        LookupRequest historicalRequest = receivedRequests.get(1);
        assertThat(historicalRequest.getBucketsReqsCount()).isEqualTo(1);
        assertThat(historicalRequest.getBucketsReqAt(0).getBucketId()).isEqualTo(1);
        assertThat(historicalRequest.getBucketsReqAt(0).getOriginalPartitionName())
                .isEqualTo("dt=20200101");
    }

    @Test
    void testNormalLookupsKeepExistingBatching() throws Exception {
        List<LookupRequest> receivedRequests = Collections.synchronizedList(new ArrayList<>());
        gateway.setLookupHandler(
                request -> {
                    receivedRequests.add(request);
                    return createPartitionNameEchoResponse(request);
                });

        LookupQuery query1 = new LookupQuery(DATA1_TABLE_PATH_PK, TABLE_BUCKET, bytes("key1"));
        LookupQuery query2 = new LookupQuery(DATA1_TABLE_PATH_PK, TABLE_BUCKET, bytes("key2"));

        lookupSender.sendLookups(1, LookupType.LOOKUP, Arrays.asList(query1, query2));

        assertThat(query1.future().get(5, TimeUnit.SECONDS)).isEqualTo(responseValue("", "key1"));
        assertThat(query2.future().get(5, TimeUnit.SECONDS)).isEqualTo(responseValue("", "key2"));
        assertThat(receivedRequests).hasSize(1);
        LookupRequest request = receivedRequests.get(0);
        assertThat(request.getBucketsReqsCount()).isEqualTo(1);
        assertThat(request.getBucketsReqAt(0).getKeysCount()).isEqualTo(2);
        assertThat(request.getBucketsReqAt(0).hasOriginalPartitionName()).isFalse();
    }

    @Test
    void testSendLookupRequestWithNotLeaderOrFollowerException() {
        assertThat(metadataUpdater.getBucketLocation(tb1))
                .hasValue(
                        new BucketLocation(
                                PhysicalTablePath.of(DATA1_TABLE_PATH_PK),
                                tb1,
                                1,
                                new int[] {1, 2, 3}));

        // Configure gateway to always return NotLeaderOrFollowerException for all attempts
        // (including retries)
        gateway.setLookupHandler(
                request ->
                        createFailedResponse(
                                request,
                                new NotLeaderOrFollowerException(
                                        "mock not leader or follower exception.")));

        // send LookupRequest through the queue so that retry mechanism can work
        LookupQuery lookupQuery = new LookupQuery(DATA1_TABLE_PATH_PK, tb1, new byte[0]);
        CompletableFuture<byte[]> result = lookupQuery.future();
        assertThat(result).isNotDone();
        lookupQueue.appendLookup(lookupQuery);

        // Wait for all retries to complete and verify it eventually fails. This case will be failed
        // after timeout.
        assertThatThrownBy(() -> result.get(2, TimeUnit.SECONDS))
                .isInstanceOf(java.util.concurrent.TimeoutException.class);

        // Verify that retries happened (should be 1, because server meta invalidated)
        assertThat(lookupQuery.retries()).isEqualTo(1);

        // When NotLeaderOrFollowerException is received, the bucketLocation will be removed from
        // metadata updater to trigger get the latest bucketLocation in next lookup round.
        assertThat(metadataUpdater.getBucketLocation(tb1)).isNotPresent();
    }

    @Test
    void testSendPrefixLookupRequestWithNotLeaderOrFollowerException() {
        assertThat(metadataUpdater.getBucketLocation(tb1))
                .hasValue(
                        new BucketLocation(
                                PhysicalTablePath.of(DATA1_TABLE_PATH_PK),
                                tb1,
                                1,
                                new int[] {1, 2, 3}));

        // Configure gateway to always return NotLeaderOrFollowerException for all attempts
        // (including retries)
        gateway.setPrefixLookupHandler(
                request ->
                        createFailedPrefixLookupResponse(
                                request,
                                new NotLeaderOrFollowerException(
                                        "mock not leader or follower exception.")));

        // send PrefixLookupRequest through the queue so that retry mechanism can work
        PrefixLookupQuery prefixLookupQuery =
                new PrefixLookupQuery(DATA1_TABLE_PATH_PK, tb1, new byte[0]);
        CompletableFuture<List<byte[]>> future = prefixLookupQuery.future();
        assertThat(future).isNotDone();
        lookupQueue.appendLookup(prefixLookupQuery);

        // Wait for all retries to complete and verify it eventually fails. This case will be failed
        // after timeout.
        assertThatThrownBy(() -> future.get(2, TimeUnit.SECONDS))
                .isInstanceOf(java.util.concurrent.TimeoutException.class);

        // Verify that retries happened (should be 1, because server meta invalidated)
        assertThat(prefixLookupQuery.retries()).isEqualTo(1);

        // When NotLeaderOrFollowerException is received, the bucketLocation will be removed from
        // metadata updater to trigger get the latest bucketLocation in next lookup round.
        assertThat(metadataUpdater.getBucketLocation(tb1)).isNotPresent();
    }

    @Test
    void testRetriableExceptionTriggersRetry() throws Exception {
        // setup: fail twice with retriable exception, then succeed
        AtomicInteger attemptCount = new AtomicInteger(0);
        gateway.setLookupHandler(
                request -> {
                    int attempt = attemptCount.incrementAndGet();
                    if (attempt <= 2) {
                        // first two attempts fail with retriable exception
                        return createFailedResponse(
                                request, new TimeoutException("simulated timeout"));
                    } else {
                        // third attempt succeeds
                        return createSuccessResponse(request, "value".getBytes());
                    }
                });

        // execute: submit lookup
        byte[] key = "key".getBytes();
        LookupQuery query = new LookupQuery(DATA1_TABLE_PATH_PK, TABLE_BUCKET, key);
        lookupQueue.appendLookup(query);

        // verify: eventually succeeds after retries
        byte[] result = query.future().get(5, TimeUnit.SECONDS);
        assertThat(result).isEqualTo("value".getBytes());
        assertThat(attemptCount.get()).isEqualTo(3);
        assertThat(query.retries()).isEqualTo(2); // retried 2 times
    }

    @Test
    void testRetriableErrorRetriesWithExponentialBackoff() throws Exception {
        // Without jitter, the retries are delayed by exactly 100ms, 200ms and 400ms.
        long initialBackoffMs = 100;
        restartLookupSender(new ExponentialBackoff(initialBackoffMs, 2, 10_000, 0));
        // Use the same clock as the retry deadlines, so each gap is at least the backoff.
        List<Long> attemptMs = Collections.synchronizedList(new ArrayList<>());
        gateway.setLookupHandler(
                request -> {
                    attemptMs.add(System.currentTimeMillis());
                    if (attemptMs.size() <= MAX_RETRIES) {
                        return createFailedResponse(
                                request, new TimeoutException("simulated timeout"));
                    }
                    return createSuccessResponse(request, bytes("value"));
                });

        LookupQuery query = new LookupQuery(DATA1_TABLE_PATH_PK, TABLE_BUCKET, bytes("key"));
        lookupQueue.appendLookup(query);

        assertThat(query.future().get(10, TimeUnit.SECONDS)).isEqualTo(bytes("value"));
        assertThat(attemptMs).hasSize(1 + MAX_RETRIES);
        for (int i = 1; i < attemptMs.size(); i++) {
            assertThat(attemptMs.get(i) - attemptMs.get(i - 1))
                    .isGreaterThanOrEqualTo(initialBackoffMs << (i - 1));
        }
    }

    @Test
    void testZeroRetryBackoffRetriesImmediately() throws Exception {
        restartLookupSender(new ExponentialBackoff(0, 2, 0, 0.2));
        gateway.setLookupHandler(
                request ->
                        createFailedResponse(
                                request, new NotLeaderOrFollowerException("not leader")));

        LookupQuery query = new LookupQuery(DATA1_TABLE_PATH_PK, TABLE_BUCKET, bytes("key"));
        lookupSender.sendLookups(1, LookupType.LOOKUP, Collections.singletonList(query));

        assertThat(query.retries()).isEqualTo(1);
        assertThat(query.nextRetryTimeMs()).isLessThanOrEqualTo(System.currentTimeMillis());
    }

    @Test
    void testLeaderlessBucketResolvesLeaderOncePerPassAndBacksOff() throws Exception {
        ManualClock clock = new ManualClock(1_000);
        restartLookupSenderWithoutThread(new ExponentialBackoff(100, 2, 10_000, 0), clock);
        metadataUpdater.failLeaderLookups(TABLE_BUCKET, Integer.MAX_VALUE, null);
        List<LookupRequest> sentRequests = Collections.synchronizedList(new ArrayList<>());
        gateway.setLookupHandler(
                request -> {
                    sentRequests.add(request);
                    return CompletableFuture.completedFuture(new LookupResponse());
                });

        List<AbstractLookupQuery<?>> lookups =
                new ArrayList<>(
                        Arrays.asList(
                                new LookupQuery(DATA1_TABLE_PATH_PK, TABLE_BUCKET, bytes("key1")),
                                new LookupQuery(DATA1_TABLE_PATH_PK, TABLE_BUCKET, bytes("key2")),
                                new PrefixLookupQuery(
                                        DATA1_TABLE_PATH_PK, TABLE_BUCKET, bytes("prefix"))));
        List<AbstractLookupQuery<?>> firstPass = new ArrayList<>(lookups);
        firstPass.add(new LookupQuery(DATA1_TABLE_PATH_PK, OTHER_TABLE_BUCKET, bytes("healthy")));
        lookupSender.sendLookups(firstPass);
        assertLookupsWaitForLeader(lookups, 1, 1_100);
        // A bucket with a leader in the same pass is sent right away.
        assertThat(sentRequests).hasSize(1);
        assertThat(sentRequests.get(0).getBucketsReqsList())
                .extracting(PbLookupReqForBucket::getBucketId)
                .containsExactly(OTHER_TABLE_BUCKET.getBucket());

        // A lookup arriving during the backoff waits for the bucket without a metadata request.
        clock.advanceTime(50, TimeUnit.MILLISECONDS);
        LookupQuery newLookup = new LookupQuery(DATA1_TABLE_PATH_PK, TABLE_BUCKET, bytes("key3"));
        lookups.add(newLookup);
        lookupSender.sendLookups(Collections.singletonList(newLookup));
        assertLookupsWaitForLeader(lookups, 1, 1_100);

        // Once the backoff expires, the leader is looked up again and the backoff doubles.
        clock.advanceTime(50, TimeUnit.MILLISECONDS);
        lookupSender.runOnce(false);
        assertLookupsWaitForLeader(lookups, 2, 1_300);
    }

    private void assertLookupsWaitForLeader(
            List<AbstractLookupQuery<?>> lookups,
            int failedLeaderLookups,
            long nextLeaderLookupTimeMs) {
        assertThat(metadataUpdater.failedLeaderLookups()).isEqualTo(failedLeaderLookups);
        assertThat(lookupSender.getNextLeaderLookupTimeMs(TABLE_BUCKET))
                .isEqualTo(nextLeaderLookupTimeMs);
        assertThat(lookupSender.getLookupsWaitingForLeader(TABLE_BUCKET))
                .containsExactlyInAnyOrderElementsOf(lookups);
        // Held by the sender, so that LookupQueue#drain() doesn't scan them on every pass.
        assertThat(lookupQueue.getReEnqueuedLookupQueue()).isEmpty();
        for (AbstractLookupQuery<?> lookup : lookups) {
            assertThat(lookup.future()).isNotDone();
            assertThat(lookup.retries()).isZero();
        }
    }

    @Test
    void testLeaderlessBucketBackoffRestartsAfterLeaderFound() throws Exception {
        ManualClock clock = new ManualClock(1_000);
        restartLookupSenderWithoutThread(new ExponentialBackoff(100, 2, 10_000, 0), clock);
        gateway.setLookupHandler(request -> createSuccessResponse(request, bytes("value")));

        metadataUpdater.failLeaderLookups(TABLE_BUCKET, 1, null);
        LookupQuery query = new LookupQuery(DATA1_TABLE_PATH_PK, TABLE_BUCKET, bytes("key1"));
        lookupSender.sendLookups(Collections.singletonList(query));
        clock.advanceTime(100, TimeUnit.MILLISECONDS);
        lookupSender.runOnce(false);
        assertThat(query.future().get(5, TimeUnit.SECONDS)).isEqualTo(bytes("value"));

        // The bucket had a leader in between, so it backs off from the initial delay again
        // instead of the 200 ms that would follow a second failure.
        metadataUpdater.failLeaderLookups(TABLE_BUCKET, 1, null);
        LookupQuery nextQuery = new LookupQuery(DATA1_TABLE_PATH_PK, TABLE_BUCKET, bytes("key2"));
        lookupSender.sendLookups(Collections.singletonList(nextQuery));
        assertThat(metadataUpdater.failedLeaderLookups()).isEqualTo(2);
        assertThat(lookupSender.getNextLeaderLookupTimeMs(TABLE_BUCKET))
                .isEqualTo(clock.milliseconds() + 100);
    }

    @Test
    void testCachedLeaderReleasesLookupsWaitingForIt() throws Exception {
        ManualClock clock = new ManualClock(1_000);
        // The backoff is long enough that only the cached leader can end the wait.
        restartLookupSenderWithoutThread(new ExponentialBackoff(60_000, 2, 60_000, 0), clock);
        gateway.setLookupHandler(this::createPartitionNameEchoResponse);

        // A lookup of the bucket that backs off from a server error, e.g. a throttled historical
        // lookup, before the bucket loses its leader.
        LookupQuery retryingQuery =
                new LookupQuery(DATA1_TABLE_PATH_PK, TABLE_BUCKET, bytes("retrying"));
        retryingQuery.setNextRetryTimeMs(31_000);
        lookupQueue.reEnqueue(retryingQuery);
        metadataUpdater.failLeaderLookups(TABLE_BUCKET, Integer.MAX_VALUE, null);
        LookupQuery waitingQuery = new LookupQuery(DATA1_TABLE_PATH_PK, TABLE_BUCKET, bytes("key"));
        lookupSender.sendLookups(Collections.singletonList(waitingQuery));
        assertThat(lookupSender.getLookupsWaitingForLeader(TABLE_BUCKET))
                .containsExactly(waitingQuery);
        assertThat(lookupSender.getNextLeaderLookupTimeMs(TABLE_BUCKET)).isEqualTo(61_000);

        // A metadata refresh elsewhere caches the leader while no new lookup arrives for the
        // bucket. The next pass sends the lookup that waited for the leader.
        metadataUpdater.cacheLeader(TABLE_BUCKET);
        lookupSender.runOnce(false);

        assertThat(waitingQuery.future()).isDone();
        assertThat(waitingQuery.future().get()).isEqualTo(responseValue("", "key"));
        assertThat(metadataUpdater.failedLeaderLookups()).isEqualTo(1);
        assertThat(lookupSender.getNextLeaderLookupTimeMs(TABLE_BUCKET)).isNull();
        // The lookup that backs off from a server error keeps its own delay.
        assertThat(retryingQuery.future()).isNotDone();
        assertThat(retryingQuery.nextRetryTimeMs()).isEqualTo(31_000);
        assertThat(lookupQueue.getReEnqueuedLookupQueue()).containsExactly(retryingQuery);
    }

    @Test
    void testLeaderFoundForNewLookupReleasesLookupsWaitingForIt() throws Exception {
        ManualClock clock = new ManualClock(1_000);
        restartLookupSenderWithoutThread(new ExponentialBackoff(60_000, 2, 60_000, 0), clock);
        gateway.setLookupHandler(this::createPartitionNameEchoResponse);

        metadataUpdater.failLeaderLookups(TABLE_BUCKET, Integer.MAX_VALUE, null);
        LookupQuery waitingQuery =
                new LookupQuery(DATA1_TABLE_PATH_PK, TABLE_BUCKET, bytes("key1"));
        lookupSender.sendLookups(Collections.singletonList(waitingQuery));
        assertThat(lookupSender.getLookupsWaitingForLeader(TABLE_BUCKET))
                .containsExactly(waitingQuery);

        // The leader gets cached while the sender drains the queue, so the pass finds it through a
        // new lookup of the bucket, which is sent right away.
        metadataUpdater.cacheLeader(TABLE_BUCKET);
        LookupQuery newQuery = new LookupQuery(DATA1_TABLE_PATH_PK, TABLE_BUCKET, bytes("key2"));
        lookupSender.sendLookups(Collections.singletonList(newQuery));
        assertThat(newQuery.future()).isDone();
        assertThat(newQuery.future().get()).isEqualTo(responseValue("", "key2"));

        // The lookup that waited for the leader no longer waits out the backoff.
        lookupSender.runOnce(false);
        assertThat(waitingQuery.future()).isDone();
        assertThat(waitingQuery.future().get()).isEqualTo(responseValue("", "key1"));
        assertThat(metadataUpdater.failedLeaderLookups()).isEqualTo(1);
    }

    @Test
    void testLeaderlessBucketKeepsBackoffWhileItsLookupsWaitBehindFullBatches() throws Exception {
        ManualClock clock = new ManualClock(1_000);
        restartLookupSenderWithoutThread(new ExponentialBackoff(100, 2, 10_000, 0), clock);
        gateway.setLookupHandler(this::createPartitionNameEchoResponse);

        // A full batch of lookups for another bucket waits for a retry ahead of the lookup of the
        // leaderless bucket.
        List<LookupQuery> otherQueries = new ArrayList<>();
        for (int i = 0; i < MAX_BATCH_SIZE; i++) {
            LookupQuery otherQuery =
                    new LookupQuery(DATA1_TABLE_PATH_PK, OTHER_TABLE_BUCKET, bytes("other" + i));
            otherQuery.setNextRetryTimeMs(1_100);
            lookupQueue.reEnqueue(otherQuery);
            otherQueries.add(otherQuery);
        }
        metadataUpdater.failLeaderLookups(TABLE_BUCKET, Integer.MAX_VALUE, null);
        LookupQuery query = new LookupQuery(DATA1_TABLE_PATH_PK, TABLE_BUCKET, bytes("key"));
        lookupSender.sendLookups(Collections.singletonList(query));
        assertThat(lookupSender.getNextLeaderLookupTimeMs(TABLE_BUCKET)).isEqualTo(1_100);

        // Once all are due, the other bucket's lookups fill the batch and the released lookup of
        // the leaderless bucket stays in the queue.
        clock.advanceTime(100, TimeUnit.MILLISECONDS);
        lookupSender.runOnce(false);
        for (int i = 0; i < MAX_BATCH_SIZE; i++) {
            assertThat(otherQueries.get(i).future()).isDone();
            assertThat(otherQueries.get(i).future().get())
                    .isEqualTo(responseValue("", "other" + i));
        }
        assertThat(lookupQueue.getReEnqueuedLookupQueue()).containsExactly(query);

        // The bucket still backs off from its first failure, so the next one doubles the delay.
        lookupSender.runOnce(false);
        assertThat(metadataUpdater.failedLeaderLookups()).isEqualTo(2);
        assertThat(lookupSender.getNextLeaderLookupTimeMs(TABLE_BUCKET))
                .isEqualTo(clock.milliseconds() + 200);
        assertThat(lookupSender.getLookupsWaitingForLeader(TABLE_BUCKET)).containsExactly(query);
    }

    @Test
    void testLeaderlessBucketsBackOffIndependently() throws Exception {
        ManualClock clock = new ManualClock(1_000);
        restartLookupSenderWithoutThread(new ExponentialBackoff(100, 2, 10_000, 0), clock);
        gateway.setLookupHandler(this::createPartitionNameEchoResponse);
        metadataUpdater.failLeaderLookups(TABLE_BUCKET, Integer.MAX_VALUE, null);
        metadataUpdater.failLeaderLookups(OTHER_TABLE_BUCKET, Integer.MAX_VALUE, null);

        LookupQuery query = new LookupQuery(DATA1_TABLE_PATH_PK, TABLE_BUCKET, bytes("key1"));
        lookupSender.sendLookups(Collections.singletonList(query));
        clock.advanceTime(50, TimeUnit.MILLISECONDS);
        LookupQuery otherQuery =
                new LookupQuery(DATA1_TABLE_PATH_PK, OTHER_TABLE_BUCKET, bytes("key2"));
        lookupSender.sendLookups(Collections.singletonList(otherQuery));
        assertThat(lookupSender.getNextLeaderLookupTimeMs(TABLE_BUCKET)).isEqualTo(1_100);
        assertThat(lookupSender.getNextLeaderLookupTimeMs(OTHER_TABLE_BUCKET)).isEqualTo(1_150);

        // Finding the leader of one bucket neither releases nor resets the other one.
        clock.advanceTime(50, TimeUnit.MILLISECONDS);
        metadataUpdater.cacheLeader(TABLE_BUCKET);
        lookupSender.runOnce(false);
        assertThat(query.future()).isDone();
        assertThat(query.future().get()).isEqualTo(responseValue("", "key1"));
        assertThat(lookupSender.getNextLeaderLookupTimeMs(TABLE_BUCKET)).isNull();
        assertThat(otherQuery.future()).isNotDone();
        assertThat(lookupSender.getLookupsWaitingForLeader(OTHER_TABLE_BUCKET))
                .containsExactly(otherQuery);
        assertThat(lookupSender.getNextLeaderLookupTimeMs(OTHER_TABLE_BUCKET)).isEqualTo(1_150);

        // The other bucket's backoff doubles from its own failures.
        clock.advanceTime(50, TimeUnit.MILLISECONDS);
        lookupSender.runOnce(false);
        assertThat(metadataUpdater.failedLeaderLookups()).isEqualTo(3);
        assertThat(lookupSender.getNextLeaderLookupTimeMs(OTHER_TABLE_BUCKET)).isEqualTo(1_350);
        assertThat(lookupSender.getLookupsWaitingForLeader(OTHER_TABLE_BUCKET))
                .containsExactly(otherQuery);
    }

    @Test
    void testLeaderlessBucketsOfSameTableShareMetadataRefreshInPass() throws Exception {
        ManualClock clock = new ManualClock(1_000);
        restartLookupSenderWithoutThread(new ExponentialBackoff(100, 2, 10_000, 0), clock);
        gateway.setLookupHandler(this::createPartitionNameEchoResponse);
        metadataUpdater.failLeaderLookups(TABLE_BUCKET, Integer.MAX_VALUE, null);
        metadataUpdater.failLeaderLookups(OTHER_TABLE_BUCKET, Integer.MAX_VALUE, null);

        LookupQuery query = new LookupQuery(DATA1_TABLE_PATH_PK, TABLE_BUCKET, bytes("key1"));
        LookupQuery otherQuery =
                new LookupQuery(DATA1_TABLE_PATH_PK, OTHER_TABLE_BUCKET, bytes("key2"));
        LookupQuery healthyQuery =
                new LookupQuery(DATA1_TABLE_PATH_PK, THIRD_TABLE_BUCKET, bytes("key3"));
        lookupSender.sendLookups(Arrays.asList(query, otherQuery, healthyQuery));

        // The refresh for the first bucket also covers the other buckets of the table, so they
        // don't refresh the table's metadata again, and both leaderless buckets back off.
        assertThat(metadataUpdater.failedLeaderLookups()).isEqualTo(1);
        assertThat(lookupSender.getLookupsWaitingForLeader(TABLE_BUCKET)).containsExactly(query);
        assertThat(lookupSender.getNextLeaderLookupTimeMs(TABLE_BUCKET)).isEqualTo(1_100);
        assertThat(lookupSender.getLookupsWaitingForLeader(OTHER_TABLE_BUCKET))
                .containsExactly(otherQuery);
        assertThat(lookupSender.getNextLeaderLookupTimeMs(OTHER_TABLE_BUCKET)).isEqualTo(1_100);
        assertThat(healthyQuery.future()).isDone();
        assertThat(healthyQuery.future().get()).isEqualTo(responseValue("", "key3"));
    }

    @Test
    void testCloseSendsOrFailsLookupsWaitingForLeader() throws Exception {
        ManualClock clock = new ManualClock(1_000);
        restartLookupSenderWithoutThread(new ExponentialBackoff(60_000, 2, 60_000, 0), clock);
        gateway.setLookupHandler(this::createPartitionNameEchoResponse);
        metadataUpdater.failLeaderLookups(TABLE_BUCKET, Integer.MAX_VALUE, null);
        metadataUpdater.failLeaderLookups(OTHER_TABLE_BUCKET, Integer.MAX_VALUE, null);

        LookupQuery leaderlessQuery =
                new LookupQuery(DATA1_TABLE_PATH_PK, TABLE_BUCKET, bytes("key1"));
        lookupSender.sendLookups(Collections.singletonList(leaderlessQuery));
        LookupQuery recoveredQuery =
                new LookupQuery(DATA1_TABLE_PATH_PK, OTHER_TABLE_BUCKET, bytes("key2"));
        lookupSender.sendLookups(Collections.singletonList(recoveredQuery));
        assertThat(metadataUpdater.failedLeaderLookups()).isEqualTo(2);
        assertThat(lookupQueue.hasUnDrained()).isFalse();
        metadataUpdater.cacheLeader(OTHER_TABLE_BUCKET);

        // Both buckets still back off when the sender closes, and only they have lookups left.
        // The queue no longer accepts lookups, so a lookup that waits for a leader is sent if the
        // leader is found now, and fails otherwise, without keeping the other one from being sent.
        lookupSender.initiateClose();
        lookupSender.run();

        assertThat(recoveredQuery.future().get(5, TimeUnit.SECONDS))
                .isEqualTo(responseValue("", "key2"));
        assertThatThrownBy(() -> leaderlessQuery.future().get(5, TimeUnit.SECONDS))
                .isInstanceOf(ExecutionException.class)
                .hasRootCauseInstanceOf(FlussRuntimeException.class)
                .hasMessageContaining("Leader not found");
        assertThat(metadataUpdater.failedLeaderLookups()).isEqualTo(3);
        assertThat(lookupSender.getLookupsWaitingForLeader(TABLE_BUCKET)).isEmpty();
    }

    @Test
    void testSenderDoesNotBusyLoopWhileLookupsWaitForLeader() throws Exception {
        teardown();
        long batchTimeoutMs = 50;
        Configuration conf = lookupQueueConf();
        conf.set(ConfigOptions.CLIENT_LOOKUP_BATCH_TIMEOUT, Duration.ofMillis(batchTimeoutMs));
        AtomicInteger drains = new AtomicInteger();
        AtomicInteger reEnqueues = new AtomicInteger();
        LookupQueue countingQueue =
                new LookupQueue(conf) {
                    @Override
                    List<AbstractLookupQuery<?>> drain() throws Exception {
                        drains.incrementAndGet();
                        return super.drain();
                    }

                    @Override
                    void reEnqueue(AbstractLookupQuery<?> lookup) {
                        reEnqueues.incrementAndGet();
                        super.reEnqueue(lookup);
                    }

                    @Override
                    boolean tryReEnqueue(AbstractLookupQuery<?> lookup) {
                        boolean reEnqueued = super.tryReEnqueue(lookup);
                        if (reEnqueued) {
                            reEnqueues.incrementAndGet();
                        }
                        return reEnqueued;
                    }
                };
        createLookupSender(countingQueue, new ExponentialBackoff(60_000, 2, 60_000, 0));
        metadataUpdater.failLeaderLookups(TABLE_BUCKET, Integer.MAX_VALUE, null);
        // More lookups than fit in a batch: the one drained in a later pass waits for the same
        // backoff without another leader lookup.
        for (int i = 0; i <= MAX_BATCH_SIZE; i++) {
            lookupQueue.reEnqueue(
                    new LookupQuery(DATA1_TABLE_PATH_PK, TABLE_BUCKET, bytes("key" + i)));
        }
        senderThread = new Thread(lookupSender);
        senderThread.start();
        waitUntil(
                () ->
                        metadataUpdater.failedLeaderLookups() == 1
                                && lookupQueue.getReEnqueuedLookupQueue().isEmpty(),
                Duration.ofSeconds(10),
                "lookups to wait for the leader");

        // Started before the drains are counted, so the window covers every counted drain.
        long startNanos = System.nanoTime();
        int drainsBefore = drains.get();
        int reEnqueuesBefore = reEnqueues.get();
        waitUntil(
                () -> drains.get() >= drainsBefore + 5,
                Duration.ofSeconds(10),
                "the sender to drain the queue");
        long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);

        // Each of the four drains in between blocks for the batch timeout, as no lookup is ready.
        // The waiting lookups neither look up the leader nor go through the queue on each pass.
        assertThat(elapsedMs).isGreaterThanOrEqualTo(4 * batchTimeoutMs);
        assertThat(metadataUpdater.failedLeaderLookups()).isEqualTo(1);
        assertThat(reEnqueues.get()).isEqualTo(reEnqueuesBefore);
    }

    @Test
    void testLeaderlessBucketDoesNotUseUpRetries() throws Exception {
        int leaderlessPasses = MAX_RETRIES + 2;
        metadataUpdater.failLeaderLookups(TABLE_BUCKET, leaderlessPasses, null);
        gateway.setLookupHandler(request -> createSuccessResponse(request, bytes("value")));

        LookupQuery query = new LookupQuery(DATA1_TABLE_PATH_PK, TABLE_BUCKET, bytes("key"));
        lookupQueue.appendLookup(query);

        assertThat(query.future().get(5, TimeUnit.SECONDS)).isEqualTo(bytes("value"));
        assertThat(metadataUpdater.failedLeaderLookups()).isEqualTo(leaderlessPasses);
        assertThat(query.retries()).isZero();
    }

    @Test
    void testPartitionNotExistCompletesAllLookupsForBucket() throws Exception {
        ManualClock clock = new ManualClock(1_000);
        restartLookupSenderWithoutThread(new ExponentialBackoff(100, 2, 10_000, 0), clock);
        metadataUpdater.failLeaderLookups(TABLE_BUCKET, 1, null);
        LookupQuery waitingQuery =
                new LookupQuery(DATA1_TABLE_PATH_PK, TABLE_BUCKET, bytes("waiting"));
        lookupSender.sendLookups(Collections.singletonList(waitingQuery));
        assertThat(lookupSender.getLookupsWaitingForLeader(TABLE_BUCKET))
                .containsExactly(waitingQuery);

        // The refresh after the backoff finds that the partition was dropped.
        clock.advanceTime(100, TimeUnit.MILLISECONDS);
        metadataUpdater.failLeaderLookups(
                TABLE_BUCKET,
                Integer.MAX_VALUE,
                new PartitionNotExistException("partition dropped"));
        LookupQuery query1 = new LookupQuery(DATA1_TABLE_PATH_PK, TABLE_BUCKET, bytes("key1"));
        LookupQuery query2 = new LookupQuery(DATA1_TABLE_PATH_PK, TABLE_BUCKET, bytes("key2"));
        lookupSender.sendLookups(Arrays.asList(query1, query2));
        assertThat(metadataUpdater.failedLeaderLookups()).isEqualTo(2);
        assertThat(lookupSender.getNextLeaderLookupTimeMs(TABLE_BUCKET)).isNull();
        assertFailedWithPartitionNotExist(Arrays.asList(query1, query2));
        // The waiting lookup goes back to the queue and fails in the next batch.
        assertThat(waitingQuery.future()).isNotDone();
        assertThat(lookupQueue.getReEnqueuedLookupQueue()).containsExactly(waitingQuery);

        lookupSender.runOnce(false);
        assertThat(metadataUpdater.failedLeaderLookups()).isEqualTo(3);
        assertFailedWithPartitionNotExist(Collections.singletonList(waitingQuery));
        assertThat(lookupQueue.hasUnDrained()).isFalse();
    }

    @Test
    void testDroppedPartitionDoesNotBlockSenderOnReroutedLookups() throws Exception {
        teardown();
        ManualClock clock = new ManualClock(1_000);
        AtomicBoolean expireBackoffDuringDrain = new AtomicBoolean();
        LookupQueue queue =
                new LookupQueue(lookupQueueConf(), clock) {
                    @Override
                    List<AbstractLookupQuery<?>> drain() throws Exception {
                        List<AbstractLookupQuery<?>> drained = super.drain();
                        if (expireBackoffDuringDrain.compareAndSet(true, false)) {
                            clock.advanceTime(100, TimeUnit.MILLISECONDS);
                        }
                        return drained;
                    }
                };
        createLookupSender(queue, new ExponentialBackoff(100, 2, 10_000, 0));
        gateway.setLookupHandler(this::createPartitionNameEchoResponse);

        // More lookups wait for the leader than the queue holds. Like a primary key lookuper, each
        // one that fails is rerouted by a callback that enqueues a replacement lookup.
        metadataUpdater.failLeaderLookups(TABLE_BUCKET, Integer.MAX_VALUE, null);
        List<LookupQuery> waitingQueries = new ArrayList<>();
        List<LookupQuery> replacements = Collections.synchronizedList(new ArrayList<>());
        for (int i = 0; i <= lookupQueue.getLookupQueue().remainingCapacity(); i++) {
            waitingQueries.add(
                    reroutedLookup(bytes("waiting" + i), bytes("rerouted" + i), replacements));
        }
        lookupSender.sendLookups(new ArrayList<>(waitingQueries));
        assertThat(lookupSender.getLookupsWaitingForLeader(TABLE_BUCKET))
                .containsExactlyElementsOf(waitingQueries);

        // The partition is dropped, and the bucket's backoff expires while a new lookup for it is
        // drained, so that lookup finds out while the others still wait.
        metadataUpdater.failLeaderLookups(
                TABLE_BUCKET,
                Integer.MAX_VALUE,
                new PartitionNotExistException("partition dropped"));
        LookupQuery lateQuery = reroutedLookup(bytes("late"), bytes("rerouted-late"), replacements);
        lookupQueue.appendLookup(lateQuery);
        expireBackoffDuringDrain.set(true);

        // Run the passes on another thread, as the sender thread does, so that a pass blocked on
        // the full queue fails the test instead of blocking it.
        ExecutorService senderExecutor = Executors.newSingleThreadExecutor();
        try {
            runOnceOn(senderExecutor);
            assertFailedWithPartitionNotExist(Collections.singletonList(lateQuery));
            // The waiting lookups fail in later batches instead of all at once.
            for (LookupQuery waitingQuery : waitingQueries) {
                assertThat(waitingQuery.future()).isNotDone();
            }
            assertThat(lookupQueue.getReEnqueuedLookupQueue())
                    .containsExactlyElementsOf(waitingQueries);

            // Their replacements don't fit in the queue, but the sender doesn't wait for space.
            runOnceOn(senderExecutor);
            assertFailedWithPartitionNotExist(waitingQueries);

            runOnceOn(senderExecutor);
            assertThat(replacements).hasSize(waitingQueries.size() + 1);
            for (LookupQuery replacement : replacements) {
                assertThat(replacement.future().get(5, TimeUnit.SECONDS))
                        .isEqualTo(
                                responseValue(
                                        "", new String(replacement.key(), StandardCharsets.UTF_8)));
            }
            assertThat(lookupQueue.hasUnDrained()).isFalse();
        } finally {
            senderExecutor.shutdownNow();
        }
    }

    @Test
    void testDroppedPartitionKeepsWaitingLookupsWhenQueueClosesConcurrently() throws Exception {
        teardown();
        ManualClock clock = new ManualClock(1_000);
        AtomicBoolean closeOnReEnqueue = new AtomicBoolean();
        LookupQueue queue =
                new LookupQueue(lookupQueueConf(), clock) {
                    @Override
                    boolean tryReEnqueue(AbstractLookupQuery<?> lookup) {
                        boolean reEnqueued = super.tryReEnqueue(lookup);
                        // The client is closed while the waiting lookups go back to the queue.
                        if (closeOnReEnqueue.get()) {
                            close();
                        }
                        return reEnqueued;
                    }
                };
        createLookupSender(queue, new ExponentialBackoff(100, 2, 10_000, 0));
        gateway.setLookupHandler(this::createPartitionNameEchoResponse);
        metadataUpdater.failLeaderLookups(TABLE_BUCKET, Integer.MAX_VALUE, null);
        List<LookupQuery> waitingQueries =
                Arrays.asList(
                        new LookupQuery(DATA1_TABLE_PATH_PK, TABLE_BUCKET, bytes("waiting1")),
                        new LookupQuery(DATA1_TABLE_PATH_PK, TABLE_BUCKET, bytes("waiting2")));
        lookupSender.sendLookups(new ArrayList<>(waitingQueries));

        // After the backoff, a new lookup finds that the partition was dropped, and the queue
        // closes after the first waiting lookup is moved back to it.
        clock.advanceTime(100, TimeUnit.MILLISECONDS);
        metadataUpdater.failLeaderLookups(
                TABLE_BUCKET,
                Integer.MAX_VALUE,
                new PartitionNotExistException("partition dropped"));
        closeOnReEnqueue.set(true);
        // The healthy lookup is grouped first, so that it is pending when the queue closes.
        LookupQuery healthyQuery =
                new LookupQuery(DATA1_TABLE_PATH_PK, OTHER_TABLE_BUCKET, bytes("healthy"));
        LookupQuery query = new LookupQuery(DATA1_TABLE_PATH_PK, TABLE_BUCKET, bytes("key"));
        lookupSender.sendLookups(Arrays.asList(healthyQuery, query));
        assertFailedWithPartitionNotExist(Collections.singletonList(query));
        // The pass goes on, and the lookup that didn't make it back to the queue stays with the
        // bucket.
        assertThat(healthyQuery.future().get(5, TimeUnit.SECONDS))
                .isEqualTo(responseValue("", "healthy"));
        assertThat(lookupSender.getLookupsWaitingForLeader(TABLE_BUCKET))
                .containsExactly(waitingQueries.get(1));

        // Both waiting lookups still complete when the sender closes.
        lookupSender.initiateClose();
        lookupSender.run();
        assertFailedWithPartitionNotExist(waitingQueries);
    }

    /**
     * Creates a lookup for {@link #TABLE_BUCKET} that, when it fails, enqueues a replacement lookup
     * for {@link #OTHER_TABLE_BUCKET}.
     */
    private LookupQuery reroutedLookup(
            byte[] key, byte[] replacementKey, List<LookupQuery> replacements) {
        LookupQuery lookup = new LookupQuery(DATA1_TABLE_PATH_PK, TABLE_BUCKET, key);
        lookup.future()
                .whenComplete(
                        (result, error) -> {
                            if (error != null) {
                                LookupQuery replacement =
                                        new LookupQuery(
                                                DATA1_TABLE_PATH_PK,
                                                OTHER_TABLE_BUCKET,
                                                replacementKey);
                                replacements.add(replacement);
                                lookupQueue.appendLookup(replacement);
                            }
                        });
        return lookup;
    }

    private void runOnceOn(ExecutorService senderExecutor) throws Exception {
        senderExecutor
                .submit(
                        () -> {
                            lookupSender.runOnce(false);
                            return null;
                        })
                .get(10, TimeUnit.SECONDS);
    }

    private static void assertFailedWithPartitionNotExist(List<LookupQuery> queries) {
        for (LookupQuery query : queries) {
            assertThatThrownBy(() -> query.future().get(5, TimeUnit.SECONDS))
                    .isInstanceOf(ExecutionException.class)
                    .hasRootCauseInstanceOf(PartitionNotExistException.class);
        }
    }

    @Test
    void testHistoricalLookupThrottleRetryBackoffAndMaxRetries() throws Exception {
        // Throttled historical lookup should be retried and eventually succeed.
        AtomicInteger attemptCount = new AtomicInteger(0);
        gateway.setLookupHandler(
                request -> {
                    int attempt = attemptCount.incrementAndGet();
                    if (attempt == 1) {
                        return createFailedResponse(
                                request,
                                new HistoricalPartitionThrottledException(
                                        "historical lookup throttled"));
                    }
                    return createSuccessResponse(request, bytes("historical-value"));
                });

        LookupQuery query =
                new LookupQuery(
                        DATA1_TABLE_PATH_PK, TABLE_BUCKET, bytes("key"), false, "dt=20200101");
        lookupQueue.appendLookup(query);

        assertThat(query.future().get(5, TimeUnit.SECONDS)).isEqualTo(bytes("historical-value"));
        assertThat(attemptCount.get()).isEqualTo(2);
        assertThat(query.retries()).isEqualTo(1);

        // Delayed historical retry should not block a new normal lookup from the main queue.
        AtomicInteger historicalAttempts = new AtomicInteger(0);
        AtomicInteger normalAttempts = new AtomicInteger(0);
        CountDownLatch firstHistoricalAttemptStarted = new CountDownLatch(1);
        CountDownLatch normalQueryAppended = new CountDownLatch(1);
        List<String> requestOrder = Collections.synchronizedList(new ArrayList<>());
        gateway.setLookupHandler(
                request -> {
                    PbLookupReqForBucket bucketRequest = request.getBucketsReqAt(0);
                    if (bucketRequest.hasOriginalPartitionName()) {
                        int attempt = historicalAttempts.incrementAndGet();
                        if (attempt == 1) {
                            firstHistoricalAttemptStarted.countDown();
                            awaitLatch(normalQueryAppended);
                            requestOrder.add("historical-throttled");
                            return createFailedResponse(
                                    request,
                                    new HistoricalPartitionThrottledException(
                                            "historical lookup throttled"));
                        }
                        requestOrder.add("historical-success");
                        return createSuccessResponse(request, bytes("historical-value"));
                    }

                    normalAttempts.incrementAndGet();
                    requestOrder.add("normal-success");
                    return createSuccessResponse(request, bytes("normal-value"));
                });

        LookupQuery historicalQuery =
                new LookupQuery(
                        DATA1_TABLE_PATH_PK,
                        TABLE_BUCKET,
                        bytes("historical-key"),
                        false,
                        "dt=20200101");
        lookupQueue.appendLookup(historicalQuery);

        assertThat(firstHistoricalAttemptStarted.await(5, TimeUnit.SECONDS)).isTrue();
        LookupQuery normalQuery =
                new LookupQuery(DATA1_TABLE_PATH_PK, TABLE_BUCKET, bytes("normal-key"));
        lookupQueue.appendLookup(normalQuery);
        normalQueryAppended.countDown();

        assertThat(normalQuery.future().get(1, TimeUnit.SECONDS)).isEqualTo(bytes("normal-value"));
        assertThat(historicalQuery.future().get(5, TimeUnit.SECONDS))
                .isEqualTo(bytes("historical-value"));
        assertThat(historicalAttempts.get()).isEqualTo(2);
        assertThat(normalAttempts.get()).isEqualTo(1);
        assertThat(requestOrder)
                .containsExactly("historical-throttled", "normal-success", "historical-success");

        // A historical lookup that keeps being throttled should fail after max retries.
        AtomicInteger alwaysThrottledAttempts = new AtomicInteger(0);
        gateway.setLookupHandler(
                request -> {
                    alwaysThrottledAttempts.incrementAndGet();
                    return createFailedResponse(
                            request,
                            new HistoricalPartitionThrottledException(
                                    "historical lookup throttled"));
                });

        LookupQuery alwaysThrottledQuery =
                new LookupQuery(
                        DATA1_TABLE_PATH_PK, TABLE_BUCKET, bytes("key"), false, "dt=20200101");
        lookupQueue.appendLookup(alwaysThrottledQuery);

        assertThatThrownBy(() -> alwaysThrottledQuery.future().get(10, TimeUnit.SECONDS))
                .isInstanceOf(ExecutionException.class)
                .hasRootCauseInstanceOf(HistoricalPartitionThrottledException.class);
        assertThat(alwaysThrottledAttempts.get()).isEqualTo(1 + MAX_RETRIES);
        assertThat(alwaysThrottledQuery.retries()).isEqualTo(MAX_RETRIES);
    }

    @Test
    void testNonRetriableExceptionDoesNotRetry() {
        // setup: fail with non-retriable exception
        gateway.setLookupHandler(
                request ->
                        createFailedResponse(
                                request, new TableNotExistException("table not found")));

        // execute: submit lookup
        byte[] key = "key".getBytes();
        LookupQuery query = new LookupQuery(DATA1_TABLE_PATH_PK, TABLE_BUCKET, key);
        lookupQueue.appendLookup(query);

        // verify: fails immediately without retry
        assertThatThrownBy(() -> query.future().get(5, TimeUnit.SECONDS))
                .isInstanceOf(ExecutionException.class)
                .hasRootCauseInstanceOf(TableNotExistException.class);
        assertThat(query.retries()).isEqualTo(0); // no retries
    }

    @Test
    void testInvalidBucketRoutingFailsWithoutRetryAndInvalidatesMetadata() {
        AtomicInteger attemptCount = new AtomicInteger();
        gateway.setLookupHandler(
                request -> {
                    attemptCount.incrementAndGet();
                    return createFailedResponse(
                            request, new InvalidBucketRoutingException("invalid bucket routing"));
                });

        LookupQuery query = new LookupQuery(DATA1_TABLE_PATH_PK, TABLE_BUCKET, new byte[0]);
        lookupQueue.appendLookup(query);

        assertThatThrownBy(() -> query.future().get(5, TimeUnit.SECONDS))
                .isInstanceOf(ExecutionException.class)
                .hasRootCauseInstanceOf(InvalidBucketRoutingException.class);
        assertThat(attemptCount).hasValue(1);
        assertThat(query.retries()).isZero();
        assertThat(metadataUpdater.getBucketLocation(TABLE_BUCKET)).isEmpty();
    }

    @Test
    void testMaxRetriesEnforced() {
        // setup: always fail with retriable exception
        AtomicInteger attemptCount = new AtomicInteger(0);
        gateway.setLookupHandler(
                request -> {
                    attemptCount.incrementAndGet();
                    return createFailedResponse(request, new TimeoutException("timeout"));
                });

        // execute: submit lookup
        byte[] key = "key".getBytes();
        LookupQuery query = new LookupQuery(DATA1_TABLE_PATH_PK, TABLE_BUCKET, key);
        lookupQueue.appendLookup(query);

        // verify: eventually fails after max retries
        assertThatThrownBy(() -> query.future().get(5, TimeUnit.SECONDS))
                .isInstanceOf(ExecutionException.class)
                .hasRootCauseInstanceOf(TimeoutException.class);

        // should attempt: 1 initial + MAX_RETRIES retries
        assertThat(attemptCount.get()).isEqualTo(1 + MAX_RETRIES);
        assertThat(query.retries()).isEqualTo(MAX_RETRIES);
    }

    @Test
    void testRetryStopsIfFutureCompleted() throws Exception {
        // setup: always fail with retriable exception
        AtomicInteger attemptCount = new AtomicInteger(0);
        gateway.setLookupHandler(
                request -> {
                    int attempt = attemptCount.incrementAndGet();
                    if (attempt == 1) {
                        // first attempt fails
                        return createFailedResponse(request, new TimeoutException("timeout"));
                    } else {
                        try {
                            // Avoid attempting again too quickly
                            Thread.sleep(100);
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                        // subsequent attempts should not happen if we complete the future
                        throw new AssertionError(
                                "Should not retry after future is completed externally");
                    }
                });

        // execute: submit lookup
        byte[] key = "key".getBytes();
        LookupQuery query = new LookupQuery(DATA1_TABLE_PATH_PK, TABLE_BUCKET, key);
        lookupQueue.appendLookup(query);

        // complete the future externally before retry happens
        waitUntil(() -> attemptCount.get() >= 1, Duration.ofSeconds(5), "first attempt to be made");
        query.future().complete("external".getBytes());

        // verify: completed externally
        byte[] result = query.future().get(1, TimeUnit.SECONDS);
        assertThat(result).isEqualTo("external".getBytes());
        // retries is less than 3, because we stop the query so it won't send again.
        assertThat(query.retries()).isGreaterThanOrEqualTo(0).isLessThan(3);
        assertThat(attemptCount.get()).isGreaterThanOrEqualTo(1).isLessThan(4);
    }

    @Test
    void testDifferentExceptionTypesHandledCorrectly() throws Exception {
        // test multiple exception types
        testException(new TimeoutException("timeout"), true, 3); // retriable, should retry
        testException(new InvalidTableException("invalid"), false, 0); // non-retriable, no retry
        testException(new TableNotExistException("not exist"), false, 0); // non-retriable, no retry
    }

    @Test
    void testPrefixLookupRetry() throws Exception {
        // setup: fail twice with retriable exception, then succeed
        AtomicInteger attemptCount = new AtomicInteger(0);
        gateway.setPrefixLookupHandler(
                request -> {
                    int attempt = attemptCount.incrementAndGet();
                    if (attempt <= 2) {
                        // first two attempts fail
                        return createFailedPrefixLookupResponse(
                                request, new TimeoutException("timeout"));
                    } else {
                        // third attempt succeeds
                        return createSuccessPrefixLookupResponse(request);
                    }
                });

        // execute: submit prefix lookup
        byte[] prefixKey = "prefix".getBytes();
        PrefixLookupQuery query =
                new PrefixLookupQuery(DATA1_TABLE_PATH_PK, TABLE_BUCKET, prefixKey);
        lookupQueue.appendLookup(query);

        // verify: eventually succeeds after retries
        query.future().get(5, TimeUnit.SECONDS);
        assertThat(attemptCount.get()).isEqualTo(3);
        assertThat(query.retries()).isEqualTo(2);
    }

    @Test
    void testMultipleConcurrentLookupsWithRetries() throws Exception {
        // setup: first attempt fails, second succeeds
        AtomicInteger attemptCount = new AtomicInteger(0);
        gateway.setLookupHandler(
                request -> {
                    int attempt = attemptCount.incrementAndGet();
                    if (attempt % 2 == 1) {
                        // odd attempts fail
                        return createFailedResponse(request, new TimeoutException("timeout"));
                    } else {
                        // even attempts succeed
                        return createSuccessResponse(request, ("value" + attempt).getBytes());
                    }
                });

        // execute: submit multiple lookups
        LookupQuery query1 = new LookupQuery(DATA1_TABLE_PATH_PK, TABLE_BUCKET, "key1".getBytes());
        LookupQuery query2 = new LookupQuery(DATA1_TABLE_PATH_PK, TABLE_BUCKET, "key2".getBytes());
        LookupQuery query3 = new LookupQuery(DATA1_TABLE_PATH_PK, TABLE_BUCKET, "key3".getBytes());

        lookupQueue.appendLookup(query1);
        lookupQueue.appendLookup(query2);
        lookupQueue.appendLookup(query3);

        // verify: all succeed after retries
        assertThat(query1.future().get(5, TimeUnit.SECONDS)).isNotNull();
        assertThat(query2.future().get(5, TimeUnit.SECONDS)).isNotNull();
        assertThat(query3.future().get(5, TimeUnit.SECONDS)).isNotNull();
        // Note: lookups are batched together, so attemptCount reflects batch attempts, not
        // individual lookups
        assertThat(attemptCount.get())
                .isGreaterThanOrEqualTo(2); // at least 1 failure + 1 success for the batch
    }

    @Test
    void testLookupRequestCarriesPinnedRoutingBucketCount() throws Exception {
        // TOCTOU: the bucket count pinned at T1 (lookup time) must be carried to T2 (send time)
        // as the request's routing_bucket_count, not re-read from cluster metadata at T2.
        List<LookupRequest> receivedRequests = Collections.synchronizedList(new ArrayList<>());
        gateway.setLookupHandler(
                request -> {
                    receivedRequests.add(request);
                    return createSuccessResponse(request, "value".getBytes());
                });

        // T1: create query with bucketCount=4 (the partition's actual count at lookup time)
        LookupQuery query =
                new LookupQuery(DATA1_TABLE_PATH_PK, TABLE_BUCKET, bytes("key"), false, null, 4);
        // The pinned value is visible on the query object
        assertThat(query.bucketCount()).isEqualTo(4);

        lookupSender.sendLookups(1, LookupType.LOOKUP, Collections.singletonList(query));

        // T2: the request must carry the T1-pinned count as routing_bucket_count
        assertThat(receivedRequests).hasSize(1);
        LookupRequest request = receivedRequests.get(0);
        assertThat(request.getBucketsReqAt(0).hasRoutingBucketCount()).isTrue();
        assertThat(request.getBucketsReqAt(0).getRoutingBucketCount()).isEqualTo(4);

        // A legacy query (bucketCount=0) must not set routing_bucket_count at all, letting
        // the server's epoch check decide.
        receivedRequests.clear();
        LookupQuery legacyQuery = new LookupQuery(DATA1_TABLE_PATH_PK, TABLE_BUCKET, bytes("key"));
        assertThat(legacyQuery.bucketCount()).isEqualTo(0);

        lookupSender.sendLookups(1, LookupType.LOOKUP, Collections.singletonList(legacyQuery));

        assertThat(receivedRequests).hasSize(1);
        assertThat(receivedRequests.get(0).getBucketsReqAt(0).hasRoutingBucketCount()).isFalse();
    }

    @Test
    void testPrefixLookupBatchesPreserveEachLimitPolicy() throws Exception {
        List<PrefixLookupRequest> requests = new ArrayList<>();
        gateway.setPrefixLookupHandler(
                request -> {
                    requests.add(request);
                    return createSuccessPrefixLookupResponse(request);
                });
        PrefixLookupQuery unlimited =
                new PrefixLookupQuery(DATA1_TABLE_PATH_PK, TABLE_BUCKET, bytes("unlimited"));
        PrefixLookupQuery truncate =
                new PrefixLookupQuery(
                        DATA1_TABLE_PATH_PK, TABLE_BUCKET, bytes("truncate"), 0, 2, false);
        PrefixLookupQuery fail =
                new PrefixLookupQuery(DATA1_TABLE_PATH_PK, TABLE_BUCKET, bytes("fail"), 0, 2, true);
        PrefixLookupQuery samePolicy =
                new PrefixLookupQuery(DATA1_TABLE_PATH_PK, TABLE_BUCKET, bytes("same"), 0, 2, true);
        lookupSender.sendLookups(
                1, LookupType.PREFIX_LOOKUP, Arrays.asList(unlimited, truncate, fail, samePolicy));
        assertThat(requests).hasSize(3);
        assertThat(requests)
                .anySatisfy(
                        request -> {
                            assertThat(request.hasLimit()).isFalse();
                            assertThat(request.hasFailOnLimitExceeded()).isFalse();
                            assertThat(request.getBucketsReqAt(0).getKeysCount()).isEqualTo(1);
                        });
        assertThat(requests)
                .anySatisfy(
                        request -> {
                            assertThat(request.hasLimit()).isTrue();
                            assertThat(request.getLimit()).isEqualTo(2);
                            assertThat(request.isFailOnLimitExceeded()).isFalse();
                            assertThat(request.getBucketsReqAt(0).getKeysCount()).isEqualTo(1);
                        });
        assertThat(requests)
                .anySatisfy(
                        request -> {
                            assertThat(request.hasLimit()).isTrue();
                            assertThat(request.getLimit()).isEqualTo(2);
                            assertThat(request.isFailOnLimitExceeded()).isTrue();
                            assertThat(request.getBucketsReqAt(0).getKeysCount()).isEqualTo(2);
                        });
        for (PrefixLookupQuery query : Arrays.asList(unlimited, truncate, fail, samePolicy)) {
            assertThat(query.future().get(5, TimeUnit.SECONDS)).isEmpty();
        }
    }

    @Test
    void testPrefixCandidateOverflowIsNotRetried() throws Exception {
        gateway.setPrefixLookupHandler(
                request ->
                        createFailedPrefixLookupResponse(
                                request,
                                new LookupLimitExceededException("candidate limit exceeded")));
        PrefixLookupQuery query =
                new PrefixLookupQuery(
                        DATA1_TABLE_PATH_PK, TABLE_BUCKET, bytes("prefix"), 0, 2, true);
        lookupSender.sendLookups(1, LookupType.PREFIX_LOOKUP, Collections.singletonList(query));
        assertThatThrownBy(() -> query.future().get(5, TimeUnit.SECONDS))
                .hasRootCauseInstanceOf(LookupLimitExceededException.class);
        assertThat(query.retries()).isZero();
    }

    @Test
    void testPrefixLookupRequestCarriesPinnedRoutingBucketCount() throws Exception {
        // TOCTOU: same anchoring for prefix lookup path.
        List<PrefixLookupRequest> receivedRequests =
                Collections.synchronizedList(new ArrayList<>());
        gateway.setPrefixLookupHandler(
                request -> {
                    receivedRequests.add(request);
                    return createSuccessPrefixLookupResponse(request);
                });

        // T1: create prefix query with bucketCount=4
        PrefixLookupQuery query =
                new PrefixLookupQuery(DATA1_TABLE_PATH_PK, TABLE_BUCKET, bytes("prefix"), 4);
        assertThat(query.bucketCount()).isEqualTo(4);

        lookupSender.sendLookups(1, LookupType.PREFIX_LOOKUP, Collections.singletonList(query));

        // T2: the request must carry the T1-pinned count as routing_bucket_count
        assertThat(receivedRequests).hasSize(1);
        PrefixLookupRequest request = receivedRequests.get(0);
        assertThat(request.getBucketsReqAt(0).hasRoutingBucketCount()).isTrue();
        assertThat(request.getBucketsReqAt(0).getRoutingBucketCount()).isEqualTo(4);
    }

    // Helper methods

    private CompletableFuture<LookupResponse> createPartitionNameEchoResponse(
            LookupRequest request) {
        LookupResponse response = new LookupResponse();
        for (PbLookupReqForBucket bucketRequest : request.getBucketsReqsList()) {
            PbLookupRespForBucket bucketResponse = response.addBucketsResp();
            bucketResponse.setBucketId(bucketRequest.getBucketId());
            if (bucketRequest.hasPartitionId()) {
                bucketResponse.setPartitionId(bucketRequest.getPartitionId());
            }
            if (bucketRequest.hasOriginalPartitionName()) {
                bucketResponse.setOriginalPartitionName(bucketRequest.getOriginalPartitionName());
            }
            String partitionName =
                    bucketRequest.hasOriginalPartitionName()
                            ? bucketRequest.getOriginalPartitionName()
                            : "";
            for (int i = 0; i < bucketRequest.getKeysCount(); i++) {
                bucketResponse
                        .addValue()
                        .setValues(
                                responseValue(
                                        partitionName,
                                        new String(
                                                bucketRequest.getKeyAt(i),
                                                StandardCharsets.UTF_8)));
            }
        }
        return CompletableFuture.completedFuture(response);
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static byte[] responseValue(String partitionName, String key) {
        return bytes(partitionName + ":" + key);
    }

    private static void awaitLatch(CountDownLatch latch) {
        try {
            assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    private void testException(Exception exception, boolean shouldRetry, int expectedRetries)
            throws Exception {
        // reset gateway
        AtomicInteger attemptCount = new AtomicInteger(0);
        gateway.setLookupHandler(
                request -> {
                    attemptCount.incrementAndGet();
                    return createFailedResponse(request, exception);
                });

        // execute
        byte[] key = ("key-" + exception.getClass().getSimpleName()).getBytes();
        LookupQuery query = new LookupQuery(DATA1_TABLE_PATH_PK, TABLE_BUCKET, key);
        lookupQueue.appendLookup(query);

        // verify
        assertThatThrownBy(() -> query.future().get(5, TimeUnit.SECONDS))
                .isInstanceOf(ExecutionException.class);

        if (shouldRetry) {
            assertThat(attemptCount.get()).isEqualTo(1 + MAX_RETRIES);
            assertThat(query.retries()).isEqualTo(expectedRetries);
        } else {
            assertThat(attemptCount.get()).isEqualTo(1); // only initial attempt
            assertThat(query.retries()).isEqualTo(expectedRetries);
        }

        // wait a bit to ensure no more attempts
        Thread.sleep(200);
    }

    private CompletableFuture<LookupResponse> createSuccessResponse(
            LookupRequest request, byte[] value) {
        LookupResponse response = new LookupResponse();
        PbLookupRespForBucket bucketResp = response.addBucketsResp();
        bucketResp.setBucketId(TABLE_BUCKET.getBucket());
        if (TABLE_BUCKET.getPartitionId() != null) {
            bucketResp.setPartitionId(TABLE_BUCKET.getPartitionId());
        }
        if (request.getBucketsReqAt(0).hasOriginalPartitionName()) {
            bucketResp.setOriginalPartitionName(
                    request.getBucketsReqAt(0).getOriginalPartitionName());
        }
        // Add value for each key in the request
        int keyCount = request.getBucketsReqAt(0).getKeysCount();
        for (int i = 0; i < keyCount; i++) {
            bucketResp.addValue().setValues(value);
        }
        return CompletableFuture.completedFuture(response);
    }

    private CompletableFuture<LookupResponse> createFailedResponse(
            LookupRequest request, Exception exception) {
        LookupResponse response = new LookupResponse();
        PbLookupRespForBucket bucketResp = response.addBucketsResp();
        bucketResp.setBucketId(TABLE_BUCKET.getBucket());
        if (TABLE_BUCKET.getPartitionId() != null) {
            bucketResp.setPartitionId(TABLE_BUCKET.getPartitionId());
        }
        if (request.getBucketsReqAt(0).hasOriginalPartitionName()) {
            bucketResp.setOriginalPartitionName(
                    request.getBucketsReqAt(0).getOriginalPartitionName());
        }
        ApiError error = ApiError.fromThrowable(exception);
        bucketResp.setErrorCode(error.error().code());
        bucketResp.setErrorMessage(error.formatErrMsg());
        return CompletableFuture.completedFuture(response);
    }

    private CompletableFuture<PrefixLookupResponse> createSuccessPrefixLookupResponse(
            PrefixLookupRequest request) {
        PrefixLookupResponse response = new PrefixLookupResponse();
        // Create response for each prefix key in request
        PbPrefixLookupRespForBucket bucketResp = response.addBucketsResp();
        bucketResp.setBucketId(TABLE_BUCKET.getBucket());
        if (TABLE_BUCKET.getPartitionId() != null) {
            bucketResp.setPartitionId(TABLE_BUCKET.getPartitionId());
        }
        // Add empty value list for each prefix key
        int keyCount = request.getBucketsReqAt(0).getKeysCount();
        for (int i = 0; i < keyCount; i++) {
            bucketResp.addValueList(); // empty list is valid for prefix lookup
        }
        return CompletableFuture.completedFuture(response);
    }

    private CompletableFuture<PrefixLookupResponse> createFailedPrefixLookupResponse(
            PrefixLookupRequest request, Exception exception) {
        PrefixLookupResponse response = new PrefixLookupResponse();
        PbPrefixLookupRespForBucket bucketResp = response.addBucketsResp();
        bucketResp.setBucketId(TABLE_BUCKET.getBucket());
        if (TABLE_BUCKET.getPartitionId() != null) {
            bucketResp.setPartitionId(TABLE_BUCKET.getPartitionId());
        }
        ApiError error = ApiError.fromThrowable(exception);
        bucketResp.setErrorCode(error.error().code());
        bucketResp.setErrorMessage(error.formatErrMsg());
        return CompletableFuture.completedFuture(response);
    }

    /**
     * A configurable {@link TabletServerGateway} for testing that allows setting custom handlers
     * for lookup operations.
     */
    private static class ConfigurableTestTabletServerGateway extends TestTabletServerGateway {

        private java.util.function.Function<LookupRequest, CompletableFuture<LookupResponse>>
                lookupHandler;
        private java.util.function.Function<
                        PrefixLookupRequest, CompletableFuture<PrefixLookupResponse>>
                prefixLookupHandler;

        public ConfigurableTestTabletServerGateway() {
            super(false, Collections.emptySet());
        }

        public void setLookupHandler(
                java.util.function.Function<LookupRequest, CompletableFuture<LookupResponse>>
                        handler) {
            this.lookupHandler = handler;
        }

        public void setPrefixLookupHandler(
                java.util.function.Function<
                                PrefixLookupRequest, CompletableFuture<PrefixLookupResponse>>
                        handler) {
            this.prefixLookupHandler = handler;
        }

        @Override
        public CompletableFuture<LookupResponse> lookup(LookupRequest request) {
            if (lookupHandler != null) {
                return lookupHandler.apply(request);
            }
            return CompletableFuture.completedFuture(new LookupResponse());
        }

        @Override
        public CompletableFuture<PrefixLookupResponse> prefixLookup(PrefixLookupRequest request) {
            if (prefixLookupHandler != null) {
                return prefixLookupHandler.apply(request);
            }
            return CompletableFuture.completedFuture(new PrefixLookupResponse());
        }
    }

    /**
     * A {@link TestingMetadataUpdater} in which buckets can lose their leader. While a bucket has
     * none, the cached metadata has no leader for it and the given number of metadata refreshes in
     * {@link #leaderFor} fail for it, as when the bucket has no leader or the refresh fails.
     */
    private static class TestingLeaderlessMetadataUpdater extends TestingMetadataUpdater {

        private final Cluster clusterWithLeaders;
        /** Buckets without a leader -> metadata refreshes left to fail for them. */
        private final Map<TableBucket, Integer> remainingFailures = new HashMap<>();

        /** Buckets without a leader -> the error their metadata refreshes fail with, if given. */
        private final Map<TableBucket, RuntimeException> injectedErrors = new HashMap<>();

        private final AtomicInteger failedLeaderLookups = new AtomicInteger();

        TestingLeaderlessMetadataUpdater(
                Map<TablePath, TableInfo> tableInfos, TestTabletServerGateway gateway) {
            super(
                    COORDINATOR,
                    Arrays.asList(NODE1, NODE2, NODE3),
                    tableInfos,
                    gatewaysForLeaders(gateway),
                    new Configuration());
            this.clusterWithLeaders = cluster;
        }

        /** Serves the leaders of all buckets of the test table. */
        private static Map<Integer, TestTabletServerGateway> gatewaysForLeaders(
                TestTabletServerGateway gateway) {
            Map<Integer, TestTabletServerGateway> gateways = new HashMap<>();
            gateways.put(NODE1.id(), gateway);
            gateways.put(NODE2.id(), gateway);
            gateways.put(NODE3.id(), gateway);
            return gateways;
        }

        /**
         * Removes the cached leader of the bucket and fails the next {@code failures} metadata
         * refreshes for it with {@code failure}, or a generic error if null.
         */
        synchronized void failLeaderLookups(
                TableBucket tableBucket, int failures, @Nullable RuntimeException failure) {
            if (failure == null) {
                injectedErrors.remove(tableBucket);
            } else {
                injectedErrors.put(tableBucket, failure);
            }
            remainingFailures.put(tableBucket, failures);
            updateCluster(withoutLeaders(clusterWithLeaders, remainingFailures.keySet()));
        }

        /** Caches the leader of the bucket, as a metadata refresh elsewhere would. */
        synchronized void cacheLeader(TableBucket tableBucket) {
            remainingFailures.remove(tableBucket);
            injectedErrors.remove(tableBucket);
            updateCluster(withoutLeaders(clusterWithLeaders, remainingFailures.keySet()));
        }

        int failedLeaderLookups() {
            return failedLeaderLookups.get();
        }

        @Override
        public synchronized int leaderFor(TablePath tablePath, TableBucket tableBucket) {
            Integer remaining = remainingFailures.get(tableBucket);
            if (remaining != null && cluster.leaderFor(tableBucket) == null) {
                if (remaining > 0) {
                    remainingFailures.put(tableBucket, remaining - 1);
                    failedLeaderLookups.incrementAndGet();
                    RuntimeException failure = injectedErrors.get(tableBucket);
                    throw failure != null
                            ? failure
                            : new FlussRuntimeException(
                                    "Leader not found for table bucket: " + tableBucket);
                }
                // The metadata refresh finds the leader again.
                cacheLeader(tableBucket);
            }
            return super.leaderFor(tablePath, tableBucket);
        }

        private static Cluster withoutLeaders(Cluster cluster, Set<TableBucket> tableBuckets) {
            Map<PhysicalTablePath, List<BucketLocation>> bucketLocations = new HashMap<>();
            cluster.getBucketLocationsByPath()
                    .forEach(
                            (path, locations) ->
                                    bucketLocations.put(
                                            path,
                                            locations.stream()
                                                    .filter(
                                                            location ->
                                                                    !tableBuckets.contains(
                                                                            location
                                                                                    .getTableBucket()))
                                                    .collect(Collectors.toList())));
            return new Cluster(
                    new HashMap<>(cluster.getAliveTabletServers()),
                    cluster.getCoordinatorServer(),
                    bucketLocations,
                    cluster.getTableIdByPath(),
                    cluster.getPartitionIdByPath(),
                    cluster.getBucketCountByTableOrPartition(),
                    Collections.emptyMap());
        }
    }
}
