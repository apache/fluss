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
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.InvalidTableException;
import org.apache.fluss.exception.NotLeaderOrFollowerException;
import org.apache.fluss.exception.TableNotExistException;
import org.apache.fluss.exception.TimeoutException;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.rpc.gateway.TabletServerGateway;
import org.apache.fluss.rpc.messages.LookupRequest;
import org.apache.fluss.rpc.messages.LookupResponse;
import org.apache.fluss.rpc.messages.PbLookupRespForBucket;
import org.apache.fluss.rpc.messages.PbPrefixLookupRespForBucket;
import org.apache.fluss.rpc.messages.PrefixLookupRequest;
import org.apache.fluss.rpc.messages.PrefixLookupResponse;
import org.apache.fluss.rpc.protocol.ApiError;
import org.apache.fluss.server.tablet.TestTabletServerGateway;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.fluss.record.TestData.DATA1_TABLE_ID_PK;
import static org.apache.fluss.record.TestData.DATA1_TABLE_INFO_PK;
import static org.apache.fluss.record.TestData.DATA1_TABLE_PATH_PK;
import static org.apache.fluss.testutils.common.CommonTestUtils.waitUntil;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link LookupSender}. */
public class LookupSenderTest {

    private final TableBucket tb1 = new TableBucket(DATA1_TABLE_ID_PK, 0);

    private TestingMetadataUpdater metadataUpdater;
    private LookupSender lookupSender;

    private static final int MAX_RETRIES = 3;
    private static final int MAX_INFLIGHT_REQUESTS = 10;
    private static final TableBucket TABLE_BUCKET = new TableBucket(DATA1_TABLE_ID_PK, 0);

    private LookupQueue lookupQueue;
    private Thread senderThread;
    private ConfigurableTestTabletServerGateway gateway;

    @BeforeEach
    void setup() {
        // create a configurable gateway for testing
        gateway = new ConfigurableTestTabletServerGateway();

        // build metadata updater with custom gateway using builder pattern
        Map<TablePath, TableInfo> tableInfos = new HashMap<>();
        tableInfos.put(DATA1_TABLE_PATH_PK, DATA1_TABLE_INFO_PK);
        metadataUpdater =
                TestingMetadataUpdater.builder(tableInfos)
                        .withTabletServerGateway(1, gateway)
                        .build();

        Configuration conf = new Configuration();
        conf.set(ConfigOptions.CLIENT_LOOKUP_QUEUE_SIZE, 5);
        conf.set(ConfigOptions.CLIENT_LOOKUP_MAX_BATCH_SIZE, 10);
        lookupQueue = new LookupQueue(conf);

        lookupSender =
                new LookupSender(metadataUpdater, lookupQueue, MAX_INFLIGHT_REQUESTS, MAX_RETRIES);

        senderThread = new Thread(lookupSender);
        senderThread.start();
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

    // Helper methods

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
}
