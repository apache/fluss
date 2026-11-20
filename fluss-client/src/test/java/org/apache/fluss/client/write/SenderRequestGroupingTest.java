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

package org.apache.fluss.client.write;

import org.apache.fluss.client.metadata.TestingMetadataUpdater;
import org.apache.fluss.client.metrics.TestingWriterMetricGroup;
import org.apache.fluss.cluster.BucketLocation;
import org.apache.fluss.cluster.Cluster;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.MemorySize;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.row.BinaryRow;
import org.apache.fluss.row.encode.CompactedKeyEncoder;
import org.apache.fluss.rpc.entity.PutKvResultForBucket;
import org.apache.fluss.rpc.messages.PutKvRequest;
import org.apache.fluss.rpc.protocol.MergeMode;
import org.apache.fluss.server.tablet.TestTabletServerGateway;
import org.apache.fluss.utils.ExponentialBackoff;
import org.apache.fluss.utils.clock.SystemClock;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

import static org.apache.fluss.record.TestData.DATA2_ROW_TYPE;
import static org.apache.fluss.record.TestData.DATA2_SCHEMA;
import static org.apache.fluss.record.TestData.DATA2_TABLE_ID;
import static org.apache.fluss.record.TestData.DATA2_TABLE_INFO;
import static org.apache.fluss.record.TestData.DATA2_TABLE_PATH;
import static org.apache.fluss.server.utils.ServerRpcMessageUtils.makePutKvResponse;
import static org.apache.fluss.testutils.DataTestUtils.compactedRow;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for grouping KV batches into compatible write requests. */
final class SenderRequestGroupingTest {
    private TestingMetadataUpdater metadataUpdater;
    private RecordAccumulator accumulator;
    private TestingBucketAssigner bucketAssigner;
    private Sender sender;

    @BeforeEach
    void setup() {
        metadataUpdater =
                new TestingMetadataUpdater(
                        Collections.singletonMap(DATA2_TABLE_PATH, DATA2_TABLE_INFO));
        TestingWriterMetricGroup metrics = TestingWriterMetricGroup.newInstance();
        IdempotenceManager idempotenceManager =
                new IdempotenceManager(
                        false, 5, metadataUpdater.newRandomTabletServerClient(), metadataUpdater);
        Configuration conf = new Configuration();
        conf.set(ConfigOptions.CLIENT_WRITER_BUFFER_MEMORY_SIZE, new MemorySize(1024 * 1024));
        conf.set(ConfigOptions.CLIENT_WRITER_BATCH_SIZE, new MemorySize(16 * 1024));
        conf.set(ConfigOptions.CLIENT_WRITER_BUFFER_PAGE_SIZE, new MemorySize(256));
        conf.set(ConfigOptions.CLIENT_WRITER_BATCH_TIMEOUT, Duration.ZERO);
        bucketAssigner = new TestingBucketAssigner();
        accumulator =
                new RecordAccumulator(
                        conf,
                        idempotenceManager,
                        metrics,
                        SystemClock.getInstance(),
                        new ExponentialBackoff(1000, 2, 10000, 0),
                        (tableInfo, path) -> bucketAssigner);
        sender =
                new Sender(
                        accumulator,
                        5000,
                        1024 * 1024,
                        (short) -1,
                        Integer.MAX_VALUE,
                        metadataUpdater,
                        idempotenceManager,
                        metrics);
    }

    @AfterEach
    void teardown() throws Exception {
        sender.destroyResources();
    }

    @Test
    void testPutRequestsSeparateMergeModesAcrossBuckets() throws Exception {
        assertPutRequestGrouping(
                MergeMode.OVERWRITE, false, null, MergeMode.DEFAULT, false, null, 2);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testPutRequestsSeparateNullPoliciesAcrossBuckets(boolean ignoreNulls) throws Exception {
        assertPutRequestGrouping(
                MergeMode.DEFAULT, ignoreNulls, null, MergeMode.DEFAULT, !ignoreNulls, null, 2);
    }

    @ParameterizedTest
    @MethodSource("targetColumnGroups")
    void testPutRequestsGroupByTargetColumns(
            boolean ignoreNulls, int[] firstTargets, int[] secondTargets, int expectedRequests)
            throws Exception {
        assertPutRequestGrouping(
                MergeMode.DEFAULT,
                ignoreNulls,
                firstTargets,
                MergeMode.DEFAULT,
                ignoreNulls,
                secondTargets,
                expectedRequests);
    }

    private static Stream<Arguments> targetColumnGroups() {
        return Stream.of(false, true)
                .flatMap(
                        ignoreNulls ->
                                Stream.of(
                                        Arguments.of(ignoreNulls, null, new int[] {0, 1}, 2),
                                        Arguments.of(ignoreNulls, new int[] {0, 1}, null, 2),
                                        Arguments.of(
                                                ignoreNulls, new int[] {0, 1}, new int[] {0, 2}, 2),
                                        Arguments.of(
                                                ignoreNulls,
                                                new int[] {0, 1},
                                                new int[] {0, 1},
                                                1)));
    }

    private void assertPutRequestGrouping(
            MergeMode firstMode,
            boolean firstIgnoreNulls,
            int[] firstTargets,
            MergeMode secondMode,
            boolean secondIgnoreNulls,
            int[] secondTargets,
            int expectedRequests)
            throws Exception {
        PhysicalTablePath path = PhysicalTablePath.of(DATA2_TABLE_PATH);
        int[] replicas = new int[] {TestingMetadataUpdater.NODE1.id()};
        metadataUpdater.updateCluster(
                new Cluster(
                        Collections.singletonMap(
                                TestingMetadataUpdater.NODE1.id(), TestingMetadataUpdater.NODE1),
                        TestingMetadataUpdater.COORDINATOR,
                        Collections.singletonMap(
                                path,
                                Arrays.asList(
                                        new BucketLocation(
                                                path,
                                                DATA2_TABLE_ID,
                                                0,
                                                TestingMetadataUpdater.NODE1.id(),
                                                replicas),
                                        new BucketLocation(
                                                path,
                                                DATA2_TABLE_ID,
                                                1,
                                                TestingMetadataUpdater.NODE1.id(),
                                                replicas))),
                        Collections.singletonMap(DATA2_TABLE_PATH, DATA2_TABLE_ID),
                        Collections.emptyMap(),
                        Collections.emptyMap()));
        List<CompletableFuture<Exception>> results = new ArrayList<>();
        for (int bucket = 0; bucket < 2; bucket++) {
            BinaryRow row = compactedRow(DATA2_ROW_TYPE, new Object[] {bucket, "value", "kept"});
            byte[] key =
                    new CompactedKeyEncoder(DATA2_ROW_TYPE, DATA2_SCHEMA.getPrimaryKeyIndexes())
                            .encodeKey(row);
            bucketAssigner.setBucketId(bucket);
            CompletableFuture<Exception> result = new CompletableFuture<>();
            results.add(result);
            accumulator.append(
                    WriteRecord.forUpsert(
                            DATA2_TABLE_INFO,
                            path,
                            row,
                            key,
                            key,
                            WriteFormat.COMPACTED_KV,
                            bucket == 0 ? firstTargets : secondTargets,
                            bucket == 0 ? firstMode : secondMode,
                            bucket == 0 ? firstIgnoreNulls : secondIgnoreNulls),
                    (tb, offset, error) -> result.complete(error),
                    metadataUpdater.getCluster());
        }
        sender.runOnce();
        TestTabletServerGateway gateway = node1Gateway();
        assertThat(gateway.pendingRequestSize()).isEqualTo(expectedRequests);
        Set<Integer> buckets = new HashSet<>();
        for (int i = 0; i < expectedRequests; i++) {
            PutKvRequest request = (PutKvRequest) gateway.getRequest(i);
            assertThat(request.getTableId()).isEqualTo(DATA2_TABLE_ID);
            assertThat(request.getBucketsReqsCount()).isEqualTo(2 / expectedRequests);
            for (int j = 0; j < request.getBucketsReqsCount(); j++) {
                int bucket = request.getBucketsReqAt(j).getBucketId();
                assertThat(buckets.add(bucket)).isTrue();
                int[] targets = bucket == 0 ? firstTargets : secondTargets;
                assertThat(request.getTargetColumns())
                        .isEqualTo(targets == null ? new int[0] : targets);
                assertThat(request.getAggMode())
                        .isEqualTo((bucket == 0 ? firstMode : secondMode).getProtoValue());
                assertThat(request.hasIgnoreNulls() && request.isIgnoreNulls())
                        .isEqualTo(bucket == 0 ? firstIgnoreNulls : secondIgnoreNulls);
            }
        }
        assertThat(buckets).containsExactlyInAnyOrder(0, 1);
        while (gateway.pendingRequestSize() > 0) {
            PutKvRequest request = (PutKvRequest) gateway.getRequest(0);
            List<PutKvResultForBucket> responses = new ArrayList<>();
            for (int i = 0; i < request.getBucketsReqsCount(); i++) {
                responses.add(
                        new PutKvResultForBucket(
                                new TableBucket(
                                        DATA2_TABLE_ID, request.getBucketsReqAt(i).getBucketId()),
                                1L));
            }
            gateway.response(0, makePutKvResponse(responses));
        }
        for (CompletableFuture<Exception> result : results) {
            assertThat(result.get()).isNull();
        }
    }

    private TestTabletServerGateway node1Gateway() {
        return (TestTabletServerGateway)
                metadataUpdater.newTabletServerClientForNode(TestingMetadataUpdater.NODE1.id());
    }
}
