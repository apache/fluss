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
import org.apache.fluss.cluster.ServerNode;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.PartitionNotExistException;
import org.apache.fluss.metadata.DataLakeFormat;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.TestingSchemaGetter;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.ProjectedRow;
import org.apache.fluss.rpc.messages.LookupRequest;
import org.apache.fluss.rpc.messages.LookupResponse;
import org.apache.fluss.rpc.messages.PbLookupReqForBucket;
import org.apache.fluss.rpc.messages.PbLookupRespForBucket;
import org.apache.fluss.rpc.protocol.ApiError;
import org.apache.fluss.server.tablet.TestTabletServerGateway;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.apache.fluss.client.metadata.TestingMetadataUpdater.COORDINATOR;
import static org.apache.fluss.client.metadata.TestingMetadataUpdater.NODE1;
import static org.apache.fluss.utils.PartitionUtils.HISTORICAL_PARTITION_VALUE;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link PrimaryKeyLookuper}. */
class PrimaryKeyLookuperTest {

    private static final TablePath TABLE_PATH = TablePath.of("test_db", "partitioned_pk_table");
    private static final long TABLE_ID = 1L;
    private static final long ACTIVE_PARTITION_ID = 11L;
    private static final long OTHER_ACTIVE_PARTITION_ID = 12L;
    private static final long HISTORICAL_PARTITION_ID = 99L;
    private static final String PARTITION_A = "20000101";
    private static final String PARTITION_B = "20000102";
    private static final PhysicalTablePath PARTITION_A_PATH =
            PhysicalTablePath.of(TABLE_PATH, PARTITION_A);
    private static final PhysicalTablePath PARTITION_B_PATH =
            PhysicalTablePath.of(TABLE_PATH, PARTITION_B);
    private static final PhysicalTablePath HISTORICAL_PARTITION_PATH =
            PhysicalTablePath.of(TABLE_PATH, HISTORICAL_PARTITION_VALUE);

    @Test
    void testFallbackUsesOriginalPartitionWhenLookupRowIsReused() throws Exception {
        TableInfo tableInfo = createTableInfo();
        ControllableLookupGateway gateway = new ControllableLookupGateway();
        // Keep both A and B in metadata so the test can detect which partition the fallback
        // invalidates. The gateway holds each RPC response until the test explicitly completes it.
        TestingMetadataUpdater metadataUpdater =
                TestingMetadataUpdater.builder(Collections.singletonMap(TABLE_PATH, tableInfo))
                        .withTabletServerGateway(NODE1.id(), gateway)
                        .build();
        metadataUpdater.updateCluster(createCluster());

        LookupClient lookupClient = new LookupClient(new Configuration(), metadataUpdater);
        try {
            PrimaryKeyLookuper lookuper =
                    new PrimaryKeyLookuper(
                            tableInfo,
                            new TestingSchemaGetter(tableInfo.getSchemaId(), tableInfo.getSchema()),
                            metadataUpdater,
                            lookupClient,
                            false);
            ProjectedRow reusedLookupKey =
                    ProjectedRow.from(new int[] {0, 1})
                            .replaceRow(GenericRow.of(1, BinaryString.fromString(PARTITION_A)));

            // Start request A and wait until its normal-partition RPC reaches the gateway. This
            // guarantees that A's asynchronous callback is pending before the backing row changes.
            CompletableFuture<LookupResult> resultFuture = lookuper.lookup(reusedLookupKey);
            PendingLookup normalLookup = gateway.pollLookup();
            assertThat(normalLookup).isNotNull();
            assertThat(normalLookup.request.getBucketsReqsCount()).isEqualTo(1);
            PbLookupReqForBucket normalBucket = normalLookup.request.getBucketsReqAt(0);
            assertThat(normalBucket.getPartitionId()).isEqualTo(ACTIVE_PARTITION_ID);
            assertThat(normalBucket.hasOriginalPartitionName()).isFalse();

            // Simulate request B replacing the reusable backing row, then fail A's pending RPC.
            // The resulting callback must use the partition name captured from A, not read B.
            reusedLookupKey.replaceRow(GenericRow.of(2, BinaryString.fromString(PARTITION_B)));
            normalLookup.respondWithError(
                    new PartitionNotExistException("The cached normal partition was deleted."));

            // Verify that the fallback targets the historical system partition while preserving
            // A's original partition name for the lake lookup.
            PendingLookup historicalLookup = gateway.pollLookup();
            assertThat(historicalLookup).isNotNull();
            assertThat(historicalLookup.request.getBucketsReqsCount()).isEqualTo(1);
            PbLookupReqForBucket historicalBucket = historicalLookup.request.getBucketsReqAt(0);
            assertThat(historicalBucket.getPartitionId()).isEqualTo(HISTORICAL_PARTITION_ID);
            assertThat(historicalBucket.getOriginalPartitionName()).isEqualTo(PARTITION_A);
            historicalLookup.respondEmpty();

            assertThat(resultFuture.get(5, TimeUnit.SECONDS).getRowList()).isEmpty();
            // The stale route for A should be removed, while B's unrelated metadata must remain.
            assertThat(metadataUpdater.getPartitionId(PARTITION_A_PATH)).isEmpty();
            assertThat(metadataUpdater.getPartitionId(PARTITION_B_PATH))
                    .hasValue(OTHER_ACTIVE_PARTITION_ID);
        } finally {
            lookupClient.close(Duration.ofSeconds(5));
        }
    }

    private static TableInfo createTableInfo() {
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("dt", DataTypes.STRING())
                        .column("value", DataTypes.STRING())
                        .primaryKey("id", "dt")
                        .build();
        TableDescriptor tableDescriptor =
                TableDescriptor.builder()
                        .schema(schema)
                        .partitionedBy("dt")
                        .distributedBy(1, "id")
                        .property(ConfigOptions.TABLE_AUTO_PARTITION_ENABLED, true)
                        .property(ConfigOptions.TABLE_DATALAKE_ENABLED, true)
                        .property(ConfigOptions.TABLE_DATALAKE_FORMAT, DataLakeFormat.PAIMON)
                        .property(ConfigOptions.TABLE_DATALAKE_HISTORICAL_PARTITION_ENABLED, true)
                        .build();
        return TableInfo.of(TABLE_PATH, TABLE_ID, 1, tableDescriptor, null, 0L, 0L);
    }

    private static Cluster createCluster() {
        Map<PhysicalTablePath, List<BucketLocation>> bucketLocationsByPath = new HashMap<>();
        bucketLocationsByPath.put(
                PARTITION_A_PATH,
                Collections.singletonList(
                        createBucketLocation(PARTITION_A_PATH, ACTIVE_PARTITION_ID)));
        bucketLocationsByPath.put(
                PARTITION_B_PATH,
                Collections.singletonList(
                        createBucketLocation(PARTITION_B_PATH, OTHER_ACTIVE_PARTITION_ID)));
        bucketLocationsByPath.put(
                HISTORICAL_PARTITION_PATH,
                Collections.singletonList(
                        createBucketLocation(HISTORICAL_PARTITION_PATH, HISTORICAL_PARTITION_ID)));

        Map<PhysicalTablePath, Long> partitionIdsByPath = new HashMap<>();
        partitionIdsByPath.put(PARTITION_A_PATH, ACTIVE_PARTITION_ID);
        partitionIdsByPath.put(PARTITION_B_PATH, OTHER_ACTIVE_PARTITION_ID);
        partitionIdsByPath.put(HISTORICAL_PARTITION_PATH, HISTORICAL_PARTITION_ID);

        Map<Integer, ServerNode> tabletServers = Collections.singletonMap(NODE1.id(), NODE1);
        return new Cluster(
                tabletServers,
                COORDINATOR,
                bucketLocationsByPath,
                Collections.singletonMap(TABLE_PATH, TABLE_ID),
                partitionIdsByPath);
    }

    private static BucketLocation createBucketLocation(
            PhysicalTablePath physicalTablePath, long partitionId) {
        return new BucketLocation(
                physicalTablePath,
                new TableBucket(TABLE_ID, partitionId, 0),
                NODE1.id(),
                new int[] {NODE1.id()});
    }

    private static LookupResponse createErrorResponse(LookupRequest request, Exception exception) {
        LookupResponse response = new LookupResponse();
        PbLookupRespForBucket responseBucket = addResponseBucket(request, response);
        ApiError error = ApiError.fromThrowable(exception);
        responseBucket.setErrorCode(error.error().code());
        responseBucket.setErrorMessage(error.formatErrMsg());
        return response;
    }

    private static LookupResponse createEmptyResponse(LookupRequest request) {
        LookupResponse response = new LookupResponse();
        PbLookupRespForBucket responseBucket = addResponseBucket(request, response);
        int keyCount = request.getBucketsReqAt(0).getKeysCount();
        for (int i = 0; i < keyCount; i++) {
            responseBucket.addValue();
        }
        return response;
    }

    private static PbLookupRespForBucket addResponseBucket(
            LookupRequest request, LookupResponse response) {
        PbLookupReqForBucket requestBucket = request.getBucketsReqAt(0);
        PbLookupRespForBucket responseBucket = response.addBucketsResp();
        responseBucket.setBucketId(requestBucket.getBucketId());
        if (requestBucket.hasPartitionId()) {
            responseBucket.setPartitionId(requestBucket.getPartitionId());
        }
        if (requestBucket.hasOriginalPartitionName()) {
            responseBucket.setOriginalPartitionName(requestBucket.getOriginalPartitionName());
        }
        return responseBucket;
    }

    private static final class ControllableLookupGateway extends TestTabletServerGateway {
        private final BlockingQueue<PendingLookup> pendingLookups = new LinkedBlockingQueue<>();

        private ControllableLookupGateway() {
            super(false, Collections.emptySet());
        }

        @Override
        public CompletableFuture<LookupResponse> lookup(LookupRequest request) {
            PendingLookup pendingLookup = new PendingLookup(request);
            pendingLookups.add(pendingLookup);
            return pendingLookup.responseFuture;
        }

        private PendingLookup pollLookup() throws InterruptedException {
            return pendingLookups.poll(5, TimeUnit.SECONDS);
        }
    }

    private static final class PendingLookup {
        private final LookupRequest request;
        private final CompletableFuture<LookupResponse> responseFuture;

        private PendingLookup(LookupRequest request) {
            this.request = request;
            this.responseFuture = new CompletableFuture<>();
        }

        private void respondWithError(Exception exception) {
            responseFuture.complete(createErrorResponse(request, exception));
        }

        private void respondEmpty() {
            responseFuture.complete(createEmptyResponse(request));
        }
    }
}
