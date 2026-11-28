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

package org.apache.fluss.client;

import org.apache.fluss.client.table.Table;
import org.apache.fluss.cluster.BucketLocation;
import org.apache.fluss.cluster.Endpoint;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.AuthorizationException;
import org.apache.fluss.exception.NetworkException;
import org.apache.fluss.exception.TableNotExistException;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.metrics.groups.MetricGroup;
import org.apache.fluss.metrics.util.NOPMetricsGroup;
import org.apache.fluss.rpc.RpcServer;
import org.apache.fluss.rpc.TestingTabletGatewayService;
import org.apache.fluss.rpc.messages.GetTableInfoRequest;
import org.apache.fluss.rpc.messages.GetTableInfoResponse;
import org.apache.fluss.rpc.messages.MetadataRequest;
import org.apache.fluss.rpc.messages.MetadataResponse;
import org.apache.fluss.rpc.messages.PbBucketMetadata;
import org.apache.fluss.rpc.messages.PbTableMetadata;
import org.apache.fluss.rpc.netty.server.RequestsMetrics;
import org.apache.fluss.testutils.common.CheckedThread;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests metadata readiness through the public connection API and a local RPC service. */
class FlussConnectionMetadataTest {
    private static final TablePath TABLE_PATH = TablePath.of("db", "docs");
    private static final long TABLE_ID = 1622L;
    private static final long REPLACED_TABLE_ID = 1900L;
    private static final TableDescriptor DESCRIPTOR =
            TableDescriptor.builder()
                    .schema(Schema.newBuilder().column("id", DataTypes.INT()).build())
                    .distributedBy(1)
                    .build();

    @Test
    void testGetTablePopulatesMappingFromTableInfo() throws Exception {
        TestingMetadataService service = new TestingMetadataService();
        withConnection(service, connection -> assertTableMapping(connection, service, 1));
    }

    @Test
    void testGetTableRetriesTransientAdminFailure() throws Exception {
        TestingMetadataService service = new TestingMetadataService();
        service.failures = 1;
        withConnection(service, connection -> assertTableMapping(connection, service, 2));
    }

    @Test
    void testGetTablePropagatesExhaustedAdminFailure() throws Exception {
        TestingMetadataService service = new TestingMetadataService();
        service.failures = 2;
        withConnection(service, connection -> assertFailedInitialization(connection, service, 2));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testGetTableDoesNotRetryPermanentAdminFailure(boolean unauthorized) throws Exception {
        TestingMetadataService service = new TestingMetadataService();
        service.failures = 2;
        service.failure =
                unauthorized
                        ? new AuthorizationException("table access denied")
                        : new TableNotExistException("table does not exist");
        withConnection(service, connection -> assertFailedInitialization(connection, service, 1));
    }

    @Test
    void testGetTableIgnoresAdminResponseOlderThanRefreshedMetadata() throws Exception {
        TestingMetadataService service = new TestingMetadataService();
        service.tableMetadataInResponse = true;
        withConnection(
                service,
                connection -> {
                    service.parkNextTableInfoRequest();
                    CheckedThread staleGetTable =
                            new CheckedThread("stale-get-table") {
                                @Override
                                public void go() throws Exception {
                                    connection.getTable(TABLE_PATH).close();
                                }
                            };
                    staleGetTable.start();
                    assertThat(service.awaitParkedTableInfoRequest()).isTrue();

                    // the table is replaced while the first Admin response is held back, so the
                    // second getTable resolves the routing of the new table
                    service.tableId = REPLACED_TABLE_ID;
                    try (Table table = connection.getTable(TABLE_PATH)) {
                        assertThat(table.getTableInfo().getTableId()).isEqualTo(REPLACED_TABLE_ID);
                    }
                    assertThat(cachedBucket(connection, REPLACED_TABLE_ID)).isPresent();

                    service.releaseParkedTableInfoRequest();
                    staleGetTable.sync(10_000);

                    assertThat(connection.getMetadataUpdater().getCluster().getTableId(TABLE_PATH))
                            .hasValue(REPLACED_TABLE_ID);
                    assertThat(connection.getMetadataUpdater().getCluster().getTablePath(TABLE_ID))
                            .isEmpty();
                    assertThat(cachedBucket(connection, REPLACED_TABLE_ID)).isPresent();
                });
    }

    private static Optional<BucketLocation> cachedBucket(FlussConnection connection, long tableId) {
        return connection
                .getMetadataUpdater()
                .getCluster()
                .getBucketLocation(new TableBucket(tableId, null, 0));
    }

    private void assertTableMapping(
            FlussConnection connection, TestingMetadataService service, int expectedRequests)
            throws Exception {
        try (Table table = connection.getTable(TABLE_PATH)) {
            assertThat(table.getTableInfo().getTableId()).isEqualTo(TABLE_ID);
            assertThat(connection.getMetadataUpdater().getCluster().getTableId(TABLE_PATH))
                    .hasValue(TABLE_ID);
            assertThat(connection.getMetadataUpdater().getCluster().getTablePath(TABLE_ID))
                    .hasValue(TABLE_PATH);
            assertThat(service.tableInfoRequests.get()).isEqualTo(expectedRequests);
            // getTable refreshes the table routing after the access check.
            assertThat(service.metadataTableRequests.get()).isEqualTo(1);
        }
    }

    private void assertFailedInitialization(
            FlussConnection connection, TestingMetadataService service, int expectedRequests) {
        assertThatThrownBy(() -> connection.getTable(TABLE_PATH))
                .hasRootCauseInstanceOf(service.failure.getClass())
                .hasRootCauseMessage(service.failure.getMessage());
        assertThat(service.tableInfoRequests.get()).isEqualTo(expectedRequests);
        assertThat(service.metadataTableRequests.get()).isZero();
        assertThat(connection.getMetadataUpdater().getCluster().getTableId(TABLE_PATH)).isEmpty();
        assertThat(connection.getMetadataUpdater().getCluster().getTablePath(TABLE_ID)).isEmpty();
    }

    private void withConnection(TestingMetadataService service, ConnectionCheck check)
            throws Exception {
        Configuration conf = new Configuration();
        conf.set(ConfigOptions.NETTY_SERVER_NUM_WORKER_THREADS, 1);
        conf.set(ConfigOptions.NETTY_SERVER_NUM_NETWORK_THREADS, 1);
        conf.set(ConfigOptions.NETTY_CLIENT_NUM_NETWORK_THREADS, 1);
        MetricGroup metrics = NOPMetricsGroup.newInstance();
        try (RpcServer server =
                RpcServer.create(
                        conf,
                        Collections.singletonList(new Endpoint("localhost", 0, "INTERNAL")),
                        service,
                        metrics,
                        RequestsMetrics.createTabletServerRequestMetrics(metrics))) {
            server.start();
            service.port = server.getBindEndpoints().get(0).getPort();
            conf.set(
                    ConfigOptions.BOOTSTRAP_SERVERS,
                    Collections.singletonList("localhost:" + service.port));
            try (FlussConnection connection =
                    (FlussConnection) ConnectionFactory.createConnection(conf)) {
                check.accept(connection);
            }
        }
    }

    private interface ConnectionCheck {
        void accept(FlussConnection connection) throws Exception;
    }

    private static class TestingMetadataService extends TestingTabletGatewayService {
        private final AtomicInteger tableInfoRequests = new AtomicInteger();
        private final AtomicInteger metadataTableRequests = new AtomicInteger();
        private final CountDownLatch parkedTableInfoRequest = new CountDownLatch(1);
        private final CompletableFuture<GetTableInfoResponse> parkedTableInfoResponse =
                new CompletableFuture<>();
        private volatile int port;
        private volatile int failures;
        private volatile long tableId = TABLE_ID;
        private volatile RuntimeException failure = new NetworkException("table info unavailable");
        private volatile boolean tableMetadataInResponse;
        private volatile boolean parkNextTableInfo;
        private volatile long parkedTableId;

        @Override
        public CompletableFuture<MetadataResponse> metadata(MetadataRequest request) {
            if (!request.getTablePathsList().isEmpty()) {
                metadataTableRequests.incrementAndGet();
            }
            MetadataResponse response = new MetadataResponse();
            response.setCoordinatorServer().setNodeId(0).setHost("localhost").setPort(port);
            for (int id = 1; id <= 3; id++) {
                response.addTabletServer().setNodeId(id).setHost("localhost").setPort(port);
            }
            if (tableMetadataInResponse && !request.getTablePathsList().isEmpty()) {
                addTableMetadata(response);
            }
            return CompletableFuture.completedFuture(response);
        }

        @Override
        public CompletableFuture<GetTableInfoResponse> getTableInfo(GetTableInfoRequest request) {
            if (tableInfoRequests.incrementAndGet() <= failures) {
                CompletableFuture<GetTableInfoResponse> result = new CompletableFuture<>();
                result.completeExceptionally(failure);
                return result;
            }
            if (parkNextTableInfo) {
                parkNextTableInfo = false;
                parkedTableId = tableId;
                parkedTableInfoRequest.countDown();
                return parkedTableInfoResponse;
            }
            return CompletableFuture.completedFuture(tableInfoResponse(tableId));
        }

        private void parkNextTableInfoRequest() {
            parkNextTableInfo = true;
        }

        private boolean awaitParkedTableInfoRequest() throws InterruptedException {
            return parkedTableInfoRequest.await(10, TimeUnit.SECONDS);
        }

        private void releaseParkedTableInfoRequest() {
            parkedTableInfoResponse.complete(tableInfoResponse(parkedTableId));
        }

        private GetTableInfoResponse tableInfoResponse(long tableId) {
            return new GetTableInfoResponse()
                    .setTableId(tableId)
                    .setSchemaId(1)
                    .setTableJson(DESCRIPTOR.toJsonBytes())
                    .setCreatedTime(0)
                    .setModifiedTime(0);
        }

        /**
         * Answers a metadata request the way a server does for an existing and accessible table.
         */
        private void addTableMetadata(MetadataResponse response) {
            PbTableMetadata tableMetadata = response.addTableMetadata();
            tableMetadata.setTableId(tableId);
            tableMetadata.setSchemaId(1);
            tableMetadata.setTableJson(DESCRIPTOR.toJsonBytes());
            tableMetadata.setCreatedTime(0);
            tableMetadata.setModifiedTime(0);
            tableMetadata.setBucketCount(1);
            tableMetadata.setBucketCountEpoch(0L);
            tableMetadata
                    .setTablePath()
                    .setDatabaseName(TABLE_PATH.getDatabaseName())
                    .setTableName(TABLE_PATH.getTableName());
            PbBucketMetadata bucketMetadata = tableMetadata.addBucketMetadata();
            bucketMetadata.setBucketId(0);
            bucketMetadata.setLeaderId(1);
            bucketMetadata.setReplicaIds(new int[] {1});
        }
    }
}
