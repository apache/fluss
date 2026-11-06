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

import org.apache.fluss.cluster.Endpoint;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.NetworkException;
import org.apache.fluss.exception.StaleMetadataException;
import org.apache.fluss.metadata.Schema;
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
import org.apache.fluss.rpc.netty.server.RequestsMetrics;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests metadata readiness through the public connection API and a local RPC service. */
class FlussConnectionMetadataTest {
    private static final TablePath TABLE_PATH = TablePath.of("db", "docs");
    private static final long TABLE_ID = 1622L;
    private static final TableDescriptor DESCRIPTOR =
            TableDescriptor.builder()
                    .schema(Schema.newBuilder().column("id", DataTypes.INT()).build())
                    .distributedBy(1)
                    .build();

    @Test
    void testGetTableWaitsForMetadataRecovery() throws Exception {
        TestingMetadataService service = new TestingMetadataService();
        service.failures = 2;
        withConnection(
                service,
                connection -> {
                    assertThat(connection.getTable(TABLE_PATH).getTableInfo().getTableId())
                            .isEqualTo(TABLE_ID);
                    assertThat(service.tableRequests.get()).isEqualTo(3);
                    assertThat(connection.getMetadataUpdater().getCluster().getTablePath(TABLE_ID))
                            .hasValue(TABLE_PATH);
                });
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testAdminSuccessCannotMaskIncompleteInitialization(boolean failRequests) throws Exception {
        TestingMetadataService service = new TestingMetadataService();
        service.failures = failRequests ? Integer.MAX_VALUE : 0;
        service.omitMapping = !failRequests;
        withConnection(
                service,
                connection -> {
                    assertThat(connection.getAdmin().getTableInfo(TABLE_PATH).join().getTableId())
                            .isEqualTo(TABLE_ID);
                    assertThatThrownBy(() -> connection.getTable(TABLE_PATH))
                            .hasRootCauseInstanceOf(
                                    failRequests
                                            ? NetworkException.class
                                            : StaleMetadataException.class);
                    assertThat(service.tableRequests.get()).isEqualTo(3);
                    assertThat(connection.getMetadataUpdater().getCluster().getTableId(TABLE_PATH))
                            .isEmpty();
                    // Independent Admin queries remain successful even though getTable must fail.
                    assertThat(connection.getAdmin().getTableInfo(TABLE_PATH).join().getTableId())
                            .isEqualTo(TABLE_ID);
                });
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
        private final AtomicInteger tableRequests = new AtomicInteger();
        private volatile int port;
        private int failures;
        private boolean omitMapping;

        @Override
        public CompletableFuture<MetadataResponse> metadata(MetadataRequest request) {
            boolean tableRequested = !request.getTablePathsList().isEmpty();
            if (tableRequested && tableRequests.incrementAndGet() <= failures) {
                CompletableFuture<MetadataResponse> result = new CompletableFuture<>();
                result.completeExceptionally(new NetworkException("table metadata unavailable"));
                return result;
            }
            MetadataResponse response = new MetadataResponse();
            response.setCoordinatorServer().setNodeId(0).setHost("localhost").setPort(port);
            for (int id = 1; id <= 3; id++) {
                response.addTabletServer().setNodeId(id).setHost("localhost").setPort(port);
            }
            if (tableRequested && !omitMapping) {
                response.addTableMetadata()
                        .setTableId(TABLE_ID)
                        .setSchemaId(1)
                        .setTableJson(DESCRIPTOR.toJsonBytes())
                        .setCreatedTime(0)
                        .setModifiedTime(0)
                        .setTablePath()
                        .setDatabaseName(TABLE_PATH.getDatabaseName())
                        .setTableName(TABLE_PATH.getTableName());
            }
            return CompletableFuture.completedFuture(response);
        }

        @Override
        public CompletableFuture<GetTableInfoResponse> getTableInfo(GetTableInfoRequest request) {
            return CompletableFuture.completedFuture(
                    new GetTableInfoResponse()
                            .setTableId(TABLE_ID)
                            .setSchemaId(1)
                            .setTableJson(DESCRIPTOR.toJsonBytes())
                            .setCreatedTime(0)
                            .setModifiedTime(0));
        }
    }
}
