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

package org.apache.fluss.server.coordinator;

import org.apache.fluss.cluster.Endpoint;
import org.apache.fluss.cluster.ServerType;
import org.apache.fluss.metadata.IndexType;
import org.apache.fluss.metadata.IndexVisibility;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.rpc.gateway.CoordinatorGateway;
import org.apache.fluss.server.coordinator.event.DeadTabletServerEvent;
import org.apache.fluss.server.coordinator.event.NewTabletServerEvent;
import org.apache.fluss.server.metadata.ServerInfo;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.Collections;
import java.util.concurrent.TimeUnit;

import static org.apache.fluss.server.testutils.RpcMessageTestUtils.newCreateDatabaseRequest;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.newCreateTableRequest;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.newDropTableRequest;
import static org.apache.fluss.testutils.common.CommonTestUtils.retry;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests index creation admission through the coordinator RPC entry point. */
class CoordinatorIndexCompatibilityITCase {
    @RegisterExtension
    public static final FlussClusterExtension CLUSTER =
            FlussClusterExtension.builder().setNumOfTabletServers(1).build();

    @Test
    void testUnknownServerBlocksOnlyNewIndexedTables() throws Exception {
        CoordinatorGateway gateway = CLUSTER.newCoordinatorClient();
        CoordinatorEventProcessor processor =
                CLUSTER.getCoordinatorServer().getCoordinatorEventProcessor();
        gateway.createDatabase(newCreateDatabaseRequest("fluss", true)).get();
        TableDescriptor indexed = descriptor(true);
        TablePath existing = TablePath.of("fluss", "existing_indexed");
        retry(
                java.time.Duration.ofSeconds(10),
                () ->
                        assertThatCode(
                                        () ->
                                                gateway.createTable(
                                                                newCreateTableRequest(
                                                                        existing, indexed, false))
                                                        .get())
                                .doesNotThrowAnyException());

        ServerInfo unknown =
                new ServerInfo(
                        99,
                        null,
                        Collections.singletonList(new Endpoint("localhost", 1, "FLUSS")),
                        ServerType.TABLET_SERVER);
        processor.getCoordinatorEventManager().put(new NewTabletServerEvent(unknown));
        try {
            retry(
                    java.time.Duration.ofSeconds(10),
                    () ->
                            assertThat(
                                            CLUSTER.getCoordinatorServer()
                                                    .getMetadataCache()
                                                    .getLiveServers())
                                    .hasSize(2));
            TablePath rejected = TablePath.of("fluss", "rejected_indexed");
            assertThatThrownBy(
                            () ->
                                    gateway.createTable(
                                                    newCreateTableRequest(rejected, indexed, false))
                                            .get(10, TimeUnit.SECONDS))
                    .hasRootCauseMessage(
                            "Cannot create a table with secondary indexes: "
                                    + "TabletServers [99] have not confirmed support for PUT_INDEX v0.");
            assertThat(CLUSTER.getZooKeeperClient().getTable(rejected)).isEmpty();
            gateway.createTable(newCreateTableRequest(existing, indexed, true)).get();
            gateway.createTable(
                            newCreateTableRequest(
                                    TablePath.of("fluss", "ordinary_during_upgrade"),
                                    descriptor(false),
                                    false))
                    .get();
        } finally {
            processor.getCoordinatorEventManager().put(new DeadTabletServerEvent(99));
            gateway.dropTable(
                            newDropTableRequest(
                                    existing.getDatabaseName(), existing.getTableName(), true))
                    .get();
            gateway.dropTable(newDropTableRequest("fluss", "rejected_indexed", true)).get();
        }
    }

    private static TableDescriptor descriptor(boolean indexed) {
        Schema.Builder schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("val", DataTypes.STRING())
                        .primaryKey("id");
        if (indexed) {
            schema.index(
                    "idx_val",
                    IndexType.SECONDARY,
                    Collections.singletonList("val"),
                    IndexVisibility.SYNC,
                    1);
        }
        return TableDescriptor.builder()
                .schema(schema.build())
                .distributedBy(1)
                .property(org.apache.fluss.config.ConfigOptions.TABLE_REPLICATION_FACTOR, 1)
                .build();
    }
}
