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

package org.apache.fluss.server.tablet.consumer.group;

import org.apache.fluss.bucketing.BucketingFunction;
import org.apache.fluss.cluster.ServerNode;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.TableConfig;
import org.apache.fluss.exception.ConsumerGroupOwnerLoadingException;
import org.apache.fluss.exception.ConsumerGroupOwnerNotAvailableException;
import org.apache.fluss.exception.InvalidTableException;
import org.apache.fluss.metadata.DataLakeFormat;
import org.apache.fluss.metadata.LogFormat;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.rpc.gateway.AdminGateway;
import org.apache.fluss.rpc.gateway.TabletServerGateway;
import org.apache.fluss.rpc.messages.FindConsumerGroupOwnerRequest;
import org.apache.fluss.rpc.messages.FindConsumerGroupOwnerResponse;
import org.apache.fluss.server.metadata.SystemTables;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.server.zk.data.TableRegistration;
import org.apache.fluss.utils.ExceptionUtils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.time.Duration;
import java.util.concurrent.ExecutionException;

import static org.apache.fluss.server.testutils.RpcMessageTestUtils.newCreateTableRequest;
import static org.apache.fluss.testutils.common.CommonTestUtils.retry;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Integration test for the consumer-offset system table. */
class ConsumerOffsetTableITCase {

    private static final String INTERNAL_LISTENER = "INTERNAL";
    private static final String CLIENT_LISTENER = "CLIENT";

    @RegisterExtension
    public static final FlussClusterExtension FLUSS_CLUSTER_EXTENSION =
            FlussClusterExtension.builder()
                    .setNumOfTabletServers(1)
                    .setCoordinatorServerListeners(
                            String.format(
                                    "%s://localhost:0, %s://localhost:0",
                                    INTERNAL_LISTENER, CLIENT_LISTENER))
                    .setTabletServerListeners(
                            String.format(
                                    "%s://localhost:0, %s://localhost:0",
                                    INTERNAL_LISTENER, CLIENT_LISTENER))
                    .setClusterConf(clusterConfiguration())
                    .build();

    @Test
    void testInternalCreationIgnoresKvRestrictionAndClusterDataLakeFormat() throws Exception {
        String groupId = "group-with-cluster-lake-format";
        ensureConsumerOffsetTable(groupId);

        TableRegistration tableRegistration =
                FLUSS_CLUSTER_EXTENSION
                        .getZooKeeperClient()
                        .getTable(SystemTables.CONSUMER_OFFSETS)
                        .get();
        TableConfig tableConfig = tableRegistration.getTableConfig();
        assertThat(tableConfig.getDataLakeFormat()).isEmpty();
        assertThat(tableRegistration.properties)
                .doesNotContainKey(ConfigOptions.TABLE_DATALAKE_FORMAT.key());

        int standardBucket =
                BucketingFunction.of(tableConfig.getDataLakeFormat().orElse(null))
                        .bucketing(
                                ConsumerOffsetSchema.encodeGroupPrefix(groupId),
                                tableRegistration.bucketCount);
        assertThat(
                        GroupManager.from(clusterConfiguration())
                                .bucketFor(groupId, tableRegistration.bucketCount))
                .isEqualTo(standardBucket);
    }

    @Test
    void testExternalRequestCannotCreateSystemTable() throws Exception {
        // RpcClient caches connections by server uid, so close a possible internal-listener
        // connection before connecting to the same coordinator through the client listener.
        FLUSS_CLUSTER_EXTENSION
                .getRpcClient()
                .disconnect(FLUSS_CLUSTER_EXTENSION.getCoordinatorServerNode(CLIENT_LISTENER).uid())
                .get();
        AdminGateway externalGateway =
                FLUSS_CLUSTER_EXTENSION.newCoordinatorClient(CLIENT_LISTENER);
        TableDescriptor tableDescriptor = newConsumerOffsetTableDescriptor();

        assertSystemTableCreationRejected(
                externalGateway, SystemTables.CONSUMER_OFFSETS, tableDescriptor, false);
        assertSystemTableCreationRejected(
                externalGateway,
                TablePath.of(SystemTables.SYSTEM_DATABASE, "unknown_table"),
                tableDescriptor,
                false);

        ensureConsumerOffsetTable("group-before-external-idempotent-create");
        assertSystemTableCreationRejected(
                externalGateway, SystemTables.CONSUMER_OFFSETS, tableDescriptor, true);
    }

    private static void ensureConsumerOffsetTable(String groupId) throws Exception {
        ServerNode tabletServer = FLUSS_CLUSTER_EXTENSION.getTabletServerNodes().iterator().next();
        TabletServerGateway gateway =
                FLUSS_CLUSTER_EXTENSION.newTabletServerClientForNode(tabletServer.id());

        retry(
                Duration.ofMinutes(1),
                () -> {
                    try {
                        FindConsumerGroupOwnerResponse response =
                                gateway.findConsumerGroupOwner(
                                                new FindConsumerGroupOwnerRequest()
                                                        .setGroupId(groupId))
                                        .get();
                        assertThat(response.getOwner().getNodeId()).isEqualTo(tabletServer.id());
                    } catch (ExecutionException e) {
                        Throwable cause = ExceptionUtils.stripExecutionException(e);
                        if (cause instanceof ConsumerGroupOwnerNotAvailableException
                                || cause instanceof ConsumerGroupOwnerLoadingException) {
                            throw new AssertionError(cause);
                        }
                        throw e;
                    }
                });
    }

    private static void assertSystemTableCreationRejected(
            AdminGateway gateway,
            TablePath tablePath,
            TableDescriptor tableDescriptor,
            boolean ignoreIfExists) {
        assertThatThrownBy(
                        () ->
                                gateway.createTable(
                                                newCreateTableRequest(
                                                        tablePath, tableDescriptor, ignoreIfExists))
                                        .get())
                .cause()
                .isInstanceOf(InvalidTableException.class)
                .hasMessageContaining("can only be created internally");
    }

    private static TableDescriptor newConsumerOffsetTableDescriptor() {
        return TableDescriptor.builder()
                .schema(ConsumerOffsetSchema.getTableSchema())
                .logFormat(LogFormat.COMPACTED)
                .distributedBy(1, ConsumerOffsetSchema.getBucketKeyColumn())
                .build()
                .withReplicationFactor(1);
    }

    private static Configuration clusterConfiguration() {
        Configuration configuration = new Configuration();
        configuration.set(ConfigOptions.DATALAKE_FORMAT, DataLakeFormat.PAIMON);
        configuration.set(ConfigOptions.DATALAKE_ENABLED, false);
        configuration.set(ConfigOptions.INTERNAL_LISTENER_NAME, INTERNAL_LISTENER);
        configuration.set(ConfigOptions.KV_TABLE_ALLOW_CREATION, false);
        configuration.set(ConfigOptions.OFFSETS_TABLE_REPLICATION_FACTOR, 1);
        return configuration;
    }
}
