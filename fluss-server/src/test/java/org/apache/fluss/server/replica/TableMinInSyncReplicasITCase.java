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

package org.apache.fluss.server.replica;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.InvalidAlterTableException;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.rpc.gateway.CoordinatorGateway;
import org.apache.fluss.rpc.gateway.TabletServerGateway;
import org.apache.fluss.rpc.messages.PbProduceLogRespForBucket;
import org.apache.fluss.rpc.messages.PbPutKvRespForBucket;
import org.apache.fluss.rpc.protocol.Errors;
import org.apache.fluss.server.testutils.FlussClusterExtension;

import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.apache.fluss.record.TestData.DATA1;
import static org.apache.fluss.record.TestData.DATA1_SCHEMA;
import static org.apache.fluss.record.TestData.DATA1_SCHEMA_PK;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.createTable;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.newAlterTableRequest;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.newGetTableInfoRequest;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.newProduceLogRequest;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.newPutKvRequest;
import static org.apache.fluss.testutils.DataTestUtils.genKvRecordBatch;
import static org.apache.fluss.testutils.DataTestUtils.genMemoryLogRecordsByObject;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests table-level minimum ISR configuration through coordinator and write RPCs. */
class TableMinInSyncReplicasITCase {

    @RegisterExtension
    public static final FlussClusterExtension FLUSS_CLUSTER_EXTENSION =
            FlussClusterExtension.builder()
                    .setNumOfTabletServers(1)
                    .setClusterConf(clusterConfig())
                    .build();

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testCreateAndRejectAlter(boolean primaryKey) throws Exception {
        String key = ConfigOptions.TABLE_LOG_MIN_IN_SYNC_REPLICAS_NUMBER.key();
        TablePath overriddenPath = TablePath.of("min_isr", "overridden_" + primaryKey);
        TableDescriptor overriddenDescriptor =
                TableDescriptor.builder()
                        .schema(primaryKey ? DATA1_SCHEMA_PK : DATA1_SCHEMA)
                        .distributedBy(1)
                        .property(ConfigOptions.TABLE_REPLICATION_FACTOR, 1)
                        .property(ConfigOptions.TABLE_LOG_MIN_IN_SYNC_REPLICAS_NUMBER, 1)
                        .build();
        long overriddenId =
                createTable(FLUSS_CLUSTER_EXTENSION, overriddenPath, overriddenDescriptor);
        Replica overriddenReplica =
                FLUSS_CLUSTER_EXTENSION.waitAndGetLeaderReplica(new TableBucket(overriddenId, 0));
        assertWriteResult(overriddenReplica, primaryKey, Errors.NONE);

        Map<String, String> properties = new HashMap<>(overriddenDescriptor.getProperties());
        properties.remove(key);
        TablePath inheritedPath = TablePath.of("min_isr", "inherited_" + primaryKey);
        long inheritedId =
                createTable(
                        FLUSS_CLUSTER_EXTENSION,
                        inheritedPath,
                        overriddenDescriptor.withProperties(properties));
        Replica inheritedReplica =
                FLUSS_CLUSTER_EXTENSION.waitAndGetLeaderReplica(new TableBucket(inheritedId, 0));
        assertWriteResult(inheritedReplica, primaryKey, Errors.NOT_ENOUGH_REPLICAS_EXCEPTION);

        assertAlterRejected(overriddenReplica, primaryKey, Errors.NONE);
        assertAlterRejected(inheritedReplica, primaryKey, Errors.NOT_ENOUGH_REPLICAS_EXCEPTION);
    }

    private static void assertAlterRejected(
            Replica replica, boolean primaryKey, Errors expectedWriteResult) throws Exception {
        String key = ConfigOptions.TABLE_LOG_MIN_IN_SYNC_REPLICAS_NUMBER.key();
        TablePath tablePath = replica.getTableInfo().getTablePath();
        CoordinatorGateway coordinator = FLUSS_CLUSTER_EXTENSION.newCoordinatorClient();
        TableDescriptor originalDescriptor =
                TableDescriptor.fromJsonBytes(
                        coordinator
                                .getTableInfo(newGetTableInfoRequest(tablePath))
                                .get()
                                .getTableJson());

        for (boolean reset : new boolean[] {false, true}) {
            assertThatThrownBy(
                            () ->
                                    coordinator
                                            .alterTable(
                                                    newAlterTableRequest(
                                                            tablePath,
                                                            reset
                                                                    ? Collections.emptyMap()
                                                                    : Collections.singletonMap(
                                                                            key, "1"),
                                                            reset
                                                                    ? Collections.singletonList(key)
                                                                    : Collections.emptyList(),
                                                            Collections.emptyList(),
                                                            false))
                                            .get())
                    .hasRootCauseInstanceOf(InvalidAlterTableException.class)
                    .hasStackTraceContaining(key);
            TableDescriptor persistedDescriptor =
                    TableDescriptor.fromJsonBytes(
                            coordinator
                                    .getTableInfo(newGetTableInfoRequest(tablePath))
                                    .get()
                                    .getTableJson());
            assertThat(persistedDescriptor.getProperties())
                    .isEqualTo(originalDescriptor.getProperties());
            assertWriteResult(replica, primaryKey, expectedWriteResult);
        }
    }

    private static void assertWriteResult(Replica replica, boolean primaryKey, Errors expected)
            throws Exception {
        TableBucket bucket = replica.getTableBucket();
        TabletServerGateway gateway =
                FLUSS_CLUSTER_EXTENSION.newTabletServerClientForNode(
                        FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(bucket));
        int errorCode;
        if (primaryKey) {
            PbPutKvRespForBucket response =
                    gateway.putKv(
                                    newPutKvRequest(
                                            bucket.getTableId(),
                                            bucket.getBucket(),
                                            -1,
                                            genKvRecordBatch(new Object[] {1, "a"})))
                            .get()
                            .getBucketsRespsList()
                            .get(0);
            errorCode = response.hasErrorCode() ? response.getErrorCode() : Errors.NONE.code();
        } else {
            PbProduceLogRespForBucket response =
                    gateway.produceLog(
                                    newProduceLogRequest(
                                            bucket.getTableId(),
                                            bucket.getBucket(),
                                            -1,
                                            genMemoryLogRecordsByObject(DATA1)))
                            .get()
                            .getBucketsRespsList()
                            .get(0);
            errorCode = response.hasErrorCode() ? response.getErrorCode() : Errors.NONE.code();
        }
        assertThat(errorCode).isEqualTo(expected.code());
    }

    private static Configuration clusterConfig() {
        Configuration conf = new Configuration();
        // The inherited policy rejects writes on RF=1 tables; a table override of 1 permits them.
        conf.set(ConfigOptions.LOG_REPLICA_MIN_IN_SYNC_REPLICAS_NUMBER, 2);
        return conf;
    }
}
