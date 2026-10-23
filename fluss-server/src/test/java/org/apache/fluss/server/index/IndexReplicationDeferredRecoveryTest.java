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

package org.apache.fluss.server.index;

import org.apache.fluss.cluster.Endpoint;
import org.apache.fluss.cluster.ServerType;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.metadata.IndexType;
import org.apache.fluss.metadata.IndexVisibility;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.rpc.entity.PutKvResultForBucket;
import org.apache.fluss.rpc.protocol.MergeMode;
import org.apache.fluss.server.metadata.ClusterMetadata;
import org.apache.fluss.server.metadata.ServerInfo;
import org.apache.fluss.server.metadata.TableMetadata;
import org.apache.fluss.server.replica.Replica;
import org.apache.fluss.server.replica.ReplicaTestBase;
import org.apache.fluss.server.zk.data.TableRegistration;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.utils.IndexTableUtils;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static org.apache.fluss.record.TestData.DATA_1_WITH_KEY_AND_VALUE;
import static org.apache.fluss.record.TestData.DEFAULT_REMOTE_DATA_DIR;
import static org.apache.fluss.server.coordinator.CoordinatorContext.INITIAL_COORDINATOR_EPOCH;
import static org.apache.fluss.testutils.DataTestUtils.genKvRecordBatch;
import static org.apache.fluss.testutils.common.CommonTestUtils.retry;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies that a main-table leader whose index replication was deferred for missing index-table
 * metadata resumes replication once that metadata reaches the server metadata cache.
 */
class IndexReplicationDeferredRecoveryTest extends ReplicaTestBase {

    private static final short PUT_KV_VERSION = 1;
    private static final int SCHEMA_ID = 1;
    private static final long MAIN_TABLE_ID = 700001L;
    private static final long INDEX_TABLE_ID = 700002L;
    private static final String INDEX_NAME = "idx_b";
    private static final TablePath MAIN_TABLE_PATH =
            TablePath.of("test_db_1", "deferred_index_main_pk");

    @Test
    void testDeferredIndexReplicationResumesAfterMetadataArrives() throws Exception {
        // 1. Register the main table (carrying one secondary index) in ZooKeeper only. The index
        // table metadata is deliberately absent from the TabletServer metadata cache, which is what
        // IndexSpecFactory reads, so becoming leader must defer index replication.
        Map<String, String> mainProperties = new HashMap<>();
        mainProperties.put(ConfigOptions.TABLE_REPLICATION_FACTOR.key(), "1");
        registerTableInZkClient(
                MAIN_TABLE_PATH,
                mainSchema(),
                MAIN_TABLE_ID,
                Collections.singletonList("a"),
                mainProperties);

        makeKvTableAsLeader(MAIN_TABLE_ID, MAIN_TABLE_PATH, 0);
        TableBucket sourceBucket = new TableBucket(MAIN_TABLE_ID, 0);
        IndexSendBuffer sendBuffer = replicaManager.getIndexSendBuffer();

        // 2. Pre-condition: nothing has been staged for this source bucket yet.
        assertThat(sendBuffer.pendingBytes(sourceBucket)).isZero();
        assertThat(sendBuffer.buckets()).isEmpty();

        // 3. Publish the index table metadata through the production metadata-update entry point.
        Replica mainReplica = replicaManager.getReplicaOrException(sourceBucket);
        TableInfo mainTableInfo = mainReplica.getTableInfo();
        Schema.Index index = mainTableInfo.getSchema().getIndexes().get(0);
        TablePath indexTablePath =
                TablePath.of(
                        MAIN_TABLE_PATH.getDatabaseName(),
                        IndexTableUtils.indexTableName(
                                MAIN_TABLE_PATH.getTableName(), index.getIndexName()));
        TableDescriptor indexDescriptor =
                IndexTableDescriptorFactory.derive(
                        mainTableInfo.toTableDescriptor(), mainTableInfo.getTableId(), index);
        zkClient.registerTable(
                indexTablePath,
                TableRegistration.newTable(
                        INDEX_TABLE_ID, DEFAULT_REMOTE_DATA_DIR, indexDescriptor));
        zkClient.registerFirstSchema(indexTablePath, indexDescriptor.getSchema());
        long now = System.currentTimeMillis();
        TableInfo indexTableInfo =
                TableInfo.of(
                        indexTablePath,
                        INDEX_TABLE_ID,
                        SCHEMA_ID,
                        indexDescriptor,
                        DEFAULT_REMOTE_DATA_DIR,
                        now,
                        now);
        replicaManager.maybeUpdateMetadataCache(
                INITIAL_COORDINATOR_EPOCH,
                new ClusterMetadata(
                        coordinatorServerInfo(),
                        aliveTabletServerInfos(),
                        Collections.singletonList(
                                new TableMetadata(indexTableInfo, Collections.emptyList())),
                        Collections.emptyList()));

        // 4. Write rows to the main-table leader so index batches can be derived from the WAL.
        CompletableFuture<List<PutKvResultForBucket>> putFuture = new CompletableFuture<>();
        replicaManager.putRecordsToKv(
                20000,
                1,
                Collections.singletonMap(sourceBucket, genKvRecordBatch(DATA_1_WITH_KEY_AND_VALUE)),
                null,
                MergeMode.DEFAULT,
                PUT_KV_VERSION,
                putFuture::complete);
        assertThat(putFuture.get()).hasSize(1);

        // 5. The deferred replicator must now be running and staging encoded index batches.
        retry(
                Duration.ofSeconds(30),
                () -> assertThat(sendBuffer.pendingBytes(sourceBucket)).isGreaterThan(0L));
    }

    private static Schema mainSchema() {
        return Schema.newBuilder()
                .column("a", DataTypes.INT())
                .column("b", DataTypes.STRING())
                .primaryKey("a")
                .index(
                        INDEX_NAME,
                        IndexType.SECONDARY,
                        Collections.singletonList("b"),
                        IndexVisibility.ASYNC,
                        3)
                .build();
    }

    private static ServerInfo coordinatorServerInfo() {
        return new ServerInfo(
                0,
                null,
                Endpoint.fromListenersString("CLIENT://localhost:1234"),
                ServerType.COORDINATOR);
    }

    private static Set<ServerInfo> aliveTabletServerInfos() {
        return new HashSet<>(
                Arrays.asList(
                        new ServerInfo(
                                TABLET_SERVER_ID,
                                "rack1",
                                Endpoint.fromListenersString("CLIENT://localhost:90"),
                                ServerType.TABLET_SERVER),
                        new ServerInfo(
                                2,
                                "rack2",
                                Endpoint.fromListenersString("CLIENT://localhost:91"),
                                ServerType.TABLET_SERVER),
                        new ServerInfo(
                                3,
                                "rack3",
                                Endpoint.fromListenersString("CLIENT://localhost:92"),
                                ServerType.TABLET_SERVER)));
    }
}
