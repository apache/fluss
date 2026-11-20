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

package org.apache.fluss.server.coordinator.bulkload;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.BulkLoadNotFoundException;
import org.apache.fluss.exception.BulkLoadStateException;
import org.apache.fluss.exception.BulkLoadTargetWithdrawnException;
import org.apache.fluss.exception.InvalidBulkLoadRequestException;
import org.apache.fluss.metadata.DatabaseDescriptor;
import org.apache.fluss.metadata.MergeEngineType;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.rpc.messages.BeginBulkLoadRequest;
import org.apache.fluss.rpc.messages.BeginBulkLoadResponse;
import org.apache.fluss.rpc.messages.PbBulkLoadHandle;
import org.apache.fluss.server.coordinator.LakeCatalogDynamicLoader;
import org.apache.fluss.server.coordinator.MetadataManager;
import org.apache.fluss.server.zk.NOPErrorHandler;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.ZooKeeperExtension;
import org.apache.fluss.server.zk.data.PartitionRegistration;
import org.apache.fluss.server.zk.data.ZkData;
import org.apache.fluss.server.zk.data.bulkload.BulkLoadTransaction;
import org.apache.fluss.testutils.common.AllCallbackWrapper;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.Arrays;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Request-level regression tests against real metadata, without waiting for replicas at Begin. */
class BulkLoadManagerTest {
    @RegisterExtension
    static final AllCallbackWrapper<ZooKeeperExtension> ZK =
            new AllCallbackWrapper<>(new ZooKeeperExtension());

    private static final TablePath PATH = TablePath.of("db", "orders");
    private ZooKeeperClient client;
    private MetadataManager metadata;
    private BulkLoadManager manager;
    private Configuration conf;

    @BeforeEach
    void setUp() {
        client = ZK.getCustomExtension().getZooKeeperClient(NOPErrorHandler.INSTANCE);
        ZK.getCustomExtension().cleanupRoot();
        conf = new Configuration();
        metadata =
                new MetadataManager(client, conf, new LakeCatalogDynamicLoader(conf, null, true));
        metadata.createDatabase("db", DatabaseDescriptor.EMPTY, false);
        manager = new BulkLoadManager(conf, client, metadata, () -> -1);
    }

    @Test
    void testBeginRetryAndFirstAbortExplanation() throws Exception {
        create(false);
        BeginBulkLoadResponse begin = manager.begin(PATH, request("load-1"));
        PbBulkLoadHandle handle = begin.getStatus().getHandle();
        assertThat(begin.getTargetsCount()).isEqualTo(1);
        assertThat(client.listBucketSnapshotIds(new TableBucket(handle.getTableId(), 0))).isEmpty();
        manager.finish(manager.transaction(handle), true, null);
        BulkLoadTransaction retry =
                manager.finish(manager.transaction(handle), true, "later explanation");
        assertThat(retry.abortMessage).isNull();
        assertThat(retry.finishedTime).isNotNull();
        assertThat(manager.begin(PATH, request("load-1")).getStatus().getState())
                .isEqualTo(BulkLoadTransaction.ABORTED);
        assertThat(manager.list(PATH, Collections.emptyList()).getTransactionsCount()).isZero();
        assertThatThrownBy(() -> manager.finish(manager.transaction(handle), false, null))
                .isInstanceOf(BulkLoadStateException.class);
        assertThat(manager.begin(PATH, request("load-2")).getStatus().getState())
                .isEqualTo(BulkLoadTransaction.RUNNING);
    }

    @Test
    void testPartitionSelectionIsFixedAcrossDropAndRecreation() throws Exception {
        long tableId = create(true);
        partition(tableId, 10, "p1");
        partition(tableId, 11, "p2");
        BeginBulkLoadRequest request = request("load-1");
        request.addAllPartitionNames(Arrays.asList("p2", "p1"));
        BeginBulkLoadResponse begin = manager.begin(PATH, request);
        PbBulkLoadHandle handle = begin.getStatus().getHandle();
        assertThat(begin.getTargetAt(0).getPartitionId()).isEqualTo(10);
        assertThat(manager.list(PATH, Arrays.asList("p1", "p2")).getTransactionsCount())
                .isEqualTo(1);
        client.deletePartition(PATH, "p1");
        partition(tableId, 12, "p1");
        assertThatThrownBy(
                        () -> manager.allocate(manager.transaction(handle), 10L, 0, bucket -> 0L))
                .isInstanceOf(BulkLoadTargetWithdrawnException.class);
        assertThat(client.getRecoverySnapshotSelection(new TableBucket(tableId, 12L, 0))).isEmpty();
        client.deletePartition(PATH, "p2");
        assertThat(manager.info(manager.transaction(handle)).getTargetAt(0).isWithdrawn()).isTrue();
        assertThat(manager.info(manager.transaction(handle)).getTargetAt(1).isWithdrawn()).isTrue();
        assertThat(manager.begin(PATH, request).getTargetAt(0).getPartitionId()).isEqualTo(10);
        assertThat(manager.list(PATH, Collections.singletonList("p1")).getTransactionsCount())
                .isZero();
        assertThat(manager.finish(manager.transaction(handle), false, null).state)
                .isEqualTo(BulkLoadTransaction.COMMITTED);
    }

    @Test
    void testOldHandleCannotAddressRecreatedTable() throws Exception {
        create(false);
        PbBulkLoadHandle handle = manager.begin(PATH, request("load-1")).getStatus().getHandle();
        metadata.dropTable(PATH, false);
        create(false);
        assertThatThrownBy(() -> manager.info(manager.transaction(handle)))
                .isInstanceOf(BulkLoadNotFoundException.class);
        assertThatThrownBy(() -> manager.finish(manager.transaction(handle), true, null))
                .isInstanceOf(BulkLoadNotFoundException.class);
        assertThat(manager.list(PATH, Collections.emptyList()).getTransactionsCount()).isZero();
    }

    @Test
    void testTargetLimitAndDuplicateSelectionRejectBeforeOwnership() throws Exception {
        long tableId = create(true);
        partition(tableId, 10, "p1");
        partition(tableId, 11, "p2");
        conf.set(ConfigOptions.COORDINATOR_BULKLOAD_MAX_TARGETS, 1);
        manager = new BulkLoadManager(conf, client, metadata, () -> -1);
        BeginBulkLoadRequest request = request("load-1");
        request.addAllPartitionNames(Arrays.asList("p1", "p2"));
        assertThatThrownBy(() -> manager.begin(PATH, request))
                .isInstanceOf(InvalidBulkLoadRequestException.class);
        assertThat(manager.list(PATH, Collections.emptyList()).getTransactionsCount()).isZero();
        request.clearPartitionNames();
        request.addAllPartitionNames(Arrays.asList("p1", "p1"));
        assertThatThrownBy(() -> manager.begin(PATH, request))
                .isInstanceOf(InvalidBulkLoadRequestException.class);
    }

    @Test
    void testBeginRejectsMergeEngineTable() {
        metadata.createTable(
                PATH,
                client.getDefaultRemoteDataDir(),
                tableDescriptor()
                        .property(ConfigOptions.TABLE_MERGE_ENGINE, MergeEngineType.FIRST_ROW)
                        .build(),
                null,
                false);

        assertThatThrownBy(() -> manager.begin(PATH, request("load-1")))
                .isInstanceOf(InvalidBulkLoadRequestException.class)
                .hasMessageContaining("merge engine");
    }

    private long create(boolean partitioned) {
        TableDescriptor.Builder descriptor = tableDescriptor();
        if (partitioned) {
            descriptor.partitionedBy("p");
        }
        return metadata.createTable(
                PATH, client.getDefaultRemoteDataDir(), descriptor.build(), null, false);
    }

    private static TableDescriptor.Builder tableDescriptor() {
        return TableDescriptor.builder()
                .schema(
                        Schema.newBuilder()
                                .column("p", DataTypes.STRING())
                                .column("k", DataTypes.INT())
                                .primaryKey("p", "k")
                                .build())
                .distributedBy(2, "k");
    }

    private void partition(long tableId, long id, String name) throws Exception {
        client.getCuratorClient()
                .create()
                .creatingParentsIfNeeded()
                .forPath(
                        ZkData.PartitionZNode.path(PATH, name),
                        ZkData.PartitionZNode.encode(
                                new PartitionRegistration(
                                        tableId, id, client.getDefaultRemoteDataDir(), 2)));
    }

    private static BeginBulkLoadRequest request(String id) {
        BeginBulkLoadRequest request = new BeginBulkLoadRequest().setBulkLoadId(id);
        request.setTablePath().setDatabaseName("db").setTableName("orders");
        return request;
    }
}
