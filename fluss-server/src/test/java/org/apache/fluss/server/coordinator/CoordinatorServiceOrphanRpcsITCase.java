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

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.rpc.gateway.CoordinatorGateway;
import org.apache.fluss.rpc.messages.AcquireKvSnapshotLeaseRequest;
import org.apache.fluss.rpc.messages.ListKvSnapshotsRequest;
import org.apache.fluss.rpc.messages.ListKvSnapshotsResponse;
import org.apache.fluss.rpc.messages.ListRemoteLogManifestsRequest;
import org.apache.fluss.rpc.messages.ListRemoteLogManifestsResponse;
import org.apache.fluss.rpc.messages.PbKvSnapshot;
import org.apache.fluss.rpc.messages.PbRemoteLogManifestEntry;
import org.apache.fluss.rpc.messages.PutKvResponse;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.server.testutils.RpcMessageTestUtils;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.data.RemoteLogManifestHandle;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.time.Duration;
import java.util.concurrent.ExecutionException;

import static org.apache.fluss.record.TestData.DATA1_SCHEMA_PK;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.newPutKvRequest;
import static org.apache.fluss.testutils.DataTestUtils.genKvRecords;
import static org.apache.fluss.testutils.DataTestUtils.toKvRecordBatch;
import static org.apache.fluss.testutils.common.CommonTestUtils.retry;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** ITCase for orphan-cleanup-related coordinator RPCs: ListRemoteLogManifests, ListKvSnapshots. */
class CoordinatorServiceOrphanRpcsITCase {

    @RegisterExtension
    public static final FlussClusterExtension FLUSS_CLUSTER_EXTENSION =
            FlussClusterExtension.builder()
                    .setNumOfTabletServers(1)
                    .setClusterConf(
                            new Configuration()
                                    .set(ConfigOptions.KV_MAX_RETAINED_SNAPSHOTS, 2)
                                    .set(ConfigOptions.KV_SNAPSHOT_INTERVAL, Duration.ofHours(1)))
                    .build();

    private static final TablePath PK_TABLE_PATH = TablePath.of("orphan_rpc_test_db", "pk_table");
    private static final TableDescriptor PK_TABLE_DESCRIPTOR =
            TableDescriptor.builder().schema(DATA1_SCHEMA_PK).distributedBy(1, "a").build();

    private long pkTableId;

    @BeforeEach
    void setupTables() throws Exception {
        pkTableId =
                RpcMessageTestUtils.createTable(
                        FLUSS_CLUSTER_EXTENSION, PK_TABLE_PATH, PK_TABLE_DESCRIPTOR);
        FLUSS_CLUSTER_EXTENSION.waitUntilTableReady(pkTableId);
    }

    // -------------------------------------------------------------------------
    // ListRemoteLogManifests
    // -------------------------------------------------------------------------

    @Test
    void listManifestsForUnpartitionedTable() throws Exception {
        long tableId = 999_111L;
        ZooKeeperClient zk = FLUSS_CLUSTER_EXTENSION.getZooKeeperClient();
        zk.upsertRemoteLogManifestHandle(
                new TableBucket(tableId, 0),
                new RemoteLogManifestHandle(new FsPath("oss://bucket-0/m.manifest"), 100L));
        zk.upsertRemoteLogManifestHandle(
                new TableBucket(tableId, 1),
                new RemoteLogManifestHandle(new FsPath("oss://bucket-1/m.manifest"), 200L));

        CoordinatorGateway gateway = FLUSS_CLUSTER_EXTENSION.newCoordinatorClient();
        ListRemoteLogManifestsRequest request = new ListRemoteLogManifestsRequest();
        request.setTableId(tableId);
        ListRemoteLogManifestsResponse response = gateway.listRemoteLogManifests(request).get();

        assertThat(response.getManifestsList()).hasSize(2);
        assertThat(response.getManifestsList())
                .extracting(PbRemoteLogManifestEntry::getRemoteLogEndOffset)
                .containsExactlyInAnyOrder(100L, 200L);
    }

    @Test
    void listManifestsForUnknownTableReturnsEmpty() throws Exception {
        CoordinatorGateway gateway = FLUSS_CLUSTER_EXTENSION.newCoordinatorClient();
        ListRemoteLogManifestsRequest request = new ListRemoteLogManifestsRequest();
        request.setTableId(999_999L);
        ListRemoteLogManifestsResponse response = gateway.listRemoteLogManifests(request).get();

        assertThat(response.getManifestsList()).isEmpty();
    }

    // -------------------------------------------------------------------------
    // ListKvSnapshots
    // -------------------------------------------------------------------------

    @Test
    void listSnapshotsReturnsRetainedSubset() throws Exception {
        TableBucket bucket = new TableBucket(pkTableId, 0);
        FLUSS_CLUSTER_EXTENSION.waitUntilAllReplicaReady(bucket);
        writeSnapshot(bucket, 1);
        assertActiveSnapshots(writeSnapshot(bucket, 2), writeSnapshot(bucket, 3));
    }

    @Test
    void listSnapshotsReturnsStillInUseBeyondRetention() throws Exception {
        TableBucket bucket = new TableBucket(pkTableId, 0);
        FLUSS_CLUSTER_EXTENSION.waitUntilAllReplicaReady(bucket);
        long leased = writeSnapshot(bucket, 1);
        AcquireKvSnapshotLeaseRequest lease =
                new AcquireKvSnapshotLeaseRequest()
                        .setLeaseId("orphan-rpc-test")
                        .setLeaseDurationMs(Duration.ofMinutes(5).toMillis());
        lease.addSnapshotsToLease()
                .setTableId(pkTableId)
                .addBucketSnapshot()
                .setBucketId(bucket.getBucket())
                .setSnapshotId(leased);
        assertThat(
                        FLUSS_CLUSTER_EXTENSION
                                .newCoordinatorClient()
                                .acquireKvSnapshotLease(lease)
                                .get()
                                .getUnavailableSnapshotsList())
                .isEmpty();
        writeSnapshot(bucket, 2);
        assertActiveSnapshots(leased, writeSnapshot(bucket, 3), writeSnapshot(bucket, 4));
    }

    @Test
    void listSnapshotsForUnknownTableFails() {
        CoordinatorGateway gateway = FLUSS_CLUSTER_EXTENSION.newCoordinatorClient();
        ListKvSnapshotsRequest request = new ListKvSnapshotsRequest();
        request.setTableId(999_888L);

        assertThatThrownBy(() -> gateway.listKvSnapshots(request).get())
                .isInstanceOf(ExecutionException.class);
    }

    private long writeSnapshot(TableBucket bucket, int key) throws Exception {
        PutKvResponse response =
                FLUSS_CLUSTER_EXTENSION
                        .newTabletServerClientForNode(
                                FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(bucket))
                        .putKv(
                                newPutKvRequest(
                                        pkTableId,
                                        bucket.getBucket(),
                                        -1,
                                        toKvRecordBatch(genKvRecords(new Object[] {key, "value"}))))
                        .get();
        assertThat(response.getBucketsRespAt(0).hasErrorCode()).isFalse();
        return FLUSS_CLUSTER_EXTENSION.triggerAndWaitSnapshot(bucket).getSnapshotID();
    }

    private void assertActiveSnapshots(Long... expected) {
        CoordinatorGateway gateway = FLUSS_CLUSTER_EXTENSION.newCoordinatorClient();
        ListKvSnapshotsRequest request = new ListKvSnapshotsRequest().setTableId(pkTableId);
        retry(
                Duration.ofSeconds(30),
                () -> {
                    ListKvSnapshotsResponse response = gateway.listKvSnapshots(request).get();
                    assertThat(response.getTableId()).isEqualTo(pkTableId);
                    assertThat(response.hasPartitionId()).isFalse();
                    assertThat(response.getActiveSnapshotsList())
                            .allSatisfy(snapshot -> assertThat(snapshot.getBucketId()).isZero())
                            .extracting(PbKvSnapshot::getSnapshotId)
                            .containsExactlyInAnyOrder(expected);
                });
    }
}
