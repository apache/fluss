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
import org.apache.fluss.fs.FSDataOutputStream;
import org.apache.fluss.fs.FileSystem;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.metadata.KvSnapshotFileMetadata;
import org.apache.fluss.metadata.KvSnapshotFileMetadataJsonSerde;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.KvRecord;
import org.apache.fluss.rpc.gateway.TabletServerGateway;
import org.apache.fluss.rpc.messages.NotifyLeaderAndIsrRequest;
import org.apache.fluss.rpc.messages.PutKvResponse;
import org.apache.fluss.server.entity.NotifyLeaderAndIsrData;
import org.apache.fluss.server.kv.rocksdb.RocksDBExtension;
import org.apache.fluss.server.kv.snapshot.CompletedSnapshot;
import org.apache.fluss.server.kv.snapshot.CompletedSnapshotHandle;
import org.apache.fluss.server.kv.snapshot.KvFileHandleAndLocalPath;
import org.apache.fluss.server.kv.snapshot.KvSnapshotDataUploader;
import org.apache.fluss.server.kv.snapshot.KvSnapshotHandle;
import org.apache.fluss.server.kv.snapshot.RocksIncrementalSnapshot;
import org.apache.fluss.server.kv.snapshot.SnapshotLocation;
import org.apache.fluss.server.kv.snapshot.TabletState;
import org.apache.fluss.server.replica.Replica;
import org.apache.fluss.server.replica.ReplicaManager;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.server.utils.ResourceGuard;
import org.apache.fluss.server.zk.ZkSequenceIDCounter;
import org.apache.fluss.server.zk.data.ZkData;
import org.apache.fluss.utils.CloseableRegistry;
import org.apache.fluss.utils.FlussPaths;
import org.apache.fluss.utils.types.Tuple2;

import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import static org.apache.fluss.record.TestData.DATA1_SCHEMA_PK;
import static org.apache.fluss.server.testutils.KvTestUtils.assertLookupResponse;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.createTable;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.newLookupRequest;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.newPutKvRequest;
import static org.apache.fluss.server.utils.ServerRpcMessageUtils.makeNotifyBucketLeaderAndIsr;
import static org.apache.fluss.server.utils.ServerRpcMessageUtils.makeNotifyLeaderAndIsrRequest;
import static org.apache.fluss.testutils.DataTestUtils.genKvRecords;
import static org.apache.fluss.testutils.DataTestUtils.getKeyValuePairs;
import static org.apache.fluss.testutils.DataTestUtils.toKvRecordBatch;
import static org.apache.fluss.testutils.common.CommonTestUtils.retry;
import static org.assertj.core.api.Assertions.assertThat;

/** Registration, recovery and replication from an externally produced KV snapshot. */
class ExternalKvSnapshotITCase {

    @RegisterExtension public final RocksDBExtension rocksDB = new RocksDBExtension();

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testSnapshotOnlyRecoveryAndOnlineWrites(boolean remoteLogEnabled) throws Exception {
        Configuration conf = new Configuration();
        conf.set(ConfigOptions.KV_SNAPSHOT_INTERVAL, Duration.ofHours(1));
        conf.set(ConfigOptions.KV_MAX_RETAINED_SNAPSHOTS, 1);
        conf.set(
                ConfigOptions.REMOTE_LOG_TASK_INTERVAL_DURATION,
                remoteLogEnabled ? Duration.ofHours(1) : Duration.ZERO);
        FlussClusterExtension cluster =
                FlussClusterExtension.builder()
                        .setNumOfTabletServers(2)
                        .setClusterConf(conf)
                        .build();
        try {
            cluster.start();
            TablePath path = TablePath.of("snapshot_db", "snapshot_boundary");
            long tableId =
                    createTable(
                            cluster,
                            path,
                            TableDescriptor.builder()
                                    .schema(DATA1_SCHEMA_PK)
                                    .distributedBy(1, "a")
                                    .build()
                                    .withReplicationFactor(2));
            TableBucket bucket = new TableBucket(tableId, 0);
            cluster.waitUntilAllReplicaReady(bucket);
            List<KvRecord> records = new ArrayList<>();
            records.addAll(genKvRecords(new Object[] {1, "snapshot-one"}));
            records.addAll(genKvRecords(new Object[] {2, "snapshot-two"}));
            // The target has no online writes. Keep its replicas inactive while registering
            // files produced for this table, then let normal role notifications restore them.
            for (int server = 0; server < 2; server++) {
                cluster.stopTabletServer(server);
            }
            long boundary = 10_017L;
            CompletedSnapshotHandle external =
                    produceSnapshot(cluster, path, bucket, records, boundary);
            CompletedSnapshotStoreManager manager =
                    cluster.getCoordinatorServer()
                            .getCoordinatorEventProcessor()
                            .completedSnapshotStoreManager();
            int epoch =
                    cluster.getZooKeeperClient().getCurrentEpoch().getCoordinatorEpochZkVersion();
            manager.registerExternalSnapshot(path, bucket, external, epoch);
            manager.registerExternalSnapshot(path, bucket, external, epoch);
            assertThat(manager.getOrCreateCompletedSnapshotStore(path, bucket).getNumSnapshots())
                    .isEqualTo(1);
            cluster.stopCoordinatorServer();
            cluster.startCoordinatorServer();
            for (int server = 0; server < 2; server++) {
                cluster.startTabletServer(server);
            }
            cluster.waitUntilAllReplicaReady(bucket);
            assertReplicaOffsets(cluster, bucket, boundary, boundary);
            assertRows(cluster, bucket, records);

            List<KvRecord> firstTail = genKvRecords(new Object[] {3, "first-online-write"});
            putRecords(cluster, bucket, firstTail);
            records.addAll(firstTail);
            assertReplicaOffsets(cluster, bucket, boundary, boundary + 1L);
            assertRows(cluster, bucket, records);

            NotifyLeaderAndIsrRequest repeatedNotify =
                    makeNotifyLeaderAndIsrRequest(
                            cluster.getZooKeeperClient().getCurrentEpoch().getCoordinatorEpoch(),
                            Collections.singletonList(
                                    makeNotifyBucketLeaderAndIsr(
                                            new NotifyLeaderAndIsrData(
                                                    PhysicalTablePath.of(path),
                                                    bucket,
                                                    Arrays.asList(0, 1),
                                                    cluster.waitLeaderAndIsrReady(bucket)))));
            for (int server = 0; server < 2; server++) {
                assertThat(
                                cluster.newTabletServerClientForNode(server)
                                        .notifyLeaderAndIsr(repeatedNotify)
                                        .get()
                                        .getNotifyBucketsLeaderRespAt(0)
                                        .hasErrorCode())
                        .isFalse();
            }
            assertReplicaOffsets(cluster, bucket, boundary, boundary + 1L);
            assertRows(cluster, bucket, records);

            int oldLeader = cluster.waitAndGetLeader(bucket);
            cluster.stopTabletServer(oldLeader);
            retry(
                    Duration.ofMinutes(1),
                    () -> assertThat(cluster.waitAndGetLeader(bucket)).isNotEqualTo(oldLeader));
            assertRows(cluster, bucket, records);
            List<KvRecord> nextTail = genKvRecords(new Object[] {4, "write-after-failover"});
            putRecords(cluster, bucket, nextTail);
            records.addAll(nextTail);
            assertRows(cluster, bucket, records);
            cluster.startTabletServer(oldLeader);
            cluster.waitUntilAllReplicaReady(bucket);
            assertReplicaOffsets(cluster, bucket, boundary, boundary + 2L);
            CompletedSnapshot ordinary = cluster.triggerAndWaitSnapshot(bucket);
            assertThat(ordinary.getSnapshotID()).isGreaterThan(external.getSnapshotId());
            assertThat(ordinary.getLogOffset()).isEqualTo(boundary + 2L);
            retry(
                    Duration.ofMinutes(1),
                    () ->
                            assertThat(
                                            external.getMetadataFilePath()
                                                    .getFileSystem()
                                                    .exists(external.getMetadataFilePath()))
                                    .isFalse());
            assertRows(cluster, bucket, records);
            assertThat(cluster.getZooKeeperClient().getRemoteLogManifestHandle(bucket)).isEmpty();
        } finally {
            cluster.close();
        }
    }

    private CompletedSnapshotHandle produceSnapshot(
            FlussClusterExtension cluster,
            TablePath tablePath,
            TableBucket bucket,
            List<KvRecord> records,
            long offset)
            throws Exception {
        long snapshotId =
                new ZkSequenceIDCounter(
                                cluster.getZooKeeperClient().getCuratorClient(),
                                ZkData.BucketSnapshotSequenceIdZNode.path(bucket))
                        .getAndIncrement();
        FsPath tabletDir =
                FlussPaths.remoteKvTabletDir(
                        new FsPath(cluster.getRemoteDataDir(), "kv"),
                        PhysicalTablePath.of(tablePath),
                        bucket);
        FsPath location = FlussPaths.remoteKvSnapshotDir(tabletDir, snapshotId);
        SnapshotLocation snapshotLocation =
                new SnapshotLocation(
                        location.getFileSystem(),
                        location,
                        FlussPaths.remoteKvSharedDir(tabletDir),
                        1024);
        for (Tuple2<byte[], byte[]> entry : getKeyValuePairs(records)) {
            rocksDB.getRocksDb().put(entry.f0, entry.f1);
        }
        ExecutorService uploader = Executors.newSingleThreadExecutor();
        KvSnapshotHandle files;
        try (ResourceGuard guard = new ResourceGuard();
                CloseableRegistry registry = new CloseableRegistry();
                RocksIncrementalSnapshot snapshot =
                        new RocksIncrementalSnapshot(
                                new HashMap<>(),
                                rocksDB.getRocksDb(),
                                guard,
                                new KvSnapshotDataUploader(uploader),
                                rocksDB.getRockDbDir(),
                                -1L)) {
            files =
                    snapshot.asyncSnapshot(
                                    snapshot.syncPrepareResources(snapshotId),
                                    snapshotId,
                                    new TabletState(offset, (long) records.size(), null),
                                    snapshotLocation)
                            .get(registry)
                            .getKvSnapshotHandle();
        } finally {
            uploader.shutdownNow();
        }
        KvSnapshotFileMetadata metadata =
                new KvSnapshotFileMetadata(
                        bucket,
                        snapshotId,
                        location.toString(),
                        fileMetadata(files.getSharedKvFileHandles()),
                        fileMetadata(files.getPrivateFileHandles()),
                        files.getIncrementalSize(),
                        offset,
                        (long) records.size(),
                        Collections.emptyList());
        FsPath metadataPath = CompletedSnapshot.getMetadataFilePath(location);
        try (FSDataOutputStream output =
                metadataPath
                        .getFileSystem()
                        .create(metadataPath, FileSystem.WriteMode.NO_OVERWRITE)) {
            output.write(KvSnapshotFileMetadataJsonSerde.toJson(metadata));
        }
        return new CompletedSnapshotHandle(snapshotId, metadataPath, offset);
    }

    private static List<KvSnapshotFileMetadata.FileHandle> fileMetadata(
            List<KvFileHandleAndLocalPath> files) {
        return files.stream()
                .map(
                        file ->
                                new KvSnapshotFileMetadata.FileHandle(
                                        file.getKvFileHandle().getFilePath(),
                                        file.getKvFileHandle().getSize(),
                                        file.getLocalPath()))
                .collect(Collectors.toList());
    }

    private static void putRecords(
            FlussClusterExtension cluster, TableBucket bucket, List<KvRecord> records)
            throws Exception {
        PutKvResponse response =
                cluster.newTabletServerClientForNode(cluster.waitAndGetLeader(bucket))
                        .putKv(
                                newPutKvRequest(
                                        bucket.getTableId(),
                                        bucket.getBucket(),
                                        -1,
                                        toKvRecordBatch(records)))
                        .get();
        assertThat(response.getBucketsRespAt(0).hasErrorCode()).isFalse();
    }

    private static void assertReplicaOffsets(
            FlussClusterExtension cluster, TableBucket bucket, long start, long end)
            throws Exception {
        retry(
                Duration.ofMinutes(1),
                () -> {
                    for (int server = 0; server < 2; server++) {
                        ReplicaManager replicaManager =
                                cluster.getTabletServerById(server).getReplicaManager();
                        assertThat(replicaManager.getReplica(bucket))
                                .isInstanceOf(ReplicaManager.OnlineReplica.class);
                        Replica replica = replicaManager.getReplicaOrException(bucket);
                        assertThat(replica.getLogTablet().localLogStartOffset()).isEqualTo(start);
                        assertThat(replica.getLocalLogEndOffset()).isEqualTo(end);
                        assertThat(replica.getLogHighWatermark()).isEqualTo(end);
                    }
                });
    }

    private static void assertRows(
            FlussClusterExtension cluster, TableBucket bucket, List<KvRecord> records)
            throws Exception {
        TabletServerGateway gateway =
                cluster.newTabletServerClientForNode(cluster.waitAndGetLeader(bucket));
        for (Tuple2<byte[], byte[]> keyValue : getKeyValuePairs(records)) {
            assertLookupResponse(
                    gateway.lookup(
                                    newLookupRequest(
                                            bucket.getTableId(), bucket.getBucket(), keyValue.f0))
                            .get(),
                    keyValue.f1);
        }
    }
}
