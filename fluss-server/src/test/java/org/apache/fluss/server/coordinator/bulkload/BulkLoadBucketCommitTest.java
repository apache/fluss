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

import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.BulkLoadConflictException;
import org.apache.fluss.exception.InvalidBulkLoadSnapshotException;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.metadata.DatabaseDescriptor;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.server.coordinator.CompletedSnapshotStoreManager;
import org.apache.fluss.server.coordinator.LakeCatalogDynamicLoader;
import org.apache.fluss.server.coordinator.MetadataManager;
import org.apache.fluss.server.kv.snapshot.CompletedSnapshotHandle;
import org.apache.fluss.server.metrics.group.TestingMetricGroups;
import org.apache.fluss.server.testutils.TestingBulkLoadSnapshot;
import org.apache.fluss.server.zk.NOPErrorHandler;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.ZooKeeperExtension;
import org.apache.fluss.server.zk.data.BucketSnapshot;
import org.apache.fluss.server.zk.data.ZkData;
import org.apache.fluss.server.zk.data.bulkload.BulkLoadBucket;
import org.apache.fluss.server.zk.data.bulkload.BulkLoadTransaction;
import org.apache.fluss.shaded.zookeeper3.org.apache.zookeeper.KeeperException;
import org.apache.fluss.testutils.common.AllCallbackWrapper;
import org.apache.fluss.utils.json.JsonSerdeUtils;
import org.apache.fluss.utils.json.TableDescriptorJsonSerde;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.file.Path;
import java.util.Collections;

import static org.apache.fluss.record.TestData.DATA1_SCHEMA_PK;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Exercises the Coordinator commit against a prepared allocation and real ZooKeeper state. */
class BulkLoadBucketCommitTest {
    @RegisterExtension
    static final AllCallbackWrapper<ZooKeeperExtension> ZK =
            new AllCallbackWrapper<>(new ZooKeeperExtension());

    private static final TablePath TABLE = TablePath.of("db", "orders");
    private static final String LOAD_ID = "load";
    private static final long SNAPSHOT_ID = 7;
    private static final long WAL_OFFSET = 100;

    @TempDir Path remote;

    private ZooKeeperClient client;
    private BulkLoadManager manager;
    private BulkLoadMetadataStore store;
    private CompletedSnapshotStoreManager snapshots;
    private TableBucket bucket;
    private String remoteDataDir;

    @BeforeEach
    void setUp() throws Exception {
        ZooKeeperExtension zk = ZK.getCustomExtension();
        client = zk.getZooKeeperClient(NOPErrorHandler.INSTANCE);
        zk.cleanupRoot();
        Configuration conf = new Configuration();
        MetadataManager metadata =
                new MetadataManager(client, conf, new LakeCatalogDynamicLoader(conf, null, true));
        metadata.createDatabase(TABLE.getDatabaseName(), DatabaseDescriptor.EMPTY, false);
        long tableId =
                metadata.createTable(
                        TABLE,
                        remote.toUri().toString(),
                        TableDescriptor.builder()
                                .schema(DATA1_SCHEMA_PK)
                                .distributedBy(1, "a")
                                .build(),
                        null,
                        false);
        bucket = new TableBucket(tableId, 0);
        TableInfo table = metadata.getTable(TABLE);
        remoteDataDir = table.getRemoteDataDir();
        // Seed only the persisted state produced before bucket commit.
        String path = "/bulk_load/tables/" + tableId + "/transactions/" + LOAD_ID;
        createNode(path, runningTransaction(table));
        createNode(
                path + "/targets/table",
                JsonSerdeUtils.writeValueAsBytes(
                        table,
                        (target, out) -> {
                            out.writeStartObject();
                            out.writeNumberField("version", 1);
                            out.writeNumberField("bucket_count", target.getNumBuckets());
                            out.writeStringField("remote_data_dir", target.getRemoteDataDir());
                            out.writeBooleanField("withdrawn", false);
                            out.writeEndObject();
                        }));
        createNode(
                path + "/targets/table/buckets/0",
                new BulkLoadBucket(SNAPSHOT_ID, WAL_OFFSET, BulkLoadBucket.ALLOCATED).toJson());
        createNode(ZkData.BucketIdZNode.path(bucket), new byte[0]);
        int epoch =
                client.fenceBecomeCoordinatorLeader("bucket-commit").getCoordinatorEpochZkVersion();
        manager = new BulkLoadManager(client, metadata, epoch);
        store = new BulkLoadMetadataStore(client);
        snapshots =
                new CompletedSnapshotStoreManager(
                        2,
                        Runnable::run,
                        client,
                        TestingMetricGroups.COORDINATOR_METRICS,
                        ignored -> false);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testCommitPublishesSnapshotAndRetriesReuseResult(boolean hasPreviousSnapshot)
            throws Exception {
        CompletedSnapshotHandle uploaded = uploadSnapshot(SNAPSHOT_ID, WAL_OFFSET);
        long previousId = SNAPSHOT_ID - 1;
        if (hasPreviousSnapshot) {
            CompletedSnapshotHandle previous = uploadSnapshot(previousId, WAL_OFFSET);
            client.registerTableBucketSnapshot(
                    bucket,
                    new BucketSnapshot(
                            previousId, WAL_OFFSET, previous.getMetadataFilePath().toString()));
            client.updateRecoverySnapshot(bucket, previousId, null, -1);
        }
        assertThat(commit(WAL_OFFSET)).isTrue();
        assertThat(
                        store.bucket(
                                        manager.transaction(bucket.getTableId(), LOAD_ID).value,
                                        bucket)
                                .get()
                                .value
                                .state)
                .isEqualTo(BulkLoadBucket.COMMITTED);
        assertThat(client.getRecoveryBucketSnapshot(bucket))
                .contains(
                        new BucketSnapshot(
                                SNAPSHOT_ID,
                                WAL_OFFSET,
                                uploaded.getMetadataFilePath().toString()));
        assertThat(
                        snapshots
                                .getOrCreateCompletedSnapshotStore(TABLE, bucket)
                                .getActiveSnapshotIds())
                .contains(SNAPSHOT_ID);

        // A completed allocation is authoritative even when the leader offset has since changed.
        assertThat(commit(WAL_OFFSET + 1)).isFalse();
        assertThat(client.getRecoverySnapshotSelection(bucket).get().zkVersion())
                .isEqualTo(hasPreviousSnapshot ? 1 : 0);
    }

    @Test
    void testChangedWalOffsetRejectsCommitBeforePublication() throws Exception {
        uploadSnapshot(SNAPSHOT_ID, WAL_OFFSET);
        assertThatThrownBy(() -> commit(WAL_OFFSET + 1))
                .isInstanceOf(BulkLoadConflictException.class);
        assertThat(client.getTableBucketSnapshot(bucket, SNAPSHOT_ID)).isEmpty();
        assertBucketNotCommitted();
    }

    @Test
    void testSnapshotOffsetMustMatchAllocation() throws Exception {
        uploadSnapshot(SNAPSHOT_ID, WAL_OFFSET + 1);
        assertThatThrownBy(() -> commit(WAL_OFFSET))
                .isInstanceOf(InvalidBulkLoadSnapshotException.class)
                .hasMessageContaining("Snapshot recovery offset differs");
        assertThat(client.getTableBucketSnapshot(bucket, SNAPSHOT_ID)).isEmpty();
        assertBucketNotCommitted();
    }

    @Test
    void testExistingReferencePreventsPartialCommit() throws Exception {
        CompletedSnapshotHandle uploaded = uploadSnapshot(SNAPSHOT_ID, WAL_OFFSET);
        BucketSnapshot existing =
                new BucketSnapshot(
                        SNAPSHOT_ID, WAL_OFFSET, uploaded.getMetadataFilePath().toString());
        client.registerTableBucketSnapshot(bucket, existing);

        assertThatThrownBy(() -> commit(WAL_OFFSET))
                .isInstanceOf(KeeperException.NodeExistsException.class);
        assertThat(client.getTableBucketSnapshot(bucket, SNAPSHOT_ID)).contains(existing);
        assertBucketNotCommitted();
    }

    private void assertBucketNotCommitted() throws Exception {
        assertThat(
                        store.bucket(
                                        manager.transaction(bucket.getTableId(), LOAD_ID).value,
                                        bucket)
                                .get()
                                .value
                                .state)
                .isEqualTo(BulkLoadBucket.ALLOCATED);
        assertThat(client.getRecoverySnapshotSelection(bucket)).isEmpty();
    }

    private boolean commit(long leaderOffset) throws Exception {
        return manager.commitBucket(
                manager.transaction(bucket.getTableId(), LOAD_ID),
                bucket.getPartitionId(),
                bucket.getBucket(),
                ignored -> leaderOffset,
                snapshots);
    }

    private CompletedSnapshotHandle uploadSnapshot(long snapshotId, long offset) throws Exception {
        return TestingBulkLoadSnapshot.writeSnapshot(
                new FsPath(remoteDataDir),
                PhysicalTablePath.of(TABLE),
                bucket,
                snapshotId,
                offset,
                0L,
                Collections.emptyList());
    }

    private static byte[] runningTransaction(TableInfo table) {
        return JsonSerdeUtils.writeValueAsBytes(
                table,
                (info, out) -> {
                    out.writeStartObject();
                    out.writeNumberField("version", 1);
                    out.writeNumberField("state", BulkLoadTransaction.RUNNING);
                    out.writeNumberField("created_time", 10);
                    out.writeObjectFieldStart("table_path");
                    out.writeStringField("database_name", TABLE.getDatabaseName());
                    out.writeStringField("table_name", TABLE.getTableName());
                    out.writeEndObject();
                    out.writeObjectFieldStart("table_info");
                    out.writeNumberField("schema_id", info.getSchemaId());
                    out.writeFieldName("table_json");
                    TableDescriptorJsonSerde.INSTANCE.serialize(info.toTableDescriptor(), out);
                    out.writeNumberField("created_time", info.getCreatedTime());
                    out.writeNumberField("modified_time", info.getModifiedTime());
                    out.writeStringField("remote_data_dir", info.getRemoteDataDir());
                    out.writeNumberField("bucket_count_epoch", info.getBucketCountEpoch());
                    out.writeEndObject();
                    out.writeEndObject();
                });
    }

    private void createNode(String path, byte[] data) throws Exception {
        client.getCuratorClient().create().creatingParentsIfNeeded().forPath(path, data);
    }
}
