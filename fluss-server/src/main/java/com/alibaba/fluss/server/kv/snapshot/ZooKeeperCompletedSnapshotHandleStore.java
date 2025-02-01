/*
 * Copyright (c) 2024 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.server.kv.snapshot;

import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.server.zk.ZooKeeperClient;
import com.alibaba.fluss.server.zk.data.BucketSnapshot;
import com.alibaba.fluss.utils.types.Tuple2;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.alibaba.fluss.utils.Preconditions.checkNotNull;

/**
 * Class to store the handle to the snapshot to ZooKeeper.
 *
 * <p>Note: We only store a completed snapshot handle(a path to completed snapshot) to zookeeper
 * instead of the whole {@link CompletedSnapshot} object. This level of indirection is necessary to
 * keep the amount of data in ZooKeeper small. ZooKeeper is build for data in the KB range whereas
 * state can grow to multiple MBs.
 */
public class ZooKeeperCompletedSnapshotHandleStore implements CompletedSnapshotHandleStore {

    private static final Logger LOG =
            LoggerFactory.getLogger(ZooKeeperCompletedSnapshotHandleStore.class);

    private final ZooKeeperClient client;

    public ZooKeeperCompletedSnapshotHandleStore(ZooKeeperClient zooKeeperClient) {
        this.client = zooKeeperClient;
    }

    /**
     * Stores the completed snapshot handle to ZooKeeper for given table bucket and snapshot id.
     *
     * <p><strong>Important</strong>: This will <em>not</em> store the actual snapshot in ZooKeeper,
     * but store the snapshot path in ZooKeeper. This level of indirection makes sure that data in
     * ZooKeeper is small.
     *
     * <p>The operation will fail if there is already a node under the given path.
     *
     * @throws Exception If a ZooKeeper or snapshot handle operation fails
     */
    @Override
    public void add(
            TableBucket tableBucket, long snapshotId, CompletedSnapshotHandle snapshotHandle)
            throws Exception {
        checkNotNull(snapshotHandle, "completed snapshot handle");

        boolean success = false;
        try {
            client.registerTableBucketSnapshot(
                    tableBucket,
                    new BucketSnapshot(
                            snapshotId,
                            snapshotHandle.getLogOffset(),
                            snapshotHandle.getMetadataFilePath().toString()));
            success = true;
        } finally {
            if (!success) {
                // Cleanup the snapshot metadata handle if it was not written to zookeeper
                snapshotHandle.discard();
            }
        }
    }

    @Override
    public void remove(TableBucket tableBucket, long snapshotId) throws Exception {
        // TODO: it may bring concurrent delete operations when lost leadership and a new leadership
        // grant. the new leader may need to use the snapshot to restore, but the ex-leader is
        // removing it. May be handle it like https://issues.apache.org/jira/browse/FLINK-6612
        checkNotNull(tableBucket, "Table bucket");
        client.deleteTableBucketSnapshot(tableBucket, snapshotId);
    }

    @Override
    public Optional<CompletedSnapshotHandle> get(TableBucket tableBucket, long snapshotId)
            throws Exception {
        return client.getTableBucketSnapshot(tableBucket, snapshotId)
                .map(BucketSnapshot::toCompletedSnapshotHandle);
    }

    @Override
    public List<CompletedSnapshotHandle> getAllCompletedSnapshotHandles(TableBucket tableBucket)
            throws Exception {
        return client.getTableBucketAllSnapshotAndIds(tableBucket).stream()
                .map(
                        bucketSnapshotAndSnapshotId ->
                                Tuple2.of(
                                        bucketSnapshotAndSnapshotId.f0.toCompletedSnapshotHandle(),
                                        bucketSnapshotAndSnapshotId.f1))
                .sorted(Comparator.comparing(o -> o.f1))
                .map(t -> t.f0)
                .collect(Collectors.toList());
    }

    @Override
    public Optional<CompletedSnapshotHandle> getLatestCompletedSnapshotHandle(
            TableBucket tableBucket) throws Exception {
        return client.getTableBucketLatestSnapshot(tableBucket)
                .map(BucketSnapshot::toCompletedSnapshotHandle);
    }
}
