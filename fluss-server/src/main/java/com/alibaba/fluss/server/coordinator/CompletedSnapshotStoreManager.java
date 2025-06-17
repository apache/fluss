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

package com.alibaba.fluss.server.coordinator;

import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.server.kv.snapshot.CompletedSnapshot;
import com.alibaba.fluss.server.kv.snapshot.CompletedSnapshotHandle;
import com.alibaba.fluss.server.kv.snapshot.CompletedSnapshotHandleStore;
import com.alibaba.fluss.server.kv.snapshot.CompletedSnapshotStore;
import com.alibaba.fluss.server.kv.snapshot.SharedKvFileRegistry;
import com.alibaba.fluss.server.kv.snapshot.ZooKeeperCompletedSnapshotHandleStore;
import com.alibaba.fluss.server.zk.ZooKeeperClient;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.NotThreadSafe;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;

import static com.alibaba.fluss.utils.Preconditions.checkArgument;
import static com.alibaba.fluss.utils.Preconditions.checkNotNull;

/**
 * A manager to manage the {@link CompletedSnapshotStore} for each {@link TableBucket}. When the
 * {@link CompletedSnapshotStore} not exist for a {@link TableBucket}, it will create a new {@link
 * CompletedSnapshotStore} for it.
 */
@NotThreadSafe
public class CompletedSnapshotStoreManager {

    private static final Logger LOG = LoggerFactory.getLogger(CompletedSnapshotStoreManager.class);

    private final int maxNumberOfSnapshotsToRetain;
    private final ZooKeeperClient zooKeeperClient;
    private final Map<TableBucket, CompletedSnapshotStore> bucketCompletedSnapshotStores;
    private final Executor ioExecutor;

    public CompletedSnapshotStoreManager(
            int maxNumberOfSnapshotsToRetain,
            Executor ioExecutor,
            ZooKeeperClient zooKeeperClient) {
        checkArgument(
                maxNumberOfSnapshotsToRetain > 0, "maxNumberOfSnapshotsToRetain must be positive");
        this.maxNumberOfSnapshotsToRetain = maxNumberOfSnapshotsToRetain;
        this.zooKeeperClient = zooKeeperClient;
        this.bucketCompletedSnapshotStores = new HashMap<>();
        this.ioExecutor = ioExecutor;
    }

    public CompletedSnapshotStore getOrCreateCompletedSnapshotStore(TableBucket tableBucket) {
        return bucketCompletedSnapshotStores.computeIfAbsent(
                tableBucket,
                (bucket) -> {
                    try {
                        LOG.info("Creating snapshot store for table bucket {}.", bucket);
                        long start = System.currentTimeMillis();
                        CompletedSnapshotStore snapshotStore =
                                createCompletedSnapshotStore(tableBucket, ioExecutor);
                        long end = System.currentTimeMillis();
                        LOG.info(
                                "Created snapshot store for table bucket {} in {} ms.",
                                bucket,
                                end - start);
                        return snapshotStore;
                    } catch (Exception e) {
                        throw new RuntimeException(
                                "Failed to create completed snapshot store for table bucket "
                                        + bucket,
                                e);
                    }
                });
    }

    public void removeCompletedSnapshotStoreByTableBuckets(Set<TableBucket> tableBuckets) {
        for (TableBucket tableBucket : tableBuckets) {
            bucketCompletedSnapshotStores.remove(tableBucket);
        }
    }

    private CompletedSnapshotStore createCompletedSnapshotStore(
            TableBucket tableBucket, Executor ioExecutor) throws Exception {
        final CompletedSnapshotHandleStore completedSnapshotHandleStore =
                new ZooKeeperCompletedSnapshotHandleStore(zooKeeperClient);

        // Get all there is first.
        List<CompletedSnapshotHandle> initialSnapshots =
                completedSnapshotHandleStore.getAllCompletedSnapshotHandles(tableBucket);

        final int numberOfInitialSnapshots = initialSnapshots.size();

        LOG.info(
                "Found {} snapshots in {}.",
                numberOfInitialSnapshots,
                completedSnapshotHandleStore.getClass().getSimpleName());

        final List<CompletedSnapshot> retrievedSnapshots =
                new ArrayList<>(numberOfInitialSnapshots);

        LOG.info("Trying to fetch {} snapshots from storage.", numberOfInitialSnapshots);

        for (CompletedSnapshotHandle snapshotStateHandle : initialSnapshots) {
            retrievedSnapshots.add(checkNotNull(snapshotStateHandle.retrieveCompleteSnapshot()));
        }

        // register all the files to shared kv file registry
        SharedKvFileRegistry sharedKvFileRegistry = new SharedKvFileRegistry(ioExecutor);
        for (CompletedSnapshot completedSnapshot : retrievedSnapshots) {
            sharedKvFileRegistry.registerAllAfterRestored(completedSnapshot);
        }

        return new CompletedSnapshotStore(
                maxNumberOfSnapshotsToRetain,
                sharedKvFileRegistry,
                retrievedSnapshots,
                completedSnapshotHandleStore,
                ioExecutor);
    }

    public Map<TableBucket, CompletedSnapshotStore> getBucketCompletedSnapshotStores() {
        return bucketCompletedSnapshotStores;
    }
}
