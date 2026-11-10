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

import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.server.entity.NotifyKvSnapshotOffsetData;
import org.apache.fluss.server.entity.NotifyLeaderAndIsrData;
import org.apache.fluss.server.entity.NotifyLeaderAndIsrResultForBucket;
import org.apache.fluss.server.zk.data.LeaderAndIsr;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.apache.fluss.record.TestData.DATA1_TABLE_ID_PK;
import static org.apache.fluss.record.TestData.DATA1_TABLE_PATH_PK;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Per-replica Snapshot task ownership and short restoration queries. */
class ReplicaManagerBulkLoadTest extends ReplicaTestBase {
    @Test
    void testDuplicateRestoreQueriesOwnOneTaskAndMissingSnapshotCannotAcknowledge() {
        TableBucket bucket = new TableBucket(DATA1_TABLE_ID_PK, 0);
        makeKvTableAsLeader(DATA1_TABLE_ID_PK, DATA1_TABLE_PATH_PK, 0);
        Replica replica = replicaManager.getReplicaOrException(bucket);
        Object healthy = replica.getKvTablet();
        List<Runnable> tasks = new ArrayList<>();
        NotifyLeaderAndIsrData request = request(replica, 7L);
        assertThat(replica.restoreImportedSnapshot(request, tasks::add)).isFalse();
        assertThat(replica.restoreImportedSnapshot(request, tasks::add)).isFalse();
        assertThat(tasks).hasSize(1);
        tasks.remove(0).run();
        assertThat(replica.getKvTablet()).isSameAs(healthy);
        assertThat(replica.restoreImportedSnapshot(request, tasks::add)).isFalse();
        assertThat(tasks).hasSize(1);
        tasks.remove(0).run();
    }

    @Test
    void testDeleteBeforeQueuedRestoreCannotRecreateKv() {
        TableBucket bucket = new TableBucket(DATA1_TABLE_ID_PK, 0);
        makeKvTableAsLeader(DATA1_TABLE_ID_PK, DATA1_TABLE_PATH_PK, 0);
        Replica replica = replicaManager.getReplicaOrException(bucket);
        List<Runnable> tasks = new ArrayList<>();
        NotifyLeaderAndIsrData request = request(replica, 7L);
        assertThat(replica.restoreImportedSnapshot(request, tasks::add)).isFalse();
        replica.delete();
        tasks.remove(0).run();
        assertThat(replica.getKvTablet()).isNull();
        assertThatThrownBy(() -> replica.restoreImportedSnapshot(request, tasks::add))
                .isInstanceOf(IllegalStateException.class);
        assertThat(tasks).isEmpty();
    }

    @Test
    void testUnrelatedBucketNotificationDoesNotPreventRestoreQuery() throws Exception {
        TableBucket bucket = new TableBucket(DATA1_TABLE_ID_PK, 0);
        TableBucket otherBucket = new TableBucket(DATA1_TABLE_ID_PK, 1);
        makeKvTableAsLeader(DATA1_TABLE_ID_PK, DATA1_TABLE_PATH_PK, 0);
        makeKvTableAsLeader(DATA1_TABLE_ID_PK, DATA1_TABLE_PATH_PK, 1);
        Replica replica = replicaManager.getReplicaOrException(bucket);
        CompletableFuture<Void> notificationEntered = new CompletableFuture<>();
        CompletableFuture<Void> releaseNotification = new CompletableFuture<>();
        ExecutorService notificationExecutor = Executors.newSingleThreadExecutor();
        CompletableFuture<Void> notification =
                CompletableFuture.runAsync(
                        () ->
                                replicaManager.notifyKvSnapshotOffset(
                                        new NotifyKvSnapshotOffsetData(
                                                otherBucket,
                                                0L,
                                                replicaManager.getCoordinatorEpoch()),
                                        ignored -> {
                                            notificationEntered.complete(null);
                                            releaseNotification.join();
                                        }),
                        notificationExecutor);
        try {
            notificationEntered.get();
            List<NotifyLeaderAndIsrResultForBucket> results =
                    replicaManager.restoreImportedSnapshots(
                            replicaManager.getCoordinatorEpoch(),
                            Collections.singletonList(request(replica, 7L)));
            assertThat(results)
                    .singleElement()
                    .satisfies(
                            result ->
                                    assertThat(result.getErrorMessage())
                                            .isEqualTo(
                                                    "Imported Snapshot restoration is in progress."));
        } finally {
            releaseNotification.complete(null);
            try {
                notification.get();
            } finally {
                notificationExecutor.shutdownNow();
            }
        }
    }

    private NotifyLeaderAndIsrData request(Replica replica, long snapshot) {
        return new NotifyLeaderAndIsrData(
                PhysicalTablePath.of(DATA1_TABLE_PATH_PK),
                replica.getTableBucket(),
                Collections.singletonList(TABLET_SERVER_ID),
                new LeaderAndIsr(
                        TABLET_SERVER_ID,
                        replica.getLeaderEpoch(),
                        Collections.singletonList(TABLET_SERVER_ID),
                        Collections.emptyList(),
                        replica.getCoordinatorEpoch(),
                        replica.getBucketEpoch()),
                3,
                0L,
                snapshot);
    }
}
