/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
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
import com.alibaba.fluss.utils.MapUtils;

import java.time.Duration;
import java.util.Deque;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.LinkedBlockingDeque;

import static com.alibaba.fluss.testutils.common.CommonTestUtils.waitValue;

/**
 * An implementation of {@link CompletedKvSnapshotCommitter} for testing purpose which will stored
 * all reported snapshots in memory.
 */
public class TestingCompletedKvSnapshotCommitter implements CompletedKvSnapshotCommitter {

    protected final Map<TableBucket, Deque<CompletedSnapshot>> snapshots =
            MapUtils.newConcurrentHashMap();
    protected final Map<TableBucket, Map<Long, Integer>> bucketSnapshotLeaderEpoch =
            new HashMap<>();

    @Override
    public void commitKvSnapshot(
            CompletedSnapshot snapshot, int coordinatorEpoch, int bucketLeaderEpoch) {
        snapshots
                .computeIfAbsent(snapshot.getTableBucket(), k -> new LinkedBlockingDeque<>())
                .add(snapshot);
        bucketSnapshotLeaderEpoch
                .computeIfAbsent(snapshot.getTableBucket(), k -> new HashMap<>())
                .put(snapshot.getSnapshotID(), bucketLeaderEpoch);
    }

    public CompletedSnapshot waitUtilSnapshotComplete(
            TableBucket tableBucket, int snapshotIdToWait) {
        return waitValue(
                () -> {
                    CompletedSnapshot completedSnapshot = getLatestCompletedSnapshot(tableBucket);
                    if (completedSnapshot != null
                            && completedSnapshot.getSnapshotID() >= snapshotIdToWait) {
                        return Optional.of(completedSnapshot);
                    }
                    return Optional.empty();
                },
                Duration.ofMinutes(2),
                "Fail to wait for snapshot " + snapshotIdToWait + " finish.");
    }

    public CompletedSnapshot getLatestCompletedSnapshot(TableBucket tableBucket) {
        Deque<CompletedSnapshot> bucketSnapshots = snapshots.get(tableBucket);
        if (bucketSnapshots != null) {
            return bucketSnapshots.peekLast();
        }
        return null;
    }

    public int getSnapshotLeaderEpoch(TableBucket tableBucket, long snapshotId) {
        Map<Long, Integer> bucketSnapshotLeaderEpochMap =
                bucketSnapshotLeaderEpoch.get(tableBucket);
        if (bucketSnapshotLeaderEpochMap != null) {
            Integer leaderEpoch = bucketSnapshotLeaderEpochMap.get(snapshotId);
            if (leaderEpoch != null) {
                return leaderEpoch;
            }
        }
        return -1;
    }
}
