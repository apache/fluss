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

package org.apache.fluss.server.log.remote;

import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metrics.MetricNames;
import org.apache.fluss.metrics.groups.MetricGroup;
import org.apache.fluss.remote.RemoteLogSegment;
import org.apache.fluss.server.metrics.group.BucketMetricGroup;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.OptionalLong;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.apache.fluss.utils.concurrent.LockUtils.inReadLock;
import static org.apache.fluss.utils.concurrent.LockUtils.inWriteLock;

/** This class provides an in-memory cache of remote log manifest for each table bucket . */
@ThreadSafe
public class RemoteLogTablet {
    private static final long INIT_REMOTE_LOG_START_OFFSET = Long.MAX_VALUE;
    private static final long INIT_REMOTE_LOG_END_OFFSET = -1L;

    private final TableBucket tableBucket;

    private final PhysicalTablePath physicalTablePath;

    /**
     * It contains all the segment-id to {@link RemoteLogSegment} mappings which did not delete in
     * remote storage.
     */
    private final Map<UUID, RemoteLogSegment> idToRemoteLogSegment = new HashMap<>();

    /**
     * It contains segment remote log start offset to segment ids mapping which the segment did not
     * delete in remote storage.
     */
    private final NavigableMap<Long, UUID> offsetToRemoteLogSegmentId = new TreeMap<>();

    /**
     * It contains max timestamp to segment ids mapping which the segment did not delete in remote
     * storage. This can be used to find offset of the segment whose max timestamp is equal to this.
     * It maps to a set of segment ids because multiple segments can have the same timestamp.
     */
    private final NavigableMap<Long, Set<UUID>> timestampToRemoteLogSegmentId = new TreeMap<>();

    /** The lock to protect the remote log segment list. */
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    private final long ttlMs;

    /** The registered metrics for remote log. */
    private volatile MetricGroup remoteLogMetrics;

    private volatile RemoteLogManifest currentManifest;

    private volatile long remoteSizeInBytes;

    private volatile int numRemoteLogSegments;

    /**
     * It represents the remote log start offset of the segments that have copied to remote storage.
     */
    private volatile long remoteLogStartOffset;

    /**
     * It represents the remote log end offset of the segments that have copied to remote storage.
     */
    private volatile long remoteLogEndOffset;

    private volatile boolean closed = false;

    public RemoteLogTablet(
            PhysicalTablePath physicalTablePath, TableBucket tableBucket, long ttlMs) {
        this.tableBucket = tableBucket;
        this.physicalTablePath = physicalTablePath;
        this.ttlMs = ttlMs;
        this.currentManifest =
                new RemoteLogManifest(physicalTablePath, tableBucket, new ArrayList<>());
        reset();
    }

    public void registerMetrics(BucketMetricGroup bucketMetricGroup) {
        inWriteLock(
                lock,
                () -> {
                    if (remoteLogMetrics != null) {
                        remoteLogMetrics.close();
                        remoteLogMetrics = null;
                    }
                    MetricGroup metricGroup = bucketMetricGroup.addGroup("remoteLog");
                    metricGroup.gauge(MetricNames.LOG_NUM_SEGMENTS, () -> numRemoteLogSegments);
                    metricGroup.gauge(MetricNames.LOG_END_OFFSET, () -> remoteLogEndOffset);
                    metricGroup.gauge(MetricNames.REMOTE_LOG_SIZE, this::getRemoteSizeInBytes);
                    remoteLogMetrics = metricGroup;
                });
    }

    public long getRemoteSizeInBytes() {
        return remoteSizeInBytes;
    }

    public void unregisterMetrics() {
        inWriteLock(
                lock,
                () -> {
                    if (remoteLogMetrics != null) {
                        remoteLogMetrics.close();
                        remoteLogMetrics = null;
                    }
                });
    }

    /** Get all remote log segment metadata. */
    public List<RemoteLogSegment> allRemoteLogSegments() {
        return inReadLock(lock, () -> currentManifest.getRemoteLogSegmentList());
    }

    /**
     * Returns the expired segments based on the given time and lake log end offset.
     *
     * <p>Only segments that have been tiered to lake (i.e., remoteLogEndOffset <= lakeLogEndOffset)
     * can be safely deleted. This ensures that we don't delete segments that haven't been tiered to
     * lake yet.
     *
     * @param currentTimeMs the current time in milliseconds
     * @param lakeLogEndOffset the log end offset that has been synced to lake, null if data lake is
     *     disabled
     * @return list of expired segments that can be safely deleted
     */
    public List<RemoteLogSegment> expiredRemoteLogSegments(
            long currentTimeMs, Long lakeLogEndOffset) {
        if (!logExpireEnable()) {
            return Collections.emptyList();
        }
        return inReadLock(
                lock,
                () -> {
                    List<RemoteLogSegment> expiredSegments = new ArrayList<>();
                    for (Map.Entry<Long, Set<UUID>> entry :
                            timestampToRemoteLogSegmentId.entrySet()) {
                        long ts = entry.getKey();
                        if (currentTimeMs - ts > ttlMs) {
                            for (UUID uuid : entry.getValue()) {
                                RemoteLogSegment segment = idToRemoteLogSegment.get(uuid);
                                if (lakeLogEndOffset != null) {
                                    // if datalake is enabled, only include segments that have been
                                    // tiered to lake.
                                    if (segment.remoteLogEndOffset() <= lakeLogEndOffset) {
                                        expiredSegments.add(segment);
                                    }
                                } else {
                                    expiredSegments.add(segment);
                                }
                            }
                        } else {
                            // no further expired segments since the segments
                            // are sorted by timestamp.
                            break;
                        }
                    }
                    return expiredSegments;
                });
    }

    /**
     * Get the remote log segment by timestamp. The segment is the first segment whose maxTimestamp
     * bigger than or equal to the input timestamp. If there are multiple segments with the same
     * timestamp, the segment with minimum remoteLogStartOffset will be returned.
     */
    public @Nullable RemoteLogSegment findSegmentByTimestamp(long timestamp) {
        return inReadLock(
                lock,
                () -> {
                    Long ceilingKey = timestampToRemoteLogSegmentId.ceilingKey(timestamp);
                    if (ceilingKey != null) {
                        Set<UUID> segmentIds = timestampToRemoteLogSegmentId.get(ceilingKey);
                        RemoteLogSegment segmentWithMinimumOffset = null;
                        long miniOffset = Long.MAX_VALUE;
                        for (UUID id : segmentIds) {
                            RemoteLogSegment remoteLogSegment = idToRemoteLogSegment.get(id);
                            long startOffset = remoteLogSegment.remoteLogStartOffset();
                            if (startOffset < miniOffset) {
                                segmentWithMinimumOffset = remoteLogSegment;
                                miniOffset = startOffset;
                            }
                        }
                        return segmentWithMinimumOffset;
                    } else {
                        return null;
                    }
                });
    }

    /**
     * Get all remote log segments relevant to the input offset, which including these segments
     * whose remote log start offset higher that or equal to this offset, and including another one
     * segment whose remote log start offset smaller than this offset (floor key).
     */
    public List<RemoteLogSegment> relevantRemoteLogSegments(long offset) {
        return inReadLock(
                lock,
                () -> {
                    Long floorKey = offsetToRemoteLogSegmentId.floorKey(offset);
                    Collection<UUID> segmentIds =
                            offsetToRemoteLogSegmentId
                                    .tailMap(floorKey == null ? 0L : floorKey, true)
                                    .values();
                    List<RemoteLogSegment> remoteLogSegmentList = new ArrayList<>();
                    for (UUID id : segmentIds) {
                        RemoteLogSegment remoteLogSegment = idToRemoteLogSegment.get(id);
                        if (offset < remoteLogSegment.remoteLogEndOffset()) {
                            remoteLogSegmentList.add(remoteLogSegment);
                        }
                    }
                    return remoteLogSegmentList;
                });
    }

    public long getRemoteLogStartOffset() {
        return remoteLogStartOffset;
    }

    public OptionalLong getRemoteLogEndOffset() {
        return remoteLogEndOffset == -1L
                ? OptionalLong.empty()
                : OptionalLong.of(remoteLogEndOffset);
    }

    /**
     * Gets the snapshot of current remote log segment manifest. The snapshot including the exists
     * remoteLogSegment already committed.
     */
    public RemoteLogManifest currentManifest() {
        return inReadLock(lock, () -> currentManifest);
    }

    public void loadRemoteLogManifest(RemoteLogManifest manifestSnapshot) {
        inWriteLock(
                lock,
                () -> {
                    reset();
                    addAndDeleteLogSegments(
                            manifestSnapshot.getRemoteLogSegmentList(), Collections.emptyList());
                });
    }

    public void addAndDeleteLogSegments(
            List<RemoteLogSegment> addedSegments, List<RemoteLogSegment> deletedSegments) {
        if (deletedSegments.isEmpty() && addedSegments.isEmpty()) {
            return;
        }
        inWriteLock(
                lock,
                () -> {
                    long newSizeInBytes = remoteSizeInBytes;

                    // put new segments into list
                    for (RemoteLogSegment remoteLogSegment : addedSegments) {
                        UUID remoteLogSegmentId = remoteLogSegment.remoteLogSegmentId();

                        // TODO maybe need to check the leader epoch.

                        idToRemoteLogSegment.put(remoteLogSegmentId, remoteLogSegment);
                        offsetToRemoteLogSegmentId.put(
                                remoteLogSegment.remoteLogStartOffset(), remoteLogSegmentId);
                        timestampToRemoteLogSegmentId
                                .computeIfAbsent(
                                        remoteLogSegment.maxTimestamp(), k -> new HashSet<>())
                                .add(remoteLogSegmentId);

                        // update remote log end offset.
                        if (remoteLogSegment.remoteLogEndOffset() > remoteLogEndOffset) {
                            remoteLogEndOffset = remoteLogSegment.remoteLogEndOffset();
                        }

                        newSizeInBytes += remoteLogSegment.segmentSizeInBytes();
                    }

                    // remove expired segments from list
                    for (RemoteLogSegment remoteLogSegment : deletedSegments) {
                        UUID remoteLogSegmentId = remoteLogSegment.remoteLogSegmentId();

                        // TODO maybe need to check the leader epoch.

                        RemoteLogSegment removeSegment =
                                idToRemoteLogSegment.remove(remoteLogSegmentId);
                        offsetToRemoteLogSegmentId.remove(remoteLogSegment.remoteLogStartOffset());

                        // remove k,v mapping if the set is empty.
                        timestampToRemoteLogSegmentId.compute(
                                remoteLogSegment.maxTimestamp(),
                                (k, v) -> {
                                    if (v != null) {
                                        v.remove(remoteLogSegmentId);
                                        if (v.isEmpty()) {
                                            return null;
                                        }
                                    }
                                    return v;
                                });
                        if (removeSegment != null) {
                            newSizeInBytes -= removeSegment.segmentSizeInBytes();
                        }
                    }

                    remoteSizeInBytes = newSizeInBytes;
                    numRemoteLogSegments = idToRemoteLogSegment.size();

                    if (numRemoteLogSegments == 0) {
                        // reset to default values if no segments exist after expiration.
                        reset();
                    } else {
                        remoteLogStartOffset = offsetToRemoteLogSegmentId.firstKey();
                    }

                    currentManifest =
                            new RemoteLogManifest(
                                    physicalTablePath,
                                    tableBucket,
                                    new ArrayList<>(idToRemoteLogSegment.values()));
                });
    }

    private boolean logExpireEnable() {
        return ttlMs > 0;
    }

    private void reset() {
        idToRemoteLogSegment.clear();
        offsetToRemoteLogSegmentId.clear();
        timestampToRemoteLogSegmentId.clear();
        remoteSizeInBytes = 0L;
        numRemoteLogSegments = 0;
        remoteLogStartOffset = INIT_REMOTE_LOG_START_OFFSET;
        remoteLogEndOffset = INIT_REMOTE_LOG_END_OFFSET;
    }

    public void close() {
        if (!closed) {
            inWriteLock(
                    lock,
                    () -> {
                        if (!closed) {
                            reset();
                            remoteLogMetrics.close();
                            closed = true;
                        }
                    });
        }
    }

    @VisibleForTesting
    Map<UUID, RemoteLogSegment> getIdToRemoteLogSegmentMap() {
        return idToRemoteLogSegment;
    }
}
