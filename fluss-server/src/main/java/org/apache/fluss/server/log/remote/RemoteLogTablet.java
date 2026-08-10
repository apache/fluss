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
import org.apache.fluss.remote.RemoteLogManifest;
import org.apache.fluss.remote.RemoteLogSegment;
import org.apache.fluss.server.metrics.group.BucketMetricGroup;

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

    /**
     * It contains all the segment-id to {@link RemoteLogSegment} mappings which did not delete in
     * remote storage.
     */
    private final Map<UUID, RemoteLogSegment> idToRemoteLogSegment = new HashMap<>();

    /** It contains logical start offset to segment ids mapping for the current manifest view. */
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
                    metricGroup.gauge(
                            MetricNames.LOG_START_OFFSET,
                            () -> {
                                if (remoteLogStartOffset == INIT_REMOTE_LOG_START_OFFSET) {
                                    return -1L;
                                }
                                return remoteLogStartOffset;
                            });
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
        // lock-free, the currentManifest is volatile and the list is immutable.
        return currentManifest.getRemoteLogSegmentList();
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
     * Returns the remote log segment candidates for a timestamp lookup, ordered by logical start
     * offset.
     *
     * <p>A segment whose logical end is clipped retains the max timestamp of its complete physical
     * file. That timestamp may belong to the hidden suffix, so the next timestamp entry must also
     * be considered. Candidate collection stops after reaching an entry containing an unclipped
     * segment because its max timestamp is authoritative for its complete logical range.
     */
    public List<RemoteLogSegment> findSegmentsByTimestamp(long timestamp) {
        return inReadLock(
                lock,
                () -> {
                    List<RemoteLogSegment> candidates = new ArrayList<>();
                    for (Map.Entry<Long, Set<UUID>> entry :
                            timestampToRemoteLogSegmentId.tailMap(timestamp, true).entrySet()) {
                        boolean containsUnclippedSegment = false;
                        for (UUID id : entry.getValue()) {
                            RemoteLogSegment segment = idToRemoteLogSegment.get(id);
                            candidates.add(segment);
                            if (!segment.isEndOffsetClipped()) {
                                containsUnclippedSegment = true;
                            }
                        }
                        if (containsUnclippedSegment) {
                            break;
                        }
                    }
                    candidates.sort(
                            (left, right) ->
                                    Long.compare(
                                            left.logicalStartOffset(), right.logicalStartOffset()));
                    return candidates;
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
                    Map.Entry<Long, UUID> floorEntry =
                            offsetToRemoteLogSegmentId.floorEntry(offset);
                    if (floorEntry == null) {
                        return Collections.emptyList();
                    }
                    RemoteLogSegment floorSegment = idToRemoteLogSegment.get(floorEntry.getValue());
                    if (offset >= floorSegment.logicalEndOffset()) {
                        return Collections.emptyList();
                    }
                    Collection<UUID> segmentIds =
                            offsetToRemoteLogSegmentId.tailMap(floorEntry.getKey(), true).values();
                    List<RemoteLogSegment> remoteLogSegmentList = new ArrayList<>();
                    for (UUID id : segmentIds) {
                        RemoteLogSegment remoteLogSegment = idToRemoteLogSegment.get(id);
                        if (offset < remoteLogSegment.logicalEndOffset()) {
                            remoteLogSegmentList.add(remoteLogSegment);
                        }
                    }
                    return remoteLogSegmentList;
                });
    }

    /**
     * Returns the maximal physically contiguous prefix safe for one FetchLog v0 response. A
     * physical overlap is resumed by the client's next fetch from its advanced offset.
     */
    public List<RemoteLogSegment> relevantRemoteLogSegmentsForFetchV0(long offset) {
        List<RemoteLogSegment> relevantSegments = relevantRemoteLogSegments(offset);
        if (relevantSegments.size() <= 1) {
            return relevantSegments;
        }
        List<RemoteLogSegment> contiguousPrefix = new ArrayList<>();
        long previousPhysicalEndOffset = -1L;
        for (RemoteLogSegment segment : relevantSegments) {
            if (!contiguousPrefix.isEmpty()
                    && segment.remoteLogStartOffset() != previousPhysicalEndOffset) {
                break;
            }
            contiguousPrefix.add(segment);
            previousPhysicalEndOffset = segment.remoteLogEndOffset();
        }
        return contiguousPrefix;
    }

    public long getRemoteLogStartOffset() {
        return remoteLogStartOffset;
    }

    public OptionalLong getRemoteLogEndOffset() {
        return remoteLogEndOffset == -1L
                ? OptionalLong.empty()
                : OptionalLong.of(remoteLogEndOffset);
    }

    /** Returns the highest exclusive offset successfully copied to remote storage. */
    public long getHighestCopiedEndOffset() {
        // lock-free, the currentManifest is volatile and the offset is immutable.
        return currentManifest.getHighestCopiedEndOffset();
    }

    /**
     * Gets the snapshot of current remote log segment manifest. The snapshot including the exists
     * remoteLogSegment already committed.
     */
    public RemoteLogManifest currentManifest() {
        // lock-free, the currentManifest is volatile
        return currentManifest;
    }

    public void loadRemoteLogManifest(RemoteLogManifest manifestSnapshot) {
        inWriteLock(
                lock,
                () -> {
                    reset();
                    for (RemoteLogSegment segment : manifestSnapshot.getRemoteLogSegmentList()) {
                        addSegment(segment);
                    }
                    remoteSizeInBytes = manifestSnapshot.getRemoteLogSize();
                    numRemoteLogSegments = manifestSnapshot.getRemoteLogSegmentList().size();
                    remoteLogStartOffset = manifestSnapshot.getRemoteLogStartOffset();
                    remoteLogEndOffset = manifestSnapshot.getRemoteLogEndOffset();
                    currentManifest = manifestSnapshot;
                });
    }

    private void addSegment(RemoteLogSegment remoteLogSegment) {
        UUID remoteLogSegmentId = remoteLogSegment.remoteLogSegmentId();
        RemoteLogSegment previous = idToRemoteLogSegment.put(remoteLogSegmentId, remoteLogSegment);
        if (previous != null) {
            offsetToRemoteLogSegmentId.remove(previous.logicalStartOffset());
        }
        offsetToRemoteLogSegmentId.put(remoteLogSegment.logicalStartOffset(), remoteLogSegmentId);
        timestampToRemoteLogSegmentId
                .computeIfAbsent(remoteLogSegment.maxTimestamp(), k -> new HashSet<>())
                .add(remoteLogSegmentId);
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
