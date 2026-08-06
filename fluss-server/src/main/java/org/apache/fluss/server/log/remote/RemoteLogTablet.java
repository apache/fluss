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
import org.apache.fluss.remote.RemoteLogSegmentReference;
import org.apache.fluss.server.metrics.group.BucketMetricGroup;
import org.apache.fluss.server.zk.data.VersionedRemoteLogManifestHandle;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import java.util.ArrayList;
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

import static org.apache.fluss.utils.Preconditions.checkArgument;
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

    /** It contains logical start offset to active remote log segment reference mappings. */
    private final NavigableMap<Long, RemoteLogSegmentReference> offsetToReference = new TreeMap<>();

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

    private volatile long orphanManifestCount;

    private volatile RemoteLogManifest currentManifest;

    private volatile @Nullable VersionedRemoteLogManifestHandle currentHandle;

    private volatile long remoteSizeInBytes;

    private volatile int numRemoteLogSegments;

    /** The inclusive start offset of the segments copied to remote storage. */
    private volatile long remoteLogStartOffset;

    /** The exclusive end offset of the segments copied to remote storage. */
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
                    metricGroup.gauge(
                            MetricNames.REMOTE_UNREFERENCED_BYTES,
                            this::getUnreferencedSizeInBytes);
                    metricGroup.gauge(
                            MetricNames.REMOTE_UNREFERENCED_SEGMENT_COUNT,
                            this::getUnreferencedSegmentCount);
                    metricGroup.gauge(
                            MetricNames.REMOTE_ORPHAN_MANIFEST_COUNT, () -> orphanManifestCount);
                    remoteLogMetrics = metricGroup;
                });
    }

    public long getRemoteSizeInBytes() {
        return remoteSizeInBytes;
    }

    /** Returns bytes retained by persisted unreferenced segment metadata. */
    public long getUnreferencedSizeInBytes() {
        return inReadLock(
                lock,
                () ->
                        currentManifest.getUnreferencedRemoteLogSegments().stream()
                                .mapToLong(
                                        segment -> segment.remoteLogSegment().segmentSizeInBytes())
                                .sum());
    }

    /** Returns the number of persisted unreferenced segments waiting for GC. */
    public int getUnreferencedSegmentCount() {
        return inReadLock(lock, () -> currentManifest.getUnreferencedRemoteLogSegments().size());
    }

    /** Updates the number of non-authoritative manifest snapshots seen by the latest sweep. */
    public void updateOrphanManifestCount(long orphanManifestCount) {
        this.orphanManifestCount = orphanManifestCount;
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

    /** Get all active remote log references in logical offset order. */
    public List<RemoteLogSegmentReference> allRemoteLogSegmentReferences() {
        return inReadLock(
                lock,
                () -> Collections.unmodifiableList(new ArrayList<>(offsetToReference.values())));
    }

    /**
     * Returns the expired segments based on the given time and lake log end offset.
     *
     * <p>Both end offsets are exclusive. Only segments that have been fully tiered to lake (i.e.,
     * {@code remoteLogEndOffset <= lakeLogEndOffset}) can be safely deleted. This ensures that we
     * don't delete segments that haven't been tiered to lake yet.
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
        List<RemoteLogSegment> segments = new ArrayList<>();
        for (RemoteLogSegmentReference reference : relevantRemoteLogSegmentReferences(offset)) {
            segments.add(reference.remoteLogSegment());
        }
        return segments;
    }

    /**
     * Returns the maximal physically contiguous segment prefix safe to include in a FetchLog v0
     * response.
     *
     * <p>V1 preserves the legacy behavior and returns the complete relevant tail. For V2, adjacent
     * non-overlapping segments can be consumed by an unchanged client in one response. The prefix
     * stops before the first physical overlap because that segment would be downloaded and then
     * discarded as stale after the preceding segment advances the client offset. The next request
     * resolves the advanced offset against the logical reference index.
     */
    public List<RemoteLogSegment> relevantRemoteLogSegmentsForFetchV0(long offset) {
        return inReadLock(
                lock,
                () -> {
                    List<RemoteLogSegmentReference> references =
                            relevantRemoteLogSegmentReferences(offset);
                    List<RemoteLogSegment> segments = new ArrayList<>();
                    if (currentManifest.getVersion() == RemoteLogManifest.VERSION_1) {
                        for (RemoteLogSegmentReference reference : references) {
                            segments.add(reference.remoteLogSegment());
                        }
                        return segments;
                    }

                    long previousPhysicalEndOffset = -1L;
                    for (RemoteLogSegmentReference reference : references) {
                        RemoteLogSegment segment = reference.remoteLogSegment();
                        if (!segments.isEmpty()
                                && segment.remoteLogStartOffset() != previousPhysicalEndOffset) {
                            break;
                        }
                        segments.add(segment);
                        previousPhysicalEndOffset = segment.remoteLogEndOffset();
                    }
                    return segments;
                });
    }

    /** Returns all active logical references needed for a sequential read from the given offset. */
    public List<RemoteLogSegmentReference> relevantRemoteLogSegmentReferences(long offset) {
        return inReadLock(
                lock,
                () -> {
                    if (offset < remoteLogStartOffset || offset >= remoteLogEndOffset) {
                        return Collections.emptyList();
                    }
                    Map.Entry<Long, RemoteLogSegmentReference> floorEntry =
                            offsetToReference.floorEntry(offset);
                    if (floorEntry == null || offset >= floorEntry.getValue().logicalEndOffset()) {
                        throw new IllegalStateException(
                                "Remote manifest contains no logical reference for offset "
                                        + offset);
                    }
                    return Collections.unmodifiableList(
                            new ArrayList<>(
                                    offsetToReference.tailMap(floorEntry.getKey(), true).values()));
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

    /** Returns the authoritative handle snapshot used to load the current manifest, if any. */
    public @Nullable VersionedRemoteLogManifestHandle currentHandle() {
        return inReadLock(lock, () -> currentHandle);
    }

    public void loadRemoteLogManifest(RemoteLogManifest manifestSnapshot) {
        replaceManifest(manifestSnapshot);
    }

    /** Loads a manifest together with the authoritative handle snapshot that selected it. */
    public void loadRemoteLogManifest(
            RemoteLogManifest manifestSnapshot, VersionedRemoteLogManifestHandle handle) {
        replaceManifest(manifestSnapshot, handle);
    }

    /** Atomically replaces all active indexes with the supplied immutable manifest snapshot. */
    public void replaceManifest(RemoteLogManifest manifest) {
        replaceManifest(manifest, currentHandle);
    }

    /** Atomically replaces all active indexes and their authoritative handle snapshot. */
    public void replaceManifest(
            RemoteLogManifest manifest, @Nullable VersionedRemoteLogManifestHandle handle) {
        checkArgument(
                manifest.getPhysicalTablePath().equals(physicalTablePath),
                "Manifest physical table path does not match remote log tablet");
        checkArgument(
                manifest.getTableBucket().equals(tableBucket),
                "Manifest table bucket does not match remote log tablet");
        inWriteLock(
                lock,
                () -> {
                    Map<UUID, RemoteLogSegment> newIdToSegment = new HashMap<>();
                    NavigableMap<Long, RemoteLogSegmentReference> newOffsetToReference =
                            new TreeMap<>();
                    NavigableMap<Long, Set<UUID>> newTimestampToSegmentIds = new TreeMap<>();
                    long newRemoteSizeInBytes = 0L;

                    for (RemoteLogSegmentReference reference :
                            manifest.getRemoteLogSegmentReferences()) {
                        RemoteLogSegment segment = reference.remoteLogSegment();
                        newIdToSegment.put(segment.remoteLogSegmentId(), segment);
                        newOffsetToReference.put(reference.logicalStartOffset(), reference);
                        newTimestampToSegmentIds
                                .computeIfAbsent(segment.maxTimestamp(), ignored -> new HashSet<>())
                                .add(segment.remoteLogSegmentId());
                        newRemoteSizeInBytes += segment.segmentSizeInBytes();
                    }

                    idToRemoteLogSegment.clear();
                    idToRemoteLogSegment.putAll(newIdToSegment);
                    offsetToReference.clear();
                    offsetToReference.putAll(newOffsetToReference);
                    timestampToRemoteLogSegmentId.clear();
                    timestampToRemoteLogSegmentId.putAll(newTimestampToSegmentIds);
                    remoteSizeInBytes = newRemoteSizeInBytes;
                    numRemoteLogSegments = newIdToSegment.size();
                    remoteLogStartOffset = manifest.getRemoteLogStartOffset();
                    remoteLogEndOffset = manifest.getRemoteLogEndOffset();
                    currentManifest = manifest;
                    currentHandle = handle;
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
                        offsetToReference.put(
                                remoteLogSegment.remoteLogStartOffset(),
                                new RemoteLogSegmentReference(
                                        remoteLogSegment,
                                        remoteLogSegment.remoteLogStartOffset(),
                                        remoteLogSegment.remoteLogEndOffset()));
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
                        offsetToReference.remove(remoteLogSegment.remoteLogStartOffset());

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
                        remoteLogStartOffset = offsetToReference.firstKey();
                    }

                    List<RemoteLogSegment> activeSegments =
                            new ArrayList<>(idToRemoteLogSegment.values());
                    activeSegments.sort(
                            (left, right) ->
                                    Long.compare(
                                            left.remoteLogStartOffset(),
                                            right.remoteLogStartOffset()));
                    currentManifest =
                            new RemoteLogManifest(physicalTablePath, tableBucket, activeSegments);
                });
    }

    private boolean logExpireEnable() {
        return ttlMs > 0;
    }

    private void reset() {
        idToRemoteLogSegment.clear();
        offsetToReference.clear();
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
