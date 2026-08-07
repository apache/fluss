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

package org.apache.fluss.remote;

import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.TableBucket;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * A remote log manifest is an immutable list of current {@link RemoteLogSegment} which represents a
 * snapshot of a remote log tablet.
 *
 * <p>Every segment uses a half-open physical offset range {@code [startOffset, endOffset)}.
 */
public final class RemoteLogManifest {
    public static final int VERSION_1 = 1;
    public static final int VERSION_2 = 2;

    private final int version;
    private final long generation;
    private final PhysicalTablePath physicalTablePath;
    private final TableBucket tableBucket;
    private final List<RemoteLogSegment> remoteLogSegmentList;
    private final @Nullable Long persistedRemoteLogStartOffset;
    private final long highestCopiedEndOffset;
    private final List<UnreferencedRemoteLogSegment> unreferencedRemoteLogSegments;
    private final List<RemoteLogSegmentReference> activeReferences;
    private final long logicalRemoteLogStartOffset;
    private final long logicalRemoteLogEndOffset;

    public RemoteLogManifest(
            PhysicalTablePath physicalTablePath,
            TableBucket tableBucket,
            List<RemoteLogSegment> remoteLogSegmentList) {
        this(
                VERSION_1,
                0L,
                physicalTablePath,
                tableBucket,
                remoteLogSegmentList,
                null,
                maxSegmentEndOffset(remoteLogSegmentList),
                Collections.emptyList());
    }

    private RemoteLogManifest(
            int version,
            long generation,
            PhysicalTablePath physicalTablePath,
            TableBucket tableBucket,
            List<RemoteLogSegment> remoteLogSegmentList,
            @Nullable Long remoteLogStartOffset,
            long highestCopiedEndOffset,
            List<UnreferencedRemoteLogSegment> unreferencedRemoteLogSegments) {
        this.version = version;
        this.generation = generation;
        this.physicalTablePath = physicalTablePath;
        this.tableBucket = tableBucket;
        this.remoteLogSegmentList =
                Collections.unmodifiableList(new ArrayList<>(remoteLogSegmentList));
        this.persistedRemoteLogStartOffset = remoteLogStartOffset;
        this.highestCopiedEndOffset = highestCopiedEndOffset;
        this.unreferencedRemoteLogSegments =
                Collections.unmodifiableList(new ArrayList<>(unreferencedRemoteLogSegments));

        validateSegmentIdentities();
        if (version == VERSION_2) {
            validateV2();
        }
        this.activeReferences =
                Collections.unmodifiableList(buildActiveReferences(remoteLogStartOffset));
        this.logicalRemoteLogStartOffset = calculateLogicalStartOffset(activeReferences);
        this.logicalRemoteLogEndOffset = calculateLogicalEndOffset(activeReferences);
    }

    public static RemoteLogManifest createV2(
            long generation,
            PhysicalTablePath physicalTablePath,
            TableBucket tableBucket,
            List<RemoteLogSegment> remoteLogSegmentList,
            @Nullable Long remoteLogStartOffset,
            long highestCopiedEndOffset,
            List<UnreferencedRemoteLogSegment> unreferencedRemoteLogSegments) {
        return new RemoteLogManifest(
                VERSION_2,
                generation,
                physicalTablePath,
                tableBucket,
                remoteLogSegmentList,
                remoteLogStartOffset,
                highestCopiedEndOffset,
                unreferencedRemoteLogSegments);
    }

    private void validateSegmentIdentities() {
        for (RemoteLogSegment remoteLogSegment : remoteLogSegmentList) {
            validateSegmentIdentity(remoteLogSegment);
        }
        for (UnreferencedRemoteLogSegment unreferencedSegment : unreferencedRemoteLogSegments) {
            validateSegmentIdentity(unreferencedSegment.remoteLogSegment());
        }
    }

    private void validateSegmentIdentity(RemoteLogSegment remoteLogSegment) {
        if (!remoteLogSegment.physicalTablePath().equals(physicalTablePath)) {
            throw new IllegalArgumentException(
                    "RemoteLogSegment's tablePath should be the same as the tablePath of RemoteLogManifestSnapshot");
        }
        if (!remoteLogSegment.tableBucket().equals(tableBucket)) {
            throw new IllegalArgumentException(
                    "RemoteLogSegment's tableBucket should be the same as the tableBucket of RemoteLogManifestSnapshot");
        }
    }

    private void validateV2() {
        if (generation <= 0L) {
            throw new IllegalArgumentException(
                    "V2 manifest generation must be greater than 0: " + generation);
        }
        if (remoteLogSegmentList.isEmpty()) {
            if (persistedRemoteLogStartOffset != null) {
                throw new IllegalArgumentException(
                        "Empty V2 manifest must not define remote log start offset");
            }
        } else {
            if (persistedRemoteLogStartOffset == null) {
                throw new IllegalArgumentException(
                        "Non-empty V2 manifest must define remote log start offset");
            }
            RemoteLogSegment firstSegment = remoteLogSegmentList.get(0);
            long firstLogicalEndOffset =
                    remoteLogSegmentList.size() == 1
                            ? firstSegment.remoteLogEndOffset()
                            : remoteLogSegmentList.get(1).remoteLogStartOffset();
            if (persistedRemoteLogStartOffset < firstSegment.remoteLogStartOffset()
                    || persistedRemoteLogStartOffset >= firstLogicalEndOffset) {
                throw new IllegalArgumentException(
                        "Remote log start offset "
                                + persistedRemoteLogStartOffset
                                + " is outside the first logical range ["
                                + firstSegment.remoteLogStartOffset()
                                + ", "
                                + firstLogicalEndOffset
                                + ")");
            }
        }

        Set<UUID> activeSegmentIds = new HashSet<>();
        RemoteLogSegment previousSegment = null;
        for (RemoteLogSegment segment : remoteLogSegmentList) {
            if (!activeSegmentIds.add(segment.remoteLogSegmentId())) {
                throw new IllegalArgumentException(
                        "Duplicate active remote log segment id: " + segment.remoteLogSegmentId());
            }
            if (previousSegment != null) {
                if (previousSegment.remoteLogStartOffset() >= segment.remoteLogStartOffset()) {
                    throw new IllegalArgumentException(
                            "Active segment start offsets must be strictly increasing");
                }
                if (previousSegment.remoteLogEndOffset() >= segment.remoteLogEndOffset()) {
                    throw new IllegalArgumentException(
                            "Active segment end offsets must be strictly increasing");
                }
                if (previousSegment.remoteLogEndOffset() < segment.remoteLogStartOffset()) {
                    throw new IllegalArgumentException("Active remote log segments contain a gap");
                }
            }
            previousSegment = segment;
        }

        Set<UUID> unreferencedSegmentIds = new HashSet<>();
        for (UnreferencedRemoteLogSegment unreferencedSegment : unreferencedRemoteLogSegments) {
            UUID segmentId = unreferencedSegment.remoteLogSegment().remoteLogSegmentId();
            if (activeSegmentIds.contains(segmentId)) {
                throw new IllegalArgumentException(
                        "Remote log segment is both active and unreferenced: " + segmentId);
            }
            if (!unreferencedSegmentIds.add(segmentId)) {
                throw new IllegalArgumentException(
                        "Duplicate unreferenced remote log segment id: " + segmentId);
            }
        }

        long persistedSegmentEndOffset = maxSegmentEndOffset(remoteLogSegmentList);
        for (UnreferencedRemoteLogSegment segment : unreferencedRemoteLogSegments) {
            persistedSegmentEndOffset =
                    Math.max(
                            persistedSegmentEndOffset,
                            segment.remoteLogSegment().remoteLogEndOffset());
        }
        if (highestCopiedEndOffset < persistedSegmentEndOffset) {
            throw new IllegalArgumentException(
                    "Highest copied end offset "
                            + highestCopiedEndOffset
                            + " is before persisted segment end offset "
                            + persistedSegmentEndOffset);
        }
        if (highestCopiedEndOffset < -1L) {
            throw new IllegalArgumentException(
                    "Highest copied end offset must be -1 or non-negative: "
                            + highestCopiedEndOffset);
        }
    }

    private static long maxSegmentEndOffset(List<RemoteLogSegment> segments) {
        long endOffset = -1L;
        for (RemoteLogSegment segment : segments) {
            endOffset = Math.max(endOffset, segment.remoteLogEndOffset());
        }
        return endOffset;
    }

    private List<RemoteLogSegmentReference> buildActiveReferences(
            @Nullable Long remoteLogStartOffset) {
        if (version == VERSION_1) {
            return buildV1ActiveReferences();
        }
        if (remoteLogSegmentList.isEmpty()) {
            return Collections.emptyList();
        }
        return buildV2ActiveReferences(remoteLogStartOffset);
    }

    private List<RemoteLogSegmentReference> buildV1ActiveReferences() {
        List<RemoteLogSegmentReference> references = new ArrayList<>(remoteLogSegmentList.size());
        for (RemoteLogSegment segment : remoteLogSegmentList) {
            references.add(
                    new RemoteLogSegmentReference(
                            segment, segment.remoteLogStartOffset(), segment.remoteLogEndOffset()));
        }
        return references;
    }

    private List<RemoteLogSegmentReference> buildV2ActiveReferences(long remoteLogStartOffset) {
        List<RemoteLogSegmentReference> references = new ArrayList<>(remoteLogSegmentList.size());
        for (int i = 0; i < remoteLogSegmentList.size(); i++) {
            RemoteLogSegment segment = remoteLogSegmentList.get(i);
            long logicalStartOffset =
                    i == 0 ? remoteLogStartOffset : segment.remoteLogStartOffset();
            long logicalEndOffset =
                    i + 1 < remoteLogSegmentList.size()
                            ? remoteLogSegmentList.get(i + 1).remoteLogStartOffset()
                            : segment.remoteLogEndOffset();
            references.add(
                    new RemoteLogSegmentReference(segment, logicalStartOffset, logicalEndOffset));
        }
        return references;
    }

    private static long calculateLogicalStartOffset(List<RemoteLogSegmentReference> references) {
        long startOffset = Long.MAX_VALUE;
        for (RemoteLogSegmentReference reference : references) {
            startOffset = Math.min(startOffset, reference.logicalStartOffset());
        }
        return startOffset;
    }

    private static long calculateLogicalEndOffset(List<RemoteLogSegmentReference> references) {
        long endOffset = -1L;
        for (RemoteLogSegmentReference reference : references) {
            endOffset = Math.max(endOffset, reference.logicalEndOffset());
        }
        return endOffset;
    }

    public RemoteLogManifest trimAndMerge(
            List<RemoteLogSegment> deletedSegments, List<RemoteLogSegment> addedSegments) {
        Set<UUID> deletedIds =
                deletedSegments.stream()
                        .map(RemoteLogSegment::remoteLogSegmentId)
                        .collect(Collectors.toSet());
        ArrayList<RemoteLogSegment> newSegments = new ArrayList<>(remoteLogSegmentList.size());
        for (RemoteLogSegment segment : remoteLogSegmentList) {
            if (!deletedIds.contains(segment.remoteLogSegmentId())) {
                newSegments.add(segment);
            }
        }
        newSegments.addAll(addedSegments);
        newSegments.sort(Comparator.comparingLong(RemoteLogSegment::remoteLogStartOffset));
        return new RemoteLogManifest(physicalTablePath, tableBucket, newSegments);
    }

    /** Returns the inclusive start offset, or {@link Long#MAX_VALUE} when the manifest is empty. */
    public long getRemoteLogStartOffset() {
        return logicalRemoteLogStartOffset;
    }

    /** Returns the exclusive end offset, or {@code -1} when the manifest is empty. */
    public long getRemoteLogEndOffset() {
        return logicalRemoteLogEndOffset;
    }

    /** Returns the highest exclusive end offset successfully copied to remote storage. */
    public long getHighestCopiedEndOffset() {
        return highestCopiedEndOffset;
    }

    public long getRemoteLogSize() {
        long size = 0;
        for (RemoteLogSegment remoteLogSegment : remoteLogSegmentList) {
            size += remoteLogSegment.segmentSizeInBytes();
        }
        return size;
    }

    public byte[] toJsonBytes() {
        return RemoteLogManifestJsonSerde.toJson(this);
    }

    public static RemoteLogManifest fromJsonBytes(byte[] jsonBytes) {
        return RemoteLogManifestJsonSerde.fromJson(jsonBytes);
    }

    public PhysicalTablePath getPhysicalTablePath() {
        return physicalTablePath;
    }

    public TableBucket getTableBucket() {
        return tableBucket;
    }

    public List<RemoteLogSegment> getRemoteLogSegmentList() {
        return remoteLogSegmentList;
    }

    public int getVersion() {
        return version;
    }

    public long getGeneration() {
        return generation;
    }

    /** Returns the V2 start field persisted in JSON, or null when it is not present. */
    @Nullable
    public Long getPersistedRemoteLogStartOffset() {
        return persistedRemoteLogStartOffset;
    }

    public List<UnreferencedRemoteLogSegment> getUnreferencedRemoteLogSegments() {
        return unreferencedRemoteLogSegments;
    }

    /** Returns the immutable logical view normalized once during Manifest construction. */
    public List<RemoteLogSegmentReference> getRemoteLogSegmentReferences() {
        return activeReferences;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RemoteLogManifest that = (RemoteLogManifest) o;
        return version == that.version
                && generation == that.generation
                && highestCopiedEndOffset == that.highestCopiedEndOffset
                && Objects.equals(physicalTablePath, that.physicalTablePath)
                && Objects.equals(tableBucket, that.tableBucket)
                && Objects.equals(remoteLogSegmentList, that.remoteLogSegmentList)
                && Objects.equals(persistedRemoteLogStartOffset, that.persistedRemoteLogStartOffset)
                && Objects.equals(
                        unreferencedRemoteLogSegments, that.unreferencedRemoteLogSegments);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                version,
                generation,
                physicalTablePath,
                tableBucket,
                remoteLogSegmentList,
                persistedRemoteLogStartOffset,
                highestCopiedEndOffset,
                unreferencedRemoteLogSegments);
    }

    @Override
    public String toString() {
        return "RemoteLogManifest{"
                + "version="
                + version
                + ", generation="
                + generation
                + ", remoteLogSegmentList="
                + remoteLogSegmentList
                + ", persistedRemoteLogStartOffset="
                + persistedRemoteLogStartOffset
                + ", highestCopiedEndOffset="
                + highestCopiedEndOffset
                + ", unreferencedRemoteLogSegments="
                + unreferencedRemoteLogSegments
                + '}';
    }
}
