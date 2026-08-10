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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * A remote log manifest is an immutable list of current {@link RemoteLogSegment} which can
 * represent a snapshot of a remote log tablet.
 */
public class RemoteLogManifest {
    private final PhysicalTablePath physicalTablePath;
    private final TableBucket tableBucket;
    private final List<RemoteLogSegment> remoteLogSegmentList;
    private final long highestCopiedEndOffset;

    public RemoteLogManifest(
            PhysicalTablePath physicalTablePath,
            TableBucket tableBucket,
            List<RemoteLogSegment> remoteLogSegmentList) {
        this(
                physicalTablePath,
                tableBucket,
                remoteLogSegmentList,
                maxPhysicalEndOffset(remoteLogSegmentList));
    }

    public RemoteLogManifest(
            PhysicalTablePath physicalTablePath,
            TableBucket tableBucket,
            List<RemoteLogSegment> remoteLogSegmentList,
            long highestCopiedEndOffset) {
        this.physicalTablePath = physicalTablePath;
        this.tableBucket = tableBucket;
        this.remoteLogSegmentList = Collections.unmodifiableList(remoteLogSegmentList);
        this.highestCopiedEndOffset = highestCopiedEndOffset;

        // sanity check
        for (RemoteLogSegment remoteLogSegment : remoteLogSegmentList) {
            if (!remoteLogSegment.physicalTablePath().equals(physicalTablePath)) {
                throw new IllegalArgumentException(
                        "RemoteLogSegment's tablePath should be the same as the tablePath of RemoteLogManifestSnapshot");
            }
            if (!remoteLogSegment.tableBucket().equals(tableBucket)) {
                throw new IllegalArgumentException(
                        "RemoteLogSegment's tableBucket should be the same as the tableBucket of RemoteLogManifestSnapshot");
            }
        }
        if (highestCopiedEndOffset < maxPhysicalEndOffset(remoteLogSegmentList)) {
            throw new IllegalArgumentException(
                    "Highest copied end offset must cover every persisted remote segment");
        }
        if (highestCopiedEndOffset < -1L) {
            throw new IllegalArgumentException(
                    "Highest copied end offset must be -1 or non-negative");
        }
    }

    public RemoteLogManifest trimAndMerge(
            List<RemoteLogSegment> deletedSegments, List<RemoteLogSegment> addedSegments) {
        Set<UUID> deletedIds =
                deletedSegments.stream()
                        .map(RemoteLogSegment::remoteLogSegmentId)
                        .collect(Collectors.toSet());
        List<RemoteLogSegment> newSegments = new ArrayList<>(remoteLogSegmentList.size());
        for (RemoteLogSegment segment : remoteLogSegmentList) {
            if (!deletedIds.contains(segment.remoteLogSegmentId())) {
                newSegments.add(segment);
            }
        }
        newSegments.sort(Comparator.comparingLong(RemoteLogSegment::logicalStartOffset));

        List<RemoteLogSegment> sortedAddedSegments = new ArrayList<>(addedSegments);
        sortedAddedSegments.sort(Comparator.comparingLong(RemoteLogSegment::remoteLogStartOffset));
        long newHighestCopiedEndOffset = highestCopiedEndOffset;
        for (RemoteLogSegment addedSegment : sortedAddedSegments) {
            newHighestCopiedEndOffset =
                    Math.max(newHighestCopiedEndOffset, addedSegment.remoteLogEndOffset());
            if (newSegments.isEmpty()) {
                newSegments.add(addedSegment);
                continue;
            }

            long currentStartOffset = newSegments.get(0).logicalStartOffset();
            long currentEndOffset = newSegments.get(newSegments.size() - 1).logicalEndOffset();
            if (addedSegment.remoteLogEndOffset() <= currentEndOffset) {
                continue;
            }
            if (addedSegment.remoteLogStartOffset() > currentEndOffset) {
                throw new IllegalArgumentException(
                        String.format(
                                "Remote log segment [%s, %s) introduces a gap after logical end %s",
                                addedSegment.remoteLogStartOffset(),
                                addedSegment.remoteLogEndOffset(),
                                currentEndOffset));
            }

            long insertionOffset =
                    Math.max(addedSegment.remoteLogStartOffset(), currentStartOffset);
            List<RemoteLogSegment> mergedSegments = new ArrayList<>();
            for (RemoteLogSegment currentSegment : newSegments) {
                if (currentSegment.logicalEndOffset() <= insertionOffset) {
                    mergedSegments.add(currentSegment);
                } else if (currentSegment.logicalStartOffset() < insertionOffset) {
                    mergedSegments.add(
                            currentSegment.withLogicalRange(
                                    currentSegment.logicalStartOffset(), insertionOffset));
                }
            }
            mergedSegments.add(
                    addedSegment.withLogicalRange(
                            insertionOffset, addedSegment.remoteLogEndOffset()));
            newSegments = mergedSegments;
        }

        return new RemoteLogManifest(
                physicalTablePath, tableBucket, newSegments, newHighestCopiedEndOffset);
    }

    public long getRemoteLogStartOffset() {
        long startOffset = Long.MAX_VALUE;
        for (RemoteLogSegment remoteLogSegment : remoteLogSegmentList) {
            if (remoteLogSegment.remoteLogStartOffset() < startOffset) {
                startOffset = remoteLogSegment.remoteLogStartOffset();
            }
        }
        return startOffset;
    }

    public long getRemoteLogEndOffset() {
        long endOffset = -1;
        for (RemoteLogSegment remoteLogSegment : remoteLogSegmentList) {
            if (endOffset == -1 || remoteLogSegment.remoteLogEndOffset() > endOffset) {
                endOffset = remoteLogSegment.remoteLogEndOffset();
            }
        }
        return endOffset;
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

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RemoteLogManifest that = (RemoteLogManifest) o;
        return highestCopiedEndOffset == that.highestCopiedEndOffset
                && Objects.equals(remoteLogSegmentList, that.remoteLogSegmentList);
    }

    @Override
    public int hashCode() {
        return Objects.hash(remoteLogSegmentList, highestCopiedEndOffset);
    }

    @Override
    public String toString() {
        return "RemoteLogManifestSnapshot{"
                + "remoteLogSegmentList="
                + remoteLogSegmentList
                + ", highestCopiedEndOffset="
                + highestCopiedEndOffset
                + '}';
    }

    private static long maxPhysicalEndOffset(List<RemoteLogSegment> segments) {
        long maxEndOffset = -1L;
        for (RemoteLogSegment segment : segments) {
            maxEndOffset = Math.max(maxEndOffset, segment.remoteLogEndOffset());
        }
        return maxEndOffset;
    }
}
