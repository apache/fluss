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

package com.alibaba.fluss.server.log.remote;

import com.alibaba.fluss.annotation.VisibleForTesting;
import com.alibaba.fluss.metadata.PhysicalTablePath;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.remote.RemoteLogSegment;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * A remote log manifest is an immutable list of current {@link RemoteLogSegment} which can
 * represent a snapshot of {@link RemoteLogTablet}.
 */
public class RemoteLogManifest {
    private final PhysicalTablePath physicalTablePath;
    private final TableBucket tableBucket;
    private final List<RemoteLogSegment> remoteLogSegmentList;

    public RemoteLogManifest(
            PhysicalTablePath physicalTablePath,
            TableBucket tableBucket,
            List<RemoteLogSegment> remoteLogSegmentList) {
        this.physicalTablePath = physicalTablePath;
        this.tableBucket = tableBucket;
        this.remoteLogSegmentList = Collections.unmodifiableList(remoteLogSegmentList);

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
        return new RemoteLogManifest(physicalTablePath, tableBucket, newSegments);
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

    @VisibleForTesting
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
        return Objects.equals(remoteLogSegmentList, that.remoteLogSegmentList);
    }

    @Override
    public int hashCode() {
        return Objects.hash(remoteLogSegmentList);
    }

    @Override
    public String toString() {
        return "RemoteLogManifestSnapshot{" + "remoteLogSegmentList=" + remoteLogSegmentList + '}';
    }
}
