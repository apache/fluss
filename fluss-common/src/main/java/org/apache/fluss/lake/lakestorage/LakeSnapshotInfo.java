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

package org.apache.fluss.lake.lakestorage;

import javax.annotation.Nullable;

/**
 * Represents the metadata information of a snapshot in a data lake table.
 *
 * @since 0.9
 */
public class LakeSnapshotInfo {

    public static final LakeSnapshotInfo EMPTY = new LakeSnapshotInfo(-1, -1, null);

    private final long snapshotId;

    private final long commitTimestampMillis;

    /**
     * The {@code fluss-offsets} property recorded in the snapshot summary.
     *
     * <p>This property has two different formats depending on the Fluss version that produced the
     * snapshot:
     *
     * <ul>
     *   <li><b>v1 (JSON format, produced by Fluss 0.8):</b> A JSON string starting with <code>
     *       '&#123;'</code> that contains the serialized {@code TableBucketOffsets} data directly.
     *   <li><b>v2 (Path format, produced by Fluss 0.9+):</b> A file path pointing to the offsets
     *       file, following the pattern: {@code
     *       {remote.data.dir}/lake/{databaseName}/{tableName}-{tableId}/metadata/{UUID}.offsets}
     * </ul>
     */
    @Nullable private final String flussOffsetsProperty;

    public LakeSnapshotInfo(
            long snapshotId, long commitTimestampMillis, @Nullable String flussOffsetsProperty) {
        this.snapshotId = snapshotId;
        this.commitTimestampMillis = commitTimestampMillis;
        this.flussOffsetsProperty = flussOffsetsProperty;
    }

    public long getSnapshotId() {
        return snapshotId;
    }

    public long getCommitTimestampMillis() {
        return commitTimestampMillis;
    }

    @Nullable
    public String getFlussOffsetsProperty() {
        return flussOffsetsProperty;
    }
}
