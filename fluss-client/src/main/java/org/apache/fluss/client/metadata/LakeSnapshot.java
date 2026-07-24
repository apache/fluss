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

package org.apache.fluss.client.metadata;

import org.apache.fluss.annotation.PublicEvolving;
import org.apache.fluss.lake.committer.LakeTieringTableState;
import org.apache.fluss.metadata.TableBucket;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

/**
 * A class representing the lake snapshot information of a table. It contains:
 * <li>The snapshot id and the log offset for each bucket.
 * <li>The opaque table-level tiering state (see {@link LakeTieringTableState}) for partitioned
 *     tables, exposed parsed via {@link #getLakeTieringTableState()} or raw via {@link
 *     #getRawTieringStateJson()}. It is {@code null} for non-partitioned tables or when talking to
 *     an old coordinator that does not report it.
 *
 * @since 0.3
 */
@PublicEvolving
public class LakeSnapshot {

    private final long snapshotId;

    // the specific log offset of the snapshot
    private final Map<TableBucket, Long> tableBucketsOffset;

    // opaque tiering-state JSON bytes; null when absent. parsed lazily (see
    // getLakeTieringTableState).
    @Nullable private final byte[] lakeTieringTableStateJson;

    public LakeSnapshot(long snapshotId, Map<TableBucket, Long> tableBucketsOffset) {
        this(snapshotId, tableBucketsOffset, null);
    }

    public LakeSnapshot(
            long snapshotId,
            Map<TableBucket, Long> tableBucketsOffset,
            @Nullable byte[] lakeTieringTableStateJson) {
        this.snapshotId = snapshotId;
        this.tableBucketsOffset = tableBucketsOffset;
        this.lakeTieringTableStateJson = lakeTieringTableStateJson;
    }

    public long getSnapshotId() {
        return snapshotId;
    }

    public Map<TableBucket, Long> getTableBucketsOffset() {
        return Collections.unmodifiableMap(tableBucketsOffset);
    }

    /** Parses and returns the tiering state (lazily), or {@code null} if absent. */
    @Nullable
    public LakeTieringTableState getLakeTieringTableState() {
        return lakeTieringTableStateJson == null
                ? null
                : LakeTieringTableState.fromJsonBytes(lakeTieringTableStateJson);
    }

    /**
     * Returns the raw, unparsed tiering-state JSON bytes ({@code null} if absent), for passing a
     * newer, unreadable state through unchanged.
     */
    @Nullable
    public byte[] getRawTieringStateJson() {
        return lakeTieringTableStateJson;
    }

    @Override
    public String toString() {
        return "LakeSnapshot{"
                + "snapshotId="
                + snapshotId
                + ", tableBucketsOffset="
                + tableBucketsOffset
                + ", lakeTieringTableStateJson="
                + Arrays.toString(lakeTieringTableStateJson)
                + '}';
    }
}
