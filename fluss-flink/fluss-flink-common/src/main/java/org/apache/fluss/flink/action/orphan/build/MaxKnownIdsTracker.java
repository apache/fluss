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

package org.apache.fluss.flink.action.orphan.build;

import org.apache.fluss.annotation.Internal;

/**
 * Accumulates {@code maxKnownTableId} and {@code maxKnownPartitionId} during a single cleanup run.
 *
 * <p>Values are updated from the successful scope-enumeration metadata lookups that already
 * materialize concrete ids for cleanup orchestration: {@code getTableInfo()} for tables and {@code
 * listPartitionInfos()} for partitions. The tracker is therefore pure RPC-derived and never sourced
 * from FS dir-name parsing.
 *
 * <p>The tracked maximums serve as <b>ID guards</b> for orphan directory detection: only
 * directories whose parsed ID is {@code <=} the observed maximum can be classified as orphan
 * candidates. Directories with higher IDs are conservatively skipped as potentially freshly
 * allocated. Because RPC failures cause the tracker to observe fewer IDs, the maximums are always a
 * lower bound of the true cluster-wide maximum — making the guard strictly more conservative (safe
 * direction) under partial failures.
 */
@Internal
public final class MaxKnownIdsTracker {

    private long maxKnownTableId = -1L;
    private long maxKnownPartitionId = -1L;

    public void observeTableId(long tableId) {
        if (tableId > maxKnownTableId) {
            maxKnownTableId = tableId;
        }
    }

    public void observePartitionId(long partitionId) {
        if (partitionId > maxKnownPartitionId) {
            maxKnownPartitionId = partitionId;
        }
    }

    public long maxKnownTableId() {
        return maxKnownTableId;
    }

    public long maxKnownPartitionId() {
        return maxKnownPartitionId;
    }
}
