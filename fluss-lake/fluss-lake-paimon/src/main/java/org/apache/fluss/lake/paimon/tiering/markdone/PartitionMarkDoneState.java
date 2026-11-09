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

package org.apache.fluss.lake.paimon.tiering.markdone;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * The partition mark-done state persisted as JSON in the lake snapshot properties committed by the
 * tiering service. It only keeps a table-level {@code initialized} cold-start flag and the pending
 * (not yet done) partitions mapped to their last update time. The done fact itself is not stored:
 * done partitions are removed (done-is-delete), it lives in the lake via the idempotent mark-done
 * actions.
 */
public class PartitionMarkDoneState {

    // True once historical lake partitions have been backfilled; false retries the backfill.
    // This does not indicate that all tracked partitions are done.
    private final boolean initialized;
    // Partition name -> last update time in epoch milliseconds.
    private final Map<String, Long> trackedPartitionLastUpdateTimes;

    /** Creates a state with a defensive copy of the tracked partition timestamps. */
    public PartitionMarkDoneState(
            boolean initialized, Map<String, Long> trackedPartitionLastUpdateTimes) {
        this.initialized = initialized;
        this.trackedPartitionLastUpdateTimes = new HashMap<>(trackedPartitionLastUpdateTimes);
    }

    /** Creates an empty state: not initialized, no pending partitions. */
    public static PartitionMarkDoneState empty() {
        return new PartitionMarkDoneState(false, new HashMap<>());
    }

    /** Whether historical lake partition backfill has completed. */
    public boolean isInitialized() {
        return initialized;
    }

    /** Returns tracked partitions and their last update times in epoch milliseconds. */
    public Map<String, Long> getTrackedPartitionLastUpdateTimes() {
        return Collections.unmodifiableMap(trackedPartitionLastUpdateTimes);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PartitionMarkDoneState that = (PartitionMarkDoneState) o;
        return initialized == that.initialized
                && Objects.equals(
                        trackedPartitionLastUpdateTimes, that.trackedPartitionLastUpdateTimes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(initialized, trackedPartitionLastUpdateTimes);
    }

    @Override
    public String toString() {
        return "PartitionMarkDoneState{"
                + "initialized="
                + initialized
                + ", trackedPartitionLastUpdateTimes="
                + trackedPartitionLastUpdateTimes
                + '}';
    }
}
