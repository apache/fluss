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

package org.apache.fluss.lake.paimon.tiering;

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
public class MarkDoneState {

    private final boolean initialized;
    // partition name -> last time the tiering service wrote data into the partition
    private final Map<String, Long> pendingPartitions;

    public MarkDoneState(boolean initialized, Map<String, Long> pendingPartitions) {
        this.initialized = initialized;
        this.pendingPartitions = new HashMap<>(pendingPartitions);
    }

    /** Creates an empty state: not initialized, no pending partitions. */
    public static MarkDoneState empty() {
        return new MarkDoneState(false, new HashMap<>());
    }

    public boolean isInitialized() {
        return initialized;
    }

    public Map<String, Long> getPendingPartitions() {
        return Collections.unmodifiableMap(pendingPartitions);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        MarkDoneState that = (MarkDoneState) o;
        return initialized == that.initialized
                && Objects.equals(pendingPartitions, that.pendingPartitions);
    }

    @Override
    public int hashCode() {
        return Objects.hash(initialized, pendingPartitions);
    }

    @Override
    public String toString() {
        return "MarkDoneState{"
                + "initialized="
                + initialized
                + ", pendingPartitions="
                + pendingPartitions
                + '}';
    }
}
