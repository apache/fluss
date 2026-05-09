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

package org.apache.fluss.flink.tiering.source.state;

import org.apache.fluss.flink.tiering.source.enumerator.TieringSourceEnumerator;

import javax.annotation.Nullable;

import java.util.Objects;

/**
 * The state of the {@link TieringSourceEnumerator}. Stores the KV snapshot lease id so that it can
 * be recovered after a checkpoint restore, avoiding orphaned leases on the server side.
 */
public class TieringSourceEnumeratorState {

    /**
     * The KV snapshot lease id used by this tiering job. May be null for jobs that were
     * checkpointed before lease support was introduced.
     */
    @Nullable private final String kvSnapshotLeaseId;

    /** Creates a state with no lease id (backward compatible with older checkpoints). */
    public TieringSourceEnumeratorState() {
        this(null);
    }

    public TieringSourceEnumeratorState(@Nullable String kvSnapshotLeaseId) {
        this.kvSnapshotLeaseId = kvSnapshotLeaseId;
    }

    @Nullable
    public String getKvSnapshotLeaseId() {
        return kvSnapshotLeaseId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TieringSourceEnumeratorState that = (TieringSourceEnumeratorState) o;
        return Objects.equals(kvSnapshotLeaseId, that.kvSnapshotLeaseId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(kvSnapshotLeaseId);
    }

    @Override
    public String toString() {
        return "TieringSourceEnumeratorState{" + "kvSnapshotLeaseId='" + kvSnapshotLeaseId + "'}";
    }
}
