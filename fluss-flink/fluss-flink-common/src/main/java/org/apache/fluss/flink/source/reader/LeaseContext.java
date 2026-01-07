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

package org.apache.fluss.flink.source.reader;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Objects;

/** Context for lease. */
public class LeaseContext implements Serializable {

    private static final long serialVersionUID = 1L;

    // kv snapshot lease id. null for log table.
    private final @Nullable String kvSnapshotLeaseId;

    // kv snapshot lease duration. null for log table.
    private final @Nullable Long kvSnapshotLeaseDurationMs;

    public LeaseContext(
            @Nullable String kvSnapshotLeaseId, @Nullable Long kvSnapshotLeaseDurationMs) {
        this.kvSnapshotLeaseId = kvSnapshotLeaseId;
        this.kvSnapshotLeaseDurationMs = kvSnapshotLeaseDurationMs;
    }

    public @Nullable String getKvSnapshotLeaseId() {
        return kvSnapshotLeaseId;
    }

    public @Nullable Long getKvSnapshotLeaseDurationMs() {
        return kvSnapshotLeaseDurationMs;
    }

    @Override
    public String toString() {
        return "LeaseContext{"
                + "kvSnapshotLeaseId='"
                + kvSnapshotLeaseId
                + '\''
                + ", kvSnapshotLeaseDurationMs="
                + kvSnapshotLeaseDurationMs
                + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        LeaseContext that = (LeaseContext) o;

        return Objects.equals(kvSnapshotLeaseId, that.kvSnapshotLeaseId)
                && Objects.equals(kvSnapshotLeaseDurationMs, that.kvSnapshotLeaseDurationMs);
    }

    @Override
    public int hashCode() {
        return Objects.hash(kvSnapshotLeaseId, kvSnapshotLeaseDurationMs);
    }
}
