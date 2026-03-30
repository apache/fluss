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

import org.apache.fluss.flink.FlinkConnectorOptions;

import org.apache.flink.configuration.ReadableConfig;

import java.io.Serializable;
import java.time.Duration;
import java.util.Objects;
import java.util.UUID;

/** Context for lease. */
public class LeaseContext implements Serializable {

    private static final long serialVersionUID = 1L;

    public static final LeaseContext DEFAULT =
            new LeaseContext(UUID.randomUUID().toString(), Duration.ofDays(1).toMillis());

    // kv snapshot lease id. null for log table.
    private final String kvSnapshotLeaseId;

    // kv snapshot lease duration. null for log table.
    private final long kvSnapshotLeaseDurationMs;

    public LeaseContext(String kvSnapshotLeaseId, long kvSnapshotLeaseDurationMs) {
        this.kvSnapshotLeaseId = kvSnapshotLeaseId;
        this.kvSnapshotLeaseDurationMs = kvSnapshotLeaseDurationMs;
    }

    public static LeaseContext fromConf(ReadableConfig tableOptions) {
        return new LeaseContext(
                tableOptions.get(FlinkConnectorOptions.SCAN_KV_SNAPSHOT_LEASE_ID),
                tableOptions.get(FlinkConnectorOptions.SCAN_KV_SNAPSHOT_LEASE_DURATION).toMillis());
    }

    public String getKvSnapshotLeaseId() {
        return kvSnapshotLeaseId;
    }

    public long getKvSnapshotLeaseDurationMs() {
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
