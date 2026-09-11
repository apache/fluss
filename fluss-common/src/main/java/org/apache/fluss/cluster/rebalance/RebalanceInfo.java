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

package org.apache.fluss.cluster.rebalance;

import org.apache.fluss.annotation.PublicEvolving;

import java.util.Objects;

import static org.apache.fluss.utils.Preconditions.checkNotNull;

/**
 * A summary of a rebalance task.
 *
 * <p>Unlike {@link RebalanceProgress}, this does not carry per-bucket detail.
 *
 * @since 1.0
 */
@PublicEvolving
public class RebalanceInfo {

    /** The rebalance id. */
    private final String rebalanceId;

    /** The final or current rebalance status. */
    private final RebalanceStatus status;

    /** The time when this rebalance task was started, or {@code -1} if unset. */
    private final long startedAtMs;

    /** The time when this rebalance task reached a final status, or {@code -1} if unset. */
    private final long completedAtMs;

    public RebalanceInfo(
            String rebalanceId, RebalanceStatus status, long startedAtMs, long completedAtMs) {
        this.rebalanceId = checkNotNull(rebalanceId);
        this.status = checkNotNull(status);
        this.startedAtMs = startedAtMs;
        this.completedAtMs = completedAtMs;
    }

    public String rebalanceId() {
        return rebalanceId;
    }

    public RebalanceStatus status() {
        return status;
    }

    public long startedAtMs() {
        return startedAtMs;
    }

    public long completedAtMs() {
        return completedAtMs;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RebalanceInfo that = (RebalanceInfo) o;
        return startedAtMs == that.startedAtMs
                && completedAtMs == that.completedAtMs
                && Objects.equals(rebalanceId, that.rebalanceId)
                && status == that.status;
    }

    @Override
    public int hashCode() {
        return Objects.hash(rebalanceId, status, startedAtMs, completedAtMs);
    }

    @Override
    public String toString() {
        return "RebalanceInfo{"
                + "rebalanceId='"
                + rebalanceId
                + '\''
                + ", status="
                + status
                + ", startedAtMs="
                + startedAtMs
                + ", completedAtMs="
                + completedAtMs
                + '}';
    }
}
