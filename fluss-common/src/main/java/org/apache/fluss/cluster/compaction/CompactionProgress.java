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

package org.apache.fluss.cluster.compaction;

import org.apache.fluss.annotation.PublicEvolving;
import org.apache.fluss.metadata.TableBucket;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** An immutable view of compaction counts, current executions, and observation times. */
@PublicEvolving
public final class CompactionProgress {
    private final CompactionInfo info;
    private final long total;
    private final long pending;
    private final long running;
    private final long succeeded;
    private final long failed;
    private final long cancelled;
    private final List<Execution> currentExecutions;
    @Nullable private final String firstError;
    private final long lastProgressTime;
    private final long observedTime;
    private final long persistedTime;

    /** Creates an immutable progress view. */
    public CompactionProgress(
            CompactionInfo info,
            long total,
            long pending,
            long running,
            long succeeded,
            long failed,
            long cancelled,
            List<Execution> currentExecutions,
            @Nullable String firstError,
            long lastProgressTime,
            long observedTime,
            long persistedTime) {
        this.info = checkNotNull(info);
        this.total = total;
        this.pending = pending;
        this.running = running;
        this.succeeded = succeeded;
        this.failed = failed;
        this.cancelled = cancelled;
        this.currentExecutions =
                Collections.unmodifiableList(new ArrayList<>(checkNotNull(currentExecutions)));
        this.firstError = firstError;
        this.lastProgressTime = lastProgressTime;
        this.observedTime = observedTime;
        this.persistedTime = persistedTime;
    }

    /** The operation summary. */
    public CompactionInfo info() {
        return info;
    }

    /** The fixed number of target buckets. */
    public long total() {
        return total;
    }

    /** The number of buckets not yet dispatched. */
    public long pending() {
        return pending;
    }

    /** The number of running buckets. */
    public long running() {
        return running;
    }

    /** The number of buckets with published snapshot results. */
    public long succeeded() {
        return succeeded;
    }

    /** The number of failed buckets. */
    public long failed() {
        return failed;
    }

    /** The number of cancelled buckets. */
    public long cancelled() {
        return cancelled;
    }

    /** The current execution on each occupied tablet server. */
    public List<Execution> currentExecutions() {
        return currentExecutions;
    }

    /** The first error, if any. */
    public Optional<String> firstError() {
        return Optional.ofNullable(firstError);
    }

    /** The last progress time in epoch milliseconds, excluding heartbeats. */
    public long lastProgressTime() {
        return lastProgressTime;
    }

    /** The observation time in epoch milliseconds. */
    public long observedTime() {
        return observedTime;
    }

    /** The progress persistence time in epoch milliseconds. */
    public long persistedTime() {
        return persistedTime;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        CompactionProgress that = (CompactionProgress) o;
        return Objects.equals(info, that.info)
                && total == that.total
                && pending == that.pending
                && running == that.running
                && succeeded == that.succeeded
                && failed == that.failed
                && cancelled == that.cancelled
                && Objects.equals(currentExecutions, that.currentExecutions)
                && Objects.equals(firstError, that.firstError)
                && lastProgressTime == that.lastProgressTime
                && observedTime == that.observedTime
                && persistedTime == that.persistedTime;
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                info,
                total,
                pending,
                running,
                succeeded,
                failed,
                cancelled,
                currentExecutions,
                firstError,
                lastProgressTime,
                observedTime,
                persistedTime);
    }

    @Override
    public String toString() {
        return "CompactionProgress{"
                + "info="
                + info
                + ", total="
                + total
                + ", pending="
                + pending
                + ", running="
                + running
                + ", succeeded="
                + succeeded
                + ", failed="
                + failed
                + ", cancelled="
                + cancelled
                + ", currentExecutions="
                + currentExecutions
                + ", firstError="
                + firstError
                + ", lastProgressTime="
                + lastProgressTime
                + ", observedTime="
                + observedTime
                + ", persistedTime="
                + persistedTime
                + '}';
    }

    /** The immutable current execution of one dispatched bucket. */
    @PublicEvolving
    public static final class Execution {
        private final int serverId;
        private final TableBucket bucket;
        private final int leaderEpoch;
        private final String phase;
        private final long phaseStartTime;
        @Nullable private final Long snapshotId;
        @Nullable private final String reason;

        /** Creates an immutable bucket execution view. */
        public Execution(
                int serverId,
                TableBucket bucket,
                int leaderEpoch,
                String phase,
                long phaseStartTime,
                @Nullable Long snapshotId,
                @Nullable String reason) {
            this.serverId = serverId;
            this.bucket = checkNotNull(bucket);
            this.leaderEpoch = leaderEpoch;
            this.phase = checkNotNull(phase);
            this.phaseStartTime = phaseStartTime;
            this.snapshotId = snapshotId;
            this.reason = reason;
        }

        /** The tablet server ID. */
        public int serverId() {
            return serverId;
        }

        /** The target bucket. */
        public TableBucket bucket() {
            return bucket;
        }

        /** The dispatched leader epoch. */
        public int leaderEpoch() {
            return leaderEpoch;
        }

        /** The execution phase: DISPATCHED, FLUSH, COMPACTION, or SNAPSHOT. */
        public String phase() {
            return phase;
        }

        /** The phase start time in epoch milliseconds. */
        public long phaseStartTime() {
            return phaseStartTime;
        }

        /** The committed snapshot ID, if available. */
        public Optional<Long> snapshotId() {
            return Optional.ofNullable(snapshotId);
        }

        /** The execution reason, if any. */
        public Optional<String> reason() {
            return Optional.ofNullable(reason);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Execution that = (Execution) o;
            return serverId == that.serverId
                    && Objects.equals(bucket, that.bucket)
                    && leaderEpoch == that.leaderEpoch
                    && Objects.equals(phase, that.phase)
                    && phaseStartTime == that.phaseStartTime
                    && Objects.equals(snapshotId, that.snapshotId)
                    && Objects.equals(reason, that.reason);
        }

        @Override
        public int hashCode() {
            return Objects.hash(
                    serverId, bucket, leaderEpoch, phase, phaseStartTime, snapshotId, reason);
        }

        @Override
        public String toString() {
            return "Execution{"
                    + "serverId="
                    + serverId
                    + ", bucket="
                    + bucket
                    + ", leaderEpoch="
                    + leaderEpoch
                    + ", phase="
                    + phase
                    + ", phaseStartTime="
                    + phaseStartTime
                    + ", snapshotId="
                    + snapshotId
                    + ", reason="
                    + reason
                    + '}';
        }
    }
}
