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

package org.apache.fluss.server.zk.data.bulkload;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.metadata.BulkLoadAbortReason;
import org.apache.fluss.metadata.BulkLoadHandle;
import org.apache.fluss.metadata.BulkLoadState;
import org.apache.fluss.metadata.PhysicalTablePath;

import javax.annotation.Nullable;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** Immutable shared BulkLoad transaction facts. ZooKeeper Stat.version is its state version. */
@Internal
public final class BulkLoadTransaction {
    private static final int MAX_ABORT_MESSAGE_BYTES = 4096;

    private final BulkLoadHandle handle;
    private final BulkLoadState state;
    private final String creatorName;
    private final String creatorType;
    private final List<BulkLoadTarget> targets;
    private final long deadlineMs;
    private final @Nullable String inputSha256;
    private final @Nullable BulkLoadAbortReason abortReason;
    private final @Nullable String abortMessage;

    /** Creates the persisted state for a fixed, nonempty set of same-table targets. */
    public BulkLoadTransaction(
            BulkLoadHandle handle,
            BulkLoadState state,
            String creatorName,
            String creatorType,
            List<BulkLoadTarget> targets,
            long deadlineMs,
            @Nullable String inputSha256,
            @Nullable BulkLoadAbortReason abortReason,
            @Nullable String abortMessage) {
        this.handle = checkNotNull(handle, "BulkLoad handle must not be null.");
        this.state = checkNotNull(state, "BulkLoad state must not be null.");
        this.creatorName = checkNotNull(creatorName, "BulkLoad creator name must not be null.");
        this.creatorType = checkNotNull(creatorType, "BulkLoad creator type must not be null.");
        checkNotNull(targets, "BulkLoad targets must not be null.");
        checkArgument(!targets.isEmpty(), "BulkLoad targets must not be empty.");
        List<BulkLoadTarget> copy = new ArrayList<>(targets);
        Set<PhysicalTablePath> paths = new HashSet<>();
        Set<Long> partitionIds = new HashSet<>();
        for (BulkLoadTarget target : copy) {
            checkNotNull(target, "BulkLoad target must not be null.");
            checkArgument(
                    handle.getTablePath().equals(target.getTarget().getTablePath()),
                    "BulkLoad targets must belong to the handle's table.");
            checkArgument(
                    paths.add(target.getTarget()) && partitionIds.add(target.getPartitionId()),
                    "BulkLoad targets must not contain duplicate paths or partition IDs.");
            checkArgument(
                    target.getPartitionId() != null || copy.size() == 1,
                    "A non-partitioned BulkLoad target must be the only target.");
        }
        copy.sort(
                Comparator.comparing(
                        BulkLoadTarget::getPartitionId,
                        Comparator.nullsFirst(Comparator.naturalOrder())));
        this.targets = Collections.unmodifiableList(copy);
        checkArgument(deadlineMs >= 0, "BulkLoad deadline must be non-negative.");
        this.deadlineMs = deadlineMs;
        this.inputSha256 = inputSha256;
        this.abortReason = abortReason;
        this.abortMessage = truncateUtf8(abortMessage);
    }

    /** Returns a transaction bound to the submitted digest, preserving its deadline. */
    public BulkLoadTransaction withInput(String sha256) {
        return new BulkLoadTransaction(
                handle,
                state,
                creatorName,
                creatorType,
                targets,
                deadlineMs,
                sha256,
                abortReason,
                abortMessage);
    }

    /** Returns a transaction with a durable commit decision. */
    public BulkLoadTransaction committing() {
        return new BulkLoadTransaction(
                handle,
                BulkLoadState.COMMITTING,
                creatorName,
                creatorType,
                targets,
                deadlineMs,
                inputSha256,
                abortReason,
                abortMessage);
    }

    /** Returns a committed result with its result-retention expiry time. */
    public BulkLoadTransaction committed(long expiry) {
        return new BulkLoadTransaction(
                handle,
                BulkLoadState.COMMITTED,
                creatorName,
                creatorType,
                targets,
                expiry,
                inputSha256,
                null,
                null);
    }

    /** Returns an aborted result with its result-retention expiry time. */
    public BulkLoadTransaction aborted(BulkLoadAbortReason reason, String message, long expiry) {
        return new BulkLoadTransaction(
                handle,
                BulkLoadState.ABORTED,
                creatorName,
                creatorType,
                targets,
                expiry,
                inputSha256,
                reason,
                message);
    }

    /** Returns the shared transaction handle. */
    public BulkLoadHandle getHandle() {
        return handle;
    }

    /** Returns the canonical transaction UUID. */
    public String getBulkLoadId() {
        return handle.getBulkLoadId();
    }

    /** Returns the durable transaction state. */
    public BulkLoadState getState() {
        return state;
    }

    /** Returns the creator's name. */
    public String getCreatorName() {
        return creatorName;
    }

    /** Returns the creator's principal type. */
    public String getCreatorType() {
        return creatorType;
    }

    /** Returns the immutable target list ordered by physical partition ID. */
    public List<BulkLoadTarget> getTargets() {
        return targets;
    }

    /** Returns the pre-commit deadline or the terminal result's expiry time. */
    public long getDeadlineMs() {
        return deadlineMs;
    }

    /** Returns the frozen input digest, if submitted. */
    @Nullable
    public String getInputSha256() {
        return inputSha256;
    }

    /** Returns the abort reason, if present. */
    @Nullable
    public BulkLoadAbortReason getAbortReason() {
        return abortReason;
    }

    /** Returns the bounded abort message, if present. */
    @Nullable
    public String getAbortMessage() {
        return abortMessage;
    }

    /** Returns the physical table ID. */
    public long getTableId() {
        return handle.getTableId();
    }

    /** Returns the original database name. */
    public String getDatabaseName() {
        return handle.getTablePath().getDatabaseName();
    }

    /** Returns the original table name. */
    public String getTableName() {
        return handle.getTablePath().getTableName();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        BulkLoadTransaction that = (BulkLoadTransaction) o;
        return deadlineMs == that.deadlineMs
                && Objects.equals(handle, that.handle)
                && state == that.state
                && Objects.equals(creatorName, that.creatorName)
                && Objects.equals(creatorType, that.creatorType)
                && Objects.equals(targets, that.targets)
                && Objects.equals(inputSha256, that.inputSha256)
                && abortReason == that.abortReason
                && Objects.equals(abortMessage, that.abortMessage);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                handle,
                state,
                creatorName,
                creatorType,
                targets,
                deadlineMs,
                inputSha256,
                abortReason,
                abortMessage);
    }

    @Override
    public String toString() {
        return "BulkLoadTransaction{"
                + "handle="
                + handle
                + ", state="
                + state
                + ", targets="
                + targets
                + '}';
    }

    @Nullable
    private static String truncateUtf8(@Nullable String value) {
        if (value == null
                || value.getBytes(StandardCharsets.UTF_8).length <= MAX_ABORT_MESSAGE_BYTES) {
            return value;
        }
        StringBuilder result = new StringBuilder();
        int used = 0;
        for (int offset = 0; offset < value.length(); ) {
            int codePoint = value.codePointAt(offset);
            int bytes =
                    new String(Character.toChars(codePoint))
                            .getBytes(StandardCharsets.UTF_8)
                            .length;
            if (used + bytes > MAX_ABORT_MESSAGE_BYTES) {
                break;
            }
            result.appendCodePoint(codePoint);
            used += bytes;
            offset += Character.charCount(codePoint);
        }
        return result.toString();
    }
}
