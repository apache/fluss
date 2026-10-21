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
import org.apache.fluss.server.zk.data.ZkData;

import javax.annotation.Nullable;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Objects;

import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** Immutable version-1 BulkLoad transaction facts. ZooKeeper Stat.version is its state version. */
@Internal
public final class BulkLoadTransaction {

    private static final int MAX_ABORT_MESSAGE_BYTES = 4096;

    private final BulkLoadHandle handle;
    private final BulkLoadState state;
    private final String creatorName;
    private final String creatorType;
    private final String remoteDataDir;
    private final int schemaId;
    private final int metadataVersion;
    private final @Nullable long[] snapshotIds;
    private final long deadlineMs;
    private final @Nullable String inputSha256;
    private final @Nullable BulkLoadAbortReason abortReason;
    private final @Nullable String abortMessage;

    /** Creates the persisted transaction state. */
    public BulkLoadTransaction(
            BulkLoadHandle handle,
            BulkLoadState state,
            String creatorName,
            String creatorType,
            String remoteDataDir,
            int schemaId,
            int metadataVersion,
            @Nullable long[] snapshotIds,
            long deadlineMs,
            @Nullable String inputSha256,
            @Nullable BulkLoadAbortReason abortReason,
            @Nullable String abortMessage) {
        this.handle = checkNotNull(handle, "BulkLoad handle must not be null.");
        this.state = checkNotNull(state, "BulkLoad state must not be null.");
        this.creatorName = checkNotNull(creatorName, "BulkLoad creator name must not be null.");
        this.creatorType = checkNotNull(creatorType, "BulkLoad creator type must not be null.");
        this.remoteDataDir =
                checkNotNull(remoteDataDir, "BulkLoad remote data directory must not be null.");
        checkArgument(schemaId >= 0, "BulkLoad schema ID must be non-negative.");
        checkArgument(metadataVersion >= 0, "BulkLoad metadata version must be non-negative.");
        checkArgument(
                snapshotIds == null || snapshotIds.length > 0,
                "BulkLoad snapshot IDs must not be empty.");
        if (snapshotIds != null) {
            for (long snapshotId : snapshotIds) {
                checkArgument(snapshotId >= 0, "BulkLoad snapshot ID must be non-negative.");
            }
        }
        checkArgument(deadlineMs >= 0, "BulkLoad deadline must be non-negative.");

        this.schemaId = schemaId;
        this.metadataVersion = metadataVersion;
        this.snapshotIds = snapshotIds == null ? null : snapshotIds.clone();
        this.deadlineMs = deadlineMs;
        this.inputSha256 = inputSha256;
        this.abortReason = abortReason;
        this.abortMessage = truncateUtf8(abortMessage);
    }

    /** Returns a fenced transaction with the allocated snapshot IDs. */
    public BulkLoadTransaction withSnapshotIds(long[] snapshotIds) {
        return new BulkLoadTransaction(
                handle,
                state,
                creatorName,
                creatorType,
                remoteDataDir,
                schemaId,
                metadataVersion,
                snapshotIds,
                deadlineMs,
                inputSha256,
                abortReason,
                abortMessage);
    }

    /** Returns a transaction bound to the submitted input digest and decision deadline. */
    public BulkLoadTransaction withInput(String sha256, long deadline) {
        return new BulkLoadTransaction(
                handle,
                state,
                creatorName,
                creatorType,
                remoteDataDir,
                schemaId,
                metadataVersion,
                snapshotIds,
                deadline,
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
                remoteDataDir,
                schemaId,
                metadataVersion,
                snapshotIds,
                deadlineMs,
                inputSha256,
                abortReason,
                abortMessage);
    }

    /** Returns a transaction whose target registration has become ACTIVE. */
    public BulkLoadTransaction activated(
            int metadataVersion, @Nullable BulkLoadAbortReason reason, @Nullable String message) {
        return new BulkLoadTransaction(
                handle,
                state,
                creatorName,
                creatorType,
                remoteDataDir,
                schemaId,
                metadataVersion,
                snapshotIds,
                deadlineMs,
                inputSha256,
                reason,
                message);
    }

    /** Returns the terminal result after target ownership has been released. */
    public BulkLoadTransaction released(int metadataVersion, long expireTimeMs) {
        return new BulkLoadTransaction(
                handle,
                state == BulkLoadState.COMMITTING ? BulkLoadState.COMMITTED : BulkLoadState.ABORTED,
                creatorName,
                creatorType,
                remoteDataDir,
                schemaId,
                metadataVersion,
                snapshotIds,
                expireTimeMs,
                inputSha256,
                abortReason,
                abortMessage);
    }

    public BulkLoadHandle getHandle() {
        return handle;
    }

    public String getBulkLoadId() {
        return handle.getBulkLoadId();
    }

    /** Returns the durable transaction state. */
    public BulkLoadState getState() {
        return state;
    }

    public String getDatabaseName() {
        return handle.getTarget().getDatabaseName();
    }

    public String getTableName() {
        return handle.getTarget().getTableName();
    }

    @Nullable
    public String getPartitionName() {
        return handle.getTarget().getPartitionName();
    }

    public long getTableId() {
        return handle.getTableId();
    }

    @Nullable
    public Long getPartitionId() {
        return handle.getPartitionId();
    }

    public String getCreatorName() {
        return creatorName;
    }

    public String getCreatorType() {
        return creatorType;
    }

    public String getRemoteDataDir() {
        return remoteDataDir;
    }

    public int getSchemaId() {
        return schemaId;
    }

    public String getMetadataPath() {
        return handle.getPartitionId() == null
                ? ZkData.TableZNode.path(handle.getTarget().getTablePath())
                : ZkData.PartitionZNode.path(
                        handle.getTarget().getTablePath(), handle.getTarget().getPartitionName());
    }

    public int getMetadataVersion() {
        return metadataVersion;
    }

    public boolean isFenceReady() {
        return snapshotIds != null;
    }

    @Nullable
    public long[] getSnapshotIds() {
        return snapshotIds == null ? null : snapshotIds.clone();
    }

    /** Returns the build, commit-decision, or terminal-retention deadline for the current phase. */
    public long getDeadlineMs() {
        return deadlineMs;
    }

    @Nullable
    public String getInputSha256() {
        return inputSha256;
    }

    @Nullable
    public BulkLoadAbortReason getAbortReason() {
        return abortReason;
    }

    @Nullable
    public String getAbortMessage() {
        return abortMessage;
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
        return schemaId == that.schemaId
                && metadataVersion == that.metadataVersion
                && Arrays.equals(snapshotIds, that.snapshotIds)
                && deadlineMs == that.deadlineMs
                && Objects.equals(handle, that.handle)
                && state == that.state
                && Objects.equals(creatorName, that.creatorName)
                && Objects.equals(creatorType, that.creatorType)
                && Objects.equals(remoteDataDir, that.remoteDataDir)
                && Objects.equals(inputSha256, that.inputSha256)
                && abortReason == that.abortReason
                && Objects.equals(abortMessage, that.abortMessage);
    }

    @Override
    public int hashCode() {
        int result =
                Objects.hash(
                        handle,
                        state,
                        creatorName,
                        creatorType,
                        remoteDataDir,
                        schemaId,
                        metadataVersion,
                        deadlineMs,
                        inputSha256,
                        abortReason,
                        abortMessage);
        return 31 * result + Arrays.hashCode(snapshotIds);
    }

    @Override
    public String toString() {
        return "BulkLoadTransaction{" + "handle=" + handle + ", state=" + state + '}';
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
