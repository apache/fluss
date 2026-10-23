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
import org.apache.fluss.metadata.TablePath;

import javax.annotation.Nullable;

import java.util.Objects;
import java.util.Optional;

import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** An immutable summary of a manual compaction operation and its submitted scope. */
@PublicEvolving
public final class CompactionInfo {
    private final String operationId;
    private final CompactionType type;
    private final CompactionStatus status;
    private final long createTime;
    @Nullable private final String databaseName;
    @Nullable private final TablePath tablePath;
    @Nullable private final String partitionName;

    /** Creates a compaction summary. Times are epoch milliseconds; absent scope levels are null. */
    public CompactionInfo(
            String operationId,
            CompactionType type,
            CompactionStatus status,
            long createTime,
            @Nullable String databaseName,
            @Nullable TablePath tablePath,
            @Nullable String partitionName) {
        this.operationId = checkNotNull(operationId);
        this.type = checkNotNull(type);
        this.status = checkNotNull(status);
        this.createTime = createTime;
        this.databaseName = databaseName;
        this.tablePath = tablePath;
        this.partitionName = partitionName;
    }

    /** The operation ID. */
    public String operationId() {
        return operationId;
    }

    /** The requested physical operation. */
    public CompactionType type() {
        return type;
    }

    /** The operation state. */
    public CompactionStatus status() {
        return status;
    }

    /** The creation time in epoch milliseconds. */
    public long createTime() {
        return createTime;
    }

    /** The database scope, or empty for the cluster scope. */
    public Optional<String> databaseName() {
        return Optional.ofNullable(databaseName);
    }

    /** The table scope, if present. */
    public Optional<TablePath> tablePath() {
        return Optional.ofNullable(tablePath);
    }

    /** The partition scope, if present. */
    public Optional<String> partitionName() {
        return Optional.ofNullable(partitionName);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        CompactionInfo that = (CompactionInfo) o;
        return Objects.equals(operationId, that.operationId)
                && Objects.equals(type, that.type)
                && Objects.equals(status, that.status)
                && createTime == that.createTime
                && Objects.equals(databaseName, that.databaseName)
                && Objects.equals(tablePath, that.tablePath)
                && Objects.equals(partitionName, that.partitionName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                operationId, type, status, createTime, databaseName, tablePath, partitionName);
    }

    @Override
    public String toString() {
        return "CompactionInfo{"
                + "operationId="
                + operationId
                + ", type="
                + type
                + ", status="
                + status
                + ", createTime="
                + createTime
                + ", databaseName="
                + databaseName
                + ", tablePath="
                + tablePath
                + ", partitionName="
                + partitionName
                + '}';
    }
}
