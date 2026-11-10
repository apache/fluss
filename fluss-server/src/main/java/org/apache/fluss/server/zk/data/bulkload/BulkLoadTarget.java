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
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.server.zk.data.ZkData;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.Objects;

import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** Immutable physical target and snapshot allocation frozen by BulkLoad Begin. */
@Internal
public final class BulkLoadTarget {
    private final PhysicalTablePath target;
    private final @Nullable Long partitionId;
    private final String remoteDataDir;
    private final int schemaId;
    private final int registrationVersion;
    private final long[] snapshotIds;

    /** Creates the persisted facts for one physical target. */
    public BulkLoadTarget(
            PhysicalTablePath target,
            @Nullable Long partitionId,
            String remoteDataDir,
            int schemaId,
            int registrationVersion,
            long[] snapshotIds) {
        this.target = checkNotNull(target, "BulkLoad target must not be null.");
        checkArgument(target.isValid(), "BulkLoad target must be valid.");
        checkArgument(
                partitionId == null || partitionId >= 0,
                "BulkLoad partition ID must be non-negative.");
        checkArgument(
                (target.getPartitionName() == null) == (partitionId == null),
                "BulkLoad partition name and partition ID must both be present or both be absent.");
        this.partitionId = partitionId;
        this.remoteDataDir =
                checkNotNull(remoteDataDir, "BulkLoad remote data directory must not be null.");
        checkArgument(schemaId >= 0, "BulkLoad schema ID must be non-negative.");
        checkArgument(
                registrationVersion >= 0, "BulkLoad registration version must be non-negative.");
        this.schemaId = schemaId;
        this.registrationVersion = registrationVersion;
        checkNotNull(snapshotIds, "BulkLoad snapshot IDs must not be null.");
        checkArgument(snapshotIds.length > 0, "BulkLoad snapshot IDs must not be empty.");
        this.snapshotIds = snapshotIds.clone();
        for (long snapshotId : this.snapshotIds) {
            checkArgument(snapshotId >= 0, "BulkLoad snapshot ID must be non-negative.");
        }
    }

    /** Returns the physical target path. */
    public PhysicalTablePath getTarget() {
        return target;
    }

    /** Returns the physical partition ID, or null for a non-partitioned target. */
    @Nullable
    public Long getPartitionId() {
        return partitionId;
    }

    /** Returns the target's remote data directory. */
    public String getRemoteDataDir() {
        return remoteDataDir;
    }

    /** Returns the schema ID frozen by Begin. */
    public int getSchemaId() {
        return schemaId;
    }

    /** Returns the observed ZooKeeper registration version used for metadata CAS. */
    public int getRegistrationVersion() {
        return registrationVersion;
    }

    /** Returns a copy of the snapshot IDs indexed by bucket ID. */
    public long[] getSnapshotIds() {
        return snapshotIds.clone();
    }

    /** Returns the ZooKeeper path of the table or partition registration. */
    public String getMetadataPath() {
        return partitionId == null
                ? ZkData.TableZNode.path(target.getTablePath())
                : ZkData.PartitionZNode.path(target.getTablePath(), target.getPartitionName());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        BulkLoadTarget that = (BulkLoadTarget) o;
        return schemaId == that.schemaId
                && registrationVersion == that.registrationVersion
                && Objects.equals(target, that.target)
                && Objects.equals(partitionId, that.partitionId)
                && Objects.equals(remoteDataDir, that.remoteDataDir)
                && Arrays.equals(snapshotIds, that.snapshotIds);
    }

    @Override
    public int hashCode() {
        return 31 * Objects.hash(target, partitionId, remoteDataDir, schemaId, registrationVersion)
                + Arrays.hashCode(snapshotIds);
    }

    @Override
    public String toString() {
        return "BulkLoadTarget{"
                + "target="
                + target
                + ", partitionId="
                + partitionId
                + ", schemaId="
                + schemaId
                + ", registrationVersion="
                + registrationVersion
                + ", snapshotIds="
                + Arrays.toString(snapshotIds)
                + '}';
    }
}
