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

package org.apache.fluss.client.metadata;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.annotation.PublicEvolving;
import org.apache.fluss.client.table.BucketCalculator;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.RowPartitionGetter;
import org.apache.fluss.row.encode.KeyEncoder;
import org.apache.fluss.rpc.messages.BeginBulkLoadResponse;
import org.apache.fluss.rpc.messages.PbBulkLoadTargetInfo;
import org.apache.fluss.rpc.messages.PbTableInfo;
import org.apache.fluss.types.RowType;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.fluss.client.utils.ClientRpcMessageUtils.toBulkLoadHandle;
import static org.apache.fluss.client.utils.ClientRpcMessageUtils.toTableInfo;
import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** Frozen, serializable input contract for the physical targets of one BulkLoad transaction. */
@PublicEvolving
public final class BulkLoadBuildContext implements Serializable {

    private static final long serialVersionUID = 1L;

    private final TablePath tablePath;
    private final BulkLoadHandle handle;
    private final byte[] serializedTable;
    private final List<byte[]> serializedTargets;
    private transient List<Target> targets;

    /** Constructs a build context from the coordinator's frozen table and target metadata. */
    @Internal
    public BulkLoadBuildContext(TablePath tablePath, BeginBulkLoadResponse response) {
        this.tablePath = tablePath;
        this.handle = toBulkLoadHandle(response.getStatus().getHandle());
        PbTableInfo frozenTable = new PbTableInfo().copyFrom(response.getTableInfo());
        TableDescriptor descriptor = TableDescriptor.fromJsonBytes(frozenTable.getTableJson());
        // Custom properties can contain connector credentials and are not needed to build
        // snapshots.
        frozenTable.setTableJson(
                descriptor
                        .withProperties(descriptor.getProperties(), Collections.emptyMap())
                        .toJsonBytes());
        this.serializedTable = frozenTable.toByteArray();
        this.serializedTargets = new ArrayList<>(response.getTargetsCount());
        for (PbBulkLoadTargetInfo target : response.getTargetsList()) {
            serializedTargets.add(target.toByteArray());
        }
        this.targets = buildTargets(frozenTable, response.getTargetsList());
    }

    /** Returns the shared transaction handle. */
    public BulkLoadHandle getHandle() {
        return handle;
    }

    /** Returns the transaction's frozen physical targets. */
    public synchronized List<Target> getTargets() {
        if (targets == null) {
            PbTableInfo table = new PbTableInfo();
            table.parseFrom(serializedTable);
            List<PbBulkLoadTargetInfo> targetInfos = new ArrayList<>(serializedTargets.size());
            for (byte[] target : serializedTargets) {
                PbBulkLoadTargetInfo targetInfo = new PbBulkLoadTargetInfo();
                targetInfo.parseFrom(target);
                targetInfos.add(targetInfo);
            }
            targets = buildTargets(table, targetInfos);
        }
        return targets;
    }

    /** Returns the frozen input contract for a table or one partition in this transaction. */
    public Target getTarget(PhysicalTablePath path) {
        for (Target target : getTargets()) {
            if (target.getPath().equals(path)) {
                return target;
            }
        }
        throw new IllegalArgumentException("Target does not belong to this BulkLoad: " + path);
    }

    private List<Target> buildTargets(
            PbTableInfo frozenTable, List<PbBulkLoadTargetInfo> frozenTargets) {
        TableInfo tableInfo = toTableInfo(tablePath, frozenTable);
        List<Target> targets = new ArrayList<>(frozenTargets.size());
        for (PbBulkLoadTargetInfo target : frozenTargets) {
            targets.add(new Target(getHandle(), tableInfo, target));
        }
        return Collections.unmodifiableList(targets);
    }

    /** Frozen routing and storage information for one table or physical partition. */
    @PublicEvolving
    public static final class Target {

        private final BulkLoadHandle handle;
        private final PhysicalTablePath path;
        private final @Nullable Long partitionId;
        private final int bucketCount;
        private final String remoteDataDir;
        private final TableInfo tableInfo;
        private final @Nullable RowPartitionGetter partitionGetter;
        private final BucketCalculator calculator;

        private Target(BulkLoadHandle handle, TableInfo tableInfo, PbBulkLoadTargetInfo target) {
            this.handle = handle;
            this.path =
                    PhysicalTablePath.of(
                            tableInfo.getTablePath(),
                            target.hasPartitionName() ? target.getPartitionName() : null);
            this.partitionId = target.hasPartitionId() ? target.getPartitionId() : null;
            this.bucketCount = target.getBucketCount();
            this.remoteDataDir = target.getRemoteDataDir();
            this.tableInfo = tableInfo;
            this.partitionGetter =
                    path.getPartitionName() == null
                            ? null
                            : new RowPartitionGetter(
                                    tableInfo.getRowType(), tableInfo.getPartitionKeys());
            this.calculator =
                    BucketCalculator.forPrimaryKeyRow(
                            tableInfo,
                            KeyEncoder.ofPrimaryKeyEncoder(
                                    tableInfo.getRowType(),
                                    tableInfo.getPhysicalPrimaryKeys(),
                                    tableInfo.getTableConfig(),
                                    tableInfo.isDefaultBucketKey()));
        }

        /** Returns the shared transaction handle. */
        public BulkLoadHandle getHandle() {
            return handle;
        }

        /** Returns the table or partition path frozen when the transaction began. */
        public PhysicalTablePath getPath() {
            return path;
        }

        /** Returns the physical partition ID, or null for a non-partitioned target. */
        @Nullable
        public Long getPartitionId() {
            return partitionId;
        }

        /** Returns the physical bucket count frozen for the target. */
        public int getBucketCount() {
            return bucketCount;
        }

        /** Returns the frozen full table row type accepted by this target. */
        public RowType getRowType() {
            return tableInfo.getRowType();
        }

        /** Returns the owning bucket of a full table row for this target. */
        public synchronized int bucketOf(InternalRow row) {
            checkNotNull(row, "row must not be null");
            if (partitionGetter != null) {
                String rowPartition = partitionGetter.getPartition(row);
                checkArgument(
                        rowPartition.equals(path.getPartitionName()),
                        "BulkLoad row belongs to partition %s, expected %s",
                        rowPartition,
                        path.getPartitionName());
            }
            return calculator.getBucketId(calculator.encodeKey(row), bucketCount);
        }

        /** Returns the frozen table metadata used when writing rows. */
        @Internal
        public TableInfo getTableInfo() {
            return tableInfo;
        }

        /** Returns the remote data directory frozen for this physical target. */
        @Internal
        public String getRemoteDataDir() {
            return remoteDataDir;
        }
    }
}
