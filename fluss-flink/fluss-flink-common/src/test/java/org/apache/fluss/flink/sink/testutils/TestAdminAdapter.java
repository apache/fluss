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

package org.apache.fluss.flink.sink.testutils;

import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.admin.CreateAclsResult;
import org.apache.fluss.client.admin.DropAclsResult;
import org.apache.fluss.client.admin.KvSnapshotLease;
import org.apache.fluss.client.admin.ListOffsetsResult;
import org.apache.fluss.client.admin.OffsetSpec;
import org.apache.fluss.client.admin.ProducerOffsetsResult;
import org.apache.fluss.client.admin.RegisterResult;
import org.apache.fluss.client.metadata.KvSnapshotMetadata;
import org.apache.fluss.client.metadata.KvSnapshots;
import org.apache.fluss.client.metadata.LakeSnapshot;
import org.apache.fluss.cluster.ServerNode;
import org.apache.fluss.cluster.rebalance.GoalType;
import org.apache.fluss.cluster.rebalance.RebalanceProgress;
import org.apache.fluss.cluster.rebalance.ServerTag;
import org.apache.fluss.config.cluster.AlterConfig;
import org.apache.fluss.config.cluster.ConfigEntry;
import org.apache.fluss.metadata.DatabaseDescriptor;
import org.apache.fluss.metadata.DatabaseInfo;
import org.apache.fluss.metadata.DatabaseSummary;
import org.apache.fluss.metadata.PartitionInfo;
import org.apache.fluss.metadata.PartitionSpec;
import org.apache.fluss.metadata.SchemaInfo;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableChange;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.security.acl.AclBinding;
import org.apache.fluss.security.acl.AclBindingFilter;

import javax.annotation.Nullable;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * An adapter class that implements {@link Admin} with all methods throwing {@link
 * UnsupportedOperationException}.
 *
 * <p>This class is designed to be extended by test-specific Admin implementations that only need to
 * override a subset of methods. Subclasses can override only the methods they need for their
 * specific test scenarios.
 *
 * <p>Example usage:
 *
 * <pre>{@code
 * class MyTestAdmin extends TestAdminAdapter {
 *     @Override
 *     public CompletableFuture<List<PartitionInfo>> listPartitionInfos(TablePath tablePath) {
 *         // Custom implementation for test
 *         return CompletableFuture.completedFuture(myPartitions);
 *     }
 * }
 * }</pre>
 */
public class TestAdminAdapter implements Admin {

    @Override
    public CompletableFuture<List<ServerNode>> getServerNodes() {
        throw new UnsupportedOperationException("Not implemented in TestAdminAdapter");
    }

    @Override
    public CompletableFuture<SchemaInfo> getTableSchema(TablePath tablePath) {
        throw new UnsupportedOperationException("Not implemented in TestAdminAdapter");
    }

    @Override
    public CompletableFuture<SchemaInfo> getTableSchema(TablePath tablePath, int schemaId) {
        throw new UnsupportedOperationException("Not implemented in TestAdminAdapter");
    }

    @Override
    public CompletableFuture<Void> createDatabase(
            String databaseName, DatabaseDescriptor databaseDescriptor, boolean ignoreIfExists) {
        throw new UnsupportedOperationException("Not implemented in TestAdminAdapter");
    }

    @Override
    public CompletableFuture<DatabaseInfo> getDatabaseInfo(String databaseName) {
        throw new UnsupportedOperationException("Not implemented in TestAdminAdapter");
    }

    @Override
    public CompletableFuture<Void> dropDatabase(
            String databaseName, boolean ignoreIfNotExists, boolean cascade) {
        throw new UnsupportedOperationException("Not implemented in TestAdminAdapter");
    }

    @Override
    public CompletableFuture<Boolean> databaseExists(String databaseName) {
        throw new UnsupportedOperationException("Not implemented in TestAdminAdapter");
    }

    @Override
    public CompletableFuture<List<String>> listDatabases() {
        throw new UnsupportedOperationException("Not implemented in TestAdminAdapter");
    }

    @Override
    public CompletableFuture<List<DatabaseSummary>> listDatabaseSummaries() {
        throw new UnsupportedOperationException("Not implemented in TestAdminAdapter");
    }

    @Override
    public CompletableFuture<Void> createTable(
            TablePath tablePath, TableDescriptor tableDescriptor, boolean ignoreIfExists) {
        throw new UnsupportedOperationException("Not implemented in TestAdminAdapter");
    }

    @Override
    public CompletableFuture<TableInfo> getTableInfo(TablePath tablePath) {
        throw new UnsupportedOperationException("Not implemented in TestAdminAdapter");
    }

    @Override
    public CompletableFuture<Void> dropTable(TablePath tablePath, boolean ignoreIfNotExists) {
        throw new UnsupportedOperationException("Not implemented in TestAdminAdapter");
    }

    @Override
    public CompletableFuture<Boolean> tableExists(TablePath tablePath) {
        throw new UnsupportedOperationException("Not implemented in TestAdminAdapter");
    }

    @Override
    public CompletableFuture<List<String>> listTables(String databaseName) {
        throw new UnsupportedOperationException("Not implemented in TestAdminAdapter");
    }

    @Override
    public CompletableFuture<Void> alterTable(
            TablePath tablePath, List<TableChange> tableChanges, boolean ignoreIfNotExists) {
        throw new UnsupportedOperationException("Not implemented in TestAdminAdapter");
    }

    @Override
    public CompletableFuture<List<PartitionInfo>> listPartitionInfos(TablePath tablePath) {
        throw new UnsupportedOperationException("Not implemented in TestAdminAdapter");
    }

    @Override
    public CompletableFuture<List<PartitionInfo>> listPartitionInfos(
            TablePath tablePath, PartitionSpec partialPartitionSpec) {
        throw new UnsupportedOperationException("Not implemented in TestAdminAdapter");
    }

    @Override
    public CompletableFuture<Void> createPartition(
            TablePath tablePath, PartitionSpec partitionSpec, boolean ignoreIfExists) {
        throw new UnsupportedOperationException("Not implemented in TestAdminAdapter");
    }

    @Override
    public CompletableFuture<Void> dropPartition(
            TablePath tablePath, PartitionSpec partitionSpec, boolean ignoreIfNotExists) {
        throw new UnsupportedOperationException("Not implemented in TestAdminAdapter");
    }

    @Override
    public CompletableFuture<KvSnapshots> getLatestKvSnapshots(TablePath tablePath) {
        throw new UnsupportedOperationException("Not implemented in TestAdminAdapter");
    }

    @Override
    public CompletableFuture<KvSnapshots> getLatestKvSnapshots(
            TablePath tablePath, String partitionName) {
        throw new UnsupportedOperationException("Not implemented in TestAdminAdapter");
    }

    @Override
    public CompletableFuture<KvSnapshotMetadata> getKvSnapshotMetadata(
            TableBucket bucket, long snapshotId) {
        throw new UnsupportedOperationException("Not implemented in TestAdminAdapter");
    }

    @Override
    public CompletableFuture<LakeSnapshot> getLatestLakeSnapshot(TablePath tablePath) {
        throw new UnsupportedOperationException("Not implemented in TestAdminAdapter");
    }

    @Override
    public CompletableFuture<LakeSnapshot> getLakeSnapshot(TablePath tablePath, long snapshotId) {
        throw new UnsupportedOperationException("Not implemented in TestAdminAdapter");
    }

    @Override
    public ListOffsetsResult listOffsets(
            TablePath tablePath, Collection<Integer> buckets, OffsetSpec offsetSpec) {
        throw new UnsupportedOperationException("Not implemented in TestAdminAdapter");
    }

    @Override
    public ListOffsetsResult listOffsets(
            TablePath tablePath,
            String partitionName,
            Collection<Integer> buckets,
            OffsetSpec offsetSpec) {
        throw new UnsupportedOperationException("Not implemented in TestAdminAdapter");
    }

    @Override
    public CompletableFuture<Collection<AclBinding>> listAcls(AclBindingFilter aclBindingFilter) {
        throw new UnsupportedOperationException("Not implemented in TestAdminAdapter");
    }

    @Override
    public CreateAclsResult createAcls(Collection<AclBinding> aclBindings) {
        throw new UnsupportedOperationException("Not implemented in TestAdminAdapter");
    }

    @Override
    public DropAclsResult dropAcls(Collection<AclBindingFilter> filters) {
        throw new UnsupportedOperationException("Not implemented in TestAdminAdapter");
    }

    @Override
    public CompletableFuture<Collection<ConfigEntry>> describeClusterConfigs() {
        throw new UnsupportedOperationException("Not implemented in TestAdminAdapter");
    }

    @Override
    public CompletableFuture<Void> alterClusterConfigs(Collection<AlterConfig> configs) {
        throw new UnsupportedOperationException("Not implemented in TestAdminAdapter");
    }

    @Override
    public CompletableFuture<Void> addServerTag(List<Integer> tabletServers, ServerTag serverTag) {
        throw new UnsupportedOperationException("Not implemented in TestAdminAdapter");
    }

    @Override
    public CompletableFuture<Void> removeServerTag(
            List<Integer> tabletServers, ServerTag serverTag) {
        throw new UnsupportedOperationException("Not implemented in TestAdminAdapter");
    }

    @Override
    public CompletableFuture<String> rebalance(List<GoalType> priorityGoals) {
        throw new UnsupportedOperationException("Not implemented in TestAdminAdapter");
    }

    @Override
    public CompletableFuture<Optional<RebalanceProgress>> listRebalanceProgress(
            @Nullable String rebalanceId) {
        throw new UnsupportedOperationException("Not implemented in TestAdminAdapter");
    }

    @Override
    public CompletableFuture<Void> cancelRebalance(@Nullable String rebalanceId) {
        throw new UnsupportedOperationException("Not implemented in TestAdminAdapter");
    }

    @Override
    public CompletableFuture<RegisterResult> registerProducerOffsets(
            String producerId, Map<TableBucket, Long> offsets) {
        throw new UnsupportedOperationException("Not implemented in TestAdminAdapter");
    }

    @Override
    public CompletableFuture<ProducerOffsetsResult> getProducerOffsets(String producerId) {
        throw new UnsupportedOperationException("Not implemented in TestAdminAdapter");
    }

    @Override
    public CompletableFuture<Void> deleteProducerOffsets(String producerId) {
        throw new UnsupportedOperationException("Not implemented in TestAdminAdapter");
    }

    @Override
    public KvSnapshotLease createKvSnapshotLease(String leaseId, long leaseDurationMs) {
        throw new UnsupportedOperationException("Not implemented in TestAdminAdapter");
    }

    @Override
    public void close() {
        // No-op for test adapter
    }

    @Override
    public CompletableFuture<LakeSnapshot> getReadableLakeSnapshot(TablePath tablePath) {
        throw new UnsupportedOperationException("Not implemented in TestAdminAdapter");
    }
}
