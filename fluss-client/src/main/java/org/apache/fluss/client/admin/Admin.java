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

package org.apache.fluss.client.admin;

import org.apache.fluss.annotation.PublicEvolving;
import org.apache.fluss.client.metadata.KvSnapshotMetadata;
import org.apache.fluss.client.metadata.KvSnapshots;
import org.apache.fluss.client.metadata.LakeSnapshot;
import org.apache.fluss.cluster.ServerNode;
import org.apache.fluss.cluster.rebalance.GoalType;
import org.apache.fluss.cluster.rebalance.RebalanceProgress;
import org.apache.fluss.cluster.rebalance.ServerTag;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.cluster.AlterConfig;
import org.apache.fluss.config.cluster.ConfigEntry;
import org.apache.fluss.exception.AuthorizationException;
import org.apache.fluss.exception.DatabaseAlreadyExistException;
import org.apache.fluss.exception.DatabaseNotEmptyException;
import org.apache.fluss.exception.DatabaseNotExistException;
import org.apache.fluss.exception.InvalidAlterTableException;
import org.apache.fluss.exception.InvalidDatabaseException;
import org.apache.fluss.exception.InvalidPartitionException;
import org.apache.fluss.exception.InvalidReplicationFactorException;
import org.apache.fluss.exception.InvalidTableException;
import org.apache.fluss.exception.KvSnapshotNotExistException;
import org.apache.fluss.exception.LakeTableSnapshotNotExistException;
import org.apache.fluss.exception.NoRebalanceInProgressException;
import org.apache.fluss.exception.NonPrimaryKeyTableException;
import org.apache.fluss.exception.PartitionAlreadyExistsException;
import org.apache.fluss.exception.PartitionNotExistException;
import org.apache.fluss.exception.RebalanceFailureException;
import org.apache.fluss.exception.SchemaNotExistException;
import org.apache.fluss.exception.ServerNotExistException;
import org.apache.fluss.exception.ServerTagAlreadyExistException;
import org.apache.fluss.exception.ServerTagNotExistException;
import org.apache.fluss.exception.TableAlreadyExistException;
import org.apache.fluss.exception.TableNotExistException;
import org.apache.fluss.exception.TableNotPartitionedException;
import org.apache.fluss.exception.TooManyBucketsException;
import org.apache.fluss.exception.TooManyPartitionsException;
import org.apache.fluss.metadata.DatabaseDescriptor;
import org.apache.fluss.metadata.DatabaseInfo;
import org.apache.fluss.metadata.PartitionInfo;
import org.apache.fluss.metadata.PartitionSpec;
import org.apache.fluss.metadata.ResolvedPartitionSpec;
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
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * The administrative client for Fluss, which supports managing and inspecting tables, servers,
 * configurations and ACLs.
 *
 * @since 0.1
 */
@PublicEvolving
public interface Admin extends AutoCloseable {

    /** Get the current server node information. asynchronously. */
    CompletableFuture<List<ServerNode>> getServerNodes();

    /**
     * Get the latest table schema of the given table asynchronously.
     *
     * <p>The following exceptions can be anticipated when calling {@code get()} on returned future.
     *
     * <ul>
     *   <li>{@link TableNotExistException} if the table does not exist.
     * </ul>
     *
     * @param tablePath the table path of the table.
     */
    CompletableFuture<SchemaInfo> getTableSchema(TablePath tablePath);

    /**
     * Get the specific table schema of the given table by schema id asynchronously.
     *
     * <p>The following exceptions can be anticipated when calling {@code get()} on returned future.
     *
     * <ul>
     *   <li>{@link TableNotExistException} if the table does not exist.
     *   <li>{@link SchemaNotExistException} if the schema does not exist.
     * </ul>
     *
     * @param tablePath the table path of the table.
     */
    CompletableFuture<SchemaInfo> getTableSchema(TablePath tablePath, int schemaId);

    /**
     * Create a new database asynchronously.
     *
     * <p>The following exceptions can be anticipated when calling {@code get()} on returned future.
     *
     * <ul>
     *   <li>{@link DatabaseAlreadyExistException} if the database already exists and {@code
     *       ignoreIfExists} is false.
     * </ul>
     *
     * @param databaseName The name of the database to create.
     * @param databaseDescriptor The descriptor of the database to create.
     * @param ignoreIfExists Flag to specify behavior when a database with the given name already
     *     exists: if set to false, throw a DatabaseAlreadyExistException, if set to true, do
     *     nothing.
     * @throws InvalidDatabaseException if the database name is invalid, e.g., contains illegal
     *     characters, or exceeds the maximum length.
     */
    CompletableFuture<Void> createDatabase(
            String databaseName, DatabaseDescriptor databaseDescriptor, boolean ignoreIfExists);

    /**
     * Get the database with the given database name asynchronously.
     *
     * <p>The following exceptions can be anticipated when calling {@code get()} on returned future.
     *
     * <ul>
     *   <li>{@link DatabaseNotExistException} if the database does not exist.
     * </ul>
     *
     * @param databaseName The database name of the database.
     */
    CompletableFuture<DatabaseInfo> getDatabaseInfo(String databaseName);

    /**
     * Drop the database with the given name asynchronously.
     *
     * <p>The following exceptions can be anticipated when calling {@code get()} on returned future.
     *
     * <ul>
     *   <li>{@link DatabaseNotExistException} if the database does not exist and {@code
     *       ignoreIfNotExists} is false.
     *   <li>{@link DatabaseNotEmptyException} if the database is not empty and {@code cascade} is
     *       false.
     * </ul>
     *
     * @param databaseName The name of the database to delete.
     * @param ignoreIfNotExists Flag to specify behavior when a database with the given name does
     *     not exist: if set to false, throw a DatabaseNotExistException, if set to true, do
     *     nothing.
     * @param cascade Flag to specify whether to delete all tables in the database.
     */
    CompletableFuture<Void> dropDatabase(
            String databaseName, boolean ignoreIfNotExists, boolean cascade);

    /**
     * Get whether database exists asynchronously.
     *
     * @param databaseName The name of the database to check.
     */
    CompletableFuture<Boolean> databaseExists(String databaseName);

    /** List all databases in fluss cluster asynchronously. */
    CompletableFuture<List<String>> listDatabases();

    /**
     * Create a new table asynchronously.
     *
     * <p>The following exceptions can be anticipated when calling {@code get()} on returned future.
     *
     * <ul>
     *   <li>{@link DatabaseNotExistException} if the database in the table path does not exist.
     *   <li>{@link TableAlreadyExistException} if the table already exists and {@code
     *       ignoreIfExists} is false.
     *   <li>{@link InvalidReplicationFactorException} if the table's replication factor is larger
     *       than the number of available tablet servers.
     * </ul>
     *
     * @param tablePath The tablePath of the table.
     * @param tableDescriptor The table to create.
     * @throws InvalidTableException if the table name is invalid, e.g., contains illegal
     *     characters, or exceeds the maximum length.
     * @throws InvalidDatabaseException if the database name is invalid, e.g., contains illegal
     *     characters, or exceeds the maximum length.
     */
    CompletableFuture<Void> createTable(
            TablePath tablePath, TableDescriptor tableDescriptor, boolean ignoreIfExists)
            throws InvalidTableException, InvalidDatabaseException;

    /**
     * Get the table with the given table path asynchronously.
     *
     * <p>The following exceptions can be anticipated when calling {@code get()} on returned future.
     *
     * <ul>
     *   <li>{@link TableNotExistException} if the table does not exist.
     * </ul>
     *
     * @param tablePath The table path of the table.
     */
    CompletableFuture<TableInfo> getTableInfo(TablePath tablePath);

    /**
     * Drop the table with the given table path asynchronously.
     *
     * <p>The following exceptions can be anticipated when calling {@code get()} on returned future.
     *
     * <ul>
     *   <li>{@link TableNotExistException} if the table does not exist and {@code
     *       ignoreIfNotExists} is false.
     * </ul>
     *
     * @param tablePath The table path of the table.
     * @param ignoreIfNotExists Flag to specify behavior when a table with the given name does not
     *     exist: if set to false, throw a TableNotExistException, if set to true, do nothing.
     */
    CompletableFuture<Void> dropTable(TablePath tablePath, boolean ignoreIfNotExists);

    /**
     * Get whether table exists asynchronously.
     *
     * @param tablePath The table path of the table.
     */
    CompletableFuture<Boolean> tableExists(TablePath tablePath);

    /**
     * List all tables in the given database in fluss cluster asynchronously.
     *
     * <p>The following exceptions can be anticipated when calling {@code get()} on returned future.
     *
     * <ul>
     *   <li>{@link DatabaseNotExistException} if the database does not exist.
     * </ul>
     *
     * @param databaseName The name of the database.
     */
    CompletableFuture<List<String>> listTables(String databaseName);

    /**
     * Alter a table with the given {@code tableChanges}.
     *
     * <p>The following exceptions can be anticipated when calling {@code get()} on returned future.
     *
     * <ul>
     *   <li>{@link DatabaseNotExistException} when the database does not exist.
     *   <li>{@link TableNotExistException} when the table does not exist, and ignoreIfNotExists is
     *       false.
     *   <li>{@link InvalidAlterTableException} if the alter operation is invalid, such as alter set
     *       a table option which is not supported to modify currently.
     * </ul>
     *
     * @param tablePath The table path of the table.
     * @param tableChanges The table changes.
     * @param ignoreIfNotExists if it is true, do nothing if table does not exist. If false, throw a
     *     TableNotExistException.
     */
    CompletableFuture<Void> alterTable(
            TablePath tablePath, List<TableChange> tableChanges, boolean ignoreIfNotExists);

    /**
     * List all partitions in the given table in fluss cluster asynchronously.
     *
     * <p>The following exceptions can be anticipated when calling {@code get()} on returned future.
     *
     * <ul>
     *   <li>{@link TableNotExistException} if the table does not exist.
     *   <li>{@link TableNotPartitionedException} if the table is not partitioned.
     * </ul>
     *
     * @param tablePath The path of the table.
     */
    CompletableFuture<List<PartitionInfo>> listPartitionInfos(TablePath tablePath);

    /**
     * List all partitions in fluss cluster that are under the given table and the given partial
     * PartitionSpec asynchronously.
     *
     * <p>The following exceptions can be anticipated when calling {@code get()} on returned future.
     *
     * <ul>
     *   <li>{@link TableNotExistException} if the table does not exist.
     *   <li>{@link TableNotPartitionedException} if the table is not partitioned.
     *   <li>{@link InvalidPartitionException} if the input partition spec is invalid.
     * </ul>
     *
     * @param tablePath The path of the table.
     * @param partialPartitionSpec Part of table partition spec
     */
    CompletableFuture<List<PartitionInfo>> listPartitionInfos(
            TablePath tablePath, PartitionSpec partialPartitionSpec);

    /**
     * Create a new partition for a partitioned table.
     *
     * <p>The following exceptions can be anticipated when calling {@code get()} on returned future.
     *
     * <ul>
     *   <li>{@link TableNotExistException} if the table does not exist.
     *   <li>{@link TableNotPartitionedException} if the table is not partitioned.
     *   <li>{@link PartitionAlreadyExistsException} if the partition already exists and {@code
     *       ignoreIfExists} is false.
     *   <li>{@link InvalidPartitionException} if the input partition spec is invalid.
     *   <li>{@link TooManyPartitionsException} if the number of partitions is larger than the
     *       maximum number of partitions of one table, see {@link ConfigOptions#MAX_PARTITION_NUM}.
     *   <li>{@link TooManyBucketsException} if the number of buckets is larger than the maximum
     *       number of buckets of one table, see {@link ConfigOptions#MAX_BUCKET_NUM}.
     * </ul>
     *
     * @param tablePath The table path of the table.
     * @param partitionSpec The partition spec to add.
     * @param ignoreIfExists Flag to specify behavior when a partition with the given name already
     *     exists: if set to false, throw a PartitionAlreadyExistsException, if set to true, do
     *     nothing.
     */
    CompletableFuture<Void> createPartition(
            TablePath tablePath, PartitionSpec partitionSpec, boolean ignoreIfExists);

    /**
     * Drop a partition from a partitioned table.
     *
     * <p>The following exceptions can be anticipated when calling {@code get()} on returned future.
     *
     * <ul>
     *   <li>{@link TableNotExistException} if the table does not exist.
     *   <li>{@link TableNotPartitionedException} if the table is not partitioned.
     *   <li>{@link PartitionNotExistException} if the partition not exists and {@code
     *       ignoreIfExists} is false.
     *   <li>{@link InvalidPartitionException} if the input partition spec is invalid.
     * </ul>
     *
     * @param tablePath The table path of the table.
     * @param partitionSpec The partition spec to drop.
     * @param ignoreIfNotExists Flag to specify behavior when a partition with the given name does
     *     not exist: if set to false, throw a PartitionNotExistException, if set to true, do
     *     nothing.
     */
    CompletableFuture<Void> dropPartition(
            TablePath tablePath, PartitionSpec partitionSpec, boolean ignoreIfNotExists);

    /**
     * Get the latest kv snapshots of the given table asynchronously. A kv snapshot is a snapshot of
     * a bucket of a primary key table at a certain point in time. Therefore, there are at-most
     * {@code N} snapshots for a primary key table, {@code N} is the number of buckets.
     *
     * <p>The following exceptions can be anticipated when calling {@code get()} on returned future.
     *
     * <ul>
     *   <li>{@link TableNotExistException} if the table does not exist.
     *   <li>{@link NonPrimaryKeyTableException} if the table is not a primary key table.
     *   <li>{@link PartitionNotExistException} if the table is partitioned, use {@link
     *       #getLatestKvSnapshots(TablePath, String)} instead to get the latest kv snapshot of a
     *       partition of a partitioned table.
     *   <li>
     * </ul>
     *
     * @param tablePath the table path of the table.
     */
    CompletableFuture<KvSnapshots> getLatestKvSnapshots(TablePath tablePath);

    /**
     * Get the latest kv snapshots of the given table partition asynchronously. A kv snapshot is a
     * snapshot of a bucket of a primary key table at a certain point in time. Therefore, there are
     * at-most {@code N} snapshots for a partition of a primary key table, {@code N} is the number
     * of buckets.
     *
     * <p>The following exceptions can be anticipated when calling {@code get()} on returned future.
     *
     * <ul>
     *   <li>{@link TableNotExistException} if the table does not exist.
     *   <li>{@link NonPrimaryKeyTableException} if the table is not a primary key table.
     *   <li>{@link PartitionNotExistException} if the partition does not exist
     *   <li>{@link TableNotPartitionedException} if the table is not partitioned, use {@link
     *       #getLatestKvSnapshots(TablePath)} instead to get the latest kv snapshots for a
     *       non-partitioned table.
     * </ul>
     *
     * @param tablePath the table path of the table.
     * @param partitionName the partition name, see {@link ResolvedPartitionSpec#getPartitionName}
     *     for the format of the partition name.
     */
    CompletableFuture<KvSnapshots> getLatestKvSnapshots(TablePath tablePath, String partitionName);

    /**
     * Get the kv snapshot metadata of the given kv snapshot asynchronously. The kv snapshot
     * metadata including the snapshot files for the kv tablet and the log offset for the changelog
     * at the snapshot time.
     *
     * <p>The following exceptions can be anticipated when calling {@code get()} on returned future.
     *
     * <ul>
     *   <li>{@link KvSnapshotNotExistException} if the snapshot does not exist.
     * </ul>
     *
     * @param bucket the table bucket of the kv snapshot.
     * @param snapshotId the snapshot id.
     */
    CompletableFuture<KvSnapshotMetadata> getKvSnapshotMetadata(
            TableBucket bucket, long snapshotId);

    /**
     * Get table lake snapshot info of the given table asynchronously.
     *
     * <p>It'll get the latest snapshot for all the buckets of the table.
     *
     * <p>The following exceptions can be anticipated when calling {@code get()} on returned future.
     *
     * <ul>
     *   <li>{@link TableNotExistException} if the table does not exist.
     *   <li>{@link LakeTableSnapshotNotExistException} if no any lake snapshot exist.
     * </ul>
     *
     * @param tablePath the table path of the table.
     */
    CompletableFuture<LakeSnapshot> getLatestLakeSnapshot(TablePath tablePath);

    /**
     * List offset for the specified buckets. This operation enables to find the beginning offset,
     * end offset as well as the offset matching a timestamp in buckets.
     *
     * @param tablePath the table path of the table.
     * @param buckets the buckets to fetch offset.
     * @param offsetSpec the offset spec to fetch.
     */
    ListOffsetsResult listOffsets(
            TablePath tablePath, Collection<Integer> buckets, OffsetSpec offsetSpec);

    /**
     * List offset for the specified buckets. This operation enables to find the beginning offset,
     * end offset as well as the offset matching a timestamp in buckets.
     *
     * @param tablePath the table path of the table.
     * @param partitionName the partition name of the partition,see {@link
     *     ResolvedPartitionSpec#getPartitionName} * for the format of the partition name.
     * @param buckets the buckets to fetch offset.
     * @param offsetSpec the offset spec to fetch.
     */
    ListOffsetsResult listOffsets(
            TablePath tablePath,
            String partitionName,
            Collection<Integer> buckets,
            OffsetSpec offsetSpec);

    /**
     * Retrieves ACL entries filtered by principal for the specified resource.
     *
     * <p>1. Validates the user has 'describe' permission on the resource. 2. Returns entries
     * matching the principal if permitted; throws an exception otherwise.
     *
     * @return A CompletableFuture containing the filtered ACL entries.
     */
    CompletableFuture<Collection<AclBinding>> listAcls(AclBindingFilter aclBindingFilter);

    /**
     * Creates multiple ACL entries in a single atomic operation.
     *
     * <p>1. Validates the user has 'alter' permission on the resource. 2. Creates the ACL entries
     * if valid and permitted.
     *
     * <p>Each entry in {@code aclBindings} must have a valid principal, operation and permission.
     *
     * @param aclBindings List of ACL entries to create.
     * @return A CompletableFuture indicating completion of the operation.
     */
    CreateAclsResult createAcls(Collection<AclBinding> aclBindings);

    /**
     * Removes multiple ACL entries in a single atomic operation.
     *
     * <p>1. Validates the user has 'alter' permission on the resource. 2. Removes entries only if
     * they exactly match the provided entries (principal, operation, permission). 3. Does not
     * remove entries if any of the ACL entries do not exist.
     *
     * @param filters List of ACL entries to remove.
     * @return A CompletableFuture indicating completion of the operation.
     */
    DropAclsResult dropAcls(Collection<AclBindingFilter> filters);

    /**
     * Describe the configs of the cluster.
     *
     * @return A CompletableFuture containing the configs of the cluster.
     */
    CompletableFuture<Collection<ConfigEntry>> describeClusterConfigs();

    /**
     * Alter the configs of the cluster.
     *
     * @param configs List of configs to alter.
     * @return A CompletableFuture indicating completion of the operation.
     */
    CompletableFuture<Void> alterClusterConfigs(Collection<AlterConfig> configs);

    /**
     * Add server tag to the specified tabletServers, one tabletServer can only have one serverTag.
     *
     * <p>If one tabletServer failed adding tag, none of the tags will take effect.
     *
     * <p>If one tabletServer already has a serverTag, and the serverTag is same with the existing
     * one, this operation will be ignored.
     *
     * <ul>
     *   <li>{@link AuthorizationException} If the authenticated user doesn't have cluster
     *       permissions.
     *   <li>{@link ServerNotExistException} If the tabletServer in {@code tabletServers} does not
     *       exist.
     *   <li>{@link ServerTagAlreadyExistException} If the server tag already exists for any one of
     *       the tabletServers, and the server tag is different from the existing one.
     * </ul>
     *
     * @param tabletServers the tabletServers we want to add server tags.
     * @param serverTag the server tag to be added.
     */
    CompletableFuture<Void> addServerTag(List<Integer> tabletServers, ServerTag serverTag);

    /**
     * Remove server tag from the specified tabletServers.
     *
     * <p>If one tabletServer failed removing tag, none of the tags will be removed.
     *
     * <p>No exception will be thrown if the server already has no any server tag now.
     *
     * <ul>
     *   <li>{@link AuthorizationException} If the authenticated user doesn't have cluster
     *       permissions.
     *   <li>{@link ServerNotExistException} If the tabletServer in {@code tabletServers} does not
     *       exist.
     *   <li>{@link ServerTagNotExistException} If the server tag does not exist for any one of the
     *       tabletServers.
     * </ul>
     *
     * @param tabletServers the tabletServers we want to remove server tags.
     */
    CompletableFuture<Void> removeServerTag(List<Integer> tabletServers, ServerTag serverTag);

    /**
     * Based on the provided {@code priorityGoals}, Fluss performs load balancing on the cluster's
     * bucket load.
     *
     * <p>More details, Fluss collects the cluster's load information and optimizes to perform load
     * balancing according to the user-defined {@code priorityGoals}.
     *
     * <p>Currently, Fluss only supports one active rebalance task in the cluster. If an uncompleted
     * rebalance task exists, Fluss will return the uncompleted rebalance task's progress.
     *
     * <p>If you want to cancel the rebalance task, you can use {@link #cancelRebalance(String)}
     *
     * <ul>
     *   <li>{@link AuthorizationException} If the authenticated user doesn't have cluster
     *       permissions.
     *   <li>{@link RebalanceFailureException} If the rebalance failed. Such as there is an
     *       inProgress execution.
     * </ul>
     *
     * @param priorityGoals the goals to be optimized.
     * @return the rebalance id. If there is no rebalance task in progress, it will trigger a new
     *     rebalance task and return the rebalance id.
     */
    CompletableFuture<String> rebalance(List<GoalType> priorityGoals);

    /**
     * List the rebalance progress.
     *
     * <ul>
     *   <li>{@link AuthorizationException} If the authenticated user doesn't have cluster
     *       permissions.
     *   <li>{@link NoRebalanceInProgressException} If there are no rebalance tasks in progress for
     *       the input rebalanceId.
     * </ul>
     *
     * @param rebalanceId the rebalance id to list progress, if it is null means list the in
     *     progress rebalance task's.
     * @return the rebalance process.
     */
    CompletableFuture<Optional<RebalanceProgress>> listRebalanceProgress(
            @Nullable String rebalanceId);

    /**
     * Cannel the rebalance task.
     *
     * <ul>
     *   <li>{@link AuthorizationException} If the authenticated user doesn't have cluster
     *       permissions.
     *   <li>{@link NoRebalanceInProgressException} If there are no rebalance tasks in progress or
     *       the rebalance id is not exists.
     * </ul>
     *
     * @param rebalanceId the rebalance id to cancel, if it is null means cancel the exists
     *     rebalance task. If rebalanceId is not exists in server, {@link
     *     NoRebalanceInProgressException} will be thrown.
     */
    CompletableFuture<Void> cancelRebalance(@Nullable String rebalanceId);
}
