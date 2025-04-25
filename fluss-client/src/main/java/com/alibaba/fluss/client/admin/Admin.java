/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.client.admin;

import com.alibaba.fluss.annotation.PublicEvolving;
import com.alibaba.fluss.client.metadata.KvSnapshotMetadata;
import com.alibaba.fluss.client.metadata.KvSnapshots;
import com.alibaba.fluss.client.metadata.LakeSnapshot;
import com.alibaba.fluss.cluster.ServerNode;
import com.alibaba.fluss.exception.DatabaseAlreadyExistException;
import com.alibaba.fluss.exception.DatabaseNotEmptyException;
import com.alibaba.fluss.exception.DatabaseNotExistException;
import com.alibaba.fluss.exception.InvalidDatabaseException;
import com.alibaba.fluss.exception.InvalidPartitionException;
import com.alibaba.fluss.exception.InvalidReplicationFactorException;
import com.alibaba.fluss.exception.InvalidTableException;
import com.alibaba.fluss.exception.KvSnapshotNotExistException;
import com.alibaba.fluss.exception.NonPrimaryKeyTableException;
import com.alibaba.fluss.exception.PartitionAlreadyExistsException;
import com.alibaba.fluss.exception.PartitionNotExistException;
import com.alibaba.fluss.exception.SchemaNotExistException;
import com.alibaba.fluss.exception.TableAlreadyExistException;
import com.alibaba.fluss.exception.TableNotExistException;
import com.alibaba.fluss.exception.TableNotPartitionedException;
import com.alibaba.fluss.metadata.DatabaseDescriptor;
import com.alibaba.fluss.metadata.DatabaseInfo;
import com.alibaba.fluss.metadata.PartitionInfo;
import com.alibaba.fluss.metadata.PartitionSpec;
import com.alibaba.fluss.metadata.ResolvedPartitionSpec;
import com.alibaba.fluss.metadata.SchemaInfo;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.metadata.TableDescriptor;
import com.alibaba.fluss.metadata.TableInfo;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.security.acl.AclBinding;
import com.alibaba.fluss.security.acl.AclBindingFilter;

import java.util.Collection;
import java.util.List;
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
}
