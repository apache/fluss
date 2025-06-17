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

package com.alibaba.fluss.server.coordinator;

import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.exception.DatabaseAlreadyExistException;
import com.alibaba.fluss.exception.DatabaseNotEmptyException;
import com.alibaba.fluss.exception.DatabaseNotExistException;
import com.alibaba.fluss.exception.FlussRuntimeException;
import com.alibaba.fluss.exception.InvalidPartitionException;
import com.alibaba.fluss.exception.PartitionAlreadyExistsException;
import com.alibaba.fluss.exception.PartitionNotExistException;
import com.alibaba.fluss.exception.SchemaNotExistException;
import com.alibaba.fluss.exception.TableAlreadyExistException;
import com.alibaba.fluss.exception.TableNotExistException;
import com.alibaba.fluss.exception.TableNotPartitionedException;
import com.alibaba.fluss.exception.TooManyBucketsException;
import com.alibaba.fluss.exception.TooManyPartitionsException;
import com.alibaba.fluss.metadata.DatabaseDescriptor;
import com.alibaba.fluss.metadata.DatabaseInfo;
import com.alibaba.fluss.metadata.ResolvedPartitionSpec;
import com.alibaba.fluss.metadata.SchemaInfo;
import com.alibaba.fluss.metadata.TableDescriptor;
import com.alibaba.fluss.metadata.TableInfo;
import com.alibaba.fluss.metadata.TablePartition;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.server.utils.LakeStorageUtils;
import com.alibaba.fluss.server.zk.ZooKeeperClient;
import com.alibaba.fluss.server.zk.data.DatabaseRegistration;
import com.alibaba.fluss.server.zk.data.PartitionAssignment;
import com.alibaba.fluss.server.zk.data.TableAssignment;
import com.alibaba.fluss.server.zk.data.TableRegistration;
import com.alibaba.fluss.shaded.zookeeper3.org.apache.zookeeper.KeeperException;
import com.alibaba.fluss.utils.function.RunnableWithException;
import com.alibaba.fluss.utils.function.ThrowingRunnable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;

import static com.alibaba.fluss.server.utils.TableDescriptorValidation.validateTableDescriptor;

/** A manager for metadata. */
public class MetadataManager {

    private static final Logger LOG = LoggerFactory.getLogger(MetadataManager.class);

    private final ZooKeeperClient zookeeperClient;
    private @Nullable final Map<String, String> defaultTableLakeOptions;
    private final int maxPartitionNum;
    private final int maxBucketNum;

    /**
     * Creates a new metadata manager.
     *
     * @param zookeeperClient the zookeeper client
     * @param conf the cluster configuration
     */
    public MetadataManager(ZooKeeperClient zookeeperClient, Configuration conf) {
        this.zookeeperClient = zookeeperClient;
        this.defaultTableLakeOptions = LakeStorageUtils.generateDefaultTableLakeOptions(conf);
        this.maxPartitionNum = conf.get(ConfigOptions.MAX_PARTITION_NUM);
        this.maxBucketNum = conf.get(ConfigOptions.MAX_BUCKET_NUM);
    }

    public void createDatabase(
            String databaseName, DatabaseDescriptor databaseDescriptor, boolean ignoreIfExists)
            throws DatabaseAlreadyExistException {
        if (databaseExists(databaseName)) {
            if (ignoreIfExists) {
                return;
            }
            throw new DatabaseAlreadyExistException(
                    "Database " + databaseName + " already exists.");
        }

        DatabaseRegistration databaseRegistration = DatabaseRegistration.of(databaseDescriptor);
        try {
            zookeeperClient.registerDatabase(databaseName, databaseRegistration);
        } catch (Exception e) {
            if (e instanceof KeeperException.NodeExistsException) {
                if (ignoreIfExists) {
                    return;
                }
                throw new DatabaseAlreadyExistException(
                        "Database " + databaseName + " already exists.");
            } else {
                throw new FlussRuntimeException("Failed to create database: " + databaseName, e);
            }
        }
    }

    public DatabaseInfo getDatabase(String databaseName) throws DatabaseNotExistException {

        Optional<DatabaseRegistration> optionalDB;
        try {
            optionalDB = zookeeperClient.getDatabase(databaseName);
        } catch (Exception e) {
            throw new FlussRuntimeException(
                    String.format("Fail to get database '%s'.", databaseName), e);
        }

        if (!optionalDB.isPresent()) {
            throw new DatabaseNotExistException("Database '" + databaseName + "' does not exist.");
        }

        DatabaseRegistration databaseReg = optionalDB.get();
        return new DatabaseInfo(
                databaseName,
                databaseReg.toDatabaseDescriptor(),
                databaseReg.createdTime,
                databaseReg.modifiedTime);
    }

    public boolean databaseExists(String databaseName) {
        return uncheck(
                () -> zookeeperClient.databaseExists(databaseName),
                "Fail to check database exists or not");
    }

    public List<String> listDatabases() {
        return uncheck(zookeeperClient::listDatabases, "Fail to list database");
    }

    public List<String> listTables(String databaseName) throws DatabaseNotExistException {
        if (!databaseExists(databaseName)) {
            throw new DatabaseNotExistException("Database " + databaseName + " does not exist.");
        }
        return uncheck(
                () -> zookeeperClient.listTables(databaseName),
                "Fail to list tables for database:" + databaseName);
    }

    /**
     * List the partitions of the given table.
     *
     * <p>Return a map from partition name to partition id.
     */
    public Map<String, Long> listPartitions(TablePath tablePath)
            throws TableNotExistException, TableNotPartitionedException {
        return listPartitions(tablePath, null);
    }

    /**
     * List the partitions of the given table and partitionSpec.
     *
     * <p>Return a map from partition name to partition id.
     */
    public Map<String, Long> listPartitions(
            TablePath tablePath, ResolvedPartitionSpec partitionFilter)
            throws TableNotExistException, TableNotPartitionedException, InvalidPartitionException {
        TableInfo tableInfo = getTable(tablePath);
        if (!tableInfo.isPartitioned()) {
            throw new TableNotPartitionedException(
                    "Table '" + tablePath + "' is not a partitioned table.");
        }
        try {
            if (partitionFilter == null) {
                return zookeeperClient.getPartitionNameAndIds(tablePath);
            } else {

                return zookeeperClient.getPartitionNameAndIds(
                        tablePath, tableInfo.getPartitionKeys(), partitionFilter);
            }
        } catch (Exception e) {
            throw new FlussRuntimeException(
                    String.format(
                            "Fail to list partitions for table: %s, partitionSpec: %s.",
                            tablePath, partitionFilter),
                    e);
        }
    }

    public void dropDatabase(String name, boolean ignoreIfNotExists, boolean cascade)
            throws DatabaseNotExistException, DatabaseNotEmptyException {
        if (!databaseExists(name)) {
            if (ignoreIfNotExists) {
                return;
            }
            throw new DatabaseNotExistException("Database " + name + " does not exist.");
        }
        if (!cascade && !listTables(name).isEmpty()) {
            throw new DatabaseNotEmptyException("Database " + name + " is not empty.");
        }

        uncheck(() -> zookeeperClient.deleteDatabase(name), "Fail to drop database: " + name);
    }

    public void dropTable(TablePath tablePath, boolean ignoreIfNotExists)
            throws TableNotExistException {
        if (!tableExists(tablePath)) {
            if (ignoreIfNotExists) {
                return;
            }
            throw new TableNotExistException("Table " + tablePath + " does not exist.");
        }

        // in here, we just delete the table node in zookeeper, which will then trigger
        // the physical deletion in tablet servers and assignments in zk
        uncheck(() -> zookeeperClient.deleteTable(tablePath), "Fail to drop table: " + tablePath);
    }

    public void completeDeleteTable(long tableId) {
        // final step for delete a table.
        // delete bucket assignments node, which will also delete the bucket state node,
        // so that all the zk nodes related to this table are deleted.
        rethrowIfIsNotNoNodeException(
                () -> zookeeperClient.deleteTableAssignment(tableId),
                String.format("Delete tablet assignment meta fail for table %s.", tableId));
    }

    public void completeDeletePartition(long partitionId) {
        // final step for delete a partition.
        // delete partition assignments node, which will also delete the bucket state node,
        // so that all the zk nodes related to this partition are deleted.
        rethrowIfIsNotNoNodeException(
                () -> zookeeperClient.deletePartitionAssignment(partitionId),
                String.format("Delete tablet assignment meta fail for partition %s.", partitionId));
    }

    /**
     * Creates the necessary metadata of the given table in zookeeper and return the table id.
     * Returns -1 if the table already exists and ignoreIfExists is true.
     *
     * @param tablePath the table path
     * @param tableToCreate the table descriptor describing the table to create
     * @param tableAssignment the table assignment, will be null when the table is partitioned table
     * @param ignoreIfExists whether to ignore if the table already exists
     * @return the table id
     */
    public long createTable(
            TablePath tablePath,
            TableDescriptor tableToCreate,
            @Nullable TableAssignment tableAssignment,
            boolean ignoreIfExists)
            throws TableAlreadyExistException, DatabaseNotExistException {
        // validate table properties before creating table
        validateTableDescriptor(tableToCreate, maxBucketNum);

        if (!databaseExists(tablePath.getDatabaseName())) {
            throw new DatabaseNotExistException(
                    "Database " + tablePath.getDatabaseName() + " does not exist.");
        }
        if (tableExists(tablePath)) {
            if (ignoreIfExists) {
                return -1;
            } else {
                throw new TableAlreadyExistException("Table " + tablePath + " already exists.");
            }
        }

        // register schema to zk
        // first register a schema to the zk, if then register the table
        // to zk fails, there's no harm to register a new schema to zk again
        try {
            zookeeperClient.registerSchema(tablePath, tableToCreate.getSchema());
        } catch (Exception e) {
            throw new FlussRuntimeException(
                    "Fail to register schema when creating table " + tablePath, e);
        }

        // register the table, we have registered the schema whose path have contained the node for
        // the table, then we won't need to create the node to store the table
        return uncheck(
                () -> {
                    // generate a table id
                    long tableId = zookeeperClient.getTableIdAndIncrement();
                    if (tableAssignment != null) {
                        // register table assignment
                        zookeeperClient.registerTableAssignment(tableId, tableAssignment);
                    }
                    // register the table
                    zookeeperClient.registerTable(
                            tablePath, TableRegistration.newTable(tableId, tableToCreate), false);
                    return tableId;
                },
                "Fail to create table " + tablePath);
    }

    public TableInfo getTable(TablePath tablePath) throws TableNotExistException {
        Optional<TableRegistration> optionalTable;
        try {
            optionalTable = zookeeperClient.getTable(tablePath);
        } catch (Exception e) {
            throw new FlussRuntimeException(String.format("Fail to get table '%s'.", tablePath), e);
        }
        if (!optionalTable.isPresent()) {
            throw new TableNotExistException("Table '" + tablePath + "' does not exist.");
        }
        TableRegistration tableReg = optionalTable.get();
        SchemaInfo schemaInfo = getLatestSchema(tablePath);
        return tableReg.toTableInfo(tablePath, schemaInfo, defaultTableLakeOptions);
    }

    public TableRegistration getTableRegistration(TablePath tablePath) {
        Optional<TableRegistration> optionalTable;
        try {
            optionalTable = zookeeperClient.getTable(tablePath);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        if (!optionalTable.isPresent()) {
            throw new TableNotExistException("Table '" + tablePath + "' does not exist.");
        }
        return optionalTable.get();
    }

    public SchemaInfo getLatestSchema(TablePath tablePath) throws SchemaNotExistException {
        final int currentSchemaId;
        try {
            currentSchemaId = zookeeperClient.getCurrentSchemaId(tablePath);
        } catch (Exception e) {
            throw new FlussRuntimeException(
                    "Failed to get latest schema id of table " + tablePath, e);
        }
        return getSchemaById(tablePath, currentSchemaId);
    }

    public SchemaInfo getSchemaById(TablePath tablePath, int schemaId)
            throws SchemaNotExistException {
        Optional<SchemaInfo> optionalSchema;
        try {
            optionalSchema = zookeeperClient.getSchemaById(tablePath, schemaId);
        } catch (Exception e) {
            throw new FlussRuntimeException(
                    String.format("Fail to get schema of %s for table %s", schemaId, tablePath), e);
        }
        if (optionalSchema.isPresent()) {
            return optionalSchema.get();
        } else {
            throw new SchemaNotExistException(
                    "Schema for table "
                            + tablePath
                            + " with schema id "
                            + schemaId
                            + " does not exist.");
        }
    }

    public boolean tableExists(TablePath tablePath) {
        // check the path of the table exists
        return uncheck(
                () -> zookeeperClient.tableExist(tablePath),
                String.format("Fail to check the table %s exist or not.", tablePath));
    }

    public long initWriterId() {
        return uncheck(
                zookeeperClient::getWriterIdAndIncrement, "Fail to get writer id from zookeeper");
    }

    public Set<String> getPartitions(TablePath tablePath) {
        return uncheck(
                () -> zookeeperClient.getPartitions(tablePath),
                "Fail to get partitions from zookeeper for table " + tablePath);
    }

    public void createPartition(
            TablePath tablePath,
            long tableId,
            PartitionAssignment partitionAssignment,
            ResolvedPartitionSpec partition,
            boolean ignoreIfExists) {
        String partitionName = partition.getPartitionName();
        Optional<TablePartition> optionalTablePartition =
                getOptionalTablePartition(tablePath, partitionName);
        if (optionalTablePartition.isPresent()) {
            if (ignoreIfExists) {
                return;
            }
            throw new PartitionAlreadyExistsException(
                    String.format(
                            "Partition '%s' already exists for table %s",
                            partition.getPartitionQualifiedName(), tablePath));
        }

        final int partitionNumber;
        try {
            partitionNumber = zookeeperClient.getPartitionNumber(tablePath);
            if (partitionNumber + 1 > maxPartitionNum) {
                throw new TooManyPartitionsException(
                        String.format(
                                "Exceed the maximum number of partitions for table %s, only allow %s partitions.",
                                tablePath, maxPartitionNum));
            }
        } catch (TooManyPartitionsException e) {
            throw e;
        } catch (Exception e) {
            throw new FlussRuntimeException(
                    String.format(
                            "Get the number of partition from zookeeper failed for table %s",
                            tablePath),
                    e);
        }

        try {
            int bucketCount = partitionAssignment.getBucketAssignments().size();
            // currently, every partition has the same bucket count
            int totalBuckets = bucketCount * (partitionNumber + 1);
            if (totalBuckets > maxBucketNum) {
                throw new TooManyBucketsException(
                        String.format(
                                "Adding partition '%s' would result in %d total buckets for table %s, exceeding the maximum of %d buckets.",
                                partition.getPartitionName(),
                                totalBuckets,
                                tablePath,
                                maxBucketNum));
            }
        } catch (TooManyBucketsException e) {
            throw e;
        } catch (Exception e) {
            throw new FlussRuntimeException(
                    String.format("Failed to check total bucket count for table %s", tablePath), e);
        }

        try {
            long partitionId = zookeeperClient.getPartitionIdAndIncrement();
            // register partition assignments to zk first
            zookeeperClient.registerPartitionAssignment(partitionId, partitionAssignment);
            // then register the partition metadata to zk
            zookeeperClient.registerPartition(tablePath, tableId, partitionName, partitionId);
            LOG.info(
                    "Register partition {} to zookeeper for table [{}].", partitionName, tablePath);
        } catch (KeeperException.NodeExistsException nodeExistsException) {
            if (!ignoreIfExists) {
                throw new PartitionAlreadyExistsException(
                        String.format(
                                "Partition '%s' already exists for table %s",
                                partition.getPartitionQualifiedName(), tablePath));
            }
        } catch (Exception e) {
            throw new FlussRuntimeException(
                    String.format(
                            "Register partition to zookeeper failed to create partition %s for table [%s]",
                            partitionName, tablePath),
                    e);
        }
    }

    public void dropPartition(
            TablePath tablePath, ResolvedPartitionSpec partition, boolean ignoreIfNotExists) {
        String partitionName = partition.getPartitionName();
        Optional<TablePartition> optionalTablePartition =
                getOptionalTablePartition(tablePath, partitionName);
        if (!optionalTablePartition.isPresent()) {
            if (ignoreIfNotExists) {
                return;
            }

            throw new PartitionNotExistException(
                    String.format(
                            "Partition '%s' does not exist for table %s",
                            partition.getPartitionQualifiedName(), tablePath));
        }

        try {
            zookeeperClient.deletePartition(tablePath, partitionName);
        } catch (Exception e) {
            LOG.error(
                    "Fail to delete partition '{}' from zookeeper for table {}.",
                    partitionName,
                    tablePath,
                    e);
        }
    }

    private Optional<TablePartition> getOptionalTablePartition(
            TablePath tablePath, String partitionName) {
        try {
            return zookeeperClient.getPartition(tablePath, partitionName);
        } catch (Exception e) {
            throw new FlussRuntimeException(
                    String.format(
                            "Fail to get partition '%s' of table %s from zookeeper.",
                            tablePath, partitionName),
                    e);
        }
    }

    private void rethrowIfIsNotNoNodeException(
            ThrowingRunnable<Exception> throwingRunnable, String exceptionMessage) {
        try {
            throwingRunnable.run();
        } catch (KeeperException.NoNodeException e) {
            // ignore
        } catch (Exception e) {
            throw new FlussRuntimeException(exceptionMessage, e);
        }
    }

    private static <T> T uncheck(Callable<T> callable, String errorMsg) {
        try {
            return callable.call();
        } catch (Exception e) {
            throw new FlussRuntimeException(errorMsg, e);
        }
    }

    private static void uncheck(RunnableWithException runnable, String errorMsg) {
        try {
            runnable.run();
        } catch (Exception e) {
            throw new FlussRuntimeException(errorMsg, e);
        }
    }
}
