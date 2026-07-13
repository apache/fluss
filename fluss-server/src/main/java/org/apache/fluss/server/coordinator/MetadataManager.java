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

package org.apache.fluss.server.coordinator;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.DatabaseAlreadyExistException;
import org.apache.fluss.exception.DatabaseNotEmptyException;
import org.apache.fluss.exception.DatabaseNotExistException;
import org.apache.fluss.exception.FlussRuntimeException;
import org.apache.fluss.exception.InvalidAlterTableException;
import org.apache.fluss.exception.InvalidPartitionException;
import org.apache.fluss.exception.LakeTableAlreadyExistException;
import org.apache.fluss.exception.PartitionAlreadyExistsException;
import org.apache.fluss.exception.PartitionNotExistException;
import org.apache.fluss.exception.SchemaNotExistException;
import org.apache.fluss.exception.TableAlreadyExistException;
import org.apache.fluss.exception.TableNotExistException;
import org.apache.fluss.exception.TableNotPartitionedException;
import org.apache.fluss.exception.TooManyBucketsException;
import org.apache.fluss.exception.TooManyPartitionsException;
import org.apache.fluss.lake.lakestorage.LakeCatalog;
import org.apache.fluss.metadata.DataLakeFormat;
import org.apache.fluss.metadata.DatabaseDescriptor;
import org.apache.fluss.metadata.DatabaseInfo;
import org.apache.fluss.metadata.DatabaseSummary;
import org.apache.fluss.metadata.ResolvedPartitionSpec;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.SchemaInfo;
import org.apache.fluss.metadata.TableChange;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.security.acl.FlussPrincipal;
import org.apache.fluss.server.entity.DatabasePropertyChanges;
import org.apache.fluss.server.entity.TablePropertyChanges;
import org.apache.fluss.server.utils.TableDescriptorValidation;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.data.DatabaseRegistration;
import org.apache.fluss.server.zk.data.PartitionAssignment;
import org.apache.fluss.server.zk.data.PartitionRegistration;
import org.apache.fluss.server.zk.data.TableAssignment;
import org.apache.fluss.server.zk.data.TableRegistration;
import org.apache.fluss.shaded.zookeeper3.org.apache.zookeeper.KeeperException;
import org.apache.fluss.utils.function.RunnableWithException;
import org.apache.fluss.utils.function.ThrowingRunnable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import static org.apache.fluss.server.utils.TableDescriptorValidation.validateAlterTableProperties;
import static org.apache.fluss.server.utils.TableDescriptorValidation.validateAlterTableSchema;

/** A manager for metadata. */
public class MetadataManager {

    private static final Logger LOG = LoggerFactory.getLogger(MetadataManager.class);

    /**
     * Max internal retries for the side-effect-free ALTER read-modify-write when a CAS/epoch
     * conflict (BadVersionException) indicates a concurrent metadata change.
     */
    private static final int MAX_ALTER_TABLE_RETRIES = 3;

    /** The Fluss table property carrying a bucket count rescale. */
    private static final String BUCKET_NUM_PROPERTY = "bucket.num";

    private final ZooKeeperClient zookeeperClient;
    private final int maxPartitionNum;
    private final int maxBucketNum;
    private final LakeCatalogDynamicLoader lakeCatalogDynamicLoader;

    /**
     * Per-table lock ensuring ALTER bucket.num (write lock) never overlaps partition creation (read
     * lock) on the same table; fair mode prevents ALTER starvation.
     */
    private final ConcurrentHashMap<TablePath, ReadWriteLock> bucketRescaleLocks =
            new ConcurrentHashMap<>();

    public static final Set<String> SENSITIVE_TABLE_OPTIONS = new HashSet<>();

    static {
        SENSITIVE_TABLE_OPTIONS.add("password");
        SENSITIVE_TABLE_OPTIONS.add("secret");
        SENSITIVE_TABLE_OPTIONS.add("key");
    }

    /**
     * Creates a new metadata manager.
     *
     * @param zookeeperClient the zookeeper client
     * @param conf the cluster configuration
     */
    public MetadataManager(
            ZooKeeperClient zookeeperClient,
            Configuration conf,
            LakeCatalogDynamicLoader lakeCatalogDynamicLoader) {
        this.zookeeperClient = zookeeperClient;
        this.maxPartitionNum = conf.get(ConfigOptions.MAX_PARTITION_NUM);
        this.maxBucketNum = conf.get(ConfigOptions.MAX_BUCKET_NUM);
        this.lakeCatalogDynamicLoader = lakeCatalogDynamicLoader;
    }

    /**
     * Returns the per-table read-write lock guarding table-level bucket.num against partition
     * creation. Partition-creation callers must take the read lock around the whole "read
     * table-level bucket count -&gt; generate assignment -&gt; register partition" span; ALTER
     * bucket.num takes the write lock (done internally by {@link #alterTableProperties}).
     */
    public ReadWriteLock getBucketRescaleLock(TablePath tablePath) {
        return bucketRescaleLocks.computeIfAbsent(tablePath, k -> new ReentrantReadWriteLock(true));
    }

    /** Validates the table descriptor. */
    public void validateTableDescriptor(TableDescriptor tableDescriptor) {
        TableDescriptorValidation.validateTableDescriptor(
                tableDescriptor,
                maxBucketNum,
                lakeCatalogDynamicLoader.getLakeCatalogContainer().getDataLakeFormat());
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

    public void alterDatabaseProperties(
            String databaseName,
            DatabasePropertyChanges databasePropertyChanges,
            boolean ignoreIfNotExists) {
        try {
            // Check if database exists
            if (!databaseExists(databaseName)) {
                if (ignoreIfNotExists) {
                    return;
                }
                throw new DatabaseNotExistException("Database " + databaseName + " not exists.");
            }

            DatabaseRegistration databaseRegistration = getDatabaseRegistration(databaseName);
            DatabaseDescriptor currentDescriptor = databaseRegistration.toDatabaseDescriptor();

            // Create updated descriptor
            DatabaseDescriptor newDescriptor =
                    getUpdatedDatabaseDescriptor(currentDescriptor, databasePropertyChanges);

            if (newDescriptor != null) {
                // Update the database in ZooKeeper
                DatabaseRegistration updatedRegistration =
                        databaseRegistration.newProperties(newDescriptor);
                zookeeperClient.updateDatabase(databaseName, updatedRegistration);
                LOG.info("Successfully altered database properties for database: {}", databaseName);
            } else {
                LOG.info(
                        "No properties changed when alter database {}, skip update.", databaseName);
            }
        } catch (Exception e) {
            if (e instanceof DatabaseNotExistException) {
                if (ignoreIfNotExists) {
                    return;
                }
                throw (DatabaseNotExistException) e;
            } else if (e instanceof RuntimeException) {
                throw (RuntimeException) e;
            } else {
                throw new FlussRuntimeException("Failed to alter database: " + databaseName, e);
            }
        }
    }

    @Nullable
    private DatabaseDescriptor getUpdatedDatabaseDescriptor(
            DatabaseDescriptor currentDescriptor, DatabasePropertyChanges changes) {
        Map<String, String> newCustomProperties =
                new HashMap<>(currentDescriptor.getCustomProperties());
        // set properties
        newCustomProperties.putAll(changes.customPropertiesToSet);
        // reset properties
        newCustomProperties.keySet().removeAll(changes.customPropertiesToReset);

        if (newCustomProperties.equals(currentDescriptor.getCustomProperties())
                && changes.commentToSet == null) {
            return null;
        }

        String newComment;
        if (changes.commentToSet != null) {
            // If comment is set to empty string, it means to reset the comment
            if (changes.commentToSet.isEmpty()) {
                newComment = null;
            } else {
                newComment = changes.commentToSet;
            }
        } else {
            newComment = currentDescriptor.getComment().orElse(null);
        }

        return DatabaseDescriptor.builder()
                .customProperties(newCustomProperties)
                .comment(newComment)
                .build();
    }

    public DatabaseInfo getDatabase(String databaseName) throws DatabaseNotExistException {
        DatabaseRegistration databaseReg = getDatabaseRegistration(databaseName);
        return new DatabaseInfo(
                databaseName,
                databaseReg.toDatabaseDescriptor(),
                databaseReg.createdTime,
                databaseReg.modifiedTime);
    }

    public DatabaseRegistration getDatabaseRegistration(String databaseName) {
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
        return optionalDB.get();
    }

    public boolean databaseExists(String databaseName) {
        return uncheck(
                () -> zookeeperClient.databaseExists(databaseName),
                "Fail to check database exists or not");
    }

    public List<String> listDatabases() {
        return uncheck(zookeeperClient::listDatabases, "Fail to list database");
    }

    public List<DatabaseSummary> listDatabaseSummaries(Collection<String> databaseNames) {
        return uncheck(
                () -> zookeeperClient.listDatabaseSummaries(databaseNames),
                "Fail to get database summaries for " + databaseNames);
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
     * @return a map from partition name to partition registration.
     */
    public Map<String, PartitionRegistration> listPartitions(TablePath tablePath)
            throws TableNotExistException, TableNotPartitionedException {
        return listPartitions(tablePath, null);
    }

    /**
     * List the partitions of the given table and partitionSpec.
     *
     * @return a map from partition name to partition registration.
     */
    public Map<String, PartitionRegistration> listPartitions(
            TablePath tablePath, ResolvedPartitionSpec partitionFilter)
            throws TableNotExistException, TableNotPartitionedException, InvalidPartitionException {
        TableInfo tableInfo = getTable(tablePath);
        if (!tableInfo.isPartitioned()) {
            throw new TableNotPartitionedException(
                    "Table '" + tablePath + "' is not a partitioned table.");
        }
        try {
            if (partitionFilter == null) {
                return zookeeperClient.getPartitionRegistrations(tablePath);
            } else {

                return zookeeperClient.getPartitionRegistrations(
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
        if (CoordinatorServer.DEFAULT_DATABASE.equals(name)) {
            throw new UnsupportedOperationException(
                    "Cannot drop the default database '"
                            + name
                            + "'. The default database is required for cluster operation.");
        }
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

        // drop the per-table bucket-rescale lock so we don't leak lock objects for dropped tables
        bucketRescaleLocks.remove(tablePath);
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
     * @param remoteDataDir the remote data directory
     * @param tableToCreate the table descriptor describing the table to create
     * @param tableAssignment the table assignment, will be null when the table is partitioned table
     * @param ignoreIfExists whether to ignore if the table already exists
     * @return the table id
     */
    public long createTable(
            TablePath tablePath,
            String remoteDataDir,
            TableDescriptor tableToCreate,
            @Nullable TableAssignment tableAssignment,
            boolean ignoreIfExists)
            throws TableAlreadyExistException, DatabaseNotExistException {
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
            zookeeperClient.registerFirstSchema(tablePath, tableToCreate.getSchema());
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
                            tablePath,
                            TableRegistration.newTable(tableId, remoteDataDir, tableToCreate),
                            false);
                    return tableId;
                },
                "Fail to create table " + tablePath);
    }

    public void alterTableSchema(
            TablePath tablePath,
            List<TableChange> schemaChanges,
            boolean ignoreIfNotExists,
            FlussPrincipal flussPrincipal)
            throws TableNotExistException, TableNotPartitionedException {
        try {

            TableInfo table = getTable(tablePath);
            TableDescriptor tableDescriptor = table.toTableDescriptor();

            // validate the table column changes
            if (!schemaChanges.isEmpty()) {
                Schema newSchema =
                        SchemaUpdate.applySchemaChanges(table.getSchema(), schemaChanges);
                validateAlterTableSchema(table, newSchema);
                LakeCatalog.Context lakeCatalogContext =
                        new CoordinatorService.DefaultLakeCatalogContext(
                                false,
                                flussPrincipal,
                                tableDescriptor,
                                TableDescriptor.builder(tableDescriptor).schema(newSchema).build());
                // Lake First: sync to Lake before updating Fluss schema
                syncSchemaChangesToLake(tablePath, table, schemaChanges, lakeCatalogContext);

                // Update Fluss schema (ZK) after Lake sync succeeds
                if (!newSchema.equals(table.getSchema())) {
                    zookeeperClient.registerSchema(tablePath, newSchema, table.getSchemaId() + 1);
                } else {
                    LOG.info(
                            "Skipping schema evolution for table {} because the column(s) to add {} already exist.",
                            tablePath,
                            schemaChanges);
                }
            }
        } catch (Exception e) {
            if (e instanceof TableNotExistException) {
                if (ignoreIfNotExists) {
                    return;
                }
                throw (TableNotExistException) e;
            } else if (e instanceof RuntimeException) {
                throw (RuntimeException) e;
            } else {
                throw new FlussRuntimeException("Failed to alter table schema: " + tablePath, e);
            }
        }
    }

    private void syncSchemaChangesToLake(
            TablePath tablePath,
            TableInfo tableInfo,
            List<TableChange> schemaChanges,
            LakeCatalog.Context lakeCatalogContext) {
        if (!isDataLakeEnabled(tableInfo.toTableDescriptor())) {
            return;
        }

        LakeCatalog lakeCatalog =
                lakeCatalogDynamicLoader.getLakeCatalogContainer().getLakeCatalog();
        if (lakeCatalog == null) {
            throw new InvalidAlterTableException(
                    "Cannot alter schema for datalake enabled table "
                            + tablePath
                            + ", because the Fluss cluster doesn't enable datalake tables.");
        }

        try {
            lakeCatalog.alterTable(tablePath, schemaChanges, lakeCatalogContext);
        } catch (TableNotExistException e) {
            throw new FlussRuntimeException(
                    "Lake table doesn't exist for lake-enabled table "
                            + tablePath
                            + ", which shouldn't happen. Please check if the lake table was deleted manually.",
                    e);
        }
    }

    private void propagateBucketCountToLake(
            TablePath tablePath,
            TableInfo tableInfo,
            int newBucketCount,
            FlussPrincipal flussPrincipal) {
        if (!isDataLakeEnabled(tableInfo.toTableDescriptor())) {
            return;
        }
        // Paimon only tracks a bucket count for Fixed Bucket tables (bucket-key non-empty).
        if (tableInfo.getBucketKeys().isEmpty()) {
            return;
        }
        LakeCatalog lakeCatalog =
                lakeCatalogDynamicLoader.getLakeCatalogContainer().getLakeCatalog();
        if (lakeCatalog == null) {
            throw new FlussRuntimeException(
                    "Cannot propagate ALTER bucket.num to the lake side for table "
                            + tablePath
                            + " because the Fluss cluster does not have a lake catalog configured.");
        }
        // The propagation travels through the unified alterTable channel as a "bucket.num"
        TableDescriptor currentDescriptor = tableInfo.toTableDescriptor();
        List<TableChange> bucketCountChange =
                Collections.singletonList(
                        TableChange.set(BUCKET_NUM_PROPERTY, String.valueOf(newBucketCount)));
        LakeCatalog.Context lakeCatalogContext =
                new CoordinatorService.DefaultLakeCatalogContext(
                        false, flussPrincipal, currentDescriptor, currentDescriptor);
        // Lake First: this runs BEFORE the Fluss ZK commit, so a lake failure aborts the ALTER
        // with the Fluss side unchanged.
        try {
            lakeCatalog.alterTable(tablePath, bucketCountChange, lakeCatalogContext);
        } catch (TableNotExistException e) {
            throw new FlussRuntimeException(
                    "Lake table doesn't exist for lake-enabled table "
                            + tablePath
                            + ", which shouldn't happen. Please check if the lake table was deleted manually.",
                    e);
        } catch (Exception e) {
            throw new FlussRuntimeException(
                    String.format(
                            "ALTER bucket.num for table %s was aborted: propagating the new "
                                    + "bucket count (%d) to the lake schema failed. The Fluss "
                                    + "side was NOT changed. Re-run the same ALTER once the "
                                    + "lake is reachable.",
                            tablePath, newBucketCount),
                    e);
        }
    }

    /**
     * Validates an ALTER bucket.num request: only partitioned tables are supported and the new
     * value must fall within [1, maxBucketNum]. Runs before the lake-side propagation so an invalid
     * ALTER never mutates lake metadata.
     */
    private void validateBucketNumRescale(
            TablePath tablePath, TableInfo tableInfo, int newBucketNum) {
        // Non-partitioned tables require creating new bucket assignments and initializing
        // LogTablets on TabletServers, which is not yet implemented.
        if (tableInfo.getPartitionKeys().isEmpty()) {
            throw new InvalidAlterTableException(
                    String.format(
                            "Cannot alter 'bucket.num' on non-partitioned table %s. "
                                    + "Non-partitioned table rescale is not yet supported.",
                            tablePath));
        }
        if (newBucketNum < 1) {
            throw new InvalidAlterTableException(
                    String.format(
                            "Cannot alter 'bucket.num' to %d on table %s. "
                                    + "The bucket count must be at least 1.",
                            newBucketNum, tablePath));
        }
        if (newBucketNum > maxBucketNum) {
            throw new TooManyBucketsException(
                    String.format(
                            "Cannot alter 'bucket.num' to %d on table %s, "
                                    + "exceeding the maximum of %d buckets per partition.",
                            newBucketNum, tablePath, maxBucketNum));
        }
    }

    /** Alters table properties and invokes the callbacks around the metadata update. */
    public void alterTableProperties(
            TablePath tablePath,
            List<TableChange> tableChanges,
            TablePropertyChanges tablePropertyChanges,
            boolean ignoreIfNotExists,
            FlussPrincipal flussPrincipal,
            BiConsumer<TableInfo, TableDescriptor> beforeUpdate,
            BiConsumer<TableInfo, TableDescriptor> afterUpdate,
            int coordinatorEpochZkVersion) {
        String newBucketNumStr =
                tablePropertyChanges.customPropertiesToSet.remove(BUCKET_NUM_PROPERTY);
        Integer newBucketNum = newBucketNumStr == null ? null : Integer.parseInt(newBucketNumStr);
        boolean bucketNumRescale = newBucketNum != null;
        // bucket.num travels to the lake through the dedicated lake-first propagation below;
        // exclude it from the changes handed to the regular lake sync to avoid a second delivery.
        List<TableChange> remainingTableChanges = tableChanges;
        if (bucketNumRescale) {
            remainingTableChanges =
                    tableChanges.stream()
                            .filter(
                                    change ->
                                            !(change instanceof TableChange.SetOption
                                                    && BUCKET_NUM_PROPERTY.equals(
                                                            ((TableChange.SetOption) change)
                                                                    .getKey())))
                            .collect(Collectors.toList());
        }
        ReadWriteLock rescaleLock = getBucketRescaleLock(tablePath);
        if (bucketNumRescale) {
            rescaleLock.writeLock().lock();
        }
        try {
            boolean lakePropagated = false;
            int attempt = 0;
            while (true) {
                try {
                    // Lake First (like alterTableSchema): a lake failure aborts the ALTER with
                    // Fluss unchanged; if ZK fails afterwards, the idempotent propagation lets a
                    // re-run converge both sides.
                    if (bucketNumRescale && !lakePropagated) {
                        TableInfo preAlterTableInfo = getTable(tablePath);
                        validateBucketNumRescale(tablePath, preAlterTableInfo, newBucketNum);
                        propagateBucketCountToLake(
                                tablePath, preAlterTableInfo, newBucketNum, flussPrincipal);
                        lakePropagated = true;
                    }
                    doAlterTablePropertiesOnce(
                            tablePath,
                            remainingTableChanges,
                            tablePropertyChanges,
                            newBucketNum,
                            flussPrincipal,
                            beforeUpdate,
                            afterUpdate,
                            coordinatorEpochZkVersion);
                    return;
                } catch (TableNotExistException e) {
                    if (ignoreIfNotExists) {
                        return;
                    }
                    throw e;
                } catch (KeeperException.BadVersionException e) {
                    // A CAS/epoch conflict means our snapshot was stale. Only the side-effect-free
                    // bucket.num backfill path lets a BadVersionException reach here.
                    if (++attempt >= MAX_ALTER_TABLE_RETRIES) {
                        throw new FlussRuntimeException(
                                String.format(
                                        "Failed to alter table properties for %s after %d retries "
                                                + "due to concurrent metadata changes; please retry.",
                                        tablePath, attempt),
                                e);
                    }
                    LOG.info(
                            "Retrying ALTER on table {} due to a concurrent metadata change "
                                    + "(attempt {}).",
                            tablePath,
                            attempt);
                } catch (RuntimeException e) {
                    throw e;
                } catch (Exception e) {
                    throw new FlussRuntimeException(
                            "Failed to alter table properties: " + tablePath, e);
                }
            }
        } finally {
            if (bucketNumRescale) {
                rescaleLock.writeLock().unlock();
            }
        }
    }

    /**
     * One attempt of the ALTER read-modify-write. All ZK writes are CAS-guarded by the table ZK
     * version read here plus the coordinator epoch version, so a stale snapshot or a deposed
     * coordinator fails with {@link KeeperException.BadVersionException} instead of committing.
     *
     * <p>Only the side-effect-free pure bucket.num path lets {@code BadVersionException} propagate
     * for the caller to retry; paths running {@link #preAlterTableProperties} (external lake side
     * effects) surface the conflict as a non-retried {@link FlussRuntimeException}.
     */
    private void doAlterTablePropertiesOnce(
            TablePath tablePath,
            List<TableChange> tableChanges,
            TablePropertyChanges tablePropertyChanges,
            @Nullable Integer newBucketNum,
            FlussPrincipal flussPrincipal,
            BiConsumer<TableInfo, TableDescriptor> beforeUpdate,
            BiConsumer<TableInfo, TableDescriptor> afterUpdate,
            int coordinatorEpochZkVersion)
            throws Exception {
        // it throws TableNotExistException if the table or database not exists
        ZooKeeperClient.VersionedData<TableRegistration> versionedTableReg =
                getTableRegistrationWithVersion(tablePath);
        TableRegistration tableReg = versionedTableReg.data();
        int tableZkVersion = versionedTableReg.zkVersion();
        SchemaInfo schemaInfo = getLatestSchema(tablePath);
        // we can't use MetadataManager#getTable here, because it will add the default
        // lake options to the table properties, which may cause the validation failure
        TableInfo tableInfo = tableReg.toTableInfo(tablePath, schemaInfo);

        // Old-partition bucket.num.actual backfill to be committed atomically with the
        // table-level bucket.num update; stays empty unless bucket.num is being changed.
        Map<String, ZooKeeperClient.VersionedData<PartitionRegistration>>
                partitionBucketCountBackfills = Collections.emptyMap();

        if (newBucketNum != null) {
            // TODO: bucket-layout ALTERs should be rejected during a rolling server
            //  upgrade. Until that is enforced by the server, the supported upgrade procedure
            //  is: upgrade clients first, prohibit ALTER bucket.num while the servers are being
            //  rolled, and enable ALTER only after every server is upgraded.
            // If bucket.num is being changed on a partitioned table, compute the backfill
            // of old partitions' current actual bucket count.
            partitionBucketCountBackfills = computePartitionBucketCountBackfill(tablePath);

            // Update the structural bucketCount field and increment bucketLayoutEpoch
            tableReg = tableReg.withBucketCount(newBucketNum);
        }

        // validate the changes
        validateAlterTableProperties(tableInfo, tablePropertyChanges.tableKeysToChange());

        TableDescriptor tableDescriptor = tableInfo.toTableDescriptor();
        TableDescriptor newDescriptor =
                getUpdatedTableDescriptor(tableDescriptor, tablePropertyChanges);

        if (newDescriptor != null) {
            // is to enable datalake for the table
            if (isDataLakeEnabled(newDescriptor) && !isDataLakeEnabled(tableDescriptor)) {
                // The table was created before cluster-level datalake was enabled.
                // Backfill `table.datalake.format` before enabling datalake on the table
                // so the updated table metadata stays consistent with the cluster setting.
                if (!tableInfo.getTableConfig().getDataLakeFormat().isPresent()) {
                    DataLakeFormat dataLakeFormat =
                            lakeCatalogDynamicLoader.getLakeCatalogContainer().getDataLakeFormat();
                    if (dataLakeFormat == null) {
                        throw new InvalidAlterTableException(
                                "Cannot alter table "
                                        + tablePath
                                        + " in data lake, because the Fluss cluster doesn't enable datalake tables.");
                    }
                    newDescriptor = newDescriptor.withDataLakeFormat(dataLakeFormat);
                }
            }

            if (newBucketNum != null) {
                // Lake-First propagation is a no-op while the table is not yet lake-enabled, so
                // enabling datalake here must create the lake table with the new bucket count.
                newDescriptor = newDescriptor.withBucketCount(newBucketNum);
            }

            // reuse the same validate logic with the createTable() method
            validateTableDescriptor(newDescriptor);

            beforeUpdate.accept(tableInfo, newDescriptor);

            // pre alter table properties, e.g. create lake table in lake storage if it's to
            // enable datalake for the table. NOTE: this may have external (lake catalog) side
            // effects and is therefore NOT safe to auto-retry.
            preAlterTableProperties(
                    tablePath, tableDescriptor, newDescriptor, tableChanges, flussPrincipal);

            // update the table to zk, together with the (possibly empty) partition backfill in
            // one atomic transaction
            TableRegistration updatedTableRegistration =
                    tableReg.newProperties(
                            newDescriptor.getProperties(), newDescriptor.getCustomProperties());
            try {
                zookeeperClient.updateTableWithPartitionBucketCountBackfill(
                        tablePath,
                        updatedTableRegistration,
                        tableZkVersion,
                        partitionBucketCountBackfills,
                        coordinatorEpochZkVersion);
            } catch (KeeperException.BadVersionException e) {
                // preAlterTableProperties above may have applied external lake side effects, so we
                // must NOT auto-retry. Surface as a retriable failure for the operator/client.
                throw new FlussRuntimeException(
                        String.format(
                                "Concurrent metadata change while altering table %s; the change was "
                                        + "not committed, please retry the ALTER.",
                                tablePath),
                        e);
            }
            afterUpdate.accept(tableInfo, newDescriptor);
        } else if (newBucketNum != null) {
            // Pure bucket.num change (side-effect-free): commit backfill + table-level update in
            // one atomic transaction; BadVersionException propagates for the caller to retry.
            zookeeperClient.updateTableWithPartitionBucketCountBackfill(
                    tablePath,
                    tableReg,
                    tableZkVersion,
                    partitionBucketCountBackfills,
                    coordinatorEpochZkVersion);
        } else {
            LOG.info("No properties changed when alter table {}, skip update table.", tablePath);
        }
    }

    /**
     * Compute the bucket-count backfill (derived from assignment size) for existing partitions
     * lacking one. Nothing is written here: the caller commits the returned registrations together
     * with the table-level bucket.num update in a single ZK transaction, CAS-guarded by the
     * versions captured here, so old partitions never observe the new table-level value without
     * their own bucket.num.actual.
     *
     * <p>Idempotent: partitions that already have a persisted bucket count are skipped.
     */
    private Map<String, ZooKeeperClient.VersionedData<PartitionRegistration>>
            computePartitionBucketCountBackfill(TablePath tablePath) {
        try {
            Map<String, ZooKeeperClient.VersionedData<PartitionRegistration>> backfills =
                    new HashMap<>();
            Set<String> partitionNames = zookeeperClient.getPartitions(tablePath);
            for (String partitionName : partitionNames) {
                Optional<ZooKeeperClient.VersionedData<PartitionRegistration>> optReg =
                        zookeeperClient.getPartitionWithVersion(tablePath, partitionName);
                if (!optReg.isPresent()) {
                    // A partial backfill would leave this partition routed by the NEW table-level
                    // value; fail the whole ALTER instead.
                    throw new InvalidAlterTableException(
                            String.format(
                                    "Cannot alter 'bucket.num' on table %s: partition '%s' is "
                                            + "listed but its registration is missing. Please "
                                            + "resolve the metadata inconsistency and retry the "
                                            + "ALTER.",
                                    tablePath, partitionName));
                }
                PartitionRegistration reg = optReg.get().data();
                int partitionZkVersion = optReg.get().zkVersion();
                if (reg.getBucketCount() != null) {
                    // Already has bucket count persisted, skip. Idempotent so retries are safe.
                    continue;
                }
                // Derive bucket count from assignment size
                long partitionId = reg.getPartitionId();
                Optional<PartitionAssignment> optAssignment =
                        zookeeperClient.getPartitionAssignment(partitionId);
                if (!optAssignment.isPresent()) {
                    // Registration exists but assignment does not — same risk as above.
                    throw new InvalidAlterTableException(
                            String.format(
                                    "Cannot alter 'bucket.num' on table %s: partition '%s' "
                                            + "(id=%d) has no readable bucket assignment. Please "
                                            + "resolve the metadata inconsistency and retry the "
                                            + "ALTER.",
                                    tablePath, partitionName, partitionId));
                }
                int actualBucketCount = optAssignment.get().getBucketAssignments().size();
                PartitionRegistration updatedReg =
                        new PartitionRegistration(
                                reg.getTableId(),
                                reg.getPartitionId(),
                                reg.getRemoteDataDir(),
                                actualBucketCount);
                backfills.put(
                        partitionName,
                        new ZooKeeperClient.VersionedData<>(updatedReg, partitionZkVersion));
            }
            return backfills;
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new FlussRuntimeException(
                    "Failed to compute partition bucket count backfill for table: " + tablePath, e);
        }
    }

    private void preAlterTableProperties(
            TablePath tablePath,
            TableDescriptor tableDescriptor,
            TableDescriptor newDescriptor,
            List<TableChange> tableChanges,
            FlussPrincipal flussPrincipal) {
        LakeCatalog.Context lakeCatalogContext =
                new CoordinatorService.DefaultLakeCatalogContext(
                        false, flussPrincipal, tableDescriptor, newDescriptor);
        LakeCatalog lakeCatalog =
                lakeCatalogDynamicLoader.getLakeCatalogContainer().getLakeCatalog();

        if (isDataLakeEnabled(newDescriptor)) {
            if (lakeCatalog == null) {
                throw new InvalidAlterTableException(
                        "Cannot alter table "
                                + tablePath
                                + " in data lake, because the Fluss cluster doesn't enable datalake tables.");
            }

            // to enable lake table
            if (!isDataLakeEnabled(tableDescriptor)) {
                // before create table in fluss, we may create in lake
                try {
                    lakeCatalog.createTable(tablePath, newDescriptor, lakeCatalogContext);
                } catch (TableAlreadyExistException e) {
                    throw new LakeTableAlreadyExistException(e.getMessage(), e);
                }
            }
        }

        // We should always alter lake table even though datalake is disabled.
        // Otherwise, if user alter the fluss table when datalake is disabled, then enable datalake
        // again, the lake table will mismatch.
        // Only sync to lake if this table has ever opted into datalake (key present regardless of
        // value).
        if (lakeCatalog != null
                && tableDescriptor
                        .getProperties()
                        .containsKey(ConfigOptions.TABLE_DATALAKE_ENABLED.key())) {
            try {
                lakeCatalog.alterTable(tablePath, tableChanges, lakeCatalogContext);
            } catch (TableNotExistException e) {
                // only throw TableNotExistException if datalake is enabled
                if (isDataLakeEnabled(newDescriptor)) {
                    throw new FlussRuntimeException(
                            "Lake table doesn't exist for lake-enabled table "
                                    + tablePath
                                    + ", which shouldn't be happened. Please check if the lake table was deleted manually.",
                            e);
                }
            }
        }
    }

    /**
     * Get a new TableDescriptor with updated properties.
     *
     * @param tableDescriptor the current table descriptor.
     * @param tablePropertyChanges the changes for the table properties
     * @return the updated TableDescriptor, or null if no properties updated.
     */
    private @Nullable TableDescriptor getUpdatedTableDescriptor(
            TableDescriptor tableDescriptor, TablePropertyChanges tablePropertyChanges) {
        Map<String, String> newProperties = new HashMap<>(tableDescriptor.getProperties());
        Map<String, String> newCustomProperties =
                new HashMap<>(tableDescriptor.getCustomProperties());

        // set properties
        newProperties.putAll(tablePropertyChanges.tablePropertiesToSet);
        newCustomProperties.putAll(tablePropertyChanges.customPropertiesToSet);

        // reset properties
        for (String key : tablePropertyChanges.tablePropertiesToReset) {
            newProperties.remove(key);
        }

        for (String key : tablePropertyChanges.customPropertiesToReset) {
            newCustomProperties.remove(key);
        }

        // no properties change happen
        if (newProperties.equals(tableDescriptor.getProperties())
                && newCustomProperties.equals(tableDescriptor.getCustomProperties())) {
            return null;
        } else {
            return tableDescriptor.withProperties(newProperties, newCustomProperties);
        }
    }

    private boolean isDataLakeEnabled(TableDescriptor tableDescriptor) {
        String dataLakeEnabledValue =
                tableDescriptor.getProperties().get(ConfigOptions.TABLE_DATALAKE_ENABLED.key());
        return Boolean.parseBoolean(dataLakeEnabledValue);
    }

    public void removeSensitiveTableOptions(Map<String, String> tableLakeOptions) {
        if (tableLakeOptions == null || tableLakeOptions.isEmpty()) {
            return;
        }

        Iterator<Map.Entry<String, String>> iterator = tableLakeOptions.entrySet().iterator();
        while (iterator.hasNext()) {
            String key = iterator.next().getKey().toLowerCase();
            if (SENSITIVE_TABLE_OPTIONS.stream().anyMatch(key::contains)) {
                iterator.remove();
            }
        }
    }

    public TableInfo getTable(TablePath tablePath) throws TableNotExistException {
        Optional<TableRegistration> optionalTable;
        try {
            optionalTable = zookeeperClient.getTable(tablePath);
        } catch (Exception e) {
            throw new FlussRuntimeException(
                    String.format("Failed to get table '%s'.", tablePath), e);
        }
        if (!optionalTable.isPresent()) {
            throw new TableNotExistException("Table '" + tablePath + "' does not exist.");
        }
        TableRegistration tableReg = optionalTable.get();
        SchemaInfo schemaInfo = getLatestSchema(tablePath);
        Map<String, String> defaultTableLakeOptions =
                lakeCatalogDynamicLoader.getLakeCatalogContainer().getDefaultTableLakeOptions();
        // Create a copy to avoid ConcurrentModificationException when multiple threads
        // call getTable() concurrently, as defaultTableLakeOptions is a shared instance
        Map<String, String> tableLakeOptions =
                defaultTableLakeOptions != null ? new HashMap<>(defaultTableLakeOptions) : null;
        removeSensitiveTableOptions(tableLakeOptions);
        return tableReg.toTableInfo(tablePath, schemaInfo, tableLakeOptions);
    }

    public Map<TablePath, TableInfo> getTables(Collection<TablePath> tablePaths)
            throws TableNotExistException {
        Map<TablePath, TableInfo> result = new HashMap<>();
        try {
            Map<TablePath, TableRegistration> tablePath2TableRegistrations =
                    zookeeperClient.getTables(tablePaths);
            // currently, we don't support schema evolution, so all schemas are version 1
            Map<TablePath, SchemaInfo> tablePath2SchemaInfos =
                    zookeeperClient.getLatestSchemas(tablePaths);
            for (TablePath tablePath : tablePaths) {
                if (!tablePath2TableRegistrations.containsKey(tablePath)) {
                    throw new TableNotExistException("Table '" + tablePath + "' does not exist.");
                }
                if (!tablePath2SchemaInfos.containsKey(tablePath)) {
                    throw new SchemaNotExistException(
                            "Schema for '" + tablePath + "' does not exist.");
                }
                TableRegistration tableReg = tablePath2TableRegistrations.get(tablePath);
                SchemaInfo schemaInfo = tablePath2SchemaInfos.get(tablePath);

                result.put(
                        tablePath,
                        tableReg.toTableInfo(
                                tablePath,
                                schemaInfo,
                                lakeCatalogDynamicLoader
                                        .getLakeCatalogContainer()
                                        .getDefaultTableLakeOptions()));
            }
        } catch (Exception e) {
            throw new FlussRuntimeException(
                    String.format("Failed to get tables '%s'.", tablePaths), e);
        }
        return result;
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

    /**
     * Reads the table registration together with the ZK version of its znode, for a subsequent
     * compare-and-set write. Throws {@link TableNotExistException} when the table does not exist.
     */
    private ZooKeeperClient.VersionedData<TableRegistration> getTableRegistrationWithVersion(
            TablePath tablePath) {
        Optional<ZooKeeperClient.VersionedData<TableRegistration>> optionalTable;
        try {
            optionalTable = zookeeperClient.getTableWithVersion(tablePath);
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
            String remoteDataDir,
            PartitionAssignment partitionAssignment,
            ResolvedPartitionSpec partition,
            boolean ignoreIfExists,
            int bucketCount) {
        String partitionName = partition.getPartitionName();
        Optional<PartitionRegistration> optionalPartitionRegistration =
                getOptionalPartitionRegistration(tablePath, partitionName);
        if (optionalPartitionRegistration.isPresent()) {
            if (ignoreIfExists) {
                return;
            }
            throw new PartitionAlreadyExistsException(
                    String.format(
                            "Partition '%s' already exists for table %s",
                            partition.getPartitionQualifiedName(), tablePath));
        }

        try {
            int partitionNumber = zookeeperClient.getPartitionNumber(tablePath);
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

        int assignmentBucketCount = partitionAssignment.getBucketAssignments().size();
        if (assignmentBucketCount > maxBucketNum) {
            throw new TooManyBucketsException(
                    String.format(
                            "Partition '%s' has %d buckets for table %s, exceeding the maximum of %d buckets per partition.",
                            partition.getPartitionName(),
                            assignmentBucketCount,
                            tablePath,
                            maxBucketNum));
        }

        try {
            long partitionId = zookeeperClient.getPartitionIdAndIncrement();
            // register partition assignments and partition metadata to zk in transaction
            zookeeperClient.registerPartitionAssignmentAndMetadata(
                    partitionId,
                    partitionName,
                    partitionAssignment,
                    remoteDataDir,
                    tablePath,
                    tableId,
                    bucketCount);
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
        Optional<PartitionRegistration> optionalPartitionRegistration =
                getOptionalPartitionRegistration(tablePath, partitionName);
        if (!optionalPartitionRegistration.isPresent()) {
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
            throw new FlussRuntimeException(
                    String.format(
                            "Fail to delete partition '%s' from zookeeper for table %s.",
                            partitionName, tablePath),
                    e);
        }
    }

    Optional<PartitionRegistration> getOptionalPartitionRegistration(
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
