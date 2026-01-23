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

package org.apache.fluss.flink.catalog;

import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.InvalidTableException;
import org.apache.fluss.flink.adapter.CatalogTableAdapter;
import org.apache.fluss.flink.lake.LakeFlinkCatalog;
import org.apache.fluss.flink.procedure.ProcedureManager;
import org.apache.fluss.flink.utils.CatalogExceptionUtils;
import org.apache.fluss.flink.utils.FlinkConversions;
import org.apache.fluss.metadata.DatabaseDescriptor;
import org.apache.fluss.metadata.PartitionInfo;
import org.apache.fluss.metadata.PartitionSpec;
import org.apache.fluss.metadata.TableChange;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.utils.ExceptionUtils;
import org.apache.fluss.utils.IOUtils;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.AbstractCatalog;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogDatabase;
import org.apache.flink.table.catalog.CatalogDatabaseImpl;
import org.apache.flink.table.catalog.CatalogFunction;
import org.apache.flink.table.catalog.CatalogMaterializedTable;
import org.apache.flink.table.catalog.CatalogPartition;
import org.apache.flink.table.catalog.CatalogPartitionSpec;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.CatalogView;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.ResolvedCatalogBaseTable;
import org.apache.flink.table.catalog.ResolvedCatalogMaterializedTable;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.DatabaseAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotEmptyException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.FunctionAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.FunctionNotExistException;
import org.apache.flink.table.catalog.exceptions.PartitionAlreadyExistsException;
import org.apache.flink.table.catalog.exceptions.PartitionNotExistException;
import org.apache.flink.table.catalog.exceptions.PartitionSpecInvalidException;
import org.apache.flink.table.catalog.exceptions.ProcedureNotExistException;
import org.apache.flink.table.catalog.exceptions.TableAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.catalog.exceptions.TableNotPartitionedException;
import org.apache.flink.table.catalog.exceptions.TablePartitionedException;
import org.apache.flink.table.catalog.stats.CatalogColumnStatistics;
import org.apache.flink.table.catalog.stats.CatalogTableStatistics;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.factories.Factory;
import org.apache.flink.table.procedures.Procedure;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.apache.flink.table.api.DataTypes.BIGINT;
import static org.apache.flink.table.api.DataTypes.STRING;
import static org.apache.flink.table.api.DataTypes.TIMESTAMP_LTZ;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.fluss.config.ConfigOptions.BOOTSTRAP_SERVERS;
import static org.apache.fluss.flink.FlinkConnectorOptions.ALTER_DISALLOW_OPTIONS;
import static org.apache.fluss.flink.adapter.SchemaAdapter.supportIndex;
import static org.apache.fluss.flink.adapter.SchemaAdapter.withIndex;
import static org.apache.fluss.flink.utils.CatalogExceptionUtils.isPartitionAlreadyExists;
import static org.apache.fluss.flink.utils.CatalogExceptionUtils.isPartitionInvalid;
import static org.apache.fluss.flink.utils.CatalogExceptionUtils.isPartitionNotExist;
import static org.apache.fluss.flink.utils.CatalogExceptionUtils.isTableInvalid;
import static org.apache.fluss.flink.utils.CatalogExceptionUtils.isTableNotExist;
import static org.apache.fluss.flink.utils.CatalogExceptionUtils.isTableNotPartitioned;
import static org.apache.fluss.flink.utils.FlinkConversions.toFlussDatabase;

/**
 * A Flink Catalog for fluss.
 *
 * <p>Currently, this class must extend the internal Flink class {@link AbstractCatalog} because an
 * incompatibility bug ( <a
 * href="https://issues.apache.org/jira/browse/FLINK-38030">FLINK-38030</a>) in flink 2.0.0.
 *
 * <p>TODO: Once this issue is resolved in a future version of Flink (likely 2.1+), refactor this
 * class to implement the public interface {@link org.apache.flink.table.catalog.Catalog} instead of
 * extending the internal class {@link AbstractCatalog}.
 */
public class FlinkCatalog extends AbstractCatalog {

    public static final String LAKE_TABLE_SPLITTER = "$lake";
    public static final String CHANGELOG_TABLE_SUFFIX = "$changelog";
    public static final String BINLOG_TABLE_SUFFIX = "$binlog";

    protected final ClassLoader classLoader;

    protected final String catalogName;
    protected final String defaultDatabase;
    protected final String bootstrapServers;
    protected final Map<String, String> securityConfigs;
    protected final LakeFlinkCatalog lakeFlinkCatalog;
    protected volatile Map<String, String> lakeCatalogProperties;
    protected final Supplier<Map<String, String>> lakeCatalogPropertiesSupplier;
    protected Connection connection;
    protected Admin admin;

    public FlinkCatalog(
            String name,
            String defaultDatabase,
            String bootstrapServers,
            ClassLoader classLoader,
            Map<String, String> securityConfigs,
            Supplier<Map<String, String>> lakeCatalogPropertiesSupplier) {
        this(
                name,
                defaultDatabase,
                bootstrapServers,
                classLoader,
                securityConfigs,
                lakeCatalogPropertiesSupplier,
                new LakeFlinkCatalog(name, classLoader));
    }

    @VisibleForTesting
    public FlinkCatalog(
            String name,
            String defaultDatabase,
            String bootstrapServers,
            ClassLoader classLoader,
            Map<String, String> securityConfigs,
            Supplier<Map<String, String>> lakeCatalogPropertiesSupplier,
            LakeFlinkCatalog lakeFlinkCatalog) {
        super(name, defaultDatabase);
        this.catalogName = name;
        this.defaultDatabase = defaultDatabase;
        this.bootstrapServers = bootstrapServers;
        this.classLoader = classLoader;
        this.securityConfigs = securityConfigs;
        this.lakeCatalogPropertiesSupplier = lakeCatalogPropertiesSupplier;
        this.lakeFlinkCatalog = lakeFlinkCatalog;
    }

    @Override
    public Optional<Factory> getFactory() {
        return Optional.of(new FlinkTableFactory(lakeFlinkCatalog));
    }

    @Override
    public void open() throws CatalogException {
        Map<String, String> flussConfigs = new HashMap<>();
        flussConfigs.put(ConfigOptions.BOOTSTRAP_SERVERS.key(), bootstrapServers);
        flussConfigs.putAll(securityConfigs);

        connection = ConnectionFactory.createConnection(Configuration.fromMap(flussConfigs));
        admin = connection.getAdmin();
        if (!databaseExists(defaultDatabase)) {
            throw new CatalogException(
                    String.format(
                            "The configured default-database '%s' does not exist in the Fluss cluster.",
                            defaultDatabase));
        }
    }

    @Override
    public void close() throws CatalogException {
        IOUtils.closeQuietly(admin, "fluss-admin");
        IOUtils.closeQuietly(connection, "fluss-connection");
        IOUtils.closeQuietly(lakeFlinkCatalog, "fluss-lake-catalog");
    }

    @Override
    public List<String> listDatabases() throws CatalogException {
        try {
            return admin.listDatabases().get();
        } catch (Exception e) {
            throw new CatalogException(
                    String.format("Failed to list all databases in %s", getName()),
                    ExceptionUtils.stripExecutionException(e));
        }
    }

    @Override
    public CatalogDatabase getDatabase(String databaseName)
            throws DatabaseNotExistException, CatalogException {
        if (!databaseExists(databaseName)) {
            throw new DatabaseNotExistException(getName(), databaseName);
        }
        try {
            DatabaseDescriptor databaseDescriptor =
                    admin.getDatabaseInfo(databaseName).get().getDatabaseDescriptor();
            return new CatalogDatabaseImpl(
                    databaseDescriptor.getCustomProperties(),
                    databaseDescriptor.getComment().orElse(null));
        } catch (Exception e) {
            throw new CatalogException(
                    String.format("Failed to get database %s in %s", databaseName, getName()),
                    ExceptionUtils.stripExecutionException(e));
        }
    }

    @Override
    public boolean databaseExists(String databaseName) throws CatalogException {
        try {
            return admin.databaseExists(databaseName).get();
        } catch (Exception e) {
            throw new CatalogException(
                    String.format(
                            "Failed to check if database %s exists in %s", databaseName, getName()),
                    ExceptionUtils.stripExecutionException(e));
        }
    }

    @Override
    public void createDatabase(
            String databaseName, CatalogDatabase database, boolean ignoreIfExists)
            throws DatabaseAlreadyExistException, CatalogException {
        try {
            admin.createDatabase(databaseName, toFlussDatabase(database), ignoreIfExists).get();
        } catch (Exception e) {
            Throwable t = ExceptionUtils.stripExecutionException(e);
            if (CatalogExceptionUtils.isDatabaseAlreadyExist(t)) {
                throw new DatabaseAlreadyExistException(getName(), databaseName);
            } else {
                throw new CatalogException(
                        String.format(
                                "Failed to create database %s in %s", databaseName, getName()),
                        t);
            }
        }
    }

    @Override
    public void dropDatabase(String databaseName, boolean ignoreIfNotExists, boolean cascade)
            throws DatabaseNotExistException, DatabaseNotEmptyException, CatalogException {
        try {
            admin.dropDatabase(databaseName, ignoreIfNotExists, cascade).get();
        } catch (Exception e) {
            Throwable t = ExceptionUtils.stripExecutionException(e);
            if (CatalogExceptionUtils.isDatabaseNotExist(t)) {
                throw new DatabaseNotExistException(getName(), databaseName);
            } else if (CatalogExceptionUtils.isDatabaseNotEmpty(t)) {
                throw new DatabaseNotEmptyException(getName(), databaseName);
            } else {
                throw new CatalogException(
                        String.format("Failed to drop database %s in %s", databaseName, getName()),
                        t);
            }
        }
    }

    @Override
    public void alterDatabase(String databaseName, CatalogDatabase catalogDatabase, boolean b)
            throws DatabaseNotExistException, CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<String> listTables(String databaseName)
            throws DatabaseNotExistException, CatalogException {
        try {
            return admin.listTables(databaseName).get();
        } catch (Exception e) {
            Throwable t = ExceptionUtils.stripExecutionException(e);
            if (CatalogExceptionUtils.isDatabaseNotExist(t)) {
                throw new DatabaseNotExistException(getName(), databaseName);
            }
            throw new CatalogException(
                    String.format(
                            "Failed to list all tables in database %s in %s",
                            databaseName, getName()),
                    t);
        }
    }

    @Override
    public List<String> listViews(String s) throws DatabaseNotExistException, CatalogException {
        return Collections.emptyList();
    }

    @Override
    public CatalogBaseTable getTable(ObjectPath objectPath)
            throws TableNotExistException, CatalogException {
        // may be should be as a datalake table
        String tableName = objectPath.getObjectName();

        // Check if this is a virtual table ($changelog or $binlog)
        if (tableName.endsWith(CHANGELOG_TABLE_SUFFIX)
                && !tableName.contains(LAKE_TABLE_SPLITTER)) {
            return getVirtualChangelogTable(objectPath);
        } else if (tableName.endsWith(BINLOG_TABLE_SUFFIX)
                && !tableName.contains(LAKE_TABLE_SPLITTER)) {
            // TODO: Implement binlog virtual table in future
            throw new UnsupportedOperationException(
                    String.format(
                            "$binlog virtual tables are not yet supported for table %s",
                            objectPath));
        }

        TablePath tablePath = toTablePath(objectPath);
        try {
            TableInfo tableInfo;
            // table name contains $lake, means to read from datalake
            if (tableName.contains(LAKE_TABLE_SPLITTER)) {
                tableInfo =
                        admin.getTableInfo(
                                        TablePath.of(
                                                objectPath.getDatabaseName(),
                                                tableName.split("\\" + LAKE_TABLE_SPLITTER)[0]))
                                .get();
                // we need to make sure the table enable datalake
                if (!tableInfo.getTableConfig().isDataLakeEnabled()) {
                    throw new UnsupportedOperationException(
                            String.format(
                                    "Table %s is not datalake enabled.",
                                    TablePath.of(
                                            objectPath.getDatabaseName(),
                                            tableName.split("\\" + LAKE_TABLE_SPLITTER)[0])));
                }

                return getLakeTable(
                        objectPath.getDatabaseName(),
                        tableName,
                        tableInfo.getProperties(),
                        getLakeCatalogProperties());
            } else {
                tableInfo = admin.getTableInfo(tablePath).get();
            }

            // should be as a fluss table
            CatalogBaseTable catalogBaseTable = FlinkConversions.toFlinkTable(tableInfo);
            // add bootstrap servers option
            Map<String, String> newOptions = new HashMap<>(catalogBaseTable.getOptions());
            newOptions.put(BOOTSTRAP_SERVERS.key(), bootstrapServers);
            newOptions.putAll(securityConfigs);
            // add lake properties
            if (tableInfo.getTableConfig().isDataLakeEnabled()) {
                for (Map.Entry<String, String> lakePropertyEntry :
                        getLakeCatalogProperties().entrySet()) {
                    String key = "table.datalake." + lakePropertyEntry.getKey();
                    newOptions.put(key, lakePropertyEntry.getValue());
                }
            }
            if (CatalogBaseTable.TableKind.TABLE == catalogBaseTable.getTableKind()) {
                CatalogTable table = ((CatalogTable) catalogBaseTable).copy(newOptions);
                if (supportIndex()) {
                    table = wrapWithIndexes(table, tableInfo);
                }
                return table;
            } else if (CatalogBaseTable.TableKind.MATERIALIZED_TABLE
                    == catalogBaseTable.getTableKind()) {
                return ((CatalogMaterializedTable) catalogBaseTable).copy(newOptions);
            } else {
                throw new CatalogException(
                        String.format(
                                "Failed to get table %s in %s, only CatalogTable and CatalogMaterializedTable are supported.",
                                objectPath, getName()));
            }
        } catch (Exception e) {
            Throwable t = ExceptionUtils.stripExecutionException(e);
            if (isTableNotExist(t)) {
                throw new TableNotExistException(getName(), objectPath);
            } else {
                throw new CatalogException(
                        String.format("Failed to get table %s in %s", objectPath, getName()), t);
            }
        }
    }

    protected CatalogBaseTable getLakeTable(
            String databaseName,
            String tableName,
            Configuration properties,
            Map<String, String> lakeCatalogProperties)
            throws TableNotExistException, CatalogException {
        String[] tableComponents = tableName.split("\\" + LAKE_TABLE_SPLITTER);
        if (tableComponents.length == 1) {
            // should be pattern like table_name$lake
            tableName = tableComponents[0];
        } else {
            // pattern is table_name$lake$snapshots
            // Need to reconstruct: table_name + $snapshots
            tableName = String.join("", tableComponents);
        }
        return lakeFlinkCatalog
                .getLakeCatalog(properties, lakeCatalogProperties)
                .getTable(new ObjectPath(databaseName, tableName));
    }

    @Override
    public boolean tableExists(ObjectPath objectPath) throws CatalogException {
        TablePath tablePath = toTablePath(objectPath);
        try {
            return admin.tableExists(tablePath).get();
        } catch (Exception e) {
            throw new CatalogException(
                    String.format(
                            "Failed to check if table %s exists in %s", objectPath, getName()),
                    ExceptionUtils.stripExecutionException(e));
        }
    }

    @Override
    public void dropTable(ObjectPath objectPath, boolean ignoreIfNotExists)
            throws TableNotExistException, CatalogException {
        TablePath tablePath = toTablePath(objectPath);
        try {
            admin.dropTable(tablePath, ignoreIfNotExists).get();
        } catch (Exception e) {
            Throwable t = ExceptionUtils.stripExecutionException(e);
            if (isTableNotExist(t)) {
                throw new TableNotExistException(getName(), objectPath);
            } else {
                throw new CatalogException(
                        String.format("Failed to drop table %s in %s", objectPath, getName()), t);
            }
        }
    }

    @Override
    public void renameTable(ObjectPath objectPath, String s, boolean b)
            throws TableNotExistException, TableAlreadyExistException, CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void createTable(ObjectPath objectPath, CatalogBaseTable table, boolean ignoreIfExist)
            throws TableAlreadyExistException, DatabaseNotExistException, CatalogException {
        if (table instanceof CatalogView) {
            throw new UnsupportedOperationException(
                    "CREATE [TEMPORARY] VIEW is not supported for Fluss catalog");
        }

        checkArgument(
                table instanceof ResolvedCatalogTable
                        || table instanceof ResolvedCatalogMaterializedTable,
                "table should be resolved");

        TablePath tablePath = toTablePath(objectPath);
        TableDescriptor tableDescriptor =
                FlinkConversions.toFlussTable((ResolvedCatalogBaseTable<?>) table);
        try {
            admin.createTable(tablePath, tableDescriptor, ignoreIfExist).get();
        } catch (Exception e) {
            Throwable t = ExceptionUtils.stripExecutionException(e);
            if (CatalogExceptionUtils.isDatabaseNotExist(t)) {
                throw new DatabaseNotExistException(getName(), objectPath.getDatabaseName());
            } else if (CatalogExceptionUtils.isTableAlreadyExist(t)) {
                throw new TableAlreadyExistException(getName(), objectPath);
            } else if (CatalogExceptionUtils.isLakeTableAlreadyExist(t)) {
                throw new CatalogException(t.getMessage());
            } else if (isTableInvalid(t)) {
                throw new InvalidTableException(t.getMessage());
            } else {
                throw new CatalogException(
                        String.format("Failed to create table %s in %s", objectPath, getName()), t);
            }
        }
    }

    @Override
    public void alterTable(
            ObjectPath objectPath,
            CatalogBaseTable newTable,
            List<org.apache.flink.table.catalog.TableChange> tableChanges,
            boolean ignoreIfNotExists)
            throws TableNotExistException, CatalogException {
        TablePath tablePath = toTablePath(objectPath);

        List<TableChange> flussTableChanges =
                tableChanges.stream()
                        .filter(Objects::nonNull)
                        .flatMap(change -> FlinkConversions.toFlussTableChanges(change).stream())
                        .collect(Collectors.toList());

        // some connector options are table storage related, not allowed to alter
        for (TableChange change : flussTableChanges) {
            String key = null;
            if (change instanceof TableChange.SetOption) {
                key = ((TableChange.SetOption) change).getKey();
            } else if (change instanceof TableChange.ResetOption) {
                key = ((TableChange.ResetOption) change).getKey();
            }
            if (key != null && ALTER_DISALLOW_OPTIONS.contains(key)) {
                throw new CatalogException(
                        "The option '" + key + "' is not supported to alter yet.");
            }
        }

        try {
            admin.alterTable(tablePath, flussTableChanges, ignoreIfNotExists).get();
        } catch (Exception e) {
            Throwable t = ExceptionUtils.stripExecutionException(e);
            if (CatalogExceptionUtils.isTableNotExist(t)) {
                throw new TableNotExistException(getName(), objectPath);
            } else if (isTableInvalid(t)) {
                throw new InvalidTableException(t.getMessage());
            } else {
                throw new CatalogException(
                        String.format("Failed to alter table %s in %s", objectPath, getName()), t);
            }
        }
    }

    @Override
    public void alterTable(
            ObjectPath objectPath, CatalogBaseTable newTable, boolean ignoreIfNotExist)
            throws TableNotExistException, CatalogException {
        throw new UnsupportedOperationException(
                "alterTable(objectPath, newTable, ignoreIfNotExist) method is not supported, please upgrade your Flink to 1.18+. ");
    }

    @SuppressWarnings("checkstyle:WhitespaceAround")
    @Override
    public List<CatalogPartitionSpec> listPartitions(ObjectPath objectPath)
            throws TableNotExistException, TableNotPartitionedException, CatalogException {
        List<CatalogPartitionSpec> catalogPartitionSpecs = new ArrayList<>();
        try {
            catalogPartitionSpecs = listPartitions(objectPath, null);
        } catch (TableNotExistException | TableNotPartitionedException | CatalogException e) {
            throw e;
        } catch (PartitionSpecInvalidException t) {
            // do nothing
        }
        return catalogPartitionSpecs;
    }

    @Override
    public List<CatalogPartitionSpec> listPartitions(
            ObjectPath objectPath, CatalogPartitionSpec catalogPartitionSpec)
            throws TableNotExistException, TableNotPartitionedException,
                    PartitionSpecInvalidException, CatalogException {

        // TODO lake table should support.
        if (objectPath.getObjectName().contains(LAKE_TABLE_SPLITTER)) {
            return Collections.emptyList();
        }

        try {
            TablePath tablePath = toTablePath(objectPath);
            List<PartitionInfo> partitionInfos;
            if (catalogPartitionSpec != null) {
                Map<String, String> partitionSpec = catalogPartitionSpec.getPartitionSpec();
                partitionInfos =
                        admin.listPartitionInfos(tablePath, new PartitionSpec(partitionSpec)).get();
            } else {
                partitionInfos = admin.listPartitionInfos(tablePath).get();
            }
            List<CatalogPartitionSpec> catalogPartitionSpecs = new ArrayList<>();
            for (PartitionInfo partitionInfo : partitionInfos) {
                catalogPartitionSpecs.add(
                        new CatalogPartitionSpec(partitionInfo.getPartitionSpec().getSpecMap()));
            }
            return catalogPartitionSpecs;
        } catch (Exception e) {
            Throwable t = ExceptionUtils.stripExecutionException(e);
            if (isTableNotExist(t)) {
                throw new TableNotExistException(getName(), objectPath);
            } else if (isTableNotPartitioned(t)) {
                throw new TableNotPartitionedException(getName(), objectPath);
            } else if (isPartitionInvalid(t) && catalogPartitionSpec != null) {
                throw new PartitionSpecInvalidException(
                        getName(), new ArrayList<>(), objectPath, catalogPartitionSpec);
            } else {
                throw new CatalogException(
                        String.format(
                                "Failed to list partitions of table %s in %s, by partitionSpec %s",
                                objectPath, getName(), catalogPartitionSpec),
                        t);
            }
        }
    }

    @Override
    public List<CatalogPartitionSpec> listPartitionsByFilter(
            ObjectPath objectPath, List<Expression> list)
            throws TableNotExistException, TableNotPartitionedException, CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public CatalogPartition getPartition(
            ObjectPath objectPath, CatalogPartitionSpec catalogPartitionSpec)
            throws PartitionNotExistException, CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean partitionExists(ObjectPath objectPath, CatalogPartitionSpec catalogPartitionSpec)
            throws CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void createPartition(
            ObjectPath objectPath,
            CatalogPartitionSpec catalogPartitionSpec,
            CatalogPartition catalogPartition,
            boolean b)
            throws TableNotExistException, TableNotPartitionedException,
                    PartitionSpecInvalidException, PartitionAlreadyExistsException,
                    CatalogException {
        TablePath tablePath = toTablePath(objectPath);
        PartitionSpec partitionSpec = new PartitionSpec(catalogPartitionSpec.getPartitionSpec());
        try {
            admin.createPartition(tablePath, partitionSpec, b).get();
        } catch (Exception e) {
            Throwable t = ExceptionUtils.stripExecutionException(e);
            if (isTableNotExist(t)) {
                throw new TableNotExistException(getName(), objectPath);
            } else if (isTableNotPartitioned(t)) {
                throw new TableNotPartitionedException(getName(), objectPath);
            } else if (isPartitionInvalid(t)) {
                List<String> partitionKeys = null;
                try {
                    TableInfo tableInfo = admin.getTableInfo(tablePath).get();
                    partitionKeys = tableInfo.getPartitionKeys();
                } catch (Exception ee) {
                    // ignore
                }
                if (partitionKeys != null
                        && !new HashSet<>(partitionKeys)
                                .containsAll(catalogPartitionSpec.getPartitionSpec().keySet())) {
                    // throw specific partition exception if getting partition keys success.
                    throw new PartitionSpecInvalidException(
                            getName(), partitionKeys, objectPath, catalogPartitionSpec, e);
                } else {
                    // throw general exception
                    throw new CatalogException(
                            "The partition value is invalid, " + e.getMessage(), e);
                }
            } else if (isPartitionAlreadyExists(t)) {
                throw new PartitionAlreadyExistsException(
                        getName(), objectPath, catalogPartitionSpec);
            } else {
                throw new CatalogException(
                        String.format(
                                "Failed to create partition with partition spec %s of table %s in %s",
                                catalogPartitionSpec, objectPath, getName()),
                        t);
            }
        }
    }

    @Override
    public void dropPartition(
            ObjectPath objectPath, CatalogPartitionSpec catalogPartitionSpec, boolean b)
            throws PartitionNotExistException, CatalogException {
        PartitionSpec partitionSpec = new PartitionSpec(catalogPartitionSpec.getPartitionSpec());
        try {
            admin.dropPartition(toTablePath(objectPath), partitionSpec, b).get();
        } catch (Exception e) {
            Throwable t = ExceptionUtils.stripExecutionException(e);
            if (isPartitionNotExist(t)) {
                throw new PartitionNotExistException(
                        getName(), objectPath, catalogPartitionSpec, e);
            } else {
                throw new CatalogException(
                        String.format(
                                "Failed to drop partition with partition spec %s of table %s in %s",
                                catalogPartitionSpec, objectPath, getName()),
                        t);
            }
        }
    }

    @Override
    public void alterPartition(
            ObjectPath objectPath,
            CatalogPartitionSpec catalogPartitionSpec,
            CatalogPartition catalogPartition,
            boolean b)
            throws PartitionNotExistException, CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<String> listFunctions(String s) throws DatabaseNotExistException, CatalogException {
        return Collections.emptyList();
    }

    @Override
    public CatalogFunction getFunction(ObjectPath functionPath)
            throws FunctionNotExistException, CatalogException {
        throw new FunctionNotExistException(getName(), functionPath);
    }

    @Override
    public boolean functionExists(ObjectPath objectPath) throws CatalogException {
        return false;
    }

    @Override
    public void createFunction(ObjectPath objectPath, CatalogFunction catalogFunction, boolean b)
            throws FunctionAlreadyExistException, DatabaseNotExistException, CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void alterFunction(ObjectPath objectPath, CatalogFunction catalogFunction, boolean b)
            throws FunctionNotExistException, CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void dropFunction(ObjectPath objectPath, boolean b)
            throws FunctionNotExistException, CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public CatalogTableStatistics getTableStatistics(ObjectPath objectPath)
            throws TableNotExistException, CatalogException {
        return CatalogTableStatistics.UNKNOWN;
    }

    @Override
    public CatalogColumnStatistics getTableColumnStatistics(ObjectPath objectPath)
            throws TableNotExistException, CatalogException {
        return CatalogColumnStatistics.UNKNOWN;
    }

    @Override
    public CatalogTableStatistics getPartitionStatistics(
            ObjectPath objectPath, CatalogPartitionSpec catalogPartitionSpec)
            throws PartitionNotExistException, CatalogException {
        return CatalogTableStatistics.UNKNOWN;
    }

    @Override
    public CatalogColumnStatistics getPartitionColumnStatistics(
            ObjectPath objectPath, CatalogPartitionSpec catalogPartitionSpec)
            throws PartitionNotExistException, CatalogException {
        return CatalogColumnStatistics.UNKNOWN;
    }

    @Override
    public void alterTableStatistics(
            ObjectPath objectPath, CatalogTableStatistics catalogTableStatistics, boolean b)
            throws TableNotExistException, CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void alterTableColumnStatistics(
            ObjectPath objectPath, CatalogColumnStatistics catalogColumnStatistics, boolean b)
            throws TableNotExistException, CatalogException, TablePartitionedException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void alterPartitionStatistics(
            ObjectPath objectPath,
            CatalogPartitionSpec catalogPartitionSpec,
            CatalogTableStatistics catalogTableStatistics,
            boolean b)
            throws PartitionNotExistException, CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void alterPartitionColumnStatistics(
            ObjectPath objectPath,
            CatalogPartitionSpec catalogPartitionSpec,
            CatalogColumnStatistics catalogColumnStatistics,
            boolean b)
            throws PartitionNotExistException, CatalogException {
        throw new UnsupportedOperationException();
    }

    protected TablePath toTablePath(ObjectPath objectPath) {
        return TablePath.of(objectPath.getDatabaseName(), objectPath.getObjectName());
    }

    @Override
    public List<String> listProcedures(String dbName)
            throws DatabaseNotExistException, CatalogException {
        if (!databaseExists(dbName)) {
            throw new DatabaseNotExistException(getName(), dbName);
        }
        return ProcedureManager.listProcedures();
    }

    @Override
    public Procedure getProcedure(ObjectPath procedurePath)
            throws ProcedureNotExistException, CatalogException {
        Optional<Procedure> procedure = ProcedureManager.getProcedure(admin, procedurePath);
        if (procedure.isPresent()) {
            return procedure.get();
        } else {
            throw new ProcedureNotExistException(catalogName, procedurePath);
        }
    }

    @VisibleForTesting
    public Map<String, String> getSecurityConfigs() {
        return securityConfigs;
    }

    @VisibleForTesting
    public Map<String, String> getLakeCatalogProperties() {
        if (lakeCatalogProperties == null) {
            synchronized (this) {
                if (lakeCatalogProperties == null) {
                    lakeCatalogProperties = lakeCatalogPropertiesSupplier.get();
                }
            }
        }
        return lakeCatalogProperties;
    }

    private CatalogTable wrapWithIndexes(CatalogTable table, TableInfo tableInfo) {

        Optional<Schema.UnresolvedPrimaryKey> pkOp = table.getUnresolvedSchema().getPrimaryKey();
        // If there is no pk, return directly.
        if (!pkOp.isPresent()) {
            return table;
        }

        List<List<String>> indexes = new ArrayList<>();
        // Pk is always an index.
        indexes.add(pkOp.get().getColumnNames());

        // Judge whether we can do prefix lookup.
        List<String> bucketKeys = tableInfo.getBucketKeys();
        // For partition table, the physical primary key is the primary key that excludes the
        // partition key
        List<String> physicalPrimaryKeys = tableInfo.getPhysicalPrimaryKeys();
        List<String> indexKeys = new ArrayList<>();
        if (isPrefixList(physicalPrimaryKeys, bucketKeys)) {
            indexKeys.addAll(bucketKeys);
            if (tableInfo.isPartitioned()) {
                indexKeys.addAll(tableInfo.getPartitionKeys());
            }
        }

        if (!indexKeys.isEmpty()) {
            indexes.add(indexKeys);
        }
        return CatalogTableAdapter.toCatalogTable(
                withIndex(table.getUnresolvedSchema(), indexes),
                table.getComment(),
                table.getPartitionKeys(),
                table.getOptions());
    }

    private static boolean isPrefixList(List<String> fullList, List<String> prefixList) {
        if (fullList.size() <= prefixList.size()) {
            return false;
        }

        for (int i = 0; i < prefixList.size(); i++) {
            if (!fullList.get(i).equals(prefixList.get(i))) {
                return false;
            }
        }
        return true;
    }

    /**
     * Creates a virtual $changelog table by modifying the base table's to include metadata columns.
     */
    private CatalogBaseTable getVirtualChangelogTable(ObjectPath objectPath)
            throws TableNotExistException, CatalogException {
        // Extract the base table name (remove $changelog suffix)
        String virtualTableName = objectPath.getObjectName();
        String baseTableName =
                virtualTableName.substring(
                        0, virtualTableName.length() - CHANGELOG_TABLE_SUFFIX.length());

        // Get the base table
        ObjectPath baseObjectPath = new ObjectPath(objectPath.getDatabaseName(), baseTableName);
        TablePath baseTablePath = toTablePath(baseObjectPath);

        try {
            // Retrieve base table info
            TableInfo tableInfo = admin.getTableInfo(baseTablePath).get();

            // Validate that this is a primary key table
            if (tableInfo.getPhysicalPrimaryKeys().isEmpty()) {
                throw new UnsupportedOperationException(
                        String.format(
                                "Virtual $changelog tables are only supported for primary key tables. "
                                        + "Table %s does not have a primary key.",
                                baseTablePath));
            }

            // Convert to Flink table
            CatalogBaseTable catalogBaseTable = FlinkConversions.toFlinkTable(tableInfo);

            if (!(catalogBaseTable instanceof CatalogTable)) {
                throw new UnsupportedOperationException(
                        "Virtual $changelog tables are only supported for regular tables");
            }

            CatalogTable baseTable = (CatalogTable) catalogBaseTable;

            // Build the changelog schema by adding metadata columns
            Schema originalSchema = baseTable.getUnresolvedSchema();
            Schema changelogSchema = buildChangelogSchema(originalSchema);

            // Copy options from base table
            Map<String, String> newOptions = new HashMap<>(baseTable.getOptions());
            newOptions.put(BOOTSTRAP_SERVERS.key(), bootstrapServers);
            newOptions.putAll(securityConfigs);

            // Create a new CatalogTable with the modified schema
            return CatalogTableAdapter.toCatalogTable(
                    changelogSchema,
                    baseTable.getComment(),
                    baseTable.getPartitionKeys(),
                    newOptions);

        } catch (Exception e) {
            Throwable t = ExceptionUtils.stripExecutionException(e);
            if (isTableNotExist(t)) {
                throw new TableNotExistException(getName(), baseObjectPath);
            } else {
                throw new CatalogException(
                        String.format(
                                "Failed to get virtual changelog table %s in %s",
                                objectPath, getName()),
                        t);
            }
        }
    }

    private Schema buildChangelogSchema(Schema originalSchema) {
        Schema.Builder builder = Schema.newBuilder();

        // Add metadata columns first
        builder.column("_change_type", STRING().notNull());
        builder.column("_log_offset", BIGINT().notNull());
        builder.column("_commit_timestamp", TIMESTAMP_LTZ(3).notNull());

        // Add all original columns (preserves all column attributes including comments)
        builder.fromColumns(originalSchema.getColumns());

        // Note: We don't copy primary keys or watermarks for virtual tables

        return builder.build();
    }
}
