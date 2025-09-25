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
import org.apache.fluss.exception.FlussRuntimeException;
import org.apache.fluss.exception.InvalidTableException;
import org.apache.fluss.flink.lake.LakeCatalog;
import org.apache.fluss.flink.procedure.ProcedureManager;
import org.apache.fluss.flink.utils.CatalogExceptionUtils;
import org.apache.fluss.flink.utils.DataLakeUtils;
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
import org.apache.flink.table.catalog.AbstractCatalog;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogDatabase;
import org.apache.flink.table.catalog.CatalogDatabaseImpl;
import org.apache.flink.table.catalog.CatalogFunction;
import org.apache.flink.table.catalog.CatalogPartition;
import org.apache.flink.table.catalog.CatalogPartitionSpec;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.CatalogView;
import org.apache.flink.table.catalog.ObjectPath;
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

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.fluss.config.ConfigOptions.BOOTSTRAP_SERVERS;
import static org.apache.fluss.flink.FlinkConnectorOptions.ALTER_DISALLOW_OPTIONS;
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

    protected final ClassLoader classLoader;

    protected final String catalogName;
    protected final String defaultDatabase;
    protected final String bootstrapServers;
    private final Map<String, String> securityConfigs;
    protected Connection connection;
    protected Admin admin;
    private volatile @Nullable LakeCatalog lakeCatalog;

    public FlinkCatalog(
            String name,
            String defaultDatabase,
            String bootstrapServers,
            ClassLoader classLoader,
            Map<String, String> securityConfigs) {
        super(name, defaultDatabase);
        this.catalogName = name;
        this.defaultDatabase = defaultDatabase;
        this.bootstrapServers = bootstrapServers;
        this.classLoader = classLoader;
        this.securityConfigs = securityConfigs;
    }

    @Override
    public Optional<Factory> getFactory() {
        return Optional.of(new FlinkTableFactory());
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
                        objectPath.getDatabaseName(), tableName, tableInfo.getProperties());
            } else {
                tableInfo = admin.getTableInfo(tablePath).get();
            }

            // should be as a fluss table
            CatalogTable catalogTable = FlinkConversions.toFlinkTable(tableInfo);
            // add bootstrap servers option
            Map<String, String> newOptions = new HashMap<>(catalogTable.getOptions());
            newOptions.put(BOOTSTRAP_SERVERS.key(), bootstrapServers);
            newOptions.putAll(securityConfigs);
            return catalogTable.copy(newOptions);
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
            String databaseName, String tableName, Configuration properties)
            throws TableNotExistException, CatalogException {
        mayInitLakeCatalogCatalog(properties);
        String[] tableComponents = tableName.split("\\" + LAKE_TABLE_SPLITTER);
        if (tableComponents.length == 1) {
            // should be pattern like table_name$lake
            tableName = tableComponents[0];
        } else {
            // be some thing like table_name$lake$snapshot
            tableName = String.join("", tableComponents);
        }
        return lakeCatalog.getTable(new ObjectPath(databaseName, tableName));
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

        checkArgument(table instanceof ResolvedCatalogTable, "table should be resolved");

        TablePath tablePath = toTablePath(objectPath);
        TableDescriptor tableDescriptor =
                FlinkConversions.toFlussTable((ResolvedCatalogTable) table);
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
                        .map(FlinkConversions::toFlussTableChange)
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

    private void mayInitLakeCatalogCatalog(Configuration tableOptions) {
        // TODO: Currently, a Fluss cluster only supports a single DataLake storage. However, in the
        //  future, it may support multiple DataLakes. The following code assumes that a single
        //  lakeCatalog is shared across multiple tables, which will no longer be valid in such
        //  cases and should be updated accordingly.
        if (lakeCatalog == null) {
            synchronized (this) {
                if (lakeCatalog == null) {
                    try {
                        Map<String, String> catalogProperties =
                                DataLakeUtils.extractLakeCatalogProperties(tableOptions);
                        lakeCatalog = new LakeCatalog(catalogName, catalogProperties, classLoader);
                    } catch (Exception e) {
                        throw new FlussRuntimeException("Failed to init paimon catalog.", e);
                    }
                }
            }
        }
    }

    @VisibleForTesting
    public Map<String, String> getSecurityConfigs() {
        return securityConfigs;
    }
}
