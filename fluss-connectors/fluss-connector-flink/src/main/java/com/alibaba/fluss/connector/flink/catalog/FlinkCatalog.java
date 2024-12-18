/*
 * Copyright (c) 2024 Alibaba Group Holding Ltd.
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

package com.alibaba.fluss.connector.flink.catalog;

import com.alibaba.fluss.client.Connection;
import com.alibaba.fluss.client.ConnectionFactory;
import com.alibaba.fluss.client.admin.Admin;
import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.connector.flink.lakehouse.LakeCatalog;
import com.alibaba.fluss.connector.flink.utils.CatalogExceptionUtils;
import com.alibaba.fluss.connector.flink.utils.FlinkConversions;
import com.alibaba.fluss.exception.FlussRuntimeException;
import com.alibaba.fluss.metadata.TableDescriptor;
import com.alibaba.fluss.metadata.TableInfo;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.utils.ExceptionUtils;
import com.alibaba.fluss.utils.IOUtils;

import org.apache.flink.table.catalog.Catalog;
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
import org.apache.flink.table.catalog.exceptions.TableAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.catalog.exceptions.TableNotPartitionedException;
import org.apache.flink.table.catalog.exceptions.TablePartitionedException;
import org.apache.flink.table.catalog.stats.CatalogColumnStatistics;
import org.apache.flink.table.catalog.stats.CatalogTableStatistics;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.factories.Factory;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.alibaba.fluss.config.ConfigOptions.BOOTSTRAP_SERVERS;
import static org.apache.flink.util.Preconditions.checkArgument;

/** A Flink Catalog for fluss. */
public class FlinkCatalog implements Catalog {

    public static final String LAKE_TABLE_SPLITTER = "$lake";

    protected final ClassLoader classLoader;

    protected final String catalogName;
    protected final @Nullable String defaultDatabase;
    protected final String bootstrapServers;
    protected Connection connection;
    protected Admin admin;

    private volatile @Nullable LakeCatalog lakeCatalog;

    public FlinkCatalog(
            String name,
            @Nullable String defaultDatabase,
            String bootstrapServers,
            ClassLoader classLoader) {
        this.catalogName = name;
        this.defaultDatabase = defaultDatabase;
        this.bootstrapServers = bootstrapServers;
        this.classLoader = classLoader;
    }

    @Override
    public Optional<Factory> getFactory() {
        return Optional.of(new FlinkTableFactory());
    }

    @Override
    public void open() throws CatalogException {
        Configuration flussConfigs = new Configuration();
        flussConfigs.setString(ConfigOptions.BOOTSTRAP_SERVERS.key(), bootstrapServers);
        connection = ConnectionFactory.createConnection(flussConfigs);
        admin = connection.getAdmin();
    }

    @Override
    public void close() throws CatalogException {
        IOUtils.closeQuietly(admin, "fluss-admin");
        IOUtils.closeQuietly(connection, "fluss-connection");
    }

    public String getName() {
        return catalogName;
    }

    @Nullable
    @Override
    public String getDefaultDatabase() throws CatalogException {
        return defaultDatabase;
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
        return new CatalogDatabaseImpl(Collections.emptyMap(), null);
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
            admin.createDatabase(databaseName, ignoreIfExists).get();
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
            admin.deleteDatabase(databaseName, ignoreIfNotExists, cascade).get();
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
                        admin.getTable(
                                        TablePath.of(
                                                objectPath.getDatabaseName(),
                                                tableName.split("\\" + LAKE_TABLE_SPLITTER)[0]))
                                .get();
                // we need to make sure the table enable datalake
                if (!tableInfo.getTableDescriptor().isDataLakeEnabled()) {
                    throw new UnsupportedOperationException(
                            String.format(
                                    "Table %s is not datalake enabled.",
                                    TablePath.of(
                                            objectPath.getDatabaseName(),
                                            tableName.split("\\" + LAKE_TABLE_SPLITTER)[0])));
                }
                return getLakeTable(objectPath.getDatabaseName(), tableName);
            } else {
                tableInfo = admin.getTable(tablePath).get();
            }

            // should be as a fluss table
            CatalogTable catalogTable = FlinkConversions.toFlinkTable(tableInfo);
            // add bootstrap servers option
            Map<String, String> newOptions = new HashMap<>(catalogTable.getOptions());
            newOptions.put(BOOTSTRAP_SERVERS.key(), bootstrapServers);
            return catalogTable.copy(newOptions);
        } catch (Exception e) {
            Throwable t = ExceptionUtils.stripExecutionException(e);
            if (CatalogExceptionUtils.isTableNotExist(t)) {
                throw new TableNotExistException(getName(), objectPath);
            } else {
                throw new CatalogException(
                        String.format("Failed to get table %s in %s", objectPath, getName()), t);
            }
        }
    }

    protected CatalogBaseTable getLakeTable(String databaseName, String tableName)
            throws TableNotExistException, CatalogException {
        mayInitLakeCatalogCatalog();
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
            admin.deleteTable(tablePath, ignoreIfNotExists).get();
        } catch (Exception e) {
            Throwable t = ExceptionUtils.stripExecutionException(e);
            if (CatalogExceptionUtils.isTableNotExist(t)) {
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
            } else {
                throw new CatalogException(
                        String.format("Failed to create table %s in %s", objectPath, getName()), t);
            }
        }
    }

    @Override
    public void alterTable(ObjectPath objectPath, CatalogBaseTable catalogBaseTable, boolean b)
            throws TableNotExistException, CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<CatalogPartitionSpec> listPartitions(ObjectPath objectPath)
            throws TableNotExistException, TableNotPartitionedException, CatalogException {
        // TODO: use admin.listPartitionInfos()
        return Collections.emptyList();
    }

    @Override
    public List<CatalogPartitionSpec> listPartitions(
            ObjectPath objectPath, CatalogPartitionSpec catalogPartitionSpec)
            throws TableNotExistException, TableNotPartitionedException,
                    PartitionSpecInvalidException, CatalogException {
        throw new UnsupportedOperationException();
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
        throw new UnsupportedOperationException();
    }

    @Override
    public void dropPartition(
            ObjectPath objectPath, CatalogPartitionSpec catalogPartitionSpec, boolean b)
            throws PartitionNotExistException, CatalogException {
        throw new UnsupportedOperationException();
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

    private void mayInitLakeCatalogCatalog() {
        if (lakeCatalog == null) {
            synchronized (this) {
                if (lakeCatalog == null) {
                    try {
                        Map<String, String> catalogProperties =
                                admin.describeLakeStorage().get().getCatalogProperties();
                        lakeCatalog = new LakeCatalog(catalogName, catalogProperties, classLoader);
                    } catch (Exception e) {
                        throw new FlussRuntimeException("Failed to init paimon catalog.", e);
                    }
                }
            }
        }
    }
}
