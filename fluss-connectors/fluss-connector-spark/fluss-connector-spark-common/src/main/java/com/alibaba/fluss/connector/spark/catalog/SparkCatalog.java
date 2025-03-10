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

package com.alibaba.fluss.connector.spark.catalog;

import com.alibaba.fluss.client.Connection;
import com.alibaba.fluss.client.ConnectionFactory;
import com.alibaba.fluss.client.admin.Admin;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.connector.spark.SparkTable;
import com.alibaba.fluss.connector.spark.exception.CatalogException;
import com.alibaba.fluss.exception.PartitionNotExistException;
import com.alibaba.fluss.exception.TableNotExistException;
import com.alibaba.fluss.exception.TableNotPartitionedException;
import com.alibaba.fluss.metadata.DatabaseDescriptor;
import com.alibaba.fluss.metadata.PartitionSpec;
import com.alibaba.fluss.metadata.TableDescriptor;
import com.alibaba.fluss.metadata.TableInfo;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.utils.CatalogExceptionUtils;
import com.alibaba.fluss.utils.ExceptionUtils;
import com.alibaba.fluss.utils.IOUtils;

import org.apache.spark.sql.catalyst.analysis.NamespaceAlreadyExistsException;
import org.apache.spark.sql.catalyst.analysis.NoSuchFunctionException;
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.analysis.NonEmptyNamespaceException;
import org.apache.spark.sql.catalyst.analysis.PartitionsAlreadyExistException;
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException;
import org.apache.spark.sql.connector.catalog.FunctionCatalog;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.NamespaceChange;
import org.apache.spark.sql.connector.catalog.SupportsNamespaces;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.catalog.TableChange;
import org.apache.spark.sql.connector.catalog.functions.UnboundFunction;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import java.io.Closeable;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static com.alibaba.fluss.connector.spark.utils.SparkConversions.toFlussClientConfig;
import static com.alibaba.fluss.connector.spark.utils.SparkConversions.toFlussTable;
import static com.alibaba.fluss.utils.CatalogExceptionUtils.isPartitionAlreadyExists;
import static com.alibaba.fluss.utils.CatalogExceptionUtils.isPartitionInvalid;
import static com.alibaba.fluss.utils.CatalogExceptionUtils.isPartitionNotExist;
import static com.alibaba.fluss.utils.CatalogExceptionUtils.isTableNotExist;
import static com.alibaba.fluss.utils.CatalogExceptionUtils.isTableNotPartitioned;
import static com.alibaba.fluss.utils.Preconditions.checkArgument;

/** A Spark Catalog for Fluss. */
public class SparkCatalog implements SupportsNamespaces, FunctionCatalog, TableCatalog, Closeable {

    private static final String[] DEFAULT_NAMESPACE = new String[] {"fluss"};

    private String catalogName;
    private Connection connection;
    private Admin admin;
    private Configuration flussConfigs;

    @Override
    public void initialize(String name, CaseInsensitiveStringMap options) {
        this.catalogName = name;
        this.flussConfigs = toFlussClientConfig(options);
        connection = ConnectionFactory.createConnection(flussConfigs);
        admin = connection.getAdmin();
    }

    @Override
    public String[] defaultNamespace() {
        return DEFAULT_NAMESPACE;
    }

    @Override
    public String name() {
        return this.catalogName;
    }

    @Override
    public boolean namespaceExists(String[] namespace) {
        isValidateNamespace(namespace);
        try {
            return admin.databaseExists(namespace[0]).get();
        } catch (Exception e) {
            throw new CatalogException(
                    String.format("Failed to check if database %s exists in %s", namespace, name()),
                    e);
        }
    }

    @Override
    public String[][] listNamespaces() {
        try {
            List<String> databases = admin.listDatabases().get();
            String[][] namespaces = new String[databases.size()][];

            for (int i = 0; i < databases.size(); ++i) {
                namespaces[i] = new String[] {databases.get(i)};
            }

            return namespaces;
        } catch (Exception e) {
            throw new CatalogException(
                    String.format("Failed to list all databases in %s", name()), e);
        }
    }

    @Override
    public String[][] listNamespaces(String[] namespace) throws NoSuchNamespaceException {
        if (namespace.length == 0) {
            return listNamespaces();
        } else {
            isValidateNamespace(namespace);
            if (namespaceExists(namespace)) {
                return new String[0][];
            }
            throw new NoSuchNamespaceException(namespace);
        }
    }

    @Override
    public Map<String, String> loadNamespaceMetadata(String[] namespace)
            throws NoSuchNamespaceException {
        isValidateNamespace(namespace);
        if (namespaceExists(namespace)) {
            return Collections.emptyMap();
        }
        throw new NoSuchNamespaceException(namespace);
    }

    @Override
    public void createNamespace(String[] namespace, Map<String, String> metadata)
            throws NamespaceAlreadyExistsException {
        isValidateNamespace(namespace);
        try {
            admin.createDatabase(
                            namespace[0],
                            DatabaseDescriptor.builder().customProperties(metadata).build(),
                            false)
                    .get();
        } catch (Exception e) {
            Throwable t = ExceptionUtils.stripExecutionException(e);
            if (CatalogExceptionUtils.isDatabaseAlreadyExist(t)) {
                throw new NamespaceAlreadyExistsException(namespace);
            } else {
                throw new CatalogException(
                        String.format("Failed to create database %s in %s", namespace, name()), t);
            }
        }
    }

    @Override
    public void alterNamespace(String[] namespace, NamespaceChange... changes)
            throws NoSuchNamespaceException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean dropNamespace(String[] namespace, boolean cascade)
            throws NoSuchNamespaceException, NonEmptyNamespaceException {
        isValidateNamespace(namespace);
        try {
            admin.dropDatabase(namespace[0], false, cascade).get();
            return true;
        } catch (Exception e) {
            Throwable t = ExceptionUtils.stripExecutionException(e);
            if (CatalogExceptionUtils.isDatabaseNotExist(t)) {
                throw new NoSuchNamespaceException(namespace);
            } else if (CatalogExceptionUtils.isDatabaseNotEmpty(t)) {
                throw new NonEmptyNamespaceException(namespace);
            } else {
                throw new CatalogException(
                        String.format("Failed to drop database %s in %s", namespace, name()), t);
            }
        }
    }

    @Override
    public Table loadTable(Identifier ident, String version) throws NoSuchTableException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Table loadTable(Identifier ident, long timestamp) throws NoSuchTableException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void invalidateTable(Identifier ident) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean tableExists(Identifier ident) {
        try {
            return admin.tableExists(toTablePath(ident)).get();
        } catch (Exception e) {
            throw new CatalogException(
                    String.format("Failed to check if table %s exists in %s", ident, name()),
                    ExceptionUtils.stripExecutionException(e));
        }
    }

    @Override
    public boolean purgeTable(Identifier ident) throws UnsupportedOperationException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Identifier[] listTables(String[] namespace) throws NoSuchNamespaceException {
        isValidateNamespace(namespace);
        try {
            List<String> tables = admin.listTables(namespace[0]).get();
            Identifier[] identifiers = new Identifier[tables.size()];
            for (int i = 0; i < tables.size(); i++) {
                identifiers[i] = Identifier.of(namespace, tables.get(i));
            }
            return identifiers;
        } catch (Exception e) {
            Throwable t = ExceptionUtils.stripExecutionException(e);
            if (CatalogExceptionUtils.isDatabaseNotExist(t)) {
                throw new NoSuchNamespaceException(namespace);
            }
            throw new CatalogException(
                    String.format(
                            "Failed to list all tables in database %s in %s", namespace, name()),
                    t);
        }
    }

    @Override
    public Table loadTable(Identifier ident) throws NoSuchTableException {
        try {
            TableInfo tableInfo = admin.getTableInfo(toTablePath(ident)).get();
            return new SparkTable(this, flussConfigs, tableInfo);
        } catch (Exception e) {
            Throwable t = ExceptionUtils.stripExecutionException(e);
            if (isTableNotExist(t)) {
                throw new NoSuchTableException(ident);
            } else {
                throw new CatalogException(
                        String.format("Failed to get table %s in %s", ident, name()), t);
            }
        }
    }

    @Override
    public Table createTable(
            Identifier ident,
            StructType schema,
            Transform[] partitions,
            Map<String, String> properties)
            throws TableAlreadyExistsException, NoSuchNamespaceException {
        try {
            TableDescriptor tableDescriptor = toFlussTable(schema, partitions, properties);
            TablePath tablePath = toTablePath(ident);
            admin.createTable(tablePath, tableDescriptor, false).get();
            return loadTable(ident);
        } catch (Exception e) {
            Throwable t = ExceptionUtils.stripExecutionException(e);
            if (CatalogExceptionUtils.isDatabaseNotExist(t)) {
                throw new NoSuchNamespaceException(ident.namespace());
            } else if (CatalogExceptionUtils.isTableAlreadyExist(t)) {
                throw new TableAlreadyExistsException(ident);
            } else {
                throw new CatalogException(
                        String.format("Failed to create table %s in %s", ident, name()), t);
            }
        }
    }

    @Override
    public Table alterTable(Identifier ident, TableChange... changes) throws NoSuchTableException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean dropTable(Identifier ident) {
        try {
            admin.dropTable(toTablePath(ident), false).get();
            return true;
        } catch (Exception e) {
            Throwable t = ExceptionUtils.stripExecutionException(e);
            throw new CatalogException(
                    String.format("Failed to drop table %s in %s", ident, name()), t);
        }
    }

    @Override
    public void renameTable(Identifier oldIdent, Identifier newIdent)
            throws NoSuchTableException, TableAlreadyExistsException {
        throw new UnsupportedOperationException();
    }

    public void createPartitions(
            TablePath tablePath, PartitionSpec partitionSpec, Map<String, String> properties)
            throws PartitionsAlreadyExistException, UnsupportedOperationException {

        try {
            admin.createPartition(tablePath, partitionSpec, true).get();
        } catch (Exception e) {
            Throwable t = ExceptionUtils.stripExecutionException(e);
            if (isTableNotExist(t)) {
                throw new TableNotExistException("Table does not exist: " + tablePath);
            } else if (isTableNotPartitioned(t)) {
                throw new TableNotPartitionedException("Table is not partitioned: " + tablePath);
            } else if (isPartitionInvalid(t)) {
                List<String> partitionKeys = null;
                try {
                    TableInfo tableInfo = admin.getTableInfo(tablePath).get();
                    partitionKeys = tableInfo.getPartitionKeys();
                } catch (Exception ee) {
                    // ignore.
                }
                if (partitionKeys == null) {
                    // throw general exception if getting partition keys failed.
                    throw new CatalogException(
                            String.format(
                                    "PartitionSpec %s does not match partition keys of table %s in catalog %s.",
                                    partitionSpec, tablePath, catalogName),
                            e);
                }

            } else if (isPartitionAlreadyExists(t)) {
                throw new PartitionsAlreadyExistException(
                        "Partition already exists: " + partitionSpec);
            } else {
                throw new CatalogException(
                        String.format(
                                "Failed to create partition with partition spec %s of table %s in %s",
                                partitionSpec, tablePath, catalogName),
                        t);
            }
        }
    }

    public boolean dropPartition(TablePath tablePath, PartitionSpec partitionSpec) {
        try {
            admin.dropPartition(tablePath, partitionSpec, true).get();
        } catch (Exception e) {
            Throwable t = ExceptionUtils.stripExecutionException(e);
            if (isPartitionNotExist(t)) {
                throw new PartitionNotExistException("Partition does not exist: " + partitionSpec);
            } else {
                throw new CatalogException(
                        String.format(
                                "Failed to drop partition with partition spec %s of table %s in %s",
                                partitionSpec, tablePath, catalogName),
                        t);
            }
        }
        return true;
    }

    @Override
    public Identifier[] listFunctions(String[] namespace) throws NoSuchNamespaceException {
        throw new UnsupportedOperationException();
    }

    @Override
    public UnboundFunction loadFunction(Identifier ident) throws NoSuchFunctionException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() {
        IOUtils.closeQuietly(admin, "fluss-admin");
        IOUtils.closeQuietly(connection, "fluss-connection");
    }

    private void isValidateNamespace(String[] namespace) {
        checkArgument(
                namespace.length == 1, "Namespace %s is not valid", Arrays.toString(namespace));
    }

    private TablePath toTablePath(Identifier ident) {
        isValidateNamespace(ident.namespace());
        return TablePath.of(ident.namespace()[0], ident.name());
    }
}
