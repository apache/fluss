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

package com.alibaba.fluss.connector.spark.catalog;

import com.alibaba.fluss.client.Connection;
import com.alibaba.fluss.client.ConnectionFactory;
import com.alibaba.fluss.client.admin.Admin;
import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.connector.spark.SparkConnectorOptions;
import com.alibaba.fluss.connector.spark.exception.CatalogException;
import com.alibaba.fluss.connector.spark.table.SparkTable;
import com.alibaba.fluss.connector.spark.utils.SparkConversions;
import com.alibaba.fluss.metadata.TableDescriptor;
import com.alibaba.fluss.metadata.TableInfo;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.utils.CatalogExceptionUtils;
import com.alibaba.fluss.utils.ExceptionUtils;
import com.alibaba.fluss.utils.IOUtils;
import com.alibaba.fluss.utils.Preconditions;

import org.apache.spark.sql.catalyst.analysis.NamespaceAlreadyExistsException;
import org.apache.spark.sql.catalyst.analysis.NoSuchFunctionException;
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.analysis.NonEmptyNamespaceException;
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException;
import org.apache.spark.sql.connector.catalog.FunctionCatalog;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.NamespaceChange;
import org.apache.spark.sql.connector.catalog.StagedTable;
import org.apache.spark.sql.connector.catalog.StagingTableCatalog;
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

/** A Spark Catalog for Fluss. */
public class SparkCatalog
        implements StagingTableCatalog,
                SupportsNamespaces,
                FunctionCatalog,
                TableCatalog,
                Closeable {

    private static final String[] DEFAULT_NAMESPACE = new String[] {"fluss"};

    private String bootstrapServers;
    private String catalogName;
    private Connection connection;
    private Admin admin;

    @Override
    public void initialize(String name, CaseInsensitiveStringMap options) {
        this.catalogName = name;
        this.bootstrapServers = options.get(SparkConnectorOptions.BOOTSTRAP_SERVERS.key());
        Configuration flussConfigs = new Configuration();
        flussConfigs.setString(ConfigOptions.BOOTSTRAP_SERVERS.key(), bootstrapServers);
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
            admin.createDatabase(namespace[0], false).get();
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
            admin.deleteDatabase(namespace[0], false, cascade).get();
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
    public StagedTable stageCreate(
            Identifier ident,
            StructType schema,
            Transform[] partitions,
            Map<String, String> properties)
            throws TableAlreadyExistsException, NoSuchNamespaceException {
        throw new UnsupportedOperationException();
    }

    @Override
    public StagedTable stageReplace(
            Identifier ident,
            StructType schema,
            Transform[] partitions,
            Map<String, String> properties)
            throws NoSuchNamespaceException, NoSuchTableException {
        throw new UnsupportedOperationException();
    }

    @Override
    public StagedTable stageCreateOrReplace(
            Identifier ident,
            StructType schema,
            Transform[] partitions,
            Map<String, String> properties)
            throws NoSuchNamespaceException {
        throw new UnsupportedOperationException();
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
            TableInfo tableInfo = admin.getTable(toTablePath(ident)).get();
            return new SparkTable(
                    tableInfo.getTablePath(), tableInfo.getTableDescriptor(), bootstrapServers);
        } catch (Exception e) {
            Throwable t = ExceptionUtils.stripExecutionException(e);
            if (CatalogExceptionUtils.isTableNotExist(t)) {
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
            TableDescriptor tableDescriptor =
                    SparkConversions.toFlussTable(schema, partitions, properties);
            TablePath tablePath = toTablePath(ident);
            admin.createTable(tablePath, tableDescriptor, false).get();
            return new SparkTable(tablePath, tableDescriptor, bootstrapServers);
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
            admin.deleteTable(toTablePath(ident), false).get();
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
        Preconditions.checkArgument(
                namespace.length == 1, "Namespace %s is not valid", Arrays.toString(namespace));
    }

    private TablePath toTablePath(Identifier ident) {
        isValidateNamespace(ident.namespace());
        return TablePath.of(ident.namespace()[0], ident.name());
    }
}
