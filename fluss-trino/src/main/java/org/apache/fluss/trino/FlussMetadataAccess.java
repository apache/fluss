/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.trino;

import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.exception.DatabaseNotExistException;
import org.apache.fluss.exception.TableNotExistException;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.shaded.guava32.com.google.common.collect.ImmutableList;
import org.apache.fluss.shaded.guava32.com.google.common.collect.ImmutableMap;

import com.google.inject.Inject;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.TableNotFoundException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

/**
 * Provides access to Fluss catalog metadata.
 *
 * <p>This class handles physical name resolution, Admin RPC calls, and translation of Fluss
 * metadata failures. Name discovery preserves ambiguous names; resolution rejects them only when
 * the requested name is ambiguous.
 */
final class FlussMetadataAccess {

    private final Admin admin;

    @Inject
    FlussMetadataAccess(FlussClientManager clientManager) {
        this.admin = checkNotNull(clientManager, "clientManager is null").getAdmin();
    }

    List<String> listSchemaNames() {
        return ImmutableList.copyOf(indexSchemas().keySet());
    }

    Optional<ResolvedSchemaName> resolveSchema(String schemaName) {
        checkNotNull(schemaName, "schemaName is null");

        String canonicalName = canonicalize(schemaName);
        return resolveName(indexSchemas(), canonicalName, "database")
                .map(physicalName -> new ResolvedSchemaName(canonicalName, physicalName));
    }

    TableNameIndex indexTables(ResolvedSchemaName schema) {
        checkNotNull(schema, "schema is null");

        try {
            return new TableNameIndex(
                    schema, indexNames(await(admin.listTables(schema.getFlussName()))));
        } catch (DatabaseNotExistException e) {
            // The database may have been dropped after resolving its name.
            return new TableNameIndex(schema, ImmutableMap.of());
        }
    }

    /**
     * Returns table-name snapshots for the requested schema or the whole catalog.
     *
     * <p>Because Trino identifiers are case-insensitive, a catalog-wide listing fails if any
     * logical schema maps to multiple case-distinct Fluss databases.
     */
    List<TableNameIndex> listTableIndexes(Optional<String> schemaName) {
        checkNotNull(schemaName, "schemaName is null");

        if (schemaName.isPresent()) {
            Optional<ResolvedSchemaName> schema = resolveSchema(schemaName.get());
            if (!schema.isPresent()) {
                return ImmutableList.of();
            }
            return ImmutableList.of(indexTables(schema.get()));
        }

        Map<String, List<String>> schemas = indexSchemas();
        ImmutableList.Builder<TableNameIndex> indexes = ImmutableList.builder();

        for (String name : schemas.keySet()) {
            String physicalName =
                    resolveName(schemas, name, "database")
                            .orElseThrow(
                                    () ->
                                            new IllegalStateException(
                                                    "Missing database in name index: " + name));
            indexes.add(indexTables(new ResolvedSchemaName(name, physicalName)));
        }

        return indexes.build();
    }

    Optional<ResolvedTableName> resolveTable(SchemaTableName tableName) {
        checkNotNull(tableName, "tableName is null");

        Optional<ResolvedSchemaName> schema = resolveSchema(tableName.getSchemaName());
        if (!schema.isPresent()) {
            return Optional.empty();
        }

        return indexTables(schema.get()).resolveTable(tableName.getTableName());
    }

    /**
     * Returns table information if the table still exists.
     *
     * <p>This method is intended for metadata discovery, where the table may disappear between
     * listing and lookup.
     */
    Optional<TableInfo> findTableInfo(ResolvedTableName table) {
        try {
            return Optional.of(
                    loadTableInfo(table.getFlussDatabaseName(), table.getFlussTableName()));
        } catch (TableNotExistException | DatabaseNotExistException e) {
            return Optional.empty();
        }
    }

    /**
     * Returns table information for an existing Trino table handle and validates its physical table
     * identity.
     */
    TableInfo getTableInfo(FlussTableHandle table) {
        try {
            TableInfo info = loadTableInfo(table.getFlussDatabaseName(), table.getFlussTableName());
            validateIdentity(table, info);
            return info;
        } catch (TableNotExistException | DatabaseNotExistException e) {
            throw new TableNotFoundException(
                    new SchemaTableName(table.getSchemaName(), table.getTableName()), e);
        }
    }

    private Map<String, List<String>> indexSchemas() {
        return indexNames(await(admin.listDatabases()));
    }

    private TableInfo loadTableInfo(String databaseName, String tableName) {
        return await(admin.getTableInfo(TablePath.of(databaseName, tableName)));
    }

    private static void validateIdentity(FlussTableHandle table, TableInfo info) {
        if (table.getTableId() != info.getTableId() || table.getSchemaId() != info.getSchemaId()) {
            throw new TrinoException(
                    NOT_SUPPORTED,
                    "Fluss table or schema changed during query planning; retry the query");
        }
    }

    private static String canonicalize(String name) {
        return name.toLowerCase(Locale.ROOT);
    }

    private static Map<String, List<String>> indexNames(List<String> names) {
        Map<String, List<String>> candidates = new LinkedHashMap<>();

        for (String name : names) {
            candidates.computeIfAbsent(canonicalize(name), ignored -> new ArrayList<>()).add(name);
        }

        ImmutableMap.Builder<String, List<String>> index = ImmutableMap.builder();
        for (Map.Entry<String, List<String>> entry : candidates.entrySet()) {
            List<String> physicalNames = new ArrayList<>(entry.getValue());
            Collections.sort(physicalNames);
            index.put(entry.getKey(), ImmutableList.copyOf(physicalNames));
        }

        return index.build();
    }

    private static Optional<String> resolveName(
            Map<String, List<String>> names, String name, String objectType) {
        List<String> candidates = names.get(canonicalize(name));
        if (candidates == null) {
            return Optional.empty();
        }

        if (candidates.size() > 1) {
            throw new TrinoException(
                    NOT_SUPPORTED,
                    "Ambiguous Fluss "
                            + objectType
                            + " '"
                            + name
                            + "': "
                            + String.join(", ", candidates));
        }

        return Optional.of(candidates.get(0));
    }

    private static <T> T await(CompletableFuture<T> future) {
        try {
            return future.join();
        } catch (CompletionException e) {
            Throwable cause = e.getCause();

            if (cause instanceof TableNotExistException) {
                throw (TableNotExistException) cause;
            }
            if (cause instanceof DatabaseNotExistException) {
                throw (DatabaseNotExistException) cause;
            }

            throw new TrinoException(
                    GENERIC_INTERNAL_ERROR, "Failed to retrieve metadata from Fluss", cause);
        }
    }

    /** Immutable table-name snapshot that resolves physical names without additional RPC calls. */
    static final class TableNameIndex {

        private final ResolvedSchemaName schema;
        private final Map<String, List<String>> names;

        private TableNameIndex(ResolvedSchemaName schema, Map<String, List<String>> names) {
            this.schema = checkNotNull(schema, "schema is null");
            this.names = checkNotNull(names, "names is null");
        }

        /**
         * Returns distinct logical table names, including names with ambiguous physical matches.
         */
        List<SchemaTableName> listTableNames() {
            ImmutableList.Builder<SchemaTableName> tables = ImmutableList.builder();

            for (String name : names.keySet()) {
                tables.add(new SchemaTableName(schema.getTrinoName(), name));
            }

            return tables.build();
        }

        /** Returns whether any physical table has the requested logical name. */
        boolean containsTable(String tableName) {
            return names.containsKey(canonicalize(tableName));
        }

        /** Resolves one logical table name and rejects ambiguity for that name only. */
        Optional<ResolvedTableName> resolveTable(String tableName) {
            String canonicalName = canonicalize(tableName);

            return resolveName(names, canonicalName, "table")
                    .map(physicalName -> schema.table(canonicalName, physicalName));
        }
    }
}
