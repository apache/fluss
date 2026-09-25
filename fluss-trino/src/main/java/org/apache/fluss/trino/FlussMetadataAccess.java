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
import org.apache.fluss.client.admin.OffsetSpec;
import org.apache.fluss.exception.DatabaseNotExistException;
import org.apache.fluss.exception.TableNotExistException;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.shaded.guava32.com.google.common.collect.ImmutableList;
import org.apache.fluss.shaded.guava32.com.google.common.collect.ImmutableMap;

import com.google.inject.Inject;
import io.trino.spi.ErrorCodeSupplier;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.TableNotFoundException;

import java.util.ArrayList;
import java.util.Collection;
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
import static org.apache.fluss.trino.FlussErrorCode.FLUSS_METADATA_ERROR;
import static org.apache.fluss.trino.FlussErrorCode.FLUSS_SPLIT_ERROR;
import static org.apache.fluss.trino.FlussTableScanValidator.validateIdentity;
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
        return ImmutableList.copyOf(loadSchemaNameMapping().keySet());
    }

    Optional<ResolvedSchemaName> resolveSchema(String schemaName) {
        checkNotNull(schemaName, "schemaName is null");

        String canonicalName = canonicalize(schemaName);
        return resolveName(loadSchemaNameMapping(), canonicalName, "database")
                .map(physicalName -> new ResolvedSchemaName(canonicalName, physicalName));
    }

    TableNameMapping loadTableNameMapping(ResolvedSchemaName schema) {
        checkNotNull(schema, "schema is null");

        try {
            return new TableNameMapping(
                    schema,
                    buildNameMapping(
                            await(
                                    admin.listTables(schema.getFlussName()),
                                    FLUSS_METADATA_ERROR,
                                    "Failed to list Fluss tables in database "
                                            + schema.getFlussName())));
        } catch (DatabaseNotExistException e) {
            // The database may have been dropped after resolving its name.
            return new TableNameMapping(schema, ImmutableMap.of());
        }
    }

    /**
     * Returns table-name mappings for the requested schema or the whole catalog.
     *
     * <p>Because Trino identifiers are case-insensitive, a catalog-wide listing fails if any
     * logical schema maps to multiple case-distinct Fluss databases.
     */
    List<TableNameMapping> listTableNameMappings(Optional<String> schemaName) {
        checkNotNull(schemaName, "schemaName is null");

        if (schemaName.isPresent()) {
            Optional<ResolvedSchemaName> schema = resolveSchema(schemaName.get());
            if (!schema.isPresent()) {
                return ImmutableList.of();
            }
            return ImmutableList.of(loadTableNameMapping(schema.get()));
        }

        Map<String, List<String>> schemas = loadSchemaNameMapping();
        ImmutableList.Builder<TableNameMapping> tableNameMappingBuilder = ImmutableList.builder();

        for (String name : schemas.keySet()) {
            String physicalName =
                    resolveName(schemas, name, "database")
                            .orElseThrow(
                                    () ->
                                            new IllegalStateException(
                                                    "Missing database in name mapping: " + name));
            tableNameMappingBuilder.add(
                    loadTableNameMapping(new ResolvedSchemaName(name, physicalName)));
        }

        return tableNameMappingBuilder.build();
    }

    Optional<ResolvedTableName> resolveTable(SchemaTableName tableName) {
        checkNotNull(tableName, "tableName is null");

        Optional<ResolvedSchemaName> schema = resolveSchema(tableName.getSchemaName());
        if (!schema.isPresent()) {
            return Optional.empty();
        }

        return loadTableNameMapping(schema.get()).resolveTable(tableName.getTableName());
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

    /** Returns one validated offset for every requested bucket. */
    Map<Integer, Long> listOffsets(
            FlussTableHandle table, Collection<Integer> buckets, OffsetSpec offsetSpec) {
        try {
            TablePath tablePath =
                    TablePath.of(table.getFlussDatabaseName(), table.getFlussTableName());

            Map<Integer, Long> offsets =
                    await(
                            admin.listOffsets(tablePath, buckets, offsetSpec).all(),
                            FLUSS_SPLIT_ERROR,
                            "Failed to list Fluss offsets for " + table);

            for (int bucket : buckets) {
                Long offset = offsets.get(bucket);
                if (offset == null || offset < 0) {
                    throw new TrinoException(
                            GENERIC_INTERNAL_ERROR,
                            "Missing or invalid Fluss offset for "
                                    + table
                                    + " bucket "
                                    + bucket
                                    + ": "
                                    + offset);
                }
            }

            return ImmutableMap.copyOf(offsets);
        } catch (TableNotExistException | DatabaseNotExistException e) {
            throw new TableNotFoundException(
                    new SchemaTableName(table.getSchemaName(), table.getTableName()), e);
        }
    }

    private Map<String, List<String>> loadSchemaNameMapping() {
        return buildNameMapping(
                await(
                        admin.listDatabases(),
                        FLUSS_METADATA_ERROR,
                        "Failed to list Fluss databases"));
    }

    private TableInfo loadTableInfo(String databaseName, String tableName) {
        TablePath tablePath = TablePath.of(databaseName, tableName);
        return await(
                admin.getTableInfo(tablePath),
                FLUSS_METADATA_ERROR,
                "Failed to get Fluss table metadata for " + tablePath);
    }

    private static String canonicalize(String name) {
        return name.toLowerCase(Locale.ROOT);
    }

    private static Map<String, List<String>> buildNameMapping(List<String> names) {
        Map<String, List<String>> candidates = new LinkedHashMap<>();

        for (String name : names) {
            candidates.computeIfAbsent(canonicalize(name), ignored -> new ArrayList<>()).add(name);
        }

        ImmutableMap.Builder<String, List<String>> nameMapping = ImmutableMap.builder();
        for (Map.Entry<String, List<String>> entry : candidates.entrySet()) {
            List<String> physicalNames = new ArrayList<>(entry.getValue());
            Collections.sort(physicalNames);
            nameMapping.put(entry.getKey(), ImmutableList.copyOf(physicalNames));
        }

        return nameMapping.build();
    }

    private static Optional<String> resolveName(
            Map<String, List<String>> mapping, String name, String objectType) {
        List<String> candidates = mapping.get(canonicalize(name));
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

    private static <T> T await(
            CompletableFuture<T> future, ErrorCodeSupplier errorCode, String message) {
        try {
            return future.join();
        } catch (CompletionException e) {
            Throwable cause = unwrapCompletionException(e);

            if (cause instanceof TableNotExistException) {
                throw (TableNotExistException) cause;
            }
            if (cause instanceof DatabaseNotExistException) {
                throw (DatabaseNotExistException) cause;
            }
            if (cause instanceof TrinoException) {
                throw (TrinoException) cause;
            }
            if (cause instanceof Error) {
                throw (Error) cause;
            }

            throw new TrinoException(errorCode, message, cause);
        }
    }

    private static Throwable unwrapCompletionException(Throwable failure) {
        while (failure instanceof CompletionException && failure.getCause() != null) {
            failure = failure.getCause();
        }
        return failure;
    }

    /** Immutable table-name snapshot that resolves physical names without additional RPC calls. */
    static final class TableNameMapping {

        private final ResolvedSchemaName schema;
        private final Map<String, List<String>> candidates;

        private TableNameMapping(ResolvedSchemaName schema, Map<String, List<String>> candidates) {
            this.schema = checkNotNull(schema, "schema is null");
            this.candidates = checkNotNull(candidates, "candidates is null");
        }

        /**
         * Returns distinct logical table names, including names with ambiguous physical matches.
         */
        List<SchemaTableName> listTableNames() {
            ImmutableList.Builder<SchemaTableName> tables = ImmutableList.builder();

            for (String name : candidates.keySet()) {
                tables.add(new SchemaTableName(schema.getTrinoName(), name));
            }

            return tables.build();
        }

        /** Returns whether any physical table has the requested logical name. */
        boolean containsTable(String tableName) {
            return candidates.containsKey(canonicalize(tableName));
        }

        /** Resolves one logical table name and rejects ambiguity for that name only. */
        Optional<ResolvedTableName> resolveTable(String tableName) {
            String canonicalName = canonicalize(tableName);

            return resolveName(candidates, canonicalName, "table")
                    .map(physicalName -> schema.table(canonicalName, physicalName));
        }
    }
}
