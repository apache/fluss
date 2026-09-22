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

import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.shaded.guava32.com.google.common.collect.ImmutableList;

import com.google.inject.Inject;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTableVersion;
import io.trino.spi.connector.RelationColumnsMetadata;
import io.trino.spi.connector.RelationCommentMetadata;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SystemTable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** Provides Trino metadata backed by the Fluss catalog. */
public final class FlussMetadata implements ConnectorMetadata {

    private final FlussMetadataAccess metadataAccess;

    @Inject
    FlussMetadata(FlussMetadataAccess metadataAccess) {
        this.metadataAccess = checkNotNull(metadataAccess, "metadataAccess is null");
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session) {
        return metadataAccess.listSchemaNames();
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName) {
        ImmutableList.Builder<SchemaTableName> tables = ImmutableList.builder();

        for (FlussMetadataAccess.TableNameIndex index :
                metadataAccess.listTableIndexes(schemaName)) {
            tables.addAll(index.listTableNames());
        }

        return tables.build();
    }

    @Override
    public ConnectorTableHandle getTableHandle(
            ConnectorSession session,
            SchemaTableName tableName,
            Optional<ConnectorTableVersion> startVersion,
            Optional<ConnectorTableVersion> endVersion) {

        if (startVersion.isPresent() || endVersion.isPresent()) {
            throw new TrinoException(
                    NOT_SUPPORTED, "Fluss connector does not support table versioning");
        }

        Optional<ResolvedTableName> resolvedTable = metadataAccess.resolveTable(tableName);

        if (!resolvedTable.isPresent()) {
            return null;
        }

        ResolvedTableName resolved = resolvedTable.get();

        Optional<TableInfo> tableInfo = metadataAccess.findTableInfo(resolved);

        if (!tableInfo.isPresent()) {
            return null;
        }

        TableInfo info = tableInfo.get();

        return new FlussTableHandle(
                resolved.getTrinoName().getSchemaName(),
                resolved.getTrinoName().getTableName(),
                resolved.getFlussDatabaseName(),
                resolved.getFlussTableName(),
                info.getTableId(),
                info.getSchemaId());
    }

    @Override
    public Optional<SystemTable> getSystemTable(
            ConnectorSession session, SchemaTableName tableName) {
        String name = tableName.getTableName();
        if (!isColumnsSystemTable(name)) {
            return Optional.empty();
        }

        Optional<ResolvedSchemaName> schema =
                metadataAccess.resolveSchema(tableName.getSchemaName());
        if (!schema.isPresent()) {
            return Optional.empty();
        }

        FlussMetadataAccess.TableNameIndex tables = metadataAccess.indexTables(schema.get());

        // Physical tables take precedence over connector-defined system tables.
        if (tables.containsTable(name)) {
            return Optional.empty();
        }

        Optional<ResolvedTableName> baseTable = tables.resolveTable(getColumnsBaseTableName(name));
        return baseTable.flatMap(
                resolvedTableName ->
                        metadataAccess
                                .findTableInfo(resolvedTableName)
                                .map(info -> new FlussColumnsSystemTable(tableName, info)));
    }

    @Override
    public SchemaTableName getTableName(ConnectorSession session, ConnectorTableHandle table) {
        FlussTableHandle handle = (FlussTableHandle) table;

        return new SchemaTableName(handle.getSchemaName(), handle.getTableName());
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(
            ConnectorSession session, ConnectorTableHandle table) {

        TableInfo info = metadataAccess.getTableInfo((FlussTableHandle) table);

        return new ConnectorTableMetadata(
                getTableName(session, table),
                getColumns(info),
                FlussTableProperties.fromTableInfo(info),
                getTableComment(info));
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(
            ConnectorSession session, ConnectorTableHandle table) {

        TableInfo info = metadataAccess.getTableInfo((FlussTableHandle) table);

        List<String> names = info.getSchema().getColumnNames();
        validateColumnNames(names);

        Map<String, ColumnHandle> handles = new LinkedHashMap<>();

        for (int position = 0; position < names.size(); position++) {
            String name = names.get(position);

            handles.put(name.toLowerCase(Locale.ROOT), new FlussColumnHandle(name, position));
        }

        return Collections.unmodifiableMap(handles);
    }

    @Override
    public ColumnMetadata getColumnMetadata(
            ConnectorSession session, ConnectorTableHandle table, ColumnHandle columnHandle) {

        FlussColumnHandle column = (FlussColumnHandle) columnHandle;

        List<Schema.Column> columns =
                metadataAccess.getTableInfo((FlussTableHandle) table).getSchema().getColumns();

        int ordinalPosition = column.getOrdinalPosition();

        if (ordinalPosition < 0 || ordinalPosition >= columns.size()) {
            throw new IllegalArgumentException(
                    "Invalid column ordinal: " + column.getOrdinalPosition());
        }

        Schema.Column field = columns.get(column.getOrdinalPosition());

        if (!field.getName().equals(column.getName())) {
            throw new IllegalArgumentException(
                    "Column handle does not match current table schema: " + column.getName());
        }

        return toColumnMetadata(field);
    }

    @Override
    public Iterator<RelationColumnsMetadata> streamRelationColumns(
            ConnectorSession session,
            Optional<String> schemaName,
            UnaryOperator<Set<SchemaTableName>> relationFilter) {

        List<RelationColumnsMetadata> relations = new ArrayList<>();

        for (ResolvedTableName table : getFilteredRelations(schemaName, relationFilter)) {

            Optional<TableInfo> tableInfo = metadataAccess.findTableInfo(table);

            if (!tableInfo.isPresent()) {
                continue;
            }

            relations.add(
                    RelationColumnsMetadata.forTable(
                            table.getTrinoName(), getColumns(tableInfo.get())));
        }

        return relations.iterator();
    }

    @Override
    public Iterator<RelationCommentMetadata> streamRelationComments(
            ConnectorSession session,
            Optional<String> schemaName,
            UnaryOperator<Set<SchemaTableName>> relationFilter) {

        List<RelationCommentMetadata> relations = new ArrayList<>();

        for (ResolvedTableName table : getFilteredRelations(schemaName, relationFilter)) {

            Optional<TableInfo> tableInfo = metadataAccess.findTableInfo(table);

            if (!tableInfo.isPresent()) {
                continue;
            }

            relations.add(
                    RelationCommentMetadata.forRelation(
                            table.getTrinoName(), getTableComment(tableInfo.get())));
        }

        return relations.iterator();
    }

    private List<ResolvedTableName> getFilteredRelations(
            Optional<String> schemaName, UnaryOperator<Set<SchemaTableName>> relationFilter) {
        Map<SchemaTableName, FlussMetadataAccess.TableNameIndex> indexesByTable =
                new LinkedHashMap<>();

        for (FlussMetadataAccess.TableNameIndex index :
                metadataAccess.listTableIndexes(schemaName)) {
            for (SchemaTableName tableName : index.listTableNames()) {
                indexesByTable.put(tableName, index);
            }
        }

        Set<SchemaTableName> filteredNames =
                relationFilter.apply(new LinkedHashSet<>(indexesByTable.keySet()));

        ImmutableList.Builder<ResolvedTableName> tables = ImmutableList.builder();

        for (SchemaTableName tableName : filteredNames) {
            FlussMetadataAccess.TableNameIndex index = indexesByTable.get(tableName);
            if (index == null) {
                continue;
            }

            index.resolveTable(tableName.getTableName()).ifPresent(tables::add);
        }

        return tables.build();
    }

    private static ColumnMetadata toColumnMetadata(Schema.Column column) {
        return ColumnMetadata.builder()
                .setName(column.getName())
                .setType(FlussTypeConverter.toTrinoType(column.getDataType()))
                .setNullable(column.getDataType().isNullable())
                .setComment(column.getComment())
                .build();
    }

    private static List<ColumnMetadata> getColumns(TableInfo info) {
        validateColumnNames(info.getSchema().getColumnNames());

        return info.getSchema().getColumns().stream()
                .map(FlussMetadata::toColumnMetadata)
                .collect(Collectors.toList());
    }

    private static Optional<String> getTableComment(TableInfo info) {
        return info.getComment().filter(comment -> !comment.isEmpty());
    }

    private static boolean isColumnsSystemTable(String tableName) {
        return tableName.endsWith(FlussColumnsSystemTable.SUFFIX)
                && tableName.length() > FlussColumnsSystemTable.SUFFIX.length();
    }

    private static String getColumnsBaseTableName(String tableName) {
        return tableName.substring(0, tableName.length() - FlussColumnsSystemTable.SUFFIX.length());
    }

    private static void validateColumnNames(List<String> names) {
        Map<String, String> canonicalNames = new LinkedHashMap<>();

        for (String name : names) {
            String canonicalName = name.toLowerCase(Locale.ROOT);

            String previous = canonicalNames.putIfAbsent(canonicalName, name);

            if (previous != null) {
                throw new TrinoException(
                        NOT_SUPPORTED, "Ambiguous Fluss columns: " + previous + " and " + name);
            }
        }
    }
}
