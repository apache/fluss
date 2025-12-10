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

package org.apache.fluss.connector.trino;

import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.connector.trino.connection.FlussClientManager;
import org.apache.fluss.connector.trino.handle.FlussColumnHandle;
import org.apache.fluss.connector.trino.handle.FlussTableHandle;
import org.apache.fluss.connector.trino.typeutils.FlussTypeUtils;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.types.DataField;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SchemaTablePrefix;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;

import javax.inject.Inject;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

/**
 * Metadata provider for Fluss Trino connector.
 *
 * <p>This class implements the ConnectorMetadata interface to provide metadata
 * information about Fluss tables, schemas, and columns to Trino.
 */
public class FlussMetadata implements ConnectorMetadata {

    private static final Logger log = Logger.get(FlussMetadata.class);

    private final FlussClientManager clientManager;
    private final TypeManager typeManager;

    @Inject
    public FlussMetadata(
            FlussClientManager clientManager,
            TypeManager typeManager) {
        this.clientManager = requireNonNull(clientManager, "clientManager is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session) {
        try {
            Admin admin = clientManager.getAdmin();
            List<String> databases = admin.listDatabases().get();
            log.debug("Listed %d schema names", databases.size());
            return databases;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while listing databases", e);
        } catch (ExecutionException e) {
            throw new RuntimeException("Failed to list databases", e);
        }
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName) {
        try {
            Admin admin = clientManager.getAdmin();
            ImmutableList.Builder<SchemaTableName> tables = ImmutableList.builder();
              
            List<String> databases = schemaName.isPresent()   
                ? ImmutableList.of(schemaName.get())
                : admin.listDatabases().get();
                  
            for (String database : databases) {
                try {
                    List<String> tableNames = admin.listTables(database).get();
                    for (String tableName : tableNames) {
                        tables.add(new SchemaTableName(database, tableName));
                    }
                } catch (Exception e) {
                    log.warn(e, "Failed to list tables in database: %s", database);
                }
            }
              
            List<SchemaTableName> result = tables.build();
            log.debug("Listed %d tables", result.size());
            return result;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while listing tables", e);
        } catch (ExecutionException e) {
            throw new RuntimeException("Failed to list tables", e);
        }
    }

    @Override
    public ConnectorTableHandle getTableHandle(
            ConnectorSession session,
            SchemaTableName tableName) {
        try {
            Admin admin = clientManager.getAdmin();
            TablePath tablePath = new TablePath(tableName.getSchemaName(), tableName.getTableName());
              
            TableInfo tableInfo = admin.getTableInfo(tablePath).get();
            if (tableInfo == null) {
                log.debug("Table not found: %s", tableName);
                return null;
            }
              
            log.debug("Got table handle for: %s", tableName);
            return new FlussTableHandle(
                    tableName.getSchemaName(),
                    tableName.getTableName(),
                    tableInfo);
        } catch (InterruptedException e) {
            throw new RuntimeException("Interrupted while getting table handle", e);
        } catch (ExecutionException e) {
            log.debug(e, "Table %s not found", tableName);
            return null;
        }
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(
            ConnectorSession session,
            ConnectorTableHandle table) {
        FlussTableHandle flussTable = (FlussTableHandle) table;
          
        try {
            ImmutableList.Builder<ColumnMetadata> columns = ImmutableList.builder();
            List<DataField> fields = flussTable.getTableInfo().getSchema().toRowType().getFields();
          
            // Get primary keys if exist
            Set<String> primaryKeys = flussTable.getTableInfo().getSchema()
                    .getPrimaryKey()
                    .map(pk -> Set.copyOf(pk.getColumnNames()))
                    .orElse(Set.of());
        
            // Get partition keys if exist  
            Set<String> partitionKeys = flussTable.getTableInfo().getTableDescriptor()
                    .getPartitionKeys()
                    .map(pk -> Set.copyOf(pk))
                    .orElse(Set.of());
          
            for (int i = 0; i < fields.size(); i++) {
                try {
                    DataField field = fields.get(i);
                    String fieldName = field.getName();
                    org.apache.fluss.types.DataType flussType = field.getType();
                    Type trinoType = FlussTypeUtils.toTrinoType(flussType, typeManager);
              
                    ColumnMetadata.Builder builder = ColumnMetadata.builder()
                            .setName(fieldName)
                            .setType(trinoType)
                            .setNullable(field.getType().isNullable());
            
                    // Add comment if exists
                    field.getDescription().ifPresent(builder::setComment);
            
                    columns.add(builder.build());
                } catch (Exception e) {
                    log.warn(e, "Error processing field metadata at index %d for table %s.%s, using default metadata",
                            i, flussTable.getSchemaName(), flussTable.getTableName());
                    
                    // Create fallback column metadata
                    DataField field = fields.get(i);
                    ColumnMetadata fallback = ColumnMetadata.builder()
                            .setName(field.getName())
                            .setType(io.trino.spi.type.VarcharType.VARCHAR)  // Fallback to VARCHAR
                            .setNullable(true)
                            .setComment("Error processing field: " + e.getMessage())
                            .build();
                    columns.add(fallback);
                }
            }
          
            log.debug("Got table metadata for: %s.%s with %d columns", 
                    flussTable.getSchemaName(), flussTable.getTableName(), fields.size());
          
            return new ConnectorTableMetadata(
                    new SchemaTableName(flussTable.getSchemaName(), flussTable.getTableName()),
                    columns.build());
        } catch (Exception e) {
            log.error(e, "Error getting table metadata for: %s.%s",
                    flussTable.getSchemaName(), flussTable.getTableName());
            throw new RuntimeException("Failed to get table metadata for: " + 
                    flussTable.getSchemaName() + "." + flussTable.getTableName(), e);
        }
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(
            ConnectorSession session,
            ConnectorTableHandle tableHandle) {
        FlussTableHandle flussTable = (FlussTableHandle) tableHandle;
          
        ImmutableMap.Builder<String, ColumnHandle> columnHandles = ImmutableMap.builder();
        List<DataField> fields = flussTable.getTableInfo().getSchema().toRowType().getFields();
        
        // Get primary keys if exist
        Set<String> primaryKeys = flussTable.getTableInfo().getSchema()
                .getPrimaryKey()
                .map(pk -> Set.copyOf(pk.getColumnNames()))
                .orElse(Set.of());
        
        // Get partition keys if exist
        Set<String> partitionKeys = flussTable.getTableInfo().getTableDescriptor()
                .getPartitionKeys()
                .map(pk -> Set.copyOf(pk))
                .orElse(Set.of());
          
        for (int i = 0; i < fields.size(); i++) {
            DataField field = fields.get(i);
            String fieldName = field.getName();
            org.apache.fluss.types.DataType flussType = field.getType();
            Type trinoType = FlussTypeUtils.toTrinoType(flussType, typeManager);
            
            boolean isPrimaryKey = primaryKeys.contains(fieldName);
            boolean isPartitionKey = partitionKeys.contains(fieldName);
            boolean isNullable = field.getType().isNullable();
              
            FlussColumnHandle columnHandle = new FlussColumnHandle(
                    fieldName, 
                    trinoType, 
                    i,
                    isPartitionKey,
                    isPrimaryKey,
                    isNullable,
                    field.getDescription(),
                    Optional.empty());
                    
            columnHandles.put(fieldName, columnHandle);
        }
        
        Map<String, ColumnHandle> result = columnHandles.build();
        log.debug("Got %d column handles for table: %s.%s", 
                result.size(), flussTable.getSchemaName(), flussTable.getTableName());
          
        return result;
    }

    @Override
    public ColumnMetadata getColumnMetadata(
            ConnectorSession session,
            ConnectorTableHandle tableHandle,
            ColumnHandle columnHandle) {
        FlussColumnHandle flussColumn = (FlussColumnHandle) columnHandle;
        return flussColumn.toColumnMetadata();
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(
            ConnectorSession session,
            SchemaTablePrefix prefix) {
        requireNonNull(prefix, "prefix is null");
        
        Map<SchemaTableName, List<ColumnMetadata>> columns = new HashMap<>();
        
        List<SchemaTableName> tables;
        if (prefix.getTable().isPresent()) {
            tables = ImmutableList.of(prefix.toSchemaTableName());
        } else {
            tables = listTables(session, prefix.getSchema());
        }
        
        for (SchemaTableName tableName : tables) {
            try {
                ConnectorTableHandle tableHandle = getTableHandle(session, tableName);
                if (tableHandle != null) {
                    ConnectorTableMetadata tableMetadata = getTableMetadata(session, tableHandle);
                    columns.put(tableName, tableMetadata.getColumns());
                }
            } catch (Exception e) {
                log.warn(e, "Failed to get columns for table: %s", tableName);
            }
        }
        
        log.debug("Listed columns for %d tables", columns.size());
        return columns;
    }
}
