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

package org.apache.fluss.connector.trino.handle;

import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.predicate.TupleDomain;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static java.util.Objects.requireNonNull;

/**
 * Table handle for Fluss Trino connector with support for:
 * - Predicate pushdown
 * - Column pruning
 * - Partition pruning
 * - Union Read optimization
 * - Limit pushdown
 */
public class FlussTableHandle implements ConnectorTableHandle {

    private final String schemaName;
    private final String tableName;
    private final TableInfo tableInfo;
    private final TablePath tablePath;
    
    // Query optimization features
    private final TupleDomain<ColumnHandle> constraint;
    private final Optional<Set<ColumnHandle>> projectedColumns;
    private final Optional<List<String>> partitionFilters;
    private final boolean unionReadEnabled;
    private final Optional<Long> limit;

    @JsonCreator
    public FlussTableHandle(
            @JsonProperty("schemaName") String schemaName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("tableInfo") TableInfo tableInfo,
            @JsonProperty("constraint") TupleDomain<ColumnHandle> constraint,
            @JsonProperty("projectedColumns") Optional<Set<ColumnHandle>> projectedColumns,
            @JsonProperty("partitionFilters") Optional<List<String>> partitionFilters,
            @JsonProperty("unionReadEnabled") boolean unionReadEnabled,
            @JsonProperty("limit") Optional<Long> limit) {
        this.schemaName = requireNonNull(schemaName, "schemaName is null");
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.tableInfo = requireNonNull(tableInfo, "tableInfo is null");
        this.tablePath = new TablePath(schemaName, tableName);
        this.constraint = requireNonNull(constraint, "constraint is null");
        this.projectedColumns = requireNonNull(projectedColumns, "projectedColumns is null");
        this.partitionFilters = requireNonNull(partitionFilters, "partitionFilters is null");
        this.unionReadEnabled = unionReadEnabled;
        this.limit = requireNonNull(limit, "limit is null");
    }

    // Convenience constructor for basic table handle
    public FlussTableHandle(String schemaName, String tableName, TableInfo tableInfo) {
        this(schemaName, tableName, tableInfo, 
             TupleDomain.all(), 
             Optional.empty(), 
             Optional.empty(), 
             true, 
             Optional.empty());
    }

    @JsonProperty
    public String getSchemaName() {
        return schemaName;
    }

    @JsonProperty
    public String getTableName() {
        return tableName;
    }

    @JsonProperty
    public TableInfo getTableInfo() {
        return tableInfo;
    }

    public TablePath getTablePath() {
        return tablePath;
    }

    @JsonProperty
    public TupleDomain<ColumnHandle> getConstraint() {
        return constraint;
    }

    @JsonProperty
    public Optional<Set<ColumnHandle>> getProjectedColumns() {
        return projectedColumns;
    }

    @JsonProperty
    public Optional<List<String>> getPartitionFilters() {
        return partitionFilters;
    }

    @JsonProperty
    public boolean isUnionReadEnabled() {
        return unionReadEnabled;
    }

    @JsonProperty
    public Optional<Long> getLimit() {
        return limit;
    }

    /**
     * Create a new table handle with applied constraint (predicate pushdown).
     */
    public FlussTableHandle withConstraint(TupleDomain<ColumnHandle> newConstraint) {
        return new FlussTableHandle(
                schemaName, tableName, tableInfo, 
                newConstraint, projectedColumns, partitionFilters, 
                unionReadEnabled, limit);
    }

    /**
     * Create a new table handle with projected columns (column pruning).
     */
    public FlussTableHandle withProjectedColumns(Set<ColumnHandle> columns) {
        return new FlussTableHandle(
                schemaName, tableName, tableInfo, 
                constraint, Optional.of(ImmutableSet.copyOf(columns)), partitionFilters, 
                unionReadEnabled, limit);
    }

    /**
     * Create a new table handle with partition filters (partition pruning).
     */
    public FlussTableHandle withPartitionFilters(List<String> filters) {
        return new FlussTableHandle(
                schemaName, tableName, tableInfo, 
                constraint, projectedColumns, Optional.of(ImmutableList.copyOf(filters)), 
                unionReadEnabled, limit);
    }

    /**
     * Create a new table handle with limit pushdown.
     */
    public FlussTableHandle withLimit(long limitValue) {
        return new FlussTableHandle(
                schemaName, tableName, tableInfo, 
                constraint, projectedColumns, partitionFilters, 
                unionReadEnabled, Optional.of(limitValue));
    }

    /**
     * Create a new table handle with Union Read setting.
     */
    public FlussTableHandle withUnionRead(boolean enabled) {
        return new FlussTableHandle(
                schemaName, tableName, tableInfo, 
                constraint, projectedColumns, partitionFilters, 
                enabled, limit);
    }

    /**
     * Check if this table handle has any optimizations applied.
     */
    public boolean hasOptimizations() {
        return !constraint.isAll() || 
               projectedColumns.isPresent() || 
               partitionFilters.isPresent() || 
               limit.isPresent();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        FlussTableHandle that = (FlussTableHandle) obj;
        return Objects.equals(schemaName, that.schemaName) &&
                Objects.equals(tableName, that.tableName) &&
                Objects.equals(constraint, that.constraint) &&
                Objects.equals(projectedColumns, that.projectedColumns) &&
                Objects.equals(partitionFilters, that.partitionFilters) &&  
                unionReadEnabled == that.unionReadEnabled &&  
                Objects.equals(limit, that.limit);  
    }  
  
    @Override  
    public int hashCode() {  
        return Objects.hash(schemaName, tableName, constraint, projectedColumns,   
                           partitionFilters, unionReadEnabled, limit);  
    }  
  
    @Override  
    public String toString() {  
        return "FlussTableHandle{" +  
                "schemaName='" + schemaName + '\'' +  
                ", tableName='" + tableName + '\'' +  
                ", constraint=" + constraint +  
                ", projectedColumns=" + projectedColumns +  
                ", partitionFilters=" + partitionFilters +  
                ", unionReadEnabled=" + unionReadEnabled +  
                ", limit=" + limit +  
                '}';  
    }  
}
