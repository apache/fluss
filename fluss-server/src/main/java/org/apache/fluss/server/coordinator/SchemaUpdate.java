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

package org.apache.fluss.server.coordinator;

import org.apache.fluss.exception.SchemaChangeException;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableChange;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.ReassignFieldId;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

/** Schema update. */
public class SchemaUpdate {

    /** Apply schema changes to the given table info and return the updated schema. */
    public static Schema applySchemaChanges(TableInfo tableInfo, List<TableChange> changes) {
        SchemaUpdate schemaUpdate = new SchemaUpdate(tableInfo);
        for (TableChange change : changes) {
            schemaUpdate = schemaUpdate.applySchemaChange(change);
        }
        return schemaUpdate.getSchema();
    }

    private final List<Schema.Column> columns;
    private final AtomicInteger highestFieldId;
    private final List<String> primaryKeys;
    private final Map<String, Schema.Column> existedColumns;
    private final List<String> autoIncrementColumns;

    public SchemaUpdate(TableInfo tableInfo) {
        this.columns = new ArrayList<>();
        this.existedColumns = new HashMap<>();
        this.highestFieldId = new AtomicInteger(tableInfo.getSchema().getHighestFieldId());
        this.primaryKeys = tableInfo.getPrimaryKeys();
        this.autoIncrementColumns = tableInfo.getSchema().getAutoIncrementColumnNames();
        this.columns.addAll(tableInfo.getSchema().getColumns());
        for (Schema.Column column : columns) {
            existedColumns.put(column.getName(), column);
        }
    }

    public Schema getSchema() {
        Schema.Builder builder =
                Schema.newBuilder()
                        .fromColumns(columns)
                        .highestFieldId((short) highestFieldId.get());
        if (!primaryKeys.isEmpty()) {
            builder.primaryKey(primaryKeys);
        }
        for (String autoIncrementColumn : autoIncrementColumns) {
            builder.enableAutoIncrement(autoIncrementColumn);
        }

        return builder.build();
    }

    public SchemaUpdate applySchemaChange(TableChange columnChange) {
        if (columnChange instanceof TableChange.AddColumn) {
            return addColumn((TableChange.AddColumn) columnChange);
        } else if (columnChange instanceof TableChange.ModifyColumn) {
            return modifiedColumn((TableChange.ModifyColumn) columnChange);
        } else if (columnChange instanceof TableChange.RenameColumn) {
            return renameColumn((TableChange.RenameColumn) columnChange);
        } else if (columnChange instanceof TableChange.DropColumn) {
            return dropColumn((TableChange.DropColumn) columnChange);
        }
        throw new IllegalArgumentException(
                "Unknown column change type " + columnChange.getClass().getName());
    }

    private SchemaUpdate addColumn(TableChange.AddColumn addColumn) {
        Schema.Column existingColumn = existedColumns.get(addColumn.getName());
        if (existingColumn != null) {
            // Allow idempotent retries: if column name/type/comment match existing, treat as no-op
            if (!existingColumn.getDataType().equals(addColumn.getDataType())
                    || !Objects.equals(
                            existingColumn.getComment().orElse(null), addColumn.getComment())) {
                throw new IllegalArgumentException(
                        "Column " + addColumn.getName() + " already exists.");
            }
            return this;
        }

        TableChange.ColumnPosition position = addColumn.getPosition();
        if (position != TableChange.ColumnPosition.last()) {
            throw new IllegalArgumentException("Only support addColumn column at last now.");
        }

        if (!addColumn.getDataType().isNullable()) {
            throw new IllegalArgumentException(
                    "Column " + addColumn.getName() + " must be nullable.");
        }

        int columnId = highestFieldId.incrementAndGet();
        DataType dataType = ReassignFieldId.reassign(addColumn.getDataType(), highestFieldId);

        Schema.Column newColumn =
                new Schema.Column(addColumn.getName(), dataType, addColumn.getComment(), columnId);
        columns.add(newColumn);
        existedColumns.put(newColumn.getName(), newColumn);
        return this;
    }

    private SchemaUpdate dropColumn(TableChange.DropColumn dropColumn) {
        throw new SchemaChangeException("Not support drop column now.");
    }

    private SchemaUpdate modifiedColumn(TableChange.ModifyColumn modifyColumn) {
        throw new SchemaChangeException("Not support modify column now.");
    }

    private SchemaUpdate renameColumn(TableChange.RenameColumn renameColumn) {
        throw new SchemaChangeException("Not support rename column now.");
    }
}
