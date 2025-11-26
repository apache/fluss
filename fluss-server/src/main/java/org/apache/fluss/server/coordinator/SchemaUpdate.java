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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/** Schema update. */
public class SchemaUpdate implements UpdateSchema {
    private final List<Schema.Column> columns;
    private final AtomicInteger highestFieldId;
    private final List<String> primaryKeys;
    private final List<String> bucketKeys;
    private final List<String> partitionKeys;
    private final Map<String, Schema.Column> existedColumns;

    public SchemaUpdate(TableInfo tableInfo) {
        this.columns = new ArrayList<>();
        this.existedColumns = new HashMap<>();
        this.highestFieldId = new AtomicInteger(tableInfo.getSchema().getHighestFieldId());
        this.primaryKeys = tableInfo.getPrimaryKeys();
        this.bucketKeys = tableInfo.getBucketKeys();
        this.partitionKeys = tableInfo.getPartitionKeys();
        this.columns.addAll(tableInfo.getSchema().getColumns());
        for (Schema.Column column : columns) {
            existedColumns.put(column.getName(), column);
        }
    }

    @Override
    public Schema getSchema() {
        Schema.Builder builder =
                Schema.newBuilder()
                        .fromColumns(columns)
                        .highestFieldId((short) highestFieldId.get());
        if (!primaryKeys.isEmpty()) {
            builder.primaryKey(primaryKeys);
        }

        return builder.build();
    }

    @Override
    public UpdateSchema applySchemaChange(TableChange columnChange) {
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

    private UpdateSchema addColumn(TableChange.AddColumn addColumn) {
        if (existedColumns.containsKey(addColumn.getName())) {
            throw new IllegalArgumentException(
                    "Column " + addColumn.getName() + " already exists.");
        }

        TableChange.ColumnPosition position = addColumn.getPosition();
        if (position != TableChange.ColumnPosition.last()) {
            throw new IllegalArgumentException("Only support addColumn column at last now.");
        }

        if (!addColumn.getDataType().isNullable()) {
            throw new IllegalArgumentException(
                    "Column " + addColumn.getName() + " must be nullable.");
        }

        Schema.Column newColumn =
                new Schema.Column(
                        addColumn.getName(),
                        addColumn.getDataType(),
                        addColumn.getComment(),
                        (byte) highestFieldId.incrementAndGet());
        columns.add(newColumn);
        existedColumns.put(newColumn.getName(), newColumn);
        return this;
    }

    private UpdateSchema dropColumn(TableChange.DropColumn dropColumn) {
        throw new SchemaChangeException("Not support drop column now.");
    }

    private UpdateSchema modifiedColumn(TableChange.ModifyColumn modifyColumn) {
        throw new SchemaChangeException("Not support modify column now.");
    }

    private UpdateSchema renameColumn(TableChange.RenameColumn renameColumn) {
        throw new SchemaChangeException("Not support rename column now.");
    }
}
