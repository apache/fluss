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

import java.util.List;
import java.util.Objects;

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

    // Now we only maintain the Builder
    private final Schema.Builder builder;

    public SchemaUpdate(TableInfo tableInfo) {
        // Initialize builder from the current table schema
        this.builder = Schema.newBuilder().fromSchema(tableInfo.getSchema());
    }

    public Schema getSchema() {
        // Validation and building are now delegated
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
        // Use the builder to check if column exists
        Schema.Column existingColumn = builder.getColumn(addColumn.getName()).orElse(null);

        if (existingColumn != null) {
            if (!existingColumn.getDataType().equals(addColumn.getDataType())
                    || !Objects.equals(
                            existingColumn.getComment().orElse(null), addColumn.getComment())) {
                throw new IllegalArgumentException(
                        "Column " + addColumn.getName() + " already exists.");
            }
            return this;
        }

        if (addColumn.getPosition() != TableChange.ColumnPosition.last()) {
            throw new IllegalArgumentException("Only support addColumn column at last now.");
        }

        if (!addColumn.getDataType().isNullable()) {
            throw new IllegalArgumentException(
                    "Column " + addColumn.getName() + " must be nullable.");
        }

        // Delegate the actual addition to the builder
        builder.column(addColumn.getName(), addColumn.getDataType());

        // Fixed: Use null check for the String comment
        String comment = addColumn.getComment();
        if (comment != null) {
            builder.withComment(comment);
        }

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
