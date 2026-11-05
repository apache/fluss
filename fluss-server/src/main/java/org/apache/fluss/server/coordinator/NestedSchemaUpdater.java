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

import org.apache.fluss.exception.InvalidAlterTableException;
import org.apache.fluss.metadata.ColumnPath;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.types.DataField;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.ReassignFieldId;
import org.apache.fluss.types.RowType;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/** Utilities for updating nested fields in a schema. */
final class NestedSchemaUpdater {

    private NestedSchemaUpdater() {}

    static Schema addField(
            Schema schema,
            ColumnPath parentPath,
            String fieldName,
            DataType dataType,
            String comment) {
        if (!dataType.isNullable()) {
            throw new InvalidAlterTableException(
                    "Nested field " + parentPath + "." + fieldName + " must be nullable.");
        }
        validatePrimaryKeyNotAffected(schema, parentPath);

        AtomicInteger highestFieldId = new AtomicInteger(schema.getHighestFieldId());
        List<Schema.Column> updatedColumns = new ArrayList<>();
        boolean[] updated = new boolean[] {false};
        ColumnPath.Segment topLevelSegment = parentPath.getSegments().get(0);
        if (topLevelSegment.getType() != ColumnPath.SegmentType.FIELD) {
            throw new InvalidAlterTableException(
                    "Nested field parent path must start with a top-level field: " + parentPath);
        }
        for (Schema.Column column : schema.getColumns()) {
            DataType updatedType = column.getDataType();
            if (column.getName().equals(topLevelSegment.getName().get())) {
                updatedType =
                        updateDataType(
                                column.getDataType(),
                                parentPath.getSegments(),
                                1,
                                fieldName,
                                dataType,
                                comment,
                                highestFieldId,
                                updated);
            }
            updatedColumns.add(
                    new Schema.Column(
                            column.getName(),
                            updatedType,
                            column.getComment().orElse(null),
                            column.getColumnId(),
                            column.getAggFunction().orElse(null)));
        }

        if (!updated[0]) {
            throw new InvalidAlterTableException("Cannot find parent path: " + parentPath);
        }

        Schema.Builder builder =
                Schema.newBuilder()
                        .fromColumns(updatedColumns)
                        .highestFieldId(highestFieldId.get());
        schema.getPrimaryKey()
                .ifPresent(
                        primaryKey ->
                                builder.primaryKeyNamed(
                                        primaryKey.getConstraintName(),
                                        primaryKey.getColumnNames()));
        for (String autoIncrementColumnName : schema.getAutoIncrementColumnNames()) {
            builder.enableAutoIncrement(autoIncrementColumnName);
        }
        return builder.build();
    }

    private static DataType updateDataType(
            DataType currentType,
            List<ColumnPath.Segment> segments,
            int segmentIndex,
            String fieldName,
            DataType fieldType,
            String comment,
            AtomicInteger highestFieldId,
            boolean[] updated) {
        if (segmentIndex >= segments.size()) {
            updated[0] = true;
            return addFieldToRowType(currentType, fieldName, fieldType, comment, highestFieldId);
        }

        ColumnPath.Segment segment = segments.get(segmentIndex);
        if (segment.getType() != ColumnPath.SegmentType.FIELD) {
            throw new InvalidAlterTableException(
                    "Only ROW field path segment is supported for nested field add now: "
                            + segment);
        }
        if (!(currentType instanceof RowType)) {
            return currentType;
        }

        RowType rowType = (RowType) currentType;
        int fieldIndex = rowType.getFieldIndex(segment.getName().get());
        if (fieldIndex < 0) {
            return currentType;
        }

        List<DataField> fields = new ArrayList<>(rowType.getFields());
        DataField field = fields.get(fieldIndex);
        DataType updatedFieldType =
                updateDataType(
                        field.getType(),
                        segments,
                        segmentIndex + 1,
                        fieldName,
                        fieldType,
                        comment,
                        highestFieldId,
                        updated);
        fields.set(
                fieldIndex,
                new DataField(
                        field.getName(),
                        updatedFieldType,
                        field.getDescription().orElse(null),
                        field.getFieldId()));
        return new RowType(rowType.isNullable(), fields);
    }

    private static DataType addFieldToRowType(
            DataType currentType,
            String fieldName,
            DataType fieldType,
            String comment,
            AtomicInteger highestFieldId) {
        if (!(currentType instanceof RowType)) {
            throw new InvalidAlterTableException("Parent path must refer to a ROW type.");
        }

        RowType rowType = (RowType) currentType;
        if (rowType.getFieldIndex(fieldName) >= 0) {
            throw new InvalidAlterTableException("Nested field " + fieldName + " already exists.");
        }

        int fieldId = highestFieldId.incrementAndGet();
        DataType reassignedFieldType = ReassignFieldId.reassign(fieldType, highestFieldId);
        List<DataField> fields = new ArrayList<>(rowType.getFields());
        fields.add(new DataField(fieldName, reassignedFieldType, comment, fieldId));
        return new RowType(rowType.isNullable(), fields);
    }

    private static void validatePrimaryKeyNotAffected(Schema schema, ColumnPath parentPath) {
        ColumnPath.Segment topLevelSegment = parentPath.getSegments().get(0);
        if (topLevelSegment.getType() != ColumnPath.SegmentType.FIELD) {
            return;
        }

        String topLevelField = topLevelSegment.getName().get();
        if (schema.getPrimaryKeyColumnNames().contains(topLevelField)) {
            throw new InvalidAlterTableException(
                    "Cannot add nested field under primary key column: " + topLevelField);
        }
    }
}
