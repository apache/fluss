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

package org.apache.fluss.lake.iceberg;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.metadata.TableDescriptor;

import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.fluss.metadata.TableDescriptor.BUCKET_COLUMN_NAME;
import static org.apache.fluss.metadata.TableDescriptor.OFFSET_COLUMN_NAME;
import static org.apache.fluss.metadata.TableDescriptor.TIMESTAMP_COLUMN_NAME;

/** Utilities for constructing and comparing the Iceberg schema managed by Fluss. */
@Internal
public final class IcebergSchemaUtils {

    /**
     * System columns that legacy Iceberg tables (created before FIP-27) carry as trailing fields.
     * Under FIP-27 these columns are no longer added to newly created (clean) tables.
     */
    public static final Map<String, Type> LEGACY_SYSTEM_COLUMNS;

    static {
        LinkedHashMap<String, Type> systemColumns = new LinkedHashMap<>();
        systemColumns.put(BUCKET_COLUMN_NAME, Types.IntegerType.get());
        systemColumns.put(OFFSET_COLUMN_NAME, Types.LongType.get());
        systemColumns.put(TIMESTAMP_COLUMN_NAME, Types.TimestampType.withZone());
        LEGACY_SYSTEM_COLUMNS = Collections.unmodifiableMap(systemColumns);
    }

    private IcebergSchemaUtils() {}

    /**
     * Creates a clean Iceberg schema from a Fluss table descriptor.
     *
     * <p>FIP-27: newly created tables contain only user columns; no system columns are appended.
     */
    public static Schema createIcebergSchema(
            TableDescriptor tableDescriptor, boolean isPrimaryKeyTable) {
        return buildIcebergSchema(tableDescriptor, isPrimaryKeyTable, false);
    }

    /**
     * Creates a legacy Iceberg schema from a Fluss table descriptor, appending the three system
     * columns (__bucket, __offset, __timestamp) after the user columns.
     *
     * <p>Used only for compatibility checks against existing legacy tables (FIP-27 pre-existing).
     */
    public static Schema createLegacyIcebergSchema(
            TableDescriptor tableDescriptor, boolean isPrimaryKeyTable) {
        return buildIcebergSchema(tableDescriptor, isPrimaryKeyTable, true);
    }

    private static Schema buildIcebergSchema(
            TableDescriptor tableDescriptor, boolean isPrimaryKeyTable, boolean includeSystemCols) {
        List<Types.NestedField> fields = new ArrayList<>();
        int fieldId = 0;

        int userColCount = tableDescriptor.getSchema().getColumns().size();
        int totalTopLevelFields =
                includeSystemCols ? userColCount + LEGACY_SYSTEM_COLUMNS.size() : userColCount;
        FlussDataTypeToIcebergDataType converter =
                new FlussDataTypeToIcebergDataType(totalTopLevelFields);

        for (org.apache.fluss.metadata.Schema.Column column :
                tableDescriptor.getSchema().getColumns()) {
            String columnName = column.getName();
            if (LEGACY_SYSTEM_COLUMNS.containsKey(columnName)) {
                throw new IllegalArgumentException(
                        "Column '"
                                + columnName
                                + "' conflicts with a reserved system column name.");
            }
            Types.NestedField field;
            if (column.getDataType().isNullable()) {
                field =
                        Types.NestedField.optional(
                                fieldId++,
                                columnName,
                                column.getDataType().accept(converter),
                                column.getComment().orElse(null));
            } else {
                field =
                        Types.NestedField.required(
                                fieldId++,
                                columnName,
                                column.getDataType().accept(converter),
                                column.getComment().orElse(null));
            }
            fields.add(field);
        }

        if (includeSystemCols) {
            for (Map.Entry<String, Type> systemColumn : LEGACY_SYSTEM_COLUMNS.entrySet()) {
                fields.add(
                        Types.NestedField.required(
                                fieldId++, systemColumn.getKey(), systemColumn.getValue()));
            }
        }

        if (isPrimaryKeyTable) {
            int[] primaryKeyIndexes = tableDescriptor.getSchema().getPrimaryKeyIndexes();
            Set<Integer> identifierFieldIds = new HashSet<>();
            for (int primaryKeyIndex : primaryKeyIndexes) {
                identifierFieldIds.add(fields.get(primaryKeyIndex).fieldId());
            }
            return new Schema(fields, identifierFieldIds);
        }
        return new Schema(fields);
    }

    /** Returns whether two Iceberg schemas have the same Fluss-managed structure. */
    public static boolean compatibleWith(Schema currentSchema, Schema expectedSchema) {
        List<Types.NestedField> currentFields =
                TypeUtil.assignIncreasingFreshIds(currentSchema).columns();
        List<Types.NestedField> expectedFields =
                TypeUtil.assignIncreasingFreshIds(expectedSchema).columns();
        if (currentFields.size() != expectedFields.size()) {
            return false;
        }
        for (int i = 0; i < currentFields.size(); i++) {
            Types.NestedField current = currentFields.get(i);
            Types.NestedField expected = expectedFields.get(i);
            if (!current.name().equals(expected.name())
                    || !current.type().equals(expected.type())
                    || current.isOptional() != expected.isOptional()) {
                return false;
            }
        }
        return true;
    }
}
