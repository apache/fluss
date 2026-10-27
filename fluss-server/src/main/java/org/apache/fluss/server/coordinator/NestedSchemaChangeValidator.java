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

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.exception.InvalidAlterTableException;
import org.apache.fluss.metadata.ColumnPath;
import org.apache.fluss.metadata.TableChange;
import org.apache.fluss.metadata.TableInfo;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/** Validator for nested field schema changes. */
final class NestedSchemaChangeValidator {

    private NestedSchemaChangeValidator() {}

    static void validate(TableInfo tableInfo, List<TableChange> changes) {
        boolean hasNestedFieldChange =
                changes.stream().anyMatch(change -> change instanceof TableChange.AddNestedField);
        if (!hasNestedFieldChange) {
            return;
        }

        if (isDataLakeEnabled(tableInfo)) {
            throw new InvalidAlterTableException(
                    "Cannot add nested field for datalake enabled table now.");
        }

        Set<String> keyColumns = new HashSet<>();
        keyColumns.addAll(tableInfo.getSchema().getPrimaryKeyColumnNames());
        keyColumns.addAll(tableInfo.getBucketKeys());
        keyColumns.addAll(tableInfo.getPartitionKeys());
        for (TableChange change : changes) {
            if (change instanceof TableChange.AddNestedField) {
                TableChange.AddNestedField addNestedField = (TableChange.AddNestedField) change;
                String topLevelField = topLevelField(addNestedField.getParentPath());
                if (keyColumns.contains(topLevelField)) {
                    throw new InvalidAlterTableException(
                            "Cannot add nested field under key column: " + topLevelField);
                }
            }
        }
    }

    private static boolean isDataLakeEnabled(TableInfo tableInfo) {
        return tableInfo.getProperties().get(ConfigOptions.TABLE_DATALAKE_ENABLED);
    }

    private static String topLevelField(ColumnPath columnPath) {
        ColumnPath.Segment segment = columnPath.getSegments().get(0);
        if (segment.getType() != ColumnPath.SegmentType.FIELD) {
            throw new InvalidAlterTableException(
                    "Nested field parent path must start with a top-level field: " + columnPath);
        }
        return segment.getName().get();
    }
}
