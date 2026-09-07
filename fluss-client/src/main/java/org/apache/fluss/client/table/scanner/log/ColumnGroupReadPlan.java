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

package org.apache.fluss.client.table.scanner.log;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.metadata.ColumnGroupSchemaGetter;
import org.apache.fluss.metadata.LogFormat;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.SchemaGetter;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.record.LogRecordReadContext;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.shaded.arrow.org.apache.arrow.memory.AllocationManager;
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.Projection;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * How a scan of a column-group table (FIP-45) maps its output columns onto the physical logs: the
 * base log holds the default-group columns, each column group holds its own columns, and the client
 * stitches rows from them by offset.
 */
@Internal
public final class ColumnGroupReadPlan {

    /** Projection over the base physical row, in output order; null for the whole base row. */
    @Nullable private final Projection baseProjection;

    /** Column groups touched by the scan, in order of first appearance. */
    private final List<String> touchedGroups;

    private final Map<String, RowType> groupRowTypes;

    /** Per output column: -1 for the base log, else the index into {@link #touchedGroups}. */
    private final int[] outputSource;

    /** Per output column: index into the base output row or into the group's physical row. */
    private final int[] outputField;

    private ColumnGroupReadPlan(
            @Nullable Projection baseProjection,
            List<String> touchedGroups,
            Map<String, RowType> groupRowTypes,
            int[] outputSource,
            int[] outputField) {
        this.baseProjection = baseProjection;
        this.touchedGroups = touchedGroups;
        this.groupRowTypes = groupRowTypes;
        this.outputSource = outputSource;
        this.outputField = outputField;
    }

    /** Plans a scan of {@code schema} with {@code projection}; null if the table has no groups. */
    @Nullable
    public static ColumnGroupReadPlan of(Schema schema, @Nullable Projection projection) {
        if (!schema.hasColumnGroups()) {
            return null;
        }
        int[] baseIndices = schema.getDefaultGroupColumnIndices();
        Map<String, List<Integer>> groups = schema.getColumnGroups();
        int[] outputColumns =
                projection == null
                        ? allColumns(schema.getColumns().size())
                        : projection.getProjection();

        List<String> touched = new ArrayList<>();
        List<Integer> baseFields = new ArrayList<>();
        int[] outputSource = new int[outputColumns.length];
        int[] outputField = new int[outputColumns.length];
        for (int i = 0; i < outputColumns.length; i++) {
            int column = outputColumns[i];
            String group = schema.getColumnGroupOf(column);
            if (group == null) {
                int basePosition = indexOf(baseIndices, column);
                outputSource[i] = -1;
                if (projection == null) {
                    outputField[i] = basePosition;
                } else {
                    outputField[i] = baseFields.size();
                    baseFields.add(basePosition);
                }
            } else {
                int groupIndex = touched.indexOf(group);
                if (groupIndex < 0) {
                    groupIndex = touched.size();
                    touched.add(group);
                }
                outputSource[i] = groupIndex;
                outputField[i] = groups.get(group).indexOf(column);
            }
        }
        Projection baseProjection = null;
        if (projection != null) {
            if (baseFields.isEmpty()) {
                // the server cannot project zero columns: carry the first base column, which the
                // output mapping never references
                baseFields.add(0);
            }
            baseProjection =
                    Projection.of(baseFields.stream().mapToInt(Integer::intValue).toArray());
        }
        Map<String, RowType> groupRowTypes = new HashMap<>();
        for (String group : touched) {
            groupRowTypes.put(group, schema.getColumnGroupRowType(group));
        }
        return new ColumnGroupReadPlan(
                baseProjection,
                Collections.unmodifiableList(touched),
                groupRowTypes,
                outputSource,
                outputField);
    }

    private static int[] allColumns(int count) {
        int[] all = new int[count];
        for (int i = 0; i < count; i++) {
            all[i] = i;
        }
        return all;
    }

    private static int indexOf(int[] array, int value) {
        for (int i = 0; i < array.length; i++) {
            if (array[i] == value) {
                return i;
            }
        }
        throw new IllegalArgumentException("Column " + value + " is not a base column.");
    }

    public List<String> touchedGroups() {
        return touchedGroups;
    }

    public int outputCount() {
        return outputSource.length;
    }

    int outputSource(int output) {
        return outputSource[output];
    }

    int outputField(int output) {
        return outputField[output];
    }

    public RowType groupRowType(String group) {
        return groupRowTypes.get(group);
    }

    /** The read context for the base log, decoding only the default-group columns. */
    public LogRecordReadContext createBaseContext(
            TableInfo tableInfo,
            boolean readFromRemote,
            LogRecordReadContext.SchemaResolution schemaResolution,
            SchemaGetter schemaGetter,
            AllocationManager.Factory allocationManagerFactory) {
        return LogRecordReadContext.createReadContext(
                tableInfo.getTableId(),
                LogFormat.ARROW,
                tableInfo.getSchemaId(),
                tableInfo.getSchema().getBaseRowType(),
                readFromRemote,
                schemaResolution,
                baseProjection,
                ColumnGroupSchemaGetter.base(schemaGetter),
                allocationManagerFactory);
    }

    /** One read context per touched column group, decoding that group's physical row. */
    public Map<String, LogRecordReadContext> createGroupContexts(
            TableInfo tableInfo,
            SchemaGetter schemaGetter,
            AllocationManager.Factory allocationManagerFactory) {
        Map<String, LogRecordReadContext> contexts = new HashMap<>();
        for (String group : touchedGroups) {
            contexts.put(
                    group,
                    LogRecordReadContext.createReadContext(
                            tableInfo.getTableId(),
                            LogFormat.ARROW,
                            tableInfo.getSchemaId(),
                            groupRowTypes.get(group),
                            false,
                            LogRecordReadContext.SchemaResolution.TARGET,
                            null,
                            ColumnGroupSchemaGetter.group(schemaGetter, group),
                            allocationManagerFactory));
        }
        return contexts;
    }

    /** Field getters over the physical row of {@code group}. */
    public InternalRow.FieldGetter[] groupFieldGetters(String group) {
        return InternalRow.createFieldGetters(groupRowTypes.get(group));
    }
}
