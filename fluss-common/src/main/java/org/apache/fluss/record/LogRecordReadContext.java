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

package org.apache.fluss.record;

import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.metadata.LogFormat;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.SchemaGetter;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.InternalRow.FieldGetter;
import org.apache.fluss.row.ProjectedRow;
import org.apache.fluss.shaded.arrow.org.apache.arrow.memory.BufferAllocator;
import org.apache.fluss.shaded.arrow.org.apache.arrow.memory.RootAllocator;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypeRoot;
import org.apache.fluss.types.RowType;
import org.apache.fluss.types.variant.ShreddingSchema;
import org.apache.fluss.utils.ArrowUtils;
import org.apache.fluss.utils.MapUtils;
import org.apache.fluss.utils.Projection;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.IntStream;

import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** A simple implementation for {@link LogRecordBatch.ReadContext}. */
@ThreadSafe
public class LogRecordReadContext implements LogRecordBatch.ReadContext, AutoCloseable {

    // the log format of the table
    private final LogFormat logFormat;
    // the schema of the data read from server or remote (which may include shredded columns)
    private final RowType dataRowType;
    // the static schemaId of the table, should support dynamic schema evolution in the future
    private final int targetSchemaId;
    // the Arrow memory buffer allocator for the table, should be null if not ARROW log format
    @Nullable private final BufferAllocator bufferAllocator;
    // the final selected fields of the read data
    private final FieldGetter[] selectedFieldGetters;
    // whether the projection is push downed to the server side and the returned data is pruned.
    private final boolean projectionPushDowned;
    private final SchemaGetter schemaGetter;
    private final ConcurrentHashMap<Integer, VectorSchemaRoot> vectorSchemaRootMap =
            MapUtils.newConcurrentHashMap();
    // the expanded projection indices (in order) for the server, including shredded columns.
    // null if no projection pushdown or no expansion needed.
    @Nullable private final int[] storageProjectionInOrder;

    public static LogRecordReadContext createReadContext(
            TableInfo tableInfo,
            boolean readFromRemote,
            @Nullable Projection projection,
            SchemaGetter schemaGetter) {
        RowType fullRowType = tableInfo.getRowType();
        RowType userRowType = tableInfo.getUserRowType();
        boolean hasShreddedColumns = fullRowType.getFieldCount() != userRowType.getFieldCount();
        LogFormat logFormat = tableInfo.getTableConfig().getLogFormat();
        // only for arrow log format, the projection can be push downed to the server side
        boolean projectionPushDowned = projection != null && logFormat == LogFormat.ARROW;
        int schemaId = tableInfo.getSchemaId();
        if (projection == null) {
            // set a default dummy projection based on user-visible columns
            projection = Projection.of(IntStream.range(0, userRowType.getFieldCount()).toArray());
        }

        if (logFormat == LogFormat.ARROW) {
            if (readFromRemote) {
                // currently, for remote read, arrow log doesn't support projection pushdown,
                // so set the rowType as is.
                int[] selectedFields = projection.getProjection();
                if (hasShreddedColumns) {
                    // For remote read with shredded columns: VectorSchemaRoot uses full schema,
                    // field getters use user-visible schema
                    return createArrowReadContext(
                            fullRowType,
                            userRowType,
                            schemaId,
                            selectedFields,
                            false,
                            schemaGetter,
                            null);
                }
                return createArrowReadContext(
                        fullRowType, schemaId, selectedFields, false, schemaGetter);
            } else {
                if (hasShreddedColumns) {
                    // Expand projection to include shredded columns for server request
                    int[] expandedInOrder =
                            expandProjectionForShredding(
                                    projection.getProjectionInOrder(),
                                    fullRowType,
                                    tableInfo.getShreddingSchemas());
                    // projected storage type (includes shredded) for VectorSchemaRoot
                    RowType projectedStorageType = fullRowType.project(expandedInOrder);
                    // projected user type for field getters
                    RowType projectedUserType = projection.projectInOrder(userRowType);
                    int[] selectedFields = projection.getReorderingIndexes();
                    return createArrowReadContext(
                            projectedStorageType,
                            projectedUserType,
                            schemaId,
                            selectedFields,
                            projectionPushDowned,
                            schemaGetter,
                            expandedInOrder);
                } else {
                    // arrow data that returned from server has been projected (in order)
                    RowType projectedRowType = projection.projectInOrder(fullRowType);
                    // need to reorder the fields for final output
                    int[] selectedFields = projection.getReorderingIndexes();
                    return createArrowReadContext(
                            projectedRowType,
                            schemaId,
                            selectedFields,
                            projectionPushDowned,
                            schemaGetter);
                }
            }
        } else if (logFormat == LogFormat.INDEXED) {
            int[] selectedFields = projection.getProjection();
            return createIndexedReadContext(userRowType, schemaId, selectedFields, schemaGetter);
        } else if (logFormat == LogFormat.COMPACTED) {
            int[] selectedFields = projection.getProjection();
            return createCompactedRowReadContext(userRowType, schemaId, selectedFields);
        } else {
            throw new IllegalArgumentException("Unsupported log format: " + logFormat);
        }
    }

    private static LogRecordReadContext createArrowReadContext(
            RowType dataRowType,
            int schemaId,
            int[] selectedFields,
            boolean projectionPushDowned,
            SchemaGetter schemaGetter) {
        // TODO: use a more reasonable memory limit
        BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
        FieldGetter[] fieldGetters = buildProjectedFieldGetters(dataRowType, selectedFields);
        return new LogRecordReadContext(
                LogFormat.ARROW,
                dataRowType,
                schemaId,
                allocator,
                fieldGetters,
                projectionPushDowned,
                schemaGetter,
                null);
    }

    /**
     * Creates an Arrow read context with shredding support. The storageRowType is used for
     * VectorSchemaRoot creation (includes shredded columns), while the userRowType is used for
     * field getters (user-visible columns only).
     */
    private static LogRecordReadContext createArrowReadContext(
            RowType storageRowType,
            RowType userRowType,
            int schemaId,
            int[] selectedFields,
            boolean projectionPushDowned,
            SchemaGetter schemaGetter,
            @Nullable int[] storageProjectionInOrder) {
        BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
        FieldGetter[] fieldGetters = buildProjectedFieldGetters(userRowType, selectedFields);
        return new LogRecordReadContext(
                LogFormat.ARROW,
                storageRowType,
                schemaId,
                allocator,
                fieldGetters,
                projectionPushDowned,
                schemaGetter,
                storageProjectionInOrder);
    }

    /**
     * Creates a LogRecordReadContext for ARROW log format, that underlying Arrow resources are not
     * reused.
     *
     * @param rowType the schema of the table
     * @param schemaId the schemaId of the table
     * @param schemaGetter the schema getter of to get schema by schemaId
     */
    @VisibleForTesting
    public static LogRecordReadContext createArrowReadContext(
            RowType rowType, int schemaId, SchemaGetter schemaGetter) {
        int[] selectedFields = IntStream.range(0, rowType.getFieldCount()).toArray();
        return createArrowReadContext(rowType, schemaId, selectedFields, false, schemaGetter);
    }

    @VisibleForTesting
    public static LogRecordReadContext createArrowReadContext(
            RowType rowType,
            int schemaId,
            SchemaGetter schemaGetter,
            boolean projectionPushDowned) {
        int[] selectedFields = IntStream.range(0, rowType.getFieldCount()).toArray();
        return createArrowReadContext(
                rowType, schemaId, selectedFields, projectionPushDowned, schemaGetter);
    }

    /**
     * Creates a LogRecordReadContext for INDEXED log format.
     *
     * @param rowType the schema of the table
     * @param schemaId the schemaId of the table
     * @param schemaGetter the schema getter of to get schema by schemaId
     */
    @VisibleForTesting
    public static LogRecordReadContext createIndexedReadContext(
            RowType rowType, int schemaId, SchemaGetter schemaGetter) {
        int[] selectedFields = IntStream.range(0, rowType.getFieldCount()).toArray();
        return createIndexedReadContext(rowType, schemaId, selectedFields, schemaGetter);
    }

    /** Creates a LogRecordReadContext for COMPACTED log format. */
    public static LogRecordReadContext createCompactedRowReadContext(
            RowType rowType, int schemaId) {
        int[] selectedFields = IntStream.range(0, rowType.getFieldCount()).toArray();
        return createCompactedRowReadContext(rowType, schemaId, selectedFields);
    }

    /**
     * Creates a LogRecordReadContext for INDEXED log format.
     *
     * @param rowType the schema of the read data
     * @param schemaId the schemaId of the table
     * @param selectedFields the final selected fields of the read data
     * @param schemaGetter the schema getter of to get schema by schemaId
     */
    public static LogRecordReadContext createIndexedReadContext(
            RowType rowType, int schemaId, int[] selectedFields, SchemaGetter schemaGetter) {
        FieldGetter[] fieldGetters = buildProjectedFieldGetters(rowType, selectedFields);
        // for INDEXED log format, the projection is NEVER push downed to the server side
        return new LogRecordReadContext(
                LogFormat.INDEXED,
                rowType,
                schemaId,
                null,
                fieldGetters,
                false,
                schemaGetter,
                null);
    }

    /**
     * Creates a LogRecordReadContext for COMPACTED log format.
     *
     * @param rowType the schema of the read data
     * @param schemaId the schemaId of the table
     * @param selectedFields the final selected fields of the read data
     */
    public static LogRecordReadContext createCompactedRowReadContext(
            RowType rowType, int schemaId, int[] selectedFields) {
        FieldGetter[] fieldGetters = buildProjectedFieldGetters(rowType, selectedFields);
        // for COMPACTED log format, the projection is NEVER push downed to the server side
        return new LogRecordReadContext(
                LogFormat.COMPACTED, rowType, schemaId, null, fieldGetters, false, null, null);
    }

    private LogRecordReadContext(
            LogFormat logFormat,
            RowType targetDataRowType,
            int targetSchemaId,
            BufferAllocator bufferAllocator,
            FieldGetter[] selectedFieldGetters,
            boolean projectionPushDowned,
            SchemaGetter schemaGetter,
            @Nullable int[] storageProjectionInOrder) {
        this.logFormat = logFormat;
        this.dataRowType = targetDataRowType;
        this.targetSchemaId = targetSchemaId;
        this.bufferAllocator = bufferAllocator;
        this.selectedFieldGetters = selectedFieldGetters;
        this.projectionPushDowned = projectionPushDowned;
        this.schemaGetter = schemaGetter;
        this.storageProjectionInOrder = storageProjectionInOrder;
    }

    @Override
    public LogFormat getLogFormat() {
        return logFormat;
    }

    @Override
    public RowType getRowType(int schemaId) {
        if (isSameRowType(schemaId)) {
            return dataRowType;
        }

        Schema schema = schemaGetter.getSchema(schemaId);
        return schema.getRowType();
    }

    /** Get the selected field getters for the read data. */
    public FieldGetter[] getSelectedFieldGetters() {
        return selectedFieldGetters;
    }

    /** Whether the projection is push downed to the server side and the returned data is pruned. */
    public boolean isProjectionPushDowned() {
        return projectionPushDowned;
    }

    /**
     * Returns the expanded projection indices (in order) for the server, which includes shredded
     * columns for any selected Variant columns. Returns null if no expansion was needed.
     */
    @Nullable
    public int[] getStorageProjectionInOrder() {
        return storageProjectionInOrder;
    }

    @Override
    public VectorSchemaRoot getVectorSchemaRoot(int schemaId) {
        if (logFormat != LogFormat.ARROW) {
            throw new IllegalArgumentException(
                    "Only Arrow log format provides vector schema root.");
        }

        RowType rowType = getRowType(schemaId);
        return vectorSchemaRootMap.computeIfAbsent(
                schemaId,
                (id) ->
                        VectorSchemaRoot.create(
                                ArrowUtils.toArrowSchema(rowType), bufferAllocator));
    }

    @Override
    public BufferAllocator getBufferAllocator() {
        if (logFormat != LogFormat.ARROW) {
            throw new IllegalArgumentException("Only Arrow log format provides buffer allocator.");
        }
        checkNotNull(bufferAllocator, "The buffer allocator is not available.");
        return bufferAllocator;
    }

    @Nullable
    @Override
    public ProjectedRow getOutputProjectedRow(int schemaId) {
        if (isSameRowType(schemaId)) {
            return null;
        }
        // TODO: should we cache the projection?
        Schema originSchema = schemaGetter.getSchema(schemaId);
        Schema expectedSchema = schemaGetter.getSchema(targetSchemaId);
        return ProjectedRow.from(originSchema, expectedSchema);
    }

    public void close() {
        vectorSchemaRootMap.values().forEach(VectorSchemaRoot::close);
        if (bufferAllocator != null) {
            bufferAllocator.close();
        }
    }

    private boolean isSameRowType(int schemaId) {
        return targetSchemaId == schemaId || isProjectionPushDowned();
    }

    private static FieldGetter[] buildProjectedFieldGetters(RowType rowType, int[] selectedFields) {
        List<DataType> dataTypeList = rowType.getChildren();
        FieldGetter[] fieldGetters = new FieldGetter[selectedFields.length];
        for (int i = 0; i < fieldGetters.length; i++) {
            // build deep field getter to support nested types
            fieldGetters[i] =
                    InternalRow.createDeepFieldGetter(
                            dataTypeList.get(selectedFields[i]), selectedFields[i]);
        }
        return fieldGetters;
    }

    /**
     * Expands projection indices to include shredded columns for any selected Variant columns. For
     * example, if the user selects column 2 (a Variant) and the full schema has shredded columns at
     * indices 3 and 4 for that Variant, the result will include [2, 3, 4] (sorted).
     *
     * @param projectionInOrder the user's projection indices in sorted order
     * @param fullRowType the full row type including shredded columns
     * @param shreddingSchemas the shredding schemas keyed by variant column name
     * @return expanded projection indices in sorted order
     */
    static int[] expandProjectionForShredding(
            int[] projectionInOrder,
            RowType fullRowType,
            Map<String, ShreddingSchema> shreddingSchemas) {
        if (shreddingSchemas.isEmpty()) {
            return projectionInOrder;
        }
        List<Integer> expanded = new ArrayList<>();
        for (int idx : projectionInOrder) {
            expanded.add(idx);
            String fieldName = fullRowType.getFields().get(idx).getName();
            if (fullRowType.getTypeAt(idx).getTypeRoot() == DataTypeRoot.VARIANT
                    && shreddingSchemas.containsKey(fieldName)) {
                // Find all shredded column indices for this variant
                ShreddingSchema schema = shreddingSchemas.get(fieldName);
                for (int i = 0; i < fullRowType.getFieldCount(); i++) {
                    if (schema.isShreddedColumn(fullRowType.getFields().get(i).getName())) {
                        expanded.add(i);
                    }
                }
            }
        }
        int[] result = expanded.stream().mapToInt(Integer::intValue).toArray();
        Arrays.sort(result);
        return result;
    }
}
