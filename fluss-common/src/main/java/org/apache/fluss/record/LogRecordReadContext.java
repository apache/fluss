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
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.ArrowUtils;
import org.apache.fluss.utils.MapUtils;
import org.apache.fluss.utils.Projection;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.IntStream;

/** A simple implementation for {@link LogRecordBatch.ReadContext}. */
@ThreadSafe
public class LogRecordReadContext implements LogRecordBatch.ReadContext, AutoCloseable {

    // the log format of the table
    private final LogFormat logFormat;
    // the schema of the date read form server or remote. (which is projected in the server side)
    private final RowType dataRowType;
    // the static schemaId of the table, should support dynamic schema evolution in the future
    private final int targetSchemaId;
    // the Arrow memory buffer allocator for the table, should be null if not ARROW log format
    @Nullable private volatile BufferAllocator bufferAllocator;
    // the final selected fields of the read data
    private final int[] selectedFields;
    private final FieldGetter[] selectedFieldGetters;
    private final RowType selectedRowType;
    // whether the projection is push downed to the server side and the returned data is pruned.
    private final boolean projectionPushDowned;
    private final SchemaGetter schemaGetter;
    private final ConcurrentHashMap<Integer, VectorSchemaRoot> vectorSchemaRootMap =
            MapUtils.newConcurrentHashMap();
    private final Object arrowResourceLock = new Object();

    public static LogRecordReadContext createReadContext(
            TableInfo tableInfo,
            boolean readFromRemote,
            @Nullable Projection projection,
            SchemaGetter schemaGetter) {
        RowType rowType = tableInfo.getRowType();
        LogFormat logFormat = tableInfo.getTableConfig().getLogFormat();
        // only for arrow log format, the projection can be push downed to the server side
        boolean projectionPushDowned = projection != null && logFormat == LogFormat.ARROW;
        int schemaId = tableInfo.getSchemaId();
        if (projection == null) {
            // set a default dummy projection to simplify code
            projection = Projection.of(IntStream.range(0, rowType.getFieldCount()).toArray());
        }

        if (logFormat == LogFormat.ARROW) {
            if (readFromRemote) {
                // currently, for remote read, arrow log doesn't support projection pushdown,
                // so set the rowType as is.
                int[] selectedFields = projection.getProjection();
                return createArrowReadContext(
                        rowType, schemaId, selectedFields, false, schemaGetter);
            } else {
                // arrow data that returned from server has been projected (in order)
                RowType projectedRowType = projection.projectInOrder(rowType);
                // need to reorder the fields for final output
                int[] selectedFields = projection.getReorderingIndexes();
                return createArrowReadContext(
                        projectedRowType,
                        schemaId,
                        selectedFields,
                        projectionPushDowned,
                        schemaGetter);
            }
        } else if (logFormat == LogFormat.INDEXED) {
            int[] selectedFields = projection.getProjection();
            return createIndexedReadContext(rowType, schemaId, selectedFields, schemaGetter);
        } else if (logFormat == LogFormat.COMPACTED) {
            int[] selectedFields = projection.getProjection();
            return createCompactedRowReadContext(rowType, schemaId, selectedFields);
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
        FieldGetter[] fieldGetters = buildProjectedFieldGetters(dataRowType, selectedFields);
        return new LogRecordReadContext(
                LogFormat.ARROW,
                dataRowType,
                schemaId,
                null,
                selectedFields,
                fieldGetters,
                projectionPushDowned,
                schemaGetter);
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
                selectedFields,
                fieldGetters,
                false,
                schemaGetter);
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
                LogFormat.COMPACTED,
                rowType,
                schemaId,
                null,
                selectedFields,
                fieldGetters,
                false,
                null);
    }

    private LogRecordReadContext(
            LogFormat logFormat,
            RowType targetDataRowType,
            int targetSchemaId,
            BufferAllocator bufferAllocator,
            int[] selectedFields,
            FieldGetter[] selectedFieldGetters,
            boolean projectionPushDowned,
            SchemaGetter schemaGetter) {
        this.logFormat = logFormat;
        this.dataRowType = targetDataRowType;
        this.targetSchemaId = targetSchemaId;
        this.bufferAllocator = bufferAllocator;
        this.selectedFields = Arrays.copyOf(selectedFields, selectedFields.length);
        this.selectedFieldGetters = selectedFieldGetters;
        this.selectedRowType = targetDataRowType.project(this.selectedFields);
        this.projectionPushDowned = projectionPushDowned;
        this.schemaGetter = schemaGetter;
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

    /** Returns the selected row type after projection and reordering. */
    public RowType getSelectedRowType() {
        return selectedRowType;
    }

    /** Whether the projection is push downed to the server side and the returned data is pruned. */
    public boolean isProjectionPushDowned() {
        return projectionPushDowned;
    }

    /**
     * Returns the output Arrow column projection for the given schema id, or {@code null} if no
     * client-side projection is needed (i.e., the projection is identity).
     *
     * <p>The returned indexes describe which columns from the read batch should be exposed in the
     * final output order. A value of {@code -1} means the output column does not exist in the read
     * schema and should be filled with nulls.
     */
    @Nullable
    public int[] getArrowColumnProjection(int schemaId) {
        int[] projection;
        ProjectedRow projectedRow = getOutputProjectedRow(schemaId);
        if (projectedRow == null) {
            projection = selectedFields;
        } else {
            int[] indexMapping = projectedRow.getIndexMapping();
            projection = new int[selectedFields.length];
            for (int i = 0; i < selectedFields.length; i++) {
                projection[i] = indexMapping[selectedFields[i]];
            }
        }
        return isIdentityProjection(projection)
                ? null
                : Arrays.copyOf(projection, projection.length);
    }

    private static boolean isIdentityProjection(int[] projection) {
        for (int i = 0; i < projection.length; i++) {
            if (projection[i] != i) {
                return false;
            }
        }
        return true;
    }

    @Override
    public VectorSchemaRoot getVectorSchemaRoot(int schemaId) {
        if (logFormat != LogFormat.ARROW) {
            throw new IllegalArgumentException(
                    "Only Arrow log format provides vector schema root.");
        }

        synchronized (arrowResourceLock) {
            VectorSchemaRoot vectorSchemaRoot = vectorSchemaRootMap.get(schemaId);
            if (vectorSchemaRoot != null) {
                return vectorSchemaRoot;
            }

            RowType rowType = getRowType(schemaId);
            vectorSchemaRoot =
                    VectorSchemaRoot.create(
                            ArrowUtils.toArrowSchema(rowType), getOrCreateBufferAllocator());
            vectorSchemaRootMap.put(schemaId, vectorSchemaRoot);
            return vectorSchemaRoot;
        }
    }

    @Override
    public BufferAllocator getBufferAllocator() {
        if (logFormat != LogFormat.ARROW) {
            throw new IllegalArgumentException("Only Arrow log format provides buffer allocator.");
        }
        return getOrCreateBufferAllocator();
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
        synchronized (arrowResourceLock) {
            vectorSchemaRootMap.values().forEach(VectorSchemaRoot::close);
            vectorSchemaRootMap.clear();
            if (bufferAllocator != null) {
                bufferAllocator.close();
                bufferAllocator = null;
            }
        }
    }

    private BufferAllocator getOrCreateBufferAllocator() {
        BufferAllocator allocator = bufferAllocator;
        if (allocator != null) {
            return allocator;
        }

        synchronized (arrowResourceLock) {
            if (bufferAllocator == null) {
                bufferAllocator = new RootAllocator(Long.MAX_VALUE);
            }
            return bufferAllocator;
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
}
