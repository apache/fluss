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
import org.apache.fluss.shaded.arrow.org.apache.arrow.memory.AllocationManager;
import org.apache.fluss.shaded.arrow.org.apache.arrow.memory.BufferAllocator;
import org.apache.fluss.shaded.arrow.org.apache.arrow.memory.BufferAllocatorUtil;
import org.apache.fluss.shaded.arrow.org.apache.arrow.memory.ChunkedAllocationManager;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.ArrowUtils;
import org.apache.fluss.utils.Projection;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.IntStream;

import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** A simple implementation for {@link LogRecordBatch.ReadContext}. */
@ThreadSafe
public class LogRecordReadContext implements LogRecordBatch.ReadContext, AutoCloseable {

    /**
     * Schema resolution strategy for reading records.
     *
     * <p>Replaces an earlier {@code boolean readAsTargetSchema} flag that was adjacent to {@code
     * boolean readFromRemote} in the factory signature and prone to accidental swaps at call sites.
     */
    public enum SchemaResolution {
        /**
         * Records are projected to the table's current target schema. Columns added via later
         * schema evolutions are hidden; columns removed from the target are dropped.
         */
        TARGET,
        /**
         * Records are returned with their original write-time schema. Callers observe schema
         * evolution: fields added in a newer schema version become visible once written with that
         * schema.
         */
        DYNAMIC
    }

    private final long tableId;
    private final LogFormat logFormat;
    /**
     * When present, provides fixed schema identity, data row type, and pre-built field getters.
     * When {@code null}, the reader dynamically adapts to each batch's schema so that columns added
     * via schema evolution are visible.
     */
    @Nullable private final ReadTarget target;

    @Nullable private final BufferAllocator bufferAllocator;
    private final boolean projectionPushDowned;
    private final SchemaGetter schemaGetter;
    private final ConcurrentHashMap<Integer, VectorSchemaRoot> vectorSchemaRootMap =
            new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Integer, FieldGetter[]> fieldGetterCache =
            new ConcurrentHashMap<>();

    // -------------------------------------------------------------------------
    //  Main factory methods
    // -------------------------------------------------------------------------

    public static LogRecordReadContext createReadContext(
            TableInfo tableInfo,
            boolean readFromRemote,
            SchemaResolution schemaResolution,
            @Nullable Projection projection,
            SchemaGetter schemaGetter) {
        return createReadContext(
                tableInfo,
                readFromRemote,
                schemaResolution,
                projection,
                schemaGetter,
                new ChunkedAllocationManager.ChunkedFactory());
    }

    /**
     * Creates a {@link LogRecordReadContext} with a custom {@link AllocationManager.Factory}.
     *
     * <p>When {@code schemaResolution} is {@link SchemaResolution#DYNAMIC} and {@code projection}
     * is {@code null}, no fixed {@link ReadTarget} is created. In that mode the reader dynamically
     * builds field getters per batch schema, so columns added via schema evolution become visible.
     *
     * @param tableInfo the table info of the table to read
     * @param readFromRemote whether the data is read from remote storage
     * @param schemaResolution how to resolve the output schema: see {@link SchemaResolution}
     * @param projection the projection to apply, or null for all fields
     * @param schemaGetter the schema getter to resolve schema by id
     * @param allocationManagerFactory the factory for creating Arrow memory allocations
     */
    public static LogRecordReadContext createReadContext(
            TableInfo tableInfo,
            boolean readFromRemote,
            SchemaResolution schemaResolution,
            @Nullable Projection projection,
            SchemaGetter schemaGetter,
            AllocationManager.Factory allocationManagerFactory) {
        checkNotNull(schemaResolution, "schemaResolution");
        boolean readAsTargetSchema = schemaResolution == SchemaResolution.TARGET;
        RowType rowType = tableInfo.getRowType();
        LogFormat logFormat = tableInfo.getTableConfig().getLogFormat();
        // only for arrow log format, the projection can be push downed to the server side
        boolean projectionPushDowned =
                logFormat == LogFormat.ARROW && !readFromRemote && projection != null;

        // Compute ReadTarget when there is a fixed output configuration.
        // When schemaResolution is DYNAMIC and no projection is set, target is null and
        // the reader dynamically adapts to each batch's schema.
        ReadTarget target = null;
        if (readAsTargetSchema || projection != null) {
            int schemaId = tableInfo.getSchemaId();
            if (projection == null) {
                // set a default dummy projection to simplify code
                projection = Projection.of(IntStream.range(0, rowType.getFieldCount()).toArray());
            }
            RowType dataRowType;
            int[] selectedFields;
            if (projectionPushDowned) {
                dataRowType = projection.projectInOrder(rowType);
                selectedFields = projection.getReorderingIndexes();
            } else {
                dataRowType = rowType;
                selectedFields = projection.getProjection();
            }
            target = new ReadTarget(schemaId, dataRowType, selectedFields, readAsTargetSchema);
        }

        if (logFormat == LogFormat.ARROW) {
            return createArrowReadContext(
                    tableInfo.getTableId(),
                    target,
                    projectionPushDowned,
                    schemaGetter,
                    allocationManagerFactory);
        } else if (logFormat == LogFormat.INDEXED) {
            return createIndexedReadContext(tableInfo.getTableId(), target, schemaGetter);
        } else if (logFormat == LogFormat.COMPACTED) {
            return createCompactedRowReadContext(tableInfo.getTableId(), target, schemaGetter);
        } else {
            throw new IllegalArgumentException("Unsupported log format: " + logFormat);
        }
    }

    // -------------------------------------------------------------------------
    //  Internal factory methods
    // -------------------------------------------------------------------------

    private static LogRecordReadContext createArrowReadContext(
            long tableId,
            @Nullable ReadTarget target,
            boolean projectionPushDowned,
            SchemaGetter schemaGetter,
            AllocationManager.Factory allocationManagerFactory) {
        // TODO: use a more reasonable memory limit
        BufferAllocator allocator =
                BufferAllocatorUtil.createBufferAllocator(allocationManagerFactory);
        return new LogRecordReadContext(
                tableId, LogFormat.ARROW, target, allocator, projectionPushDowned, schemaGetter);
    }

    private static LogRecordReadContext createIndexedReadContext(
            long tableId, @Nullable ReadTarget target, SchemaGetter schemaGetter) {
        return new LogRecordReadContext(
                tableId, LogFormat.INDEXED, target, null, false, schemaGetter);
    }

    private static LogRecordReadContext createCompactedRowReadContext(
            long tableId, @Nullable ReadTarget target, @Nullable SchemaGetter schemaGetter) {
        return new LogRecordReadContext(
                tableId, LogFormat.COMPACTED, target, null, false, schemaGetter);
    }

    // -------------------------------------------------------------------------
    //  @VisibleForTesting and convenience factory methods
    // -------------------------------------------------------------------------

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
        ReadTarget target = new ReadTarget(schemaId, rowType, selectedFields, true);
        return createArrowReadContext(
                -1L, target, false, schemaGetter, new ChunkedAllocationManager.ChunkedFactory());
    }

    @VisibleForTesting
    public static LogRecordReadContext createArrowReadContext(
            RowType rowType,
            int schemaId,
            SchemaGetter schemaGetter,
            boolean projectionPushDowned) {
        int[] selectedFields = IntStream.range(0, rowType.getFieldCount()).toArray();
        ReadTarget target = new ReadTarget(schemaId, rowType, selectedFields, true);
        return createArrowReadContext(
                -1L,
                target,
                projectionPushDowned,
                schemaGetter,
                new ChunkedAllocationManager.ChunkedFactory());
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
        ReadTarget target = new ReadTarget(schemaId, rowType, selectedFields, true);
        return createIndexedReadContext(-1L, target, schemaGetter);
    }

    /** Creates a LogRecordReadContext for COMPACTED log format. */
    public static LogRecordReadContext createCompactedRowReadContext(
            RowType rowType, int schemaId) {
        int[] selectedFields = IntStream.range(0, rowType.getFieldCount()).toArray();
        ReadTarget target = new ReadTarget(schemaId, rowType, selectedFields, false);
        return createCompactedRowReadContext(-1L, target, null);
    }

    /** Creates a LogRecordReadContext for COMPACTED log format with schema evolution support. */
    public static LogRecordReadContext createCompactedRowReadContext(
            long tableId, RowType rowType, int schemaId, SchemaGetter schemaGetter) {
        int[] selectedFields = IntStream.range(0, rowType.getFieldCount()).toArray();
        ReadTarget target = new ReadTarget(schemaId, rowType, selectedFields, true);
        return createCompactedRowReadContext(tableId, target, schemaGetter);
    }

    // -------------------------------------------------------------------------
    //  Constructor
    // -------------------------------------------------------------------------

    private LogRecordReadContext(
            long tableId,
            LogFormat logFormat,
            @Nullable ReadTarget target,
            @Nullable BufferAllocator bufferAllocator,
            boolean projectionPushDowned,
            SchemaGetter schemaGetter) {
        this.tableId = tableId;
        this.logFormat = logFormat;
        this.target = target;
        this.bufferAllocator = bufferAllocator;
        this.projectionPushDowned = projectionPushDowned;
        this.schemaGetter = schemaGetter;
    }

    // -------------------------------------------------------------------------
    //  ReadContext interface methods
    // -------------------------------------------------------------------------

    @Override
    public LogFormat getLogFormat() {
        return logFormat;
    }

    @Override
    public RowType getRowType(int schemaId) {
        if (isProjectionPushDowned()) {
            assert target != null;
            return target.dataRowType;
        }
        return schemaGetter.getSchema(schemaId).getRowType();
    }

    /**
     * Get the selected field getters for the read data. When no fixed {@link ReadTarget} is
     * configured, getters are built dynamically from the batch's actual schema so that columns
     * added via schema evolution are visible.
     */
    public FieldGetter[] getSelectedFieldGetters(int schemaId) {
        if (target != null) {
            return target.fieldGetters;
        }
        // Dynamic: build all-field getters from the batch's actual schema.
        return fieldGetterCache.computeIfAbsent(
                schemaId,
                id -> {
                    RowType batchRowType = schemaGetter.getSchema(id).getRowType();
                    int[] allFields = IntStream.range(0, batchRowType.getFieldCount()).toArray();
                    return buildProjectedFieldGetters(batchRowType, allFields);
                });
    }

    /** Whether the projection is push downed to the server side and the returned data is pruned. */
    public boolean isProjectionPushDowned() {
        return projectionPushDowned;
    }

    /** Get the schema getter. */
    public SchemaGetter getSchemaGetter() {
        return schemaGetter;
    }

    @Override
    public long getTableId() {
        return tableId;
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
        if (target == null || schemaId == target.schemaId) {
            return null;
        }
        // TODO: should we cache the projection?
        Schema originSchema = schemaGetter.getSchema(schemaId);
        Schema expectedSchema = schemaGetter.getSchema(target.schemaId);
        return ProjectedRow.from(originSchema, expectedSchema);
    }

    public void close() {
        vectorSchemaRootMap.values().forEach(VectorSchemaRoot::close);
        if (bufferAllocator != null) {
            bufferAllocator.close();
        }
    }

    // -------------------------------------------------------------------------
    //  Private helpers
    // -------------------------------------------------------------------------
    static FieldGetter[] buildProjectedFieldGetters(RowType rowType, int[] selectedFields) {
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

    // -------------------------------------------------------------------------
    //  ReadTarget
    // -------------------------------------------------------------------------

    /**
     * Encapsulates the target output configuration for reading log records. Contains the target
     * schema identity, data row type, selected field indices, and pre-built field getters.
     *
     * <p>When a {@code ReadTarget} is provided, field getters are fixed and schema evolution may be
     * enabled. When absent ({@code null}), the reader dynamically adapts to each batch's schema so
     * that columns added via schema evolution are visible.
     */
    static class ReadTarget {
        /** The schema id this configuration was built for. */
        final int schemaId;

        /** The row type of the data (may be server-projected for ARROW format). */
        final RowType dataRowType;

        /** The field indices to select from the data row type. */
        final int[] selectedFields;

        /** Pre-built field getters corresponding to {@link #selectedFields}. */
        final FieldGetter[] fieldGetters;

        /** Whether to project records from other schemas to this target schema. */
        final boolean readAsTargetSchema;

        ReadTarget(
                int schemaId,
                RowType dataRowType,
                int[] selectedFields,
                boolean readAsTargetSchema) {
            this.schemaId = schemaId;
            this.dataRowType = dataRowType;
            this.selectedFields = selectedFields;
            this.fieldGetters = buildProjectedFieldGetters(dataRowType, selectedFields);
            this.readAsTargetSchema = readAsTargetSchema;
        }
    }
}
