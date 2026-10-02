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

package org.apache.fluss.flink.source;

import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.client.initializer.NoStoppingOffsetsInitializer;
import org.apache.fluss.client.initializer.OffsetsInitializer;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.flink.FlinkConnectorOptions;
import org.apache.fluss.flink.source.deserializer.DeserializerInitContextImpl;
import org.apache.fluss.flink.source.deserializer.FlussDeserializationSchema;
import org.apache.fluss.flink.source.emitter.FlinkRecordEmitter;
import org.apache.fluss.flink.source.emitter.RowDataProjection;
import org.apache.fluss.flink.source.enumerator.FlinkSourceEnumerator;
import org.apache.fluss.flink.source.metrics.FlinkSourceReaderMetrics;
import org.apache.fluss.flink.source.reader.FlinkSourceReader;
import org.apache.fluss.flink.source.reader.LeaseContext;
import org.apache.fluss.flink.source.reader.RecordAndPos;
import org.apache.fluss.flink.source.split.SourceSplitBase;
import org.apache.fluss.flink.source.split.SourceSplitSerializer;
import org.apache.fluss.flink.source.state.FlussSourceEnumeratorStateSerializer;
import org.apache.fluss.flink.source.state.SourceEnumeratorState;
import org.apache.fluss.flink.utils.FlinkConversions;
import org.apache.fluss.lake.source.LakeSource;
import org.apache.fluss.lake.source.LakeSplit;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.predicate.Predicate;
import org.apache.fluss.types.RowType;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.List;

import static org.apache.fluss.config.ConfigOptions.CLIENT_SCANNER_IO_TMP_DIR;
import static org.apache.fluss.flink.utils.FlinkConnectorOptionsUtils.getClientScannerIoTmpDir;

/** Flink source for Fluss. */
public class FlinkSource<OUT>
        implements Source<OUT, SourceSplitBase, SourceEnumeratorState>, ResultTypeQueryable {
    private static final long serialVersionUID = 1L;

    private final Configuration flussConf;
    private final TablePath tablePath;
    private final boolean hasPrimaryKey;
    private final boolean isPartitioned;
    private final RowType scanRowType;
    @Nullable private final RowType producedRowType;
    @Nullable private final FlinkRecordEmitter.OutputProjection<OUT> outputProjection;
    @Nullable private final int[] projectedFields;
    protected final OffsetsInitializer offsetsInitializer;
    protected final OffsetsInitializer stoppingOffsetsInitializer;
    protected final long scanPartitionDiscoveryIntervalMs;
    protected final int splitPerAssignmentBatchSize;
    private final boolean streaming;
    private final Boundedness boundedness;
    private final FlussDeserializationSchema<OUT> deserializationSchema;
    @Nullable private final Predicate partitionFilters;
    @Nullable private final LakeSource<LakeSplit> lakeSource;
    private final LeaseContext leaseContext;

    @Nullable private final Predicate logRecordBatchFilter;

    public FlinkSource(
            Configuration flussConf,
            TablePath tablePath,
            boolean hasPrimaryKey,
            boolean isPartitioned,
            RowType scanRowType,
            @Nullable int[] projectedFields,
            @Nullable Predicate logRecordBatchFilter,
            OffsetsInitializer offsetsInitializer,
            long scanPartitionDiscoveryIntervalMs,
            FlussDeserializationSchema<OUT> deserializationSchema,
            boolean streaming,
            @Nullable Predicate partitionFilters,
            LeaseContext leaseContext) {
        this(
                flussConf,
                tablePath,
                hasPrimaryKey,
                isPartitioned,
                scanRowType,
                projectedFields,
                logRecordBatchFilter,
                offsetsInitializer,
                scanPartitionDiscoveryIntervalMs,
                FlinkConnectorOptions.SCAN_SPLIT_ASSIGNMENT_BATCH_SIZE.defaultValue(),
                deserializationSchema,
                streaming,
                partitionFilters,
                null,
                leaseContext);
    }

    /**
     * Creates a source with the legacy boundedness derived from the streaming flag.
     *
     * @deprecated Use the constructor that explicitly accepts stopping offsets and boundedness.
     */
    @Deprecated
    public FlinkSource(
            Configuration flussConf,
            TablePath tablePath,
            boolean hasPrimaryKey,
            boolean isPartitioned,
            RowType scanRowType,
            @Nullable int[] projectedFields,
            @Nullable Predicate logRecordBatchFilter,
            OffsetsInitializer offsetsInitializer,
            long scanPartitionDiscoveryIntervalMs,
            int splitPerAssignmentBatchSize,
            FlussDeserializationSchema<OUT> deserializationSchema,
            @Nullable RowType producedRowType,
            boolean streaming,
            @Nullable Predicate partitionFilters,
            @Nullable LakeSource<LakeSplit> lakeSource,
            LeaseContext leaseContext) {
        this(
                flussConf,
                tablePath,
                hasPrimaryKey,
                isPartitioned,
                scanRowType,
                projectedFields,
                logRecordBatchFilter,
                offsetsInitializer,
                streaming ? new NoStoppingOffsetsInitializer() : OffsetsInitializer.latest(),
                streaming ? Boundedness.CONTINUOUS_UNBOUNDED : Boundedness.BOUNDED,
                scanPartitionDiscoveryIntervalMs,
                splitPerAssignmentBatchSize,
                deserializationSchema,
                producedRowType,
                streaming,
                partitionFilters,
                lakeSource,
                leaseContext);
    }

    public FlinkSource(
            Configuration flussConf,
            TablePath tablePath,
            boolean hasPrimaryKey,
            boolean isPartitioned,
            RowType scanRowType,
            @Nullable int[] projectedFields,
            @Nullable Predicate logRecordBatchFilter,
            OffsetsInitializer offsetsInitializer,
            long scanPartitionDiscoveryIntervalMs,
            FlussDeserializationSchema<OUT> deserializationSchema,
            boolean streaming,
            @Nullable Predicate partitionFilters,
            @Nullable LakeSource<LakeSplit> lakeSource,
            LeaseContext leaseContext) {
        this(
                flussConf,
                tablePath,
                hasPrimaryKey,
                isPartitioned,
                scanRowType,
                projectedFields,
                logRecordBatchFilter,
                offsetsInitializer,
                scanPartitionDiscoveryIntervalMs,
                FlinkConnectorOptions.SCAN_SPLIT_ASSIGNMENT_BATCH_SIZE.defaultValue(),
                deserializationSchema,
                streaming,
                partitionFilters,
                lakeSource,
                leaseContext);
    }

    public FlinkSource(
            Configuration flussConf,
            TablePath tablePath,
            boolean hasPrimaryKey,
            boolean isPartitioned,
            RowType scanRowType,
            @Nullable int[] projectedFields,
            @Nullable Predicate logRecordBatchFilter,
            OffsetsInitializer offsetsInitializer,
            long scanPartitionDiscoveryIntervalMs,
            int splitPerAssignmentBatchSize,
            FlussDeserializationSchema<OUT> deserializationSchema,
            boolean streaming,
            @Nullable Predicate partitionFilters,
            LeaseContext leaseContext) {
        this(
                flussConf,
                tablePath,
                hasPrimaryKey,
                isPartitioned,
                scanRowType,
                projectedFields,
                logRecordBatchFilter,
                offsetsInitializer,
                scanPartitionDiscoveryIntervalMs,
                splitPerAssignmentBatchSize,
                deserializationSchema,
                streaming,
                partitionFilters,
                null,
                leaseContext);
    }

    public FlinkSource(
            Configuration flussConf,
            TablePath tablePath,
            boolean hasPrimaryKey,
            boolean isPartitioned,
            RowType scanRowType,
            @Nullable int[] projectedFields,
            @Nullable Predicate logRecordBatchFilter,
            OffsetsInitializer offsetsInitializer,
            long scanPartitionDiscoveryIntervalMs,
            int splitPerAssignmentBatchSize,
            FlussDeserializationSchema<OUT> deserializationSchema,
            boolean streaming,
            @Nullable Predicate partitionFilters,
            @Nullable LakeSource<LakeSplit> lakeSource,
            LeaseContext leaseContext) {
        this(
                flussConf,
                tablePath,
                hasPrimaryKey,
                isPartitioned,
                scanRowType,
                projectedFields,
                logRecordBatchFilter,
                offsetsInitializer,
                streaming ? new NoStoppingOffsetsInitializer() : OffsetsInitializer.latest(),
                streaming ? Boundedness.CONTINUOUS_UNBOUNDED : Boundedness.BOUNDED,
                scanPartitionDiscoveryIntervalMs,
                splitPerAssignmentBatchSize,
                deserializationSchema,
                null,
                streaming,
                partitionFilters,
                lakeSource,
                leaseContext);
    }

    public FlinkSource(
            Configuration flussConf,
            TablePath tablePath,
            boolean hasPrimaryKey,
            boolean isPartitioned,
            RowType scanRowType,
            @Nullable int[] projectedFields,
            @Nullable Predicate logRecordBatchFilter,
            OffsetsInitializer offsetsInitializer,
            long scanPartitionDiscoveryIntervalMs,
            int splitPerAssignmentBatchSize,
            FlussDeserializationSchema<OUT> deserializationSchema,
            @Nullable RowType producedRowType,
            boolean streaming,
            @Nullable Predicate partitionFilters,
            LeaseContext leaseContext) {
        this(
                flussConf,
                tablePath,
                hasPrimaryKey,
                isPartitioned,
                scanRowType,
                projectedFields,
                logRecordBatchFilter,
                offsetsInitializer,
                streaming ? new NoStoppingOffsetsInitializer() : OffsetsInitializer.latest(),
                streaming ? Boundedness.CONTINUOUS_UNBOUNDED : Boundedness.BOUNDED,
                scanPartitionDiscoveryIntervalMs,
                splitPerAssignmentBatchSize,
                deserializationSchema,
                producedRowType,
                streaming,
                partitionFilters,
                null,
                leaseContext);
    }

    public FlinkSource(
            Configuration flussConf,
            TablePath tablePath,
            boolean hasPrimaryKey,
            boolean isPartitioned,
            RowType scanRowType,
            @Nullable int[] projectedFields,
            @Nullable Predicate logRecordBatchFilter,
            OffsetsInitializer offsetsInitializer,
            OffsetsInitializer stoppingOffsetsInitializer,
            Boundedness boundedness,
            long scanPartitionDiscoveryIntervalMs,
            int splitPerAssignmentBatchSize,
            FlussDeserializationSchema<OUT> deserializationSchema,
            @Nullable RowType producedRowType,
            boolean streaming,
            @Nullable Predicate partitionFilters,
            @Nullable LakeSource<LakeSplit> lakeSource,
            LeaseContext leaseContext) {
        this.flussConf = flussConf;
        this.tablePath = tablePath;
        this.hasPrimaryKey = hasPrimaryKey;
        this.isPartitioned = isPartitioned;
        this.scanRowType = scanRowType;
        this.projectedFields = projectedFields;
        this.logRecordBatchFilter = logRecordBatchFilter;
        this.offsetsInitializer = offsetsInitializer;
        this.stoppingOffsetsInitializer = stoppingOffsetsInitializer;
        this.boundedness = boundedness;
        this.scanPartitionDiscoveryIntervalMs = scanPartitionDiscoveryIntervalMs;
        this.splitPerAssignmentBatchSize = splitPerAssignmentBatchSize;
        this.deserializationSchema = deserializationSchema;
        this.producedRowType = producedRowType;
        this.outputProjection = createOutputProjection(producedRowType);
        this.streaming = streaming;
        this.partitionFilters = partitionFilters;
        this.lakeSource = lakeSource;
        this.leaseContext = leaseContext;
    }

    @Override
    public Boundedness getBoundedness() {
        return boundedness;
    }

    @VisibleForTesting
    boolean isStreaming() {
        return streaming;
    }

    @VisibleForTesting
    OffsetsInitializer getStoppingOffsetsInitializer() {
        return stoppingOffsetsInitializer;
    }

    @Override
    public SplitEnumerator<SourceSplitBase, SourceEnumeratorState> createEnumerator(
            SplitEnumeratorContext<SourceSplitBase> splitEnumeratorContext) {
        return new FlinkSourceEnumerator(
                tablePath,
                flussConf,
                hasPrimaryKey,
                isPartitioned,
                splitEnumeratorContext,
                offsetsInitializer,
                stoppingOffsetsInitializer,
                boundedness,
                scanPartitionDiscoveryIntervalMs,
                splitPerAssignmentBatchSize,
                streaming,
                partitionFilters,
                lakeSource,
                leaseContext,
                false);
    }

    @Override
    public SplitEnumerator<SourceSplitBase, SourceEnumeratorState> restoreEnumerator(
            SplitEnumeratorContext<SourceSplitBase> splitEnumeratorContext,
            SourceEnumeratorState sourceEnumeratorState) {
        List<SourceSplitBase> remainingHybridLakeFlussSplits =
                sourceEnumeratorState.getRemainingHybridLakeFlussSplits();
        // A fresh null means lake splits are not initialized yet. When restoring, null means
        // nothing is pending, so normalize it here to avoid generating lake splits later.
        if (remainingHybridLakeFlussSplits == null) {
            remainingHybridLakeFlussSplits = Collections.emptyList();
        }

        return new FlinkSourceEnumerator(
                tablePath,
                flussConf,
                hasPrimaryKey,
                isPartitioned,
                splitEnumeratorContext,
                sourceEnumeratorState.getAssignedBuckets(),
                sourceEnumeratorState.getAssignedPartitions(),
                remainingHybridLakeFlussSplits,
                offsetsInitializer,
                stoppingOffsetsInitializer,
                boundedness,
                scanPartitionDiscoveryIntervalMs,
                splitPerAssignmentBatchSize,
                streaming,
                partitionFilters,
                lakeSource,
                new LeaseContext(
                        sourceEnumeratorState.getLeaseId(),
                        leaseContext.getKvSnapshotLeaseDurationMs()),
                true,
                sourceEnumeratorState.isInitialDiscoveryFinished(),
                sourceEnumeratorState.getUnassignedSplits());
    }

    @Override
    public SimpleVersionedSerializer<SourceSplitBase> getSplitSerializer() {
        return new SourceSplitSerializer(lakeSource);
    }

    @Override
    public SimpleVersionedSerializer<SourceEnumeratorState> getEnumeratorCheckpointSerializer() {
        return new FlussSourceEnumeratorStateSerializer(lakeSource);
    }

    @Override
    public SourceReader<OUT, SourceSplitBase> createReader(SourceReaderContext context)
            throws Exception {
        FutureCompletingBlockingQueue<RecordsWithSplitIds<RecordAndPos>> elementsQueue =
                new FutureCompletingBlockingQueue<>();
        FlinkSourceReaderMetrics flinkSourceReaderMetrics =
                new FlinkSourceReaderMetrics(context.metricGroup());

        Configuration readerConf = new Configuration(flussConf);
        readerConf.set(
                CLIENT_SCANNER_IO_TMP_DIR,
                getClientScannerIoTmpDir(readerConf, context.getConfiguration()));
        deserializationSchema.open(
                new DeserializerInitContextImpl(
                        context.metricGroup().addGroup("deserializer"),
                        context.getUserCodeClassLoader(),
                        scanRowType));
        FlinkRecordEmitter<OUT> recordEmitter =
                new FlinkRecordEmitter<>(deserializationSchema, outputProjection);

        return new FlinkSourceReader<>(
                elementsQueue,
                readerConf,
                tablePath,
                scanRowType,
                context,
                projectedFields,
                logRecordBatchFilter,
                flinkSourceReaderMetrics,
                recordEmitter,
                lakeSource);
    }

    @Override
    public TypeInformation<OUT> getProducedType() {
        if (producedRowType != null) {
            return deserializationSchema.getProducedType(scanRowType, producedRowType);
        }

        // if not specified produce type, inferred by scan row type.
        return deserializationSchema.getProducedType(scanRowType);
    }

    @SuppressWarnings("unchecked")
    @Nullable
    private FlinkRecordEmitter.OutputProjection<OUT> createOutputProjection(
            @Nullable RowType producedRowType) {
        if (producedRowType == null) {
            return null;
        }

        TypeInformation<OUT> naturalProducedType =
                deserializationSchema.getProducedType(scanRowType);
        if (!(naturalProducedType instanceof InternalTypeInfo)) {
            throw new IllegalArgumentException(
                    "Produced row type can only be used with RowData deserialization schemas.");
        }
        org.apache.flink.table.types.logical.RowType naturalProducedRowType =
                ((InternalTypeInfo<?>) naturalProducedType).toRowType();
        org.apache.flink.table.types.logical.RowType flinkProducedRowType =
                FlinkConversions.toFlinkRowType(producedRowType);
        int[] projection = createProjection(naturalProducedRowType, flinkProducedRowType);
        return projection == null
                ? null
                : (FlinkRecordEmitter.OutputProjection<OUT>) RowDataProjection.of(projection);
    }

    @Nullable
    private int[] createProjection(
            org.apache.flink.table.types.logical.RowType naturalProducedRowType,
            org.apache.flink.table.types.logical.RowType producedRowType) {
        if (naturalProducedRowType.equals(producedRowType)) {
            return null;
        }

        int[] projection = new int[producedRowType.getFieldCount()];
        boolean identityProjection =
                naturalProducedRowType.getFieldCount() == producedRowType.getFieldCount();
        for (int i = 0; i < producedRowType.getFieldCount(); i++) {
            String fieldName = producedRowType.getFieldNames().get(i);
            int fieldIndex = naturalProducedRowType.getFieldIndex(fieldName);
            if (fieldIndex < 0) {
                throw new IllegalArgumentException(
                        String.format(
                                "Produced field '%s' is missing in natural produced row type %s.",
                                fieldName, naturalProducedRowType));
            }
            projection[i] = fieldIndex;
            identityProjection &= fieldIndex == i;
        }
        return identityProjection ? null : projection;
    }
}
