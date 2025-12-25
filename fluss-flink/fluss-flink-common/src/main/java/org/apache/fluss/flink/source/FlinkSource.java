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

import org.apache.fluss.config.Configuration;
import org.apache.fluss.flink.source.deserializer.DeserializerInitContextImpl;
import org.apache.fluss.flink.source.deserializer.FlussDeserializationSchema;
import org.apache.fluss.flink.source.emitter.FlinkRecordEmitter;
import org.apache.fluss.flink.source.enumerator.FlinkSourceEnumerator;
import org.apache.fluss.flink.source.enumerator.initializer.OffsetsInitializer;
import org.apache.fluss.flink.source.metrics.FlinkSourceReaderMetrics;
import org.apache.fluss.flink.source.reader.FlinkSourceReader;
import org.apache.fluss.flink.source.reader.RecordAndPos;
import org.apache.fluss.flink.source.split.SourceSplitBase;
import org.apache.fluss.flink.source.split.SourceSplitSerializer;
import org.apache.fluss.flink.source.state.FlussSourceEnumeratorStateSerializer;
import org.apache.fluss.flink.source.state.SourceEnumeratorState;
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

import javax.annotation.Nullable;

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
    private final RowType sourceOutputType;
    @Nullable private final int[] projectedFields;
    protected final OffsetsInitializer offsetsInitializer;
    protected final long scanPartitionDiscoveryIntervalMs;
    private final boolean streaming;
    private final FlussDeserializationSchema<OUT> deserializationSchema;
    @Nullable private final Predicate partitionFilters;
    @Nullable private final LakeSource<LakeSplit> lakeSource;

    public FlinkSource(
            Configuration flussConf,
            TablePath tablePath,
            boolean hasPrimaryKey,
            boolean isPartitioned,
            RowType sourceOutputType,
            @Nullable int[] projectedFields,
            OffsetsInitializer offsetsInitializer,
            long scanPartitionDiscoveryIntervalMs,
            FlussDeserializationSchema<OUT> deserializationSchema,
            boolean streaming,
            @Nullable Predicate partitionFilters) {
        this(
                flussConf,
                tablePath,
                hasPrimaryKey,
                isPartitioned,
                sourceOutputType,
                projectedFields,
                offsetsInitializer,
                scanPartitionDiscoveryIntervalMs,
                deserializationSchema,
                streaming,
                partitionFilters,
                null);
    }

    public FlinkSource(
            Configuration flussConf,
            TablePath tablePath,
            boolean hasPrimaryKey,
            boolean isPartitioned,
            RowType sourceOutputType,
            @Nullable int[] projectedFields,
            OffsetsInitializer offsetsInitializer,
            long scanPartitionDiscoveryIntervalMs,
            FlussDeserializationSchema<OUT> deserializationSchema,
            boolean streaming,
            @Nullable Predicate partitionFilters,
            @Nullable LakeSource<LakeSplit> lakeSource) {
        this.flussConf = flussConf;
        this.tablePath = tablePath;
        this.hasPrimaryKey = hasPrimaryKey;
        this.isPartitioned = isPartitioned;
        this.sourceOutputType = sourceOutputType;
        this.projectedFields = projectedFields;
        this.offsetsInitializer = offsetsInitializer;
        this.scanPartitionDiscoveryIntervalMs = scanPartitionDiscoveryIntervalMs;
        this.deserializationSchema = deserializationSchema;
        this.streaming = streaming;
        this.partitionFilters = partitionFilters;
        this.lakeSource = lakeSource;
    }

    @Override
    public Boundedness getBoundedness() {
        return streaming ? Boundedness.CONTINUOUS_UNBOUNDED : Boundedness.BOUNDED;
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
                scanPartitionDiscoveryIntervalMs,
                streaming,
                partitionFilters,
                lakeSource);
    }

    @Override
    public SplitEnumerator<SourceSplitBase, SourceEnumeratorState> restoreEnumerator(
            SplitEnumeratorContext<SourceSplitBase> splitEnumeratorContext,
            SourceEnumeratorState sourceEnumeratorState) {
        return new FlinkSourceEnumerator(
                tablePath,
                flussConf,
                hasPrimaryKey,
                isPartitioned,
                splitEnumeratorContext,
                sourceEnumeratorState.getAssignedBuckets(),
                sourceEnumeratorState.getAssignedPartitions(),
                sourceEnumeratorState.getRemainingHybridLakeFlussSplits(),
                offsetsInitializer,
                scanPartitionDiscoveryIntervalMs,
                streaming,
                partitionFilters,
                lakeSource);
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

        flussConf.set(
                CLIENT_SCANNER_IO_TMP_DIR,
                getClientScannerIoTmpDir(flussConf, context.getConfiguration()));
        deserializationSchema.open(
                new DeserializerInitContextImpl(
                        context.metricGroup().addGroup("deserializer"),
                        context.getUserCodeClassLoader(),
                        sourceOutputType));
        FlinkRecordEmitter<OUT> recordEmitter = new FlinkRecordEmitter<>(deserializationSchema);

        return new FlinkSourceReader<>(
                elementsQueue,
                flussConf,
                tablePath,
                sourceOutputType,
                context,
                projectedFields,
                flinkSourceReaderMetrics,
                recordEmitter,
                lakeSource);
    }

    @Override
    public TypeInformation<OUT> getProducedType() {
        return deserializationSchema.getProducedType(sourceOutputType);
    }
}
