/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.flink.source;

import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.flink.source.deserializer.DeserializerInitContextImpl;
import com.alibaba.fluss.flink.source.deserializer.FlussDeserializationSchema;
import com.alibaba.fluss.flink.source.emitter.FlinkRecordEmitter;
import com.alibaba.fluss.flink.source.enumerator.FlinkSourceEnumerator;
import com.alibaba.fluss.flink.source.enumerator.initializer.OffsetsInitializer;
import com.alibaba.fluss.flink.source.metrics.FlinkSourceReaderMetrics;
import com.alibaba.fluss.flink.source.reader.FlinkSourceReader;
import com.alibaba.fluss.flink.source.reader.RecordAndPos;
import com.alibaba.fluss.flink.source.split.SourceSplitBase;
import com.alibaba.fluss.flink.source.split.SourceSplitSerializer;
import com.alibaba.fluss.flink.source.state.FlussSourceEnumeratorStateSerializer;
import com.alibaba.fluss.flink.source.state.SourceEnumeratorState;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.types.RowType;

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
            boolean streaming) {
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
                streaming);
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
                offsetsInitializer,
                scanPartitionDiscoveryIntervalMs,
                streaming);
    }

    @Override
    public SimpleVersionedSerializer<SourceSplitBase> getSplitSerializer() {
        return SourceSplitSerializer.INSTANCE;
    }

    @Override
    public SimpleVersionedSerializer<SourceEnumeratorState> getEnumeratorCheckpointSerializer() {
        return FlussSourceEnumeratorStateSerializer.INSTANCE;
    }

    @Override
    public SourceReader<OUT, SourceSplitBase> createReader(SourceReaderContext context)
            throws Exception {
        FutureCompletingBlockingQueue<RecordsWithSplitIds<RecordAndPos>> elementsQueue =
                new FutureCompletingBlockingQueue<>();
        FlinkSourceReaderMetrics flinkSourceReaderMetrics =
                new FlinkSourceReaderMetrics(context.metricGroup());

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
                recordEmitter);
    }

    @Override
    public TypeInformation<OUT> getProducedType() {
        return deserializationSchema.getProducedType(sourceOutputType);
    }
}
