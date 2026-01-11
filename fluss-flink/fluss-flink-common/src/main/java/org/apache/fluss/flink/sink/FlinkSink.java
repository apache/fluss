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

package org.apache.fluss.flink.sink;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.flink.adapter.SinkAdapter;
import org.apache.fluss.flink.sink.serializer.FlussSerializationSchema;
import org.apache.fluss.flink.sink.shuffle.DataStatisticsOperatorFactory;
import org.apache.fluss.flink.sink.shuffle.DistributionMode;
import org.apache.fluss.flink.sink.shuffle.StatisticsOrRecord;
import org.apache.fluss.flink.sink.shuffle.StatisticsOrRecordChannelComputer;
import org.apache.fluss.flink.sink.shuffle.StatisticsOrRecordTypeInformation;
import org.apache.fluss.flink.sink.writer.AppendSinkWriter;
import org.apache.fluss.flink.sink.writer.FlinkSinkWriter;
import org.apache.fluss.flink.sink.writer.UpsertSinkWriter;
import org.apache.fluss.metadata.DataLakeFormat;
import org.apache.fluss.metadata.TablePath;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
import org.apache.flink.runtime.metrics.groups.InternalSinkWriterMetricGroup;
import org.apache.flink.streaming.api.connector.sink2.SupportsPreWriteTopology;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.table.types.logical.RowType;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.List;

import static org.apache.fluss.flink.sink.FlinkStreamPartitioner.partition;
import static org.apache.fluss.flink.utils.FlinkConversions.toFlussRowType;

/** Flink sink for Fluss. */
class FlinkSink<InputT> extends SinkAdapter<InputT> implements SupportsPreWriteTopology<InputT> {

    private static final long serialVersionUID = 1L;

    private final SinkWriterBuilder<? extends FlinkSinkWriter, InputT> builder;

    FlinkSink(SinkWriterBuilder<? extends FlinkSinkWriter, InputT> builder) {
        this.builder = builder;
    }

    @Override
    protected SinkWriter<InputT> createWriter(
            MailboxExecutor mailboxExecutor, SinkWriterMetricGroup metricGroup) {
        FlinkSinkWriter<InputT> flinkSinkWriter = builder.createWriter(mailboxExecutor);
        flinkSinkWriter.initialize(InternalSinkWriterMetricGroup.wrap(metricGroup));
        return flinkSinkWriter;
    }

    @Override
    public DataStream<InputT> addPreWriteTopology(DataStream<InputT> input) {
        return builder.addPreWriteTopology(input);
    }

    @Internal
    interface SinkWriterBuilder<W extends FlinkSinkWriter<InputT>, InputT> extends Serializable {
        W createWriter(MailboxExecutor mailboxExecutor);

        DataStream<InputT> addPreWriteTopology(DataStream<InputT> input);
    }

    @Internal
    static class AppendSinkWriterBuilder<InputT>
            implements SinkWriterBuilder<AppendSinkWriter<InputT>, InputT> {

        private static final long serialVersionUID = 1L;

        private final TablePath tablePath;
        private final Configuration flussConfig;
        private final RowType tableRowType;
        private final int numBucket;
        private final List<String> bucketKeys;
        private final List<String> partitionKeys;
        private final @Nullable DataLakeFormat lakeFormat;
        private final DistributionMode shuffleMode;
        private final FlussSerializationSchema<InputT> flussSerializationSchema;
        private final @Nullable TypeInformation<InputT> rowTypeInformation;

        public AppendSinkWriterBuilder(
                TablePath tablePath,
                Configuration flussConfig,
                RowType tableRowType,
                int numBucket,
                List<String> bucketKeys,
                List<String> partitionKeys,
                @Nullable DataLakeFormat lakeFormat,
                DistributionMode shuffleMode,
                FlussSerializationSchema<InputT> flussSerializationSchema,
                @Nullable TypeInformation<InputT> rowTypeInformation) {
            this.tablePath = tablePath;
            this.flussConfig = flussConfig;
            this.tableRowType = tableRowType;
            this.numBucket = numBucket;
            this.bucketKeys = bucketKeys;
            this.partitionKeys = partitionKeys;
            this.lakeFormat = lakeFormat;
            this.shuffleMode = shuffleMode;
            this.flussSerializationSchema = flussSerializationSchema;
            this.rowTypeInformation = rowTypeInformation;
        }

        @Override
        public AppendSinkWriter<InputT> createWriter(MailboxExecutor mailboxExecutor) {
            return new AppendSinkWriter<>(
                    tablePath,
                    flussConfig,
                    tableRowType,
                    mailboxExecutor,
                    flussSerializationSchema);
        }

        @Override
        public DataStream<InputT> addPreWriteTopology(DataStream<InputT> input) {
            switch (shuffleMode) {
                case BUCKET_SHUFFLE:
                    if (!bucketKeys.isEmpty()) {
                        return partition(
                                input,
                                new FlinkRowDataChannelComputer<>(
                                        toFlussRowType(tableRowType),
                                        bucketKeys,
                                        partitionKeys,
                                        lakeFormat,
                                        numBucket,
                                        flussSerializationSchema),
                                input.getParallelism());
                    }
                    return input;
                case NONE:
                    return input;
                case DYNAMIC_SHUFFLE:
                    if (partitionKeys.isEmpty()) {
                        throw new UnsupportedOperationException(
                                "DYNAMIC_SHUFFLE is only supported for partition tables");
                    }

                    if (rowTypeInformation == null) {
                        throw new UnsupportedOperationException(
                                "RowTypeInformation is required for DYNAMIC_SHUFFLE mode.");
                    }
                    TypeInformation<StatisticsOrRecord<InputT>> statisticsOrRecordTypeInformation =
                            new StatisticsOrRecordTypeInformation<>(rowTypeInformation);
                    SingleOutputStreamOperator<StatisticsOrRecord<InputT>> shuffleStream =
                            input.transform(
                                            "Dynamic shuffle data statistics",
                                            statisticsOrRecordTypeInformation,
                                            new DataStatisticsOperatorFactory<>(
                                                    toFlussRowType(tableRowType),
                                                    partitionKeys,
                                                    flussSerializationSchema))
                                    .uid("Dynamic shuffle data statistics" + tablePath)
                                    // Set the parallelism same as input operator to encourage
                                    // chaining
                                    .setParallelism(input.getParallelism());

                    return partition(
                                    shuffleStream,
                                    new StatisticsOrRecordChannelComputer<>(
                                            toFlussRowType(tableRowType),
                                            bucketKeys,
                                            partitionKeys,
                                            numBucket,
                                            lakeFormat,
                                            flussSerializationSchema),
                                    input.getParallelism())
                            .flatMap(
                                    (FlatMapFunction<StatisticsOrRecord<InputT>, InputT>)
                                            (statisticsOrRecord, out) -> {
                                                if (statisticsOrRecord.hasRecord()) {
                                                    out.collect(statisticsOrRecord.record());
                                                }
                                            })
                            .uid("flat map" + tablePath)
                            // To promote operator chaining with the downstream writer operator,
                            // setting slot sharing group and the parallelism as default, {@link
                            // SinkTransformationTranslator} will set the parallelism same as sink
                            // transformation.
                            .slotSharingGroup("shuffle-partition-custom-group")
                            .returns(rowTypeInformation);

                default:
                    throw new UnsupportedOperationException(
                            "Unsupported distribution mode: " + shuffleMode);
            }
        }
    }

    @Internal
    static class UpsertSinkWriterBuilder<InputT>
            implements SinkWriterBuilder<UpsertSinkWriter<InputT>, InputT> {

        private static final long serialVersionUID = 1L;

        private final TablePath tablePath;
        private final Configuration flussConfig;
        private final RowType tableRowType;
        private final @Nullable int[] targetColumnIndexes;
        private final int numBucket;
        private final List<String> bucketKeys;
        private final List<String> partitionKeys;
        private final @Nullable DataLakeFormat lakeFormat;
        private final DistributionMode shuffleMode;
        private final FlussSerializationSchema<InputT> flussSerializationSchema;

        UpsertSinkWriterBuilder(
                TablePath tablePath,
                Configuration flussConfig,
                RowType tableRowType,
                @Nullable int[] targetColumnIndexes,
                int numBucket,
                List<String> bucketKeys,
                List<String> partitionKeys,
                @Nullable DataLakeFormat lakeFormat,
                DistributionMode shuffleMode,
                FlussSerializationSchema<InputT> flussSerializationSchema) {
            this.tablePath = tablePath;
            this.flussConfig = flussConfig;
            this.tableRowType = tableRowType;
            this.targetColumnIndexes = targetColumnIndexes;
            this.numBucket = numBucket;
            this.bucketKeys = bucketKeys;
            this.partitionKeys = partitionKeys;
            this.lakeFormat = lakeFormat;
            this.shuffleMode = shuffleMode;
            this.flussSerializationSchema = flussSerializationSchema;
        }

        @Override
        public UpsertSinkWriter<InputT> createWriter(MailboxExecutor mailboxExecutor) {
            return new UpsertSinkWriter<>(
                    tablePath,
                    flussConfig,
                    tableRowType,
                    targetColumnIndexes,
                    mailboxExecutor,
                    flussSerializationSchema);
        }

        @Override
        public DataStream<InputT> addPreWriteTopology(DataStream<InputT> input) {
            switch (shuffleMode) {
                case BUCKET_SHUFFLE:
                    return partition(
                            input,
                            new FlinkRowDataChannelComputer<>(
                                    toFlussRowType(tableRowType),
                                    bucketKeys,
                                    partitionKeys,
                                    lakeFormat,
                                    numBucket,
                                    flussSerializationSchema),
                            input.getParallelism());
                case NONE:
                    return input;
                default:
                    throw new UnsupportedOperationException(
                            "Unsupported distribution mode: " + shuffleMode);
            }
        }
    }
}
