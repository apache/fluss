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
import org.apache.fluss.flink.sink.serializer.FlussSerializationSchema;
import org.apache.fluss.flink.sink.writer.AppendSinkWriter;
import org.apache.fluss.flink.sink.writer.FlinkSinkWriter;
import org.apache.fluss.flink.sink.writer.UpsertSinkWriter;
import org.apache.fluss.metadata.DataLakeFormat;
import org.apache.fluss.metadata.TablePath;

import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.runtime.metrics.groups.InternalSinkWriterMetricGroup;
import org.apache.flink.streaming.api.connector.sink2.SupportsPreWriteTopology;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.types.logical.RowType;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;

import static org.apache.fluss.flink.sink.FlinkStreamPartitioner.partition;
import static org.apache.fluss.flink.utils.FlinkConversions.toFlussRowType;

/** Flink sink for Fluss. */
class FlinkSink<InputT> implements Sink<InputT>, SupportsPreWriteTopology<InputT> {

    private static final long serialVersionUID = 1L;

    private final SinkWriterBuilder<? extends FlinkSinkWriter, InputT> builder;

    FlinkSink(SinkWriterBuilder<? extends FlinkSinkWriter, InputT> builder) {
        this.builder = builder;
    }

    @Deprecated
    @Override
    public SinkWriter<InputT> createWriter(InitContext context) throws IOException {
        FlinkSinkWriter<InputT> flinkSinkWriter =
                builder.createWriter(context.getMailboxExecutor());
        flinkSinkWriter.initialize(InternalSinkWriterMetricGroup.wrap(context.metricGroup()));
        return flinkSinkWriter;
    }

    @Override
    public SinkWriter<InputT> createWriter(WriterInitContext context) throws IOException {
        FlinkSinkWriter<InputT> flinkSinkWriter =
                builder.createWriter(context.getMailboxExecutor());
        flinkSinkWriter.initialize(InternalSinkWriterMetricGroup.wrap(context.metricGroup()));
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
        private final boolean shuffleByBucketId;
        private final FlussSerializationSchema<InputT> flussSerializationSchema;

        public AppendSinkWriterBuilder(
                TablePath tablePath,
                Configuration flussConfig,
                RowType tableRowType,
                int numBucket,
                List<String> bucketKeys,
                List<String> partitionKeys,
                @Nullable DataLakeFormat lakeFormat,
                boolean shuffleByBucketId,
                FlussSerializationSchema<InputT> flussSerializationSchema) {
            this.tablePath = tablePath;
            this.flussConfig = flussConfig;
            this.tableRowType = tableRowType;
            this.numBucket = numBucket;
            this.bucketKeys = bucketKeys;
            this.partitionKeys = partitionKeys;
            this.lakeFormat = lakeFormat;
            this.shuffleByBucketId = shuffleByBucketId;
            this.flussSerializationSchema = flussSerializationSchema;
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
            // For append only sink, we will do bucket shuffle only if bucket keys are not empty.
            if (!bucketKeys.isEmpty() && shuffleByBucketId) {
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
            } else {
                return input;
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
        private final boolean shuffleByBucketId;
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
                boolean shuffleByBucketId,
                FlussSerializationSchema<InputT> flussSerializationSchema) {
            this.tablePath = tablePath;
            this.flussConfig = flussConfig;
            this.tableRowType = tableRowType;
            this.targetColumnIndexes = targetColumnIndexes;
            this.numBucket = numBucket;
            this.bucketKeys = bucketKeys;
            this.partitionKeys = partitionKeys;
            this.lakeFormat = lakeFormat;
            this.shuffleByBucketId = shuffleByBucketId;
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
            return shuffleByBucketId
                    ? partition(
                            input,
                            new FlinkRowDataChannelComputer<>(
                                    toFlussRowType(tableRowType),
                                    bucketKeys,
                                    partitionKeys,
                                    lakeFormat,
                                    numBucket,
                                    flussSerializationSchema),
                            input.getParallelism())
                    : input;
        }
    }
}
