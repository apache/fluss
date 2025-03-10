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

package com.alibaba.fluss.connector.flink.sink;

import com.alibaba.fluss.annotation.Internal;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.connector.flink.sink.writer.AppendSinkWriter;
import com.alibaba.fluss.connector.flink.sink.writer.FlinkSinkWriter;
import com.alibaba.fluss.connector.flink.sink.writer.UpsertSinkWriter;
import com.alibaba.fluss.metadata.TablePath;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.runtime.metrics.groups.InternalSinkWriterMetricGroup;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.Serializable;

/** Flink sink for Fluss. */
class FlinkSink implements Sink<RowData> {

    private static final long serialVersionUID = 1L;

    private final SinkWriterBuilder<? extends FlinkSinkWriter> builder;

    FlinkSink(SinkWriterBuilder<? extends FlinkSinkWriter> builder) {
        this.builder = builder;
    }

    @Deprecated
    @Override
    public SinkWriter<RowData> createWriter(InitContext context) throws IOException {
        FlinkSinkWriter flinkSinkWriter = builder.createWriter();
        flinkSinkWriter.initialize(InternalSinkWriterMetricGroup.wrap(context.metricGroup()));
        return flinkSinkWriter;
    }

    public SinkWriter<RowData> createWriter(WriterInitContext context) throws IOException {
        FlinkSinkWriter flinkSinkWriter = builder.createWriter();
        flinkSinkWriter.initialize(InternalSinkWriterMetricGroup.wrap(context.metricGroup()));
        return flinkSinkWriter;
    }

    @Internal
    interface SinkWriterBuilder<W extends FlinkSinkWriter> extends Serializable {
        W createWriter();
    }

    @Internal
    static class AppendSinkWriterBuilder implements SinkWriterBuilder<AppendSinkWriter> {

        private static final long serialVersionUID = 1L;

        private final TablePath tablePath;
        private final Configuration flussConfig;
        private final RowType tableRowType;
        private final boolean ignoreDelete;

        public AppendSinkWriterBuilder(
                TablePath tablePath,
                Configuration flussConfig,
                RowType tableRowType,
                boolean ignoreDelete) {
            this.tablePath = tablePath;
            this.flussConfig = flussConfig;
            this.tableRowType = tableRowType;
            this.ignoreDelete = ignoreDelete;
        }

        @Override
        public AppendSinkWriter createWriter() {
            return new AppendSinkWriter(tablePath, flussConfig, tableRowType, ignoreDelete);
        }
    }

    @Internal
    static class UpsertSinkWriterBuilder implements SinkWriterBuilder<UpsertSinkWriter> {

        private static final long serialVersionUID = 1L;

        private final TablePath tablePath;
        private final Configuration flussConfig;
        private final RowType tableRowType;
        private final @Nullable int[] targetColumnIndexes;
        private final boolean ignoreDelete;

        UpsertSinkWriterBuilder(
                TablePath tablePath,
                Configuration flussConfig,
                RowType tableRowType,
                @Nullable int[] targetColumnIndexes,
                boolean ignoreDelete) {
            this.tablePath = tablePath;
            this.flussConfig = flussConfig;
            this.tableRowType = tableRowType;
            this.targetColumnIndexes = targetColumnIndexes;
            this.ignoreDelete = ignoreDelete;
        }

        @Override
        public UpsertSinkWriter createWriter() {
            return new UpsertSinkWriter(
                    tablePath, flussConfig, tableRowType, targetColumnIndexes, ignoreDelete);
        }
    }
}
