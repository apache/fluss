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

package com.alibaba.fluss.flink.sink;

import com.alibaba.fluss.flink.sink.writer.FlinkSinkWriter;

import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.runtime.metrics.groups.InternalSinkWriterMetricGroup;
import org.apache.flink.table.data.RowData;

import java.io.IOException;

/** Flink sink for Fluss. */
class FlinkSink extends AbstractSink {

    private static final long serialVersionUID = 1L;

    public FlinkSink(SinkWriterBuilder<? extends FlinkSinkWriter> builder) {
        super(builder);
    }

    @Deprecated
    @Override
    public SinkWriter<RowData> createWriter(InitContext context) throws IOException {
        FlinkSinkWriter flinkSinkWriter = builder.createWriter();
        flinkSinkWriter.initialize(InternalSinkWriterMetricGroup.wrap(context.metricGroup()));
        return flinkSinkWriter;
    }
}
