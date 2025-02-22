/*
 * Copyright (c) 2024 Alibaba Group Holding Ltd.
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

package com.alibaba.fluss.connector.flink.sink.writer;

import com.alibaba.fluss.client.table.writer.AppendWriter;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.row.InternalRow;

import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

/** An append only sink writer for fluss log table. */
public class AppendSinkWriter extends FlinkSinkWriter {

    private transient AppendWriter appendWriter;

    public AppendSinkWriter(
            TablePath tablePath,
            Configuration flussConfig,
            RowType tableRowType,
            boolean ignoreDelete) {
        super(tablePath, flussConfig, tableRowType, ignoreDelete);
    }

    @Override
    public void initialize(SinkWriterMetricGroup metricGroup) {
        super.initialize(metricGroup);
        appendWriter = table.newAppend().createWriter();
        LOG.info("Finished opening Fluss {}.", this.getClass().getSimpleName());
    }

    @Override
    CompletableFuture<?> writeRow(RowKind rowKind, InternalRow internalRow) {
        return appendWriter.append(internalRow);
    }

    @Override
    public void flush(boolean endOfInput) throws IOException {
        appendWriter.flush();
        checkAsyncException();
    }
}
