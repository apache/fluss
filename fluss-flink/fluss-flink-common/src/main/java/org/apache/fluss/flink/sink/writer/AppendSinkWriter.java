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

package org.apache.fluss.flink.sink.writer;

import org.apache.fluss.client.table.writer.AppendWriter;
import org.apache.fluss.client.table.writer.TableWriter;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.flink.row.OperationType;
import org.apache.fluss.flink.sink.serializer.FlussSerializationSchema;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.InternalRow;

import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
import org.apache.flink.table.types.logical.RowType;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

import static org.apache.fluss.utils.Preconditions.checkState;

/** An append only sink writer for fluss log table. */
public class AppendSinkWriter<InputT> extends FlinkSinkWriter<InputT> {

    private transient AppendWriter appendWriter;

    public AppendSinkWriter(
            TablePath tablePath,
            Configuration flussConfig,
            RowType tableRowType,
            MailboxExecutor mailboxExecutor,
            FlussSerializationSchema<InputT> serializationSchema) {
        super(tablePath, flussConfig, tableRowType, mailboxExecutor, serializationSchema);
    }

    @Override
    public void initialize(SinkWriterMetricGroup metricGroup) {
        super.initialize(metricGroup);

        appendWriter = table.newAppend().createWriter();
        LOG.info("Finished opening Fluss {}.", this.getClass().getSimpleName());
    }

    @Override
    CompletableFuture<?> writeRow(OperationType opType, InternalRow internalRow) {
        checkState(opType == OperationType.APPEND, "Only APPEND operation is supported.");
        return appendWriter.append(internalRow);
    }

    @Override
    public void flush(boolean endOfInput) throws IOException {
        appendWriter.flush();
        checkAsyncException();
    }

    @Override
    TableWriter getTableWriter() {
        return appendWriter;
    }
}
