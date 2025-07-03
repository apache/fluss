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

package com.alibaba.fluss.flink.sink.writer;

import com.alibaba.fluss.client.table.writer.TableWriter;
import com.alibaba.fluss.client.table.writer.Upsert;
import com.alibaba.fluss.client.table.writer.UpsertWriter;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.flink.row.OperationType;
import com.alibaba.fluss.flink.sink.serializer.FlussSerializationSchema;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.row.InternalRow;

import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
import org.apache.flink.table.types.logical.RowType;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

/** An upsert sink writer or fluss primary key table. */
public class UpsertSinkWriter<InputT> extends FlinkSinkWriter<InputT> {

    private transient UpsertWriter upsertWriter;

    public UpsertSinkWriter(
            TablePath tablePath,
            Configuration flussConfig,
            RowType tableRowType,
            @Nullable int[] targetColumnIndexes,
            MailboxExecutor mailboxExecutor,
            FlussSerializationSchema<InputT> flussSerializationSchema) {
        super(
                tablePath,
                flussConfig,
                tableRowType,
                targetColumnIndexes,
                mailboxExecutor,
                flussSerializationSchema);
    }

    @Override
    public void initialize(SinkWriterMetricGroup metricGroup) {
        super.initialize(metricGroup);
        Upsert upsert = table.newUpsert();
        if (targetColumnIndexes != null) {
            upsert = upsert.partialUpdate(targetColumnIndexes);
        }
        upsertWriter = upsert.createWriter();
        LOG.info("Finished opening Fluss {}.", this.getClass().getSimpleName());
    }

    @Override
    CompletableFuture<?> writeRow(OperationType opType, InternalRow internalRow) {
        if (opType == OperationType.UPSERT) {
            return upsertWriter.upsert(internalRow);
        } else if (opType == OperationType.DELETE) {
            return upsertWriter.delete(internalRow);
        } else {
            throw new UnsupportedOperationException("Unsupported operation type: " + opType);
        }
    }

    @Override
    public void flush(boolean endOfInput) throws IOException, InterruptedException {
        upsertWriter.flush();
        checkAsyncException();
    }

    @Override
    TableWriter getTableWriter() {
        return upsertWriter;
    }
}
