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

package org.apache.fluss.record;

import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.memory.AbstractPagedOutputView;
import org.apache.fluss.row.indexed.IndexedRow;

import java.io.IOException;

import static org.apache.fluss.record.LogRecordBatch.CURRENT_LOG_MAGIC_VALUE;

/**
 * Default builder for {@link MemoryLogRecords} of log records in {@link
 * org.apache.fluss.metadata.LogFormat#INDEXED} format.
 */
public class MemoryLogRecordsIndexedBuilder extends MemoryLogRecordsRowBuilder<IndexedRow> {

    private MemoryLogRecordsIndexedBuilder(
            long baseLogOffset,
            int schemaId,
            int writeLimit,
            byte magic,
            AbstractPagedOutputView pagedOutputView,
            boolean appendOnly) {
        super(baseLogOffset, schemaId, writeLimit, magic, pagedOutputView, appendOnly);
    }

    public static MemoryLogRecordsIndexedBuilder builder(
            int schemaId, int writeLimit, AbstractPagedOutputView outputView, boolean appendOnly) {
        return new MemoryLogRecordsIndexedBuilder(
                BUILDER_DEFAULT_OFFSET,
                schemaId,
                writeLimit,
                CURRENT_LOG_MAGIC_VALUE,
                outputView,
                appendOnly);
    }

    @VisibleForTesting
    public static MemoryLogRecordsIndexedBuilder builder(
            long baseLogOffset,
            int schemaId,
            int writeLimit,
            byte magic,
            AbstractPagedOutputView outputView)
            throws IOException {
        return new MemoryLogRecordsIndexedBuilder(
                baseLogOffset, schemaId, writeLimit, magic, outputView, false);
    }

    @Override
    protected int sizeOf(IndexedRow row) {
        return IndexedLogRecord.sizeOf(row);
    }

    @Override
    protected int writeRecord(ChangeType changeType, IndexedRow row) throws IOException {
        return IndexedLogRecord.writeTo(pagedOutputView, changeType, row);
    }
}
