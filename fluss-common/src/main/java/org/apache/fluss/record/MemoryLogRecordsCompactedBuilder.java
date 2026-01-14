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
import org.apache.fluss.metadata.LogFormat;
import org.apache.fluss.row.compacted.CompactedRow;

import java.io.IOException;

import static org.apache.fluss.record.LogRecordBatch.CURRENT_LOG_MAGIC_VALUE;

/**
 * Default builder for {@link MemoryLogRecords} of log records in {@link LogFormat#COMPACTED}
 * format.
 */
public class MemoryLogRecordsCompactedBuilder extends MemoryLogRecordsRowBuilder<CompactedRow> {

    private MemoryLogRecordsCompactedBuilder(
            long baseLogOffset,
            int schemaId,
            int writeLimit,
            byte magic,
            AbstractPagedOutputView pagedOutputView,
            boolean appendOnly) {
        super(baseLogOffset, schemaId, writeLimit, magic, pagedOutputView, appendOnly);
    }

    public static MemoryLogRecordsCompactedBuilder builder(
            int schemaId, int writeLimit, AbstractPagedOutputView outputView, boolean appendOnly) {
        return new MemoryLogRecordsCompactedBuilder(
                BUILDER_DEFAULT_OFFSET,
                schemaId,
                writeLimit,
                CURRENT_LOG_MAGIC_VALUE,
                outputView,
                appendOnly);
    }

    @VisibleForTesting
    public static MemoryLogRecordsCompactedBuilder builder(
            long baseLogOffset,
            int schemaId,
            int writeLimit,
            byte magic,
            AbstractPagedOutputView outputView)
            throws IOException {
        return new MemoryLogRecordsCompactedBuilder(
                baseLogOffset, schemaId, writeLimit, magic, outputView, false);
    }

    @Override
    protected int sizeOf(CompactedRow row) {
        return CompactedLogRecord.sizeOf(row);
    }

    @Override
    protected int writeRecord(ChangeType changeType, CompactedRow row) throws IOException {
        return CompactedLogRecord.writeTo(pagedOutputView, changeType, row);
    }
}
