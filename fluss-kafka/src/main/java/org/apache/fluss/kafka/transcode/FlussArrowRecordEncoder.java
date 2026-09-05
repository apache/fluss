/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.kafka.transcode;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.memory.UnmanagedPagedOutputView;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.record.MemoryLogRecordsArrowBuilder;
import org.apache.fluss.record.bytesview.BytesView;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.arrow.ArrowWriter;
import org.apache.fluss.row.arrow.ArrowWriterPool;
import org.apache.fluss.shaded.arrow.org.apache.arrow.memory.BufferAllocator;
import org.apache.fluss.shaded.arrow.org.apache.arrow.memory.RootAllocator;

import java.util.List;

/** Encodes assembled physical Fluss rows into one native Arrow log batch. */
@Internal
public final class FlussArrowRecordEncoder {

    private static final int INITIAL_PAGE_SIZE = 4096;

    /** Encodes all rows using the table's current schema ID and Arrow compression settings. */
    public BytesView encode(List<GenericRow> rows, TableInfo tableInfo) throws Exception {
        try (BufferAllocator allocator = new RootAllocator(Integer.MAX_VALUE);
                ArrowWriterPool provider = new ArrowWriterPool(allocator)) {
            ArrowWriter writer =
                    provider.getOrCreateWriter(
                            tableInfo.getTableId(),
                            tableInfo.getSchemaId(),
                            Integer.MAX_VALUE,
                            tableInfo.getRowType(),
                            tableInfo.getTableConfig().getArrowCompressionInfo());
            MemoryLogRecordsArrowBuilder builder =
                    MemoryLogRecordsArrowBuilder.builder(
                            tableInfo.getSchemaId(),
                            writer,
                            new UnmanagedPagedOutputView(INITIAL_PAGE_SIZE),
                            true,
                            null);
            for (GenericRow row : rows) {
                builder.append(ChangeType.APPEND_ONLY, row);
            }
            return builder.build();
        }
    }
}
