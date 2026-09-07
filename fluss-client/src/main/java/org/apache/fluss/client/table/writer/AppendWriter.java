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

package org.apache.fluss.client.table.writer;

import org.apache.fluss.annotation.PublicEvolving;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.row.InternalRow;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * The writer to write data to the log table.
 *
 * @since 0.2
 */
@PublicEvolving
public interface AppendWriter extends TableWriter {

    /**
     * Append a record into a Log Table.
     *
     * @param record the record to append.
     * @return A {@link CompletableFuture} that always returns append result when complete normally.
     */
    CompletableFuture<AppendResult> append(InternalRow record);

    /**
     * Appends the columns of one column group for rows that already exist in the log (FIP-45 log
     * enrichment via append columns).
     *
     * <p>{@code rows} carry only the group's columns, in schema order, and fill the contiguous
     * base-log offsets {@code [firstSourceOffset, firstSourceOffset + rows.size())} of {@code
     * bucket}. The first offset must equal the group's current log end offset (its enrichment
     * watermark) on the bucket; a batch entirely below it is acknowledged without effect, so
     * replaying after a restart is safe, while a gap or a batch running past the base high
     * watermark fails with {@link org.apache.fluss.exception.InvalidColumnGroupOffsetException}.
     *
     * @param columnGroup the column group to fill
     * @param bucket the bucket whose rows are enriched
     * @param firstSourceOffset the base-log offset filled by the first row
     * @param rows the group rows, one per consecutive offset
     * @return the column group's watermarks on the bucket after the append
     */
    CompletableFuture<AppendColumnsResult> appendColumns(
            String columnGroup, TableBucket bucket, long firstSourceOffset, List<InternalRow> rows);
}
