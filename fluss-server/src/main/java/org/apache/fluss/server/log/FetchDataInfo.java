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

package org.apache.fluss.server.log;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.record.LogRecords;
import org.apache.fluss.record.MemoryLogRecords;

/** FetchDataInfo is used to store the data of a fetch request. */
@Internal
public class FetchDataInfo {
    private final LogOffsetMetadata fetchOffsetMetadata;
    private final LogRecords records;
    private final long skipToNextFetchOffset;

    public LogRecords getRecords() {
        return records;
    }

    public LogOffsetMetadata getFetchOffsetMetadata() {
        return fetchOffsetMetadata;
    }

    public FetchDataInfo(LogRecords records) {
        this(LogOffsetMetadata.UNKNOWN_OFFSET_METADATA, records);
    }

    public FetchDataInfo(LogOffsetMetadata fetchOffsetMetadata, LogRecords records) {
        this(fetchOffsetMetadata, records, -1L);
    }

    public FetchDataInfo(
            LogOffsetMetadata fetchOffsetMetadata, LogRecords records, long skipToNextFetchOffset) {
        this.fetchOffsetMetadata = fetchOffsetMetadata;
        this.records = records;
        this.skipToNextFetchOffset = skipToNextFetchOffset;
    }

    /**
     * Create a filtered empty response with the correct next fetch offset. This is used when all
     * batches are filtered out but we need to inform the client about the correct offset to
     * continue fetching from.
     */
    public static FetchDataInfo createFilteredEmptyResponse(
            LogOffsetMetadata fetchOffsetMetadata, long skipToNextFetchOffset) {
        return new FetchDataInfo(
                fetchOffsetMetadata, MemoryLogRecords.EMPTY, skipToNextFetchOffset);
    }

    /**
     * Gets the next valid fetch offset that the client should use for subsequent fetch requests.
     *
     * <p>This method provides explicit offset information to address scenarios where the client
     * cannot implicitly determine the next fetch offset from the lastOffset of received data. This
     * is particularly important when using record batch filters (recordBatchFilter pushdown), where
     * all batches might be filtered out, leaving no data for the client to derive the next offset
     * from.
     *
     * @return the next valid offset for fetching, or -1 if not applicable
     */
    public long getSkipToNextFetchOffset() {
        return skipToNextFetchOffset;
    }
}
