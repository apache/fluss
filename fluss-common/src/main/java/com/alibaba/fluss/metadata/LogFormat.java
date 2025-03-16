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

package com.alibaba.fluss.metadata;

import com.alibaba.fluss.record.MemoryLogRecordsArrowBuilder;
import com.alibaba.fluss.record.MemoryLogRecordsIndexedBuilder;
import com.alibaba.fluss.row.indexed.IndexedRow;

/** The format of the log records in log store. The supported formats are 'arrow' and 'indexed'. */
public enum LogFormat {

    /**
     * The log record batches are stored in Apache Arrow batch format which is a columnar-oriented
     * format. This can have better performance for analytics workloads as it is more efficient to
     * do column projection. This is the default log format.
     *
     * @see MemoryLogRecordsArrowBuilder
     */
    ARROW,

    /**
     * The log record batches are stored in {@link IndexedRow} format which is a row-oriented
     * format. It is more efficient for event-driven workloads and have smaller memory/disk
     * footprint when a log record batch doesn't have many rows. It isn't good at analytics
     * workloads (e.g., doesn't support column projection).
     *
     * @see MemoryLogRecordsIndexedBuilder
     */
    INDEXED;

    /**
     * Creates a {@link LogFormat} from the given string. The string must be either 'arrow' or
     * 'indexed'.
     */
    public static LogFormat fromString(String format) {
        switch (format.toUpperCase()) {
            case "ARROW":
                return ARROW;
            case "INDEXED":
                return INDEXED;
            default:
                throw new IllegalArgumentException("Unsupported log format: " + format);
        }
    }
}
