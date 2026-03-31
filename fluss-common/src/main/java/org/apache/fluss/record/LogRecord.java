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

import org.apache.fluss.annotation.PublicEvolving;
import org.apache.fluss.memory.MemorySegment;
import org.apache.fluss.metadata.LogFormat;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.compacted.CompactedRow;
import org.apache.fluss.row.compacted.CompactedRowDeserializer;
import org.apache.fluss.row.indexed.IndexedRow;
import org.apache.fluss.types.DataType;

/**
 * A log record is a tuple consisting of a unique offset in the log, a changeType and a row.
 *
 * @since 0.1
 */
@PublicEvolving
public interface LogRecord {

    /**
     * The offset of this record in the log.
     *
     * @return the offset
     */
    long logOffset();

    /**
     * The commit timestamp of this record in the log.
     *
     * @return the timestamp
     */
    long timestamp();

    /**
     * Get the log record's {@link ChangeType}.
     *
     * @return the record's {@link ChangeType}.
     */
    ChangeType getChangeType();

    /**
     * Get the log record's row.
     *
     * @return the log record's row
     */
    InternalRow getRow();

    /** Deserialize the row in the log record according to given log format. */
    static InternalRow deserializeInternalRow(
            int length,
            MemorySegment segment,
            int position,
            DataType[] fieldTypes,
            LogFormat logFormat) {
        if (logFormat == LogFormat.INDEXED) {
            IndexedRow indexedRow = new IndexedRow(fieldTypes);
            indexedRow.pointTo(segment, position, length);
            return indexedRow;
        } else if (logFormat == LogFormat.COMPACTED) {
            CompactedRow compactedRow =
                    new CompactedRow(fieldTypes.length, new CompactedRowDeserializer(fieldTypes));
            compactedRow.pointTo(segment, position, length);
            return compactedRow;
        } else {
            throw new IllegalArgumentException(
                    "No such internal row deserializer for: " + logFormat);
        }
    }
}
