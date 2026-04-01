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

package org.apache.fluss.client.table.scanner;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.record.LogRecord;
import org.apache.fluss.row.InternalRow;

import java.util.Objects;

/** one scan record. */
// TODO: replace this with GenericRecord in the future
@Internal
public class ScanRecord implements LogRecord {
    private static final long INVALID = -1L;

    /** Indicates that the size in bytes is unknown for this record. */
    public static final int UNKNOWN_SIZE_IN_BYTES = -1;

    private final long offset;
    private final long timestamp;
    private final ChangeType changeType;
    private final InternalRow row;
    private final int sizeInBytes;

    public ScanRecord(InternalRow row) {
        this(INVALID, INVALID, ChangeType.INSERT, row);
    }

    public ScanRecord(InternalRow row, int sizeInBytes) {
        this(INVALID, INVALID, ChangeType.INSERT, row, sizeInBytes);
    }

    public ScanRecord(long offset, long timestamp, ChangeType changeType, InternalRow row) {
        this(offset, timestamp, changeType, row, UNKNOWN_SIZE_IN_BYTES);
    }

    public ScanRecord(
            long offset, long timestamp, ChangeType changeType, InternalRow row, int sizeInBytes) {
        this.offset = offset;
        this.timestamp = timestamp;
        this.changeType = changeType;
        this.row = row;
        this.sizeInBytes = sizeInBytes;
    }

    /** The position of this record in the corresponding fluss table bucket. */
    @Override
    public long logOffset() {
        return offset;
    }

    @Override
    public long timestamp() {
        return timestamp;
    }

    @Override
    public ChangeType getChangeType() {
        return changeType;
    }

    @Override
    public InternalRow getRow() {
        return row;
    }

    public int getSizeInBytes() {
        return sizeInBytes;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ScanRecord that = (ScanRecord) o;
        return offset == that.offset
                && changeType == that.changeType
                && Objects.equals(row, that.row);
    }

    @Override
    public int hashCode() {
        return Objects.hash(offset, changeType, row);
    }

    @Override
    public String toString() {
        return changeType.shortString() + row.toString() + "@" + offset;
    }
}
