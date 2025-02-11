/*
 *  Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.alibaba.fluss.client.table.scanner;

import com.alibaba.fluss.annotation.Internal;
import com.alibaba.fluss.record.LogRecord;
import com.alibaba.fluss.record.RowKind;
import com.alibaba.fluss.row.InternalRow;

import java.util.Objects;

/** one scan record. */
// TODO: replace this with GenericRecord in the future
@Internal
public class ScanRecord implements LogRecord {
    private static final long INVALID = -1L;

    private final long offset;
    private final long timestamp;
    private final RowKind rowKind;
    private final InternalRow row;

    public ScanRecord(InternalRow row) {
        this(INVALID, INVALID, RowKind.INSERT, row);
    }

    public ScanRecord(long offset, long timestamp, RowKind rowKind, InternalRow row) {
        this.offset = offset;
        this.timestamp = timestamp;
        this.rowKind = rowKind;
        this.row = row;
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
    public RowKind getRowKind() {
        return rowKind;
    }

    @Override
    public InternalRow getRow() {
        return row;
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
        return offset == that.offset && rowKind == that.rowKind && Objects.equals(row, that.row);
    }

    @Override
    public int hashCode() {
        return Objects.hash(offset, rowKind, row);
    }

    @Override
    public String toString() {
        return rowKind.shortString() + row.toString() + "@" + offset;
    }
}
