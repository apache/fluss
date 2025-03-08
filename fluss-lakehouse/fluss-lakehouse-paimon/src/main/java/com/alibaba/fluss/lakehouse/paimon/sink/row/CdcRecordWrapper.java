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

package com.alibaba.fluss.lakehouse.paimon.sink.row;

import com.alibaba.fluss.lakehouse.paimon.record.CdcRecord;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.flink.FlinkRowWrapper;

/** A {@link CdcRecord} wrapper to convert {@link CdcRecord} to Paimon's {@link InternalRow}. */
public class CdcRecordWrapper extends FlinkRowWrapper {

    // offset, timestamp as the last two extra fields
    private final long offset;
    private final long timestamp;

    public CdcRecordWrapper(CdcRecord cdcRecord) {
        super(cdcRecord.getRowData());
        this.offset = cdcRecord.getOffset();
        this.timestamp = cdcRecord.getTimestamp();
    }

    @Override
    public int getFieldCount() {
        // plus two fields: offset, timestamp
        return super.getFieldCount() + 2;
    }

    @Override
    public boolean isNullAt(int pos) {
        if (pos < super.getFieldCount()) {
            return super.isNullAt(pos);
        }
        // is the last two fields: offset, timestamp which are never null
        return false;
    }

    @Override
    public long getLong(int pos) {
        int fieldCount = getFieldCount();
        // if it points to the last two fields
        if (pos == fieldCount - 1) {
            return timestamp;
        } else if (pos == fieldCount - 2) {
            return offset;
        }
        //  the origin RowData
        return super.getLong(pos);
    }

    @Override
    public Timestamp getTimestamp(int pos, int precision) {
        // it's timestamp field
        if (pos == getFieldCount() - 1) {
            return Timestamp.fromEpochMillis(timestamp);
        }
        return super.getTimestamp(pos, precision);
    }
}
