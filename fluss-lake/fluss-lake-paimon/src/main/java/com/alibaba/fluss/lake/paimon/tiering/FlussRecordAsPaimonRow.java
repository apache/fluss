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

package com.alibaba.fluss.lake.paimon.tiering;

import com.alibaba.fluss.lake.paimon.source.FlussRowAsPaimonRow;
import com.alibaba.fluss.record.LogRecord;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;

import static com.alibaba.fluss.lake.paimon.utils.PaimonConversions.toRowKind;
import static com.alibaba.fluss.utils.Preconditions.checkState;

/** To wrap Fluss {@link LogRecord} as paimon {@link InternalRow}. */
public class FlussRecordAsPaimonRow extends FlussRowAsPaimonRow {

    // Lake table for paimon will append three system columns: __bucket, __offset,__timestamp
    private static final int LAKE_PAIMON_SYSTEM_COLUMNS = 3;
    private final int bucket;
    private LogRecord logRecord;
    private int originRowFieldCount;

    public FlussRecordAsPaimonRow(int bucket, RowType tableTowType) {
        super(tableTowType);
        this.bucket = bucket;
    }

    public void setFlussRecord(LogRecord logRecord) {
        this.logRecord = logRecord;
        this.internalRow = logRecord.getRow();
        this.originRowFieldCount = internalRow.getFieldCount();
        checkState(
                originRowFieldCount == tableRowType.getFieldCount() - LAKE_PAIMON_SYSTEM_COLUMNS,
                "The paimon table fields count must equals to LogRecord's fields count.");
    }

    @Override
    public int getFieldCount() {
        return
        //  business (including partitions) + system (three system fields: bucket, offset,
        // timestamp)
        originRowFieldCount + LAKE_PAIMON_SYSTEM_COLUMNS;
    }

    @Override
    public RowKind getRowKind() {
        return toRowKind(logRecord.getChangeType());
    }

    @Override
    public boolean isNullAt(int pos) {
        if (pos < originRowFieldCount) {
            return super.isNullAt(pos);
        }
        // is the last three system fields: bucket, offset, timestamp which are never null
        return false;
    }

    @Override
    public int getInt(int pos) {
        if (pos == originRowFieldCount) {
            // bucket system column
            return bucket;
        }
        return super.getInt(pos);
    }

    @Override
    public long getLong(int pos) {
        if (pos == originRowFieldCount + 1) {
            //  offset system column
            return logRecord.logOffset();
        } else if (pos == originRowFieldCount + 2) {
            //  timestamp system column
            return logRecord.timestamp();
        }
        //  the origin RowData
        return super.getLong(pos);
    }

    @Override
    public Timestamp getTimestamp(int pos, int precision) {
        // it's timestamp system column
        if (pos == originRowFieldCount + 2) {
            return Timestamp.fromEpochMillis(logRecord.timestamp());
        }
        return super.getTimestamp(pos, precision);
    }
}
