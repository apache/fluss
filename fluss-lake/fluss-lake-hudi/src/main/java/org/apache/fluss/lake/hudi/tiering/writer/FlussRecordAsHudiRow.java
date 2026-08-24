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

package org.apache.fluss.lake.hudi.tiering.writer;

import org.apache.fluss.lake.hudi.utils.HudiUtils;
import org.apache.fluss.record.LogRecord;

import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;

import static org.apache.fluss.lake.hudi.HudiLakeCatalog.LEGACY_SYSTEM_COLUMNS;
import static org.apache.fluss.lake.hudi.utils.HudiConversions.toRowKind;
import static org.apache.fluss.utils.Preconditions.checkState;

/** Wraps a Fluss {@link LogRecord} as a Hudi/Flink row with Fluss system columns. */
public class FlussRecordAsHudiRow extends FlussRowAsHudiRow {

    private final int bucket;
    private final boolean isLegacy;

    // the count of user (business) columns; system columns (if any) start at this index
    private final int businessFieldCount;

    private LogRecord logRecord;

    public FlussRecordAsHudiRow(int bucket, RowType rowType) {
        super(rowType);
        this.bucket = bucket;
        // FIP-27: a legacy table carries the three trailing system columns; a clean table does not.
        this.isLegacy = HudiUtils.isLegacyTable(rowType);
        this.businessFieldCount =
                isLegacy
                        ? rowType.getFieldCount() - LEGACY_SYSTEM_COLUMNS.size()
                        : rowType.getFieldCount();
    }

    public void setFlussRecord(LogRecord logRecord) {
        this.logRecord = logRecord;
        this.internalRow = logRecord.getRow();
        checkState(
                internalRow.getFieldCount() == businessFieldCount,
                "The Fluss record's field count (%s) must equal the business field count (%s).",
                internalRow.getFieldCount(),
                businessFieldCount);
    }

    @Override
    public RowKind getRowKind() {
        return toRowKind(logRecord.getChangeType());
    }

    @Override
    public boolean isNullAt(int pos) {
        if (pos < businessFieldCount) {
            return super.isNullAt(pos);
        }
        return false;
    }

    @Override
    public int getInt(int pos) {
        if (isLegacy && pos == businessFieldCount) {
            return bucket;
        }
        return super.getInt(pos);
    }

    @Override
    public long getLong(int pos) {
        if (isLegacy) {
            if (pos == businessFieldCount + 1) {
                return logRecord.logOffset();
            } else if (pos == businessFieldCount + 2) {
                return logRecord.timestamp();
            }
        }
        return super.getLong(pos);
    }

    @Override
    public TimestampData getTimestamp(int pos, int precision) {
        if (isLegacy && pos == businessFieldCount + 2) {
            return TimestampData.fromEpochMillis(logRecord.timestamp());
        }
        return super.getTimestamp(pos, precision);
    }
}
