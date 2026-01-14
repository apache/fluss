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

package org.apache.fluss.lake.paimon.tiering;

import org.apache.fluss.lake.paimon.source.FlussRowAsPaimonRow;
import org.apache.fluss.record.LogRecord;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;

import static org.apache.fluss.lake.paimon.PaimonLakeCatalog.SYSTEM_COLUMNS;
import static org.apache.fluss.lake.paimon.utils.PaimonConversions.toRowKind;

/** To wrap Fluss {@link LogRecord} as paimon {@link InternalRow}. */
public class FlussRecordAsPaimonRow extends FlussRowAsPaimonRow {

    private final int bucket;
    private LogRecord logRecord;
    private int originRowFieldCount;
    private final int businessFieldCount;
    private final int bucketFieldIndex;
    private final int offsetFieldIndex;
    private final int timestampFieldIndex;

    public FlussRecordAsPaimonRow(int bucket, RowType tableTowType) {
        super(tableTowType);
        this.bucket = bucket;
        this.businessFieldCount = tableRowType.getFieldCount() - SYSTEM_COLUMNS.size();
        this.bucketFieldIndex = businessFieldCount;
        this.offsetFieldIndex = businessFieldCount + 1;
        this.timestampFieldIndex = businessFieldCount + 2;
    }

    public void setFlussRecord(LogRecord logRecord) {
        this.logRecord = logRecord;
        this.internalRow = logRecord.getRow();
        int flussFieldCount = internalRow.getFieldCount();
        if (flussFieldCount > businessFieldCount) {
            // Fluss record is wider than Paimon schema, which means Lake schema is not yet
            // synchronized. With "Lake First" strategy, this should not happen in normal cases.
            throw new IllegalStateException(
                    String.format(
                            "Fluss record has %d fields but Paimon schema only has %d business fields. "
                                    + "This indicates the lake schema is not yet synchronized. "
                                    + "Please retry the schema change operation.",
                            flussFieldCount, businessFieldCount));
        }
        this.originRowFieldCount = flussFieldCount;
    }

    @Override
    public int getFieldCount() {
        // business (including partitions) + system (three system fields: bucket, offset,
        // timestamp)
        return tableRowType.getFieldCount();
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
        if (pos < businessFieldCount) {
            // Padding NULL for missing business fields when Paimon schema is wider than Fluss
            return true;
        }
        // is the last three system fields: bucket, offset, timestamp which are never null
        return false;
    }

    @Override
    public int getInt(int pos) {
        if (pos == bucketFieldIndex) {
            // bucket system column
            return bucket;
        }
        if (pos >= originRowFieldCount) {
            throw new IllegalStateException(
                    String.format(
                            "Field %s is NULL because Paimon schema is wider than Fluss record.",
                            pos));
        }
        return super.getInt(pos);
    }

    @Override
    public long getLong(int pos) {
        if (pos == offsetFieldIndex) {
            //  offset system column
            return logRecord.logOffset();
        } else if (pos == timestampFieldIndex) {
            //  timestamp system column
            return logRecord.timestamp();
        }
        if (pos >= originRowFieldCount) {
            throw new IllegalStateException(
                    String.format(
                            "Field %s is NULL because Paimon schema is wider than Fluss record.",
                            pos));
        }
        //  the origin RowData
        return super.getLong(pos);
    }

    @Override
    public Timestamp getTimestamp(int pos, int precision) {
        // it's timestamp system column
        if (pos == timestampFieldIndex) {
            return Timestamp.fromEpochMillis(logRecord.timestamp());
        }
        if (pos >= originRowFieldCount) {
            throw new IllegalStateException(
                    String.format(
                            "Field %s is NULL because Paimon schema is wider than Fluss record.",
                            pos));
        }
        return super.getTimestamp(pos, precision);
    }
}
