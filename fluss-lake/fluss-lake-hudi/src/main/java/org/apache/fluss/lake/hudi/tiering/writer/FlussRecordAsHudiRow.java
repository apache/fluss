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

import org.apache.fluss.record.LogRecord;

import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;

import static org.apache.fluss.lake.hudi.utils.HudiConversions.toRowKind;
import static org.apache.fluss.utils.Preconditions.checkState;

/** Wraps a Fluss {@link LogRecord} as a Hudi/Flink row. */
public class FlussRecordAsHudiRow extends FlussRowAsHudiRow {

    // FIP-27: Hudi lake tables contain only user columns; no Fluss system columns are written.
    private final int fieldCount;

    private LogRecord logRecord;

    public FlussRecordAsHudiRow(RowType rowType) {
        super(rowType);
        this.fieldCount = rowType.getFieldCount();
    }

    public void setFlussRecord(LogRecord logRecord) {
        this.logRecord = logRecord;
        this.internalRow = logRecord.getRow();
        checkState(
                internalRow.getFieldCount() == fieldCount,
                "The Fluss record's field count (%s) must equal the Hudi table field count (%s).",
                internalRow.getFieldCount(),
                fieldCount);
    }

    @Override
    public RowKind getRowKind() {
        return toRowKind(logRecord.getChangeType());
    }
}
