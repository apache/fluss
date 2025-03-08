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

package com.alibaba.fluss.lakehouse.paimon.record;

import org.apache.flink.table.data.RowData;

import java.util.Objects;

/** A data change mess from the Fluss source. */
public class CdcRecord {

    private long offset;
    private long timestamp;
    private RowData rowData;

    public CdcRecord() {}

    public CdcRecord(long offset, long timestamp, RowData rowData) {
        this.offset = offset;
        this.timestamp = timestamp;
        this.rowData = rowData;
    }

    public long getOffset() {
        return offset;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public RowData getRowData() {
        return rowData;
    }

    public void setRowData(RowData rowData) {
        this.rowData = rowData;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof CdcRecord)) {
            return false;
        }
        CdcRecord cdcRecord = (CdcRecord) o;
        return offset == cdcRecord.offset
                && timestamp == cdcRecord.timestamp
                && Objects.equals(rowData, cdcRecord.rowData);
    }

    @Override
    public int hashCode() {
        return Objects.hash(offset, timestamp, rowData);
    }

    @Override
    public String toString() {
        return "CdcRecord{"
                + "offset="
                + offset
                + ", timestamp="
                + timestamp
                + ", rowData="
                + rowData
                + '}';
    }
}
