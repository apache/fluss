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

package com.alibaba.fluss.connector.flink.source.reader;

import com.alibaba.fluss.client.table.scanner.ScanRecord;
import com.alibaba.fluss.connector.flink.source.emitter.FlinkRecordEmitter;

import java.util.Objects;

/**
 * A record wrapping a Fluss {@link ScanRecord} and the {@code readRecordsCount} when the record is
 * from reading snapshot. When the record is from reading log, {@code readRecordsCount} will always
 * be {@link #NO_READ_RECORDS_COUNT}.
 *
 * <p>The {@code readRecordsCount} defines the point in the snapshot reader AFTER the record. Record
 * processing and updating checkpointed state happens atomically. The position points to where the
 * reader should resume after this record is processed.
 *
 * <p>For example, the very first record in a snapshot split could have a {@code readRecordsCount}
 * of one.
 *
 * <p>It's produced by {@link FlinkSourceSplitReader} and emitted to {@link FlinkRecordEmitter}
 */
public class RecordAndPos {

    public static final long NO_READ_RECORDS_COUNT = -1;

    protected ScanRecord scanRecord;

    // the read records count include this record when read this record
    protected long readRecordsCount;

    public RecordAndPos(ScanRecord scanRecord) {
        this(scanRecord, NO_READ_RECORDS_COUNT);
    }

    public RecordAndPos(ScanRecord scanRecord, long readRecordsCount) {
        this.scanRecord = scanRecord;
        this.readRecordsCount = readRecordsCount;
    }

    public long readRecordsCount() {
        return readRecordsCount;
    }

    public ScanRecord record() {
        return scanRecord;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RecordAndPos that = (RecordAndPos) o;
        return readRecordsCount == that.readRecordsCount
                && Objects.equals(scanRecord, that.scanRecord);
    }

    @Override
    public int hashCode() {
        return Objects.hash(scanRecord, readRecordsCount);
    }

    @Override
    public String toString() {
        return "RecordAndPos{"
                + "scanRecord="
                + scanRecord
                + ", readRecordsCount="
                + readRecordsCount
                + '}';
    }
}
