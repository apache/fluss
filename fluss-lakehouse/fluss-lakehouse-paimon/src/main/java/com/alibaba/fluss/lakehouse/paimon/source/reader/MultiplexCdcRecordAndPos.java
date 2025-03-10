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

package com.alibaba.fluss.lakehouse.paimon.source.reader;

import com.alibaba.fluss.lakehouse.paimon.record.CdcRecord;
import com.alibaba.fluss.lakehouse.paimon.record.MultiplexCdcRecord;
import com.alibaba.fluss.lakehouse.paimon.source.emitter.FlinkRecordEmitter;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.metadata.TablePath;

import java.util.Objects;

/**
 * A record wrapping a Fluss {@link MultiplexCdcRecord} and the {@code readRecordsCount} when the
 * record is from reading snapshot. When the record is from reading log, {@code readRecordsCount}
 * will always be 0.
 *
 * <p>The {@code readRecordsCount} defines the point in the snapshot reader AFTER the record. Record
 * processing and updating checkpointed state happens atomically. The position points to where the
 * reader should resume after this record is processed.
 *
 * <p>For example, the very first record in a snapshot split could have a {@code readRecordsCount}
 * of one.
 *
 * <p>It's produced by {@link FlinkSourceSplitReader} and emitted to {@link FlinkRecordEmitter}.
 */
public class MultiplexCdcRecordAndPos {

    private static final long NO_READ_RECORDS_COUNT = -1;

    private final MultiplexCdcRecord multiplexCdcRecord;

    // the read records count include this record when read this record
    private final long readRecordsCount;

    public MultiplexCdcRecordAndPos(
            TablePath tablePath, TableBucket tableBucket, CdcRecord cdcRecord) {
        this(new MultiplexCdcRecord(tablePath, tableBucket, cdcRecord), NO_READ_RECORDS_COUNT);
    }

    public MultiplexCdcRecordAndPos(
            TablePath tablePath,
            TableBucket tableBucket,
            CdcRecord cdcRecord,
            long readRecordsCount) {
        this(new MultiplexCdcRecord(tablePath, tableBucket, cdcRecord), readRecordsCount);
    }

    private MultiplexCdcRecordAndPos(MultiplexCdcRecord multiplexCdcRecord, long readRecordsCount) {
        this.multiplexCdcRecord = multiplexCdcRecord;
        this.readRecordsCount = readRecordsCount;
    }

    public MultiplexCdcRecord multiplexCdcRecord() {
        return multiplexCdcRecord;
    }

    public long readRecordsCount() {
        return readRecordsCount;
    }

    public MultiplexCdcRecord record() {
        return multiplexCdcRecord;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof MultiplexCdcRecordAndPos)) {
            return false;
        }
        MultiplexCdcRecordAndPos that = (MultiplexCdcRecordAndPos) o;
        return readRecordsCount == that.readRecordsCount
                && Objects.equals(multiplexCdcRecord, that.multiplexCdcRecord);
    }

    @Override
    public int hashCode() {
        return Objects.hash(multiplexCdcRecord, readRecordsCount);
    }

    @Override
    public String toString() {
        return "MultiplexCdcRecordAndPos{"
                + "multiplexCdcRecord="
                + multiplexCdcRecord
                + ", readRecordsCount="
                + readRecordsCount
                + '}';
    }
}
