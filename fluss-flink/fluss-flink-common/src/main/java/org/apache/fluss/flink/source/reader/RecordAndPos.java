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

package org.apache.fluss.flink.source.reader;

import org.apache.fluss.client.table.scanner.ScanRecord;
import org.apache.fluss.flink.source.emitter.FlinkRecordEmitter;

import java.util.Objects;

/**
 * A record wrapping a Fluss {@link ScanRecord}, the {@code readRecordsCount} when the record is
 * from reading snapshot, the code {@code currentSplitIndex} where the record is from when the Flink
 * source split read by split reader contains multiple splits(splittable unit), like {@link
 * org.apache.fluss.flink.lake.split.LakeSnapshotAndFlussLogSplit} contains multiple {@link
 * org.apache.fluss.lake.source.LakeSplit}s.
 *
 * <p>When the record is from reading log, {@code readRecordsCount} will always be {@link
 * #NO_READ_RECORDS_COUNT}.
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
    protected static final int DEFAULT_SPLIT_INDEX = 0;

    protected ScanRecord scanRecord;

    // the read records count include this record when read this record
    protected long readRecordsCount;

    // the index for the current split that the record is from
    protected int currentSplitIndex;

    public RecordAndPos(ScanRecord scanRecord) {
        this(scanRecord, NO_READ_RECORDS_COUNT, DEFAULT_SPLIT_INDEX);
    }

    public RecordAndPos(ScanRecord scanRecord, long readRecordsCount) {
        this(scanRecord, readRecordsCount, DEFAULT_SPLIT_INDEX);
    }

    public RecordAndPos(ScanRecord scanRecord, long readRecordsCount, int currentSplitIndex) {
        this.scanRecord = scanRecord;
        this.readRecordsCount = readRecordsCount;
        this.currentSplitIndex = currentSplitIndex;
    }

    public long readRecordsCount() {
        return readRecordsCount;
    }

    public int getCurrentSplitIndex() {
        return currentSplitIndex;
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
                && currentSplitIndex == that.currentSplitIndex
                && Objects.equals(scanRecord, that.scanRecord);
    }

    @Override
    public int hashCode() {
        return Objects.hash(scanRecord, readRecordsCount, currentSplitIndex);
    }

    @Override
    public String toString() {
        return "RecordAndPos{"
                + "scanRecord="
                + scanRecord
                + ", readRecordsCount="
                + readRecordsCount
                + ", currentSplitIndex="
                + currentSplitIndex
                + '}';
    }
}
