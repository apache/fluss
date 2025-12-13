/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.server.kv.autoinc;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.server.SequenceIDCounter;
import org.apache.fluss.utils.concurrent.Scheduler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.concurrent.LockUtils.inLock;

/**
 * SegmentIDGenerator is used to generate auto increment column ID. It uses two segments to store
 * the IDs and prefetch ids in background.
 */
public class SegmentIDGenerator implements IncIDGenerator {
    private static final Logger LOG = LoggerFactory.getLogger(SegmentIDGenerator.class);
    private static final int RETRY_INTERVAL_MS = 100;
    private static final int MAX_RETRY_TIMES = 200;

    private final SequenceIDCounter sequenceIDCounter;
    private final TablePath tablePath;
    private final int schemaId;
    private final int columnIdx;
    private final String columnName;

    AutoIncIdSegment[] segments;
    private int currentSegmentIdx = 0;

    private volatile boolean isInit = false;

    private final AtomicBoolean isFetching = new AtomicBoolean(false);
    private final long batchSize;
    private final long lowWaterMark;
    private final Scheduler fetchIdScheduler;

    private final ReentrantLock lock = new ReentrantLock();

    public SegmentIDGenerator(
            TablePath tablePath,
            int schemaId,
            int columnIdx,
            String columnName,
            SequenceIDCounter sequenceIDCounter,
            Scheduler scheduler,
            Configuration properties) {
        double lowWaterMarkRatio =
                properties.getDouble(ConfigOptions.TABLE_AUTO_INC_PREFETCH_LOW_WATER_MARK_RATIO);
        batchSize = properties.getLong(ConfigOptions.TABLE_AUTO_INC_BATCH_SIZE);
        lowWaterMark = (long) (lowWaterMarkRatio * batchSize);
        checkArgument(lowWaterMark > 0, "auto inc low level water mark must be greater than 0");

        this.columnName = columnName;
        this.tablePath = tablePath;
        this.schemaId = schemaId;
        this.columnIdx = columnIdx;
        this.fetchIdScheduler = scheduler;
        this.segments = new AutoIncIdSegment[] {new AutoIncIdSegment(), new AutoIncIdSegment()};
        this.sequenceIDCounter = sequenceIDCounter;
    }

    private void prefetchSegmentIds(int segmentIdx) {
        try {
            long start = sequenceIDCounter.getAndAdd(batchSize);
            LOG.info(
                    "Successfully fetch auto-increment values range [{}, {}), table_path={}, schema_id={}, column_idx={}, column_name={}.",
                    start,
                    start + batchSize,
                    tablePath,
                    schemaId,
                    columnIdx,
                    columnName);
            resetSegment(segmentIdx, start, batchSize);
        } catch (Exception e) {
            throw new RuntimeException(
                    String.format(
                            "Failed to fetch auto-increment values, table_path=%s, schema_id=%d, column_idx=%d, column_name=%s.",
                            tablePath, schemaId, columnIdx, columnName),
                    e);
        } finally {
            isFetching.set(false);
        }
    }

    @Override
    public long nextVal() {
        if (!isInit) {
            prefetchSegmentIds(nextSegmentIdx());
            switchToNextSegment();
            isInit = true;
        }

        int retries = 0;
        while (retries < MAX_RETRY_TIMES) {
            long id = getNextVal();
            if (id != -1L) {
                return id;
            }
            try {
                Thread.sleep(RETRY_INTERVAL_MS);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            retries += 1;
        }
        throw new RuntimeException(
                String.format(
                        "Failed to get auto increment id after %s retries, check the id counter or increase the batch size, table_path=%s, schema_id=%d, column_idx=%d, column_name=%s.",
                        MAX_RETRY_TIMES, tablePath, schemaId, columnIdx, columnName));
    }

    private int nextSegmentIdx() {
        return (currentSegmentIdx + 1) % 2;
    }

    private void switchToNextSegment() {
        currentSegmentIdx = nextSegmentIdx();
    }

    private void resetSegment(int segmentIdx, long start, long length) {
        inLock(lock, () -> segments[segmentIdx].reset(start, length));
    }

    private long getNextVal() {
        return inLock(
                lock,
                () -> {
                    if (!segments[currentSegmentIdx].empty()) {
                        if (segments[currentSegmentIdx].length <= lowWaterMark
                                && segments[nextSegmentIdx()].empty()
                                && isFetching.compareAndSet(false, true)) {
                            fetchIdScheduler.scheduleOnce(
                                    "prefetch-ids", () -> prefetchSegmentIds(nextSegmentIdx()));
                        }
                        return segments[currentSegmentIdx].nextVal();
                    } else if (!isFetching.get() && !segments[nextSegmentIdx()].empty()) {
                        switchToNextSegment();
                        return segments[currentSegmentIdx].nextVal();
                    } else {
                        return -1L;
                    }
                });
    }

    private static class AutoIncIdSegment {
        private long start;
        private long length;

        public AutoIncIdSegment() {
            this.start = 0;
            this.length = 0;
        }

        public void reset(long start, long length) {
            this.start = start;
            this.length = length;
        }

        public long nextVal() {
            if (length <= 0) {
                throw new IllegalStateException("Segment is empty");
            }
            length--;
            return start++;
        }

        public boolean empty() {
            return length == 0;
        }
    }
}
