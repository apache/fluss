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
import org.apache.fluss.exception.FlussRuntimeException;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.server.SequenceIDCounter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.NotThreadSafe;

/** Segment ID generator, fetch ID with a batch size. */
@NotThreadSafe
public class SegmentSequenceGenerator implements SequenceGenerator {
    private static final Logger LOG = LoggerFactory.getLogger(SegmentSequenceGenerator.class);

    private final SequenceIDCounter sequenceIDCounter;
    private final TablePath tablePath;
    private final int columnIdx;
    private final String columnName;

    private AutoIncIdSegment segment = new AutoIncIdSegment(0, 0);

    private final long batchSize;

    public SegmentSequenceGenerator(
            TablePath tablePath,
            int columnIdx,
            String columnName,
            SequenceIDCounter sequenceIDCounter,
            Configuration properties) {
        batchSize = properties.getLong(ConfigOptions.TABLE_AUTO_INC_BATCH_SIZE);
        this.columnName = columnName;
        this.tablePath = tablePath;
        this.columnIdx = columnIdx;
        this.sequenceIDCounter = sequenceIDCounter;
    }

    private void fetchSegment() {
        try {
            long start = sequenceIDCounter.getAndAdd(batchSize);
            LOG.info(
                    "Successfully fetch auto-increment values range [{}, {}), table_path={}, column_idx={}, column_name={}.",
                    start,
                    start + batchSize,
                    tablePath,
                    columnIdx,
                    columnName);
            segment = new AutoIncIdSegment(start, batchSize);
        } catch (Exception e) {
            throw new FlussRuntimeException(
                    String.format(
                            "Failed to fetch auto-increment values, table_path=%s, column_idx=%d, column_name=%s.",
                            tablePath, columnIdx, columnName),
                    e);
        }
    }

    @Override
    public long nextVal() {
        if (segment.remaining() <= 0) {
            fetchSegment();
        }
        return segment.tryNextVal();
    }

    private static class AutoIncIdSegment {
        private long current;
        private final long end;

        public AutoIncIdSegment(long start, long length) {
            this.end = start + length;
            this.current = start;
        }

        public long remaining() {
            return end - current;
        }

        public long tryNextVal() {
            long id = current++;
            if (id >= end) {
                throw new RuntimeException("Segment is empty");
            }
            return id;
        }
    }
}
