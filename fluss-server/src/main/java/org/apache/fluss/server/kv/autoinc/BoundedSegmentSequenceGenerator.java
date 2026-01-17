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

import org.apache.fluss.exception.FlussRuntimeException;
import org.apache.fluss.exception.SequenceOverflowException;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.server.SequenceIDCounter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.NotThreadSafe;

/** Segment ID generator, fetch ID with a batch size. */
@NotThreadSafe
public class BoundedSegmentSequenceGenerator implements SequenceGenerator {
    private static final Logger LOG =
            LoggerFactory.getLogger(BoundedSegmentSequenceGenerator.class);

    private final SequenceIDCounter sequenceIDCounter;
    private final TablePath tablePath;
    private final String columnName;
    private final long cacheSize;
    private final long maxAllowedValue;

    private IdSegment segment;

    public BoundedSegmentSequenceGenerator(
            TablePath tablePath,
            String columnName,
            SequenceIDCounter sequenceIDCounter,
            long idCacheSize,
            long maxAllowedValue) {
        this.cacheSize = idCacheSize;
        this.columnName = columnName;
        this.tablePath = tablePath;
        this.sequenceIDCounter = sequenceIDCounter;
        this.segment = IdSegment.EMPTY;
        this.maxAllowedValue = maxAllowedValue;
    }

    private void fetchSegment() {
        try {
            long start = sequenceIDCounter.getAndAdd(cacheSize);
            // the initial value of ZNode is 0, but we start ID from 1
            segment = new IdSegment(start + 1, start + cacheSize);
            LOG.info(
                    "Successfully fetch auto-increment values range [{}, {}], table_path={}, column_name={}.",
                    segment.current,
                    segment.end,
                    tablePath,
                    columnName);
        } catch (SequenceOverflowException sequenceOverflowException) {
            throw sequenceOverflowException;
        } catch (Exception e) {
            throw new FlussRuntimeException(
                    String.format(
                            "Failed to fetch auto-increment values, table_path=%s, column_name=%s.",
                            tablePath, columnName),
                    e);
        }
    }

    @Override
    public long nextVal() {
        if (!segment.hasNext()) {
            fetchSegment();
        }
        long id = segment.nextVal();
        if (id > maxAllowedValue) {
            throw new SequenceOverflowException(
                    String.format(
                            "Reached maximum value of sequence \"<%s>\" (%d).",
                            columnName, maxAllowedValue));
        }
        return id;
    }

    private static class IdSegment {
        private static final IdSegment EMPTY = new IdSegment(0, -1);
        final long end;
        long current;

        /** ID range from min (inclusive) to max (inclusive). */
        public IdSegment(long min, long max) {
            this.current = min;
            this.end = max;
        }

        public boolean hasNext() {
            return current <= end;
        }

        public long nextVal() {
            return current++;
        }
    }
}
