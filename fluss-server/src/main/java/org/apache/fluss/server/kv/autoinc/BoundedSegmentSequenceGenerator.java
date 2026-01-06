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

import org.apache.fluss.config.TableConfig;
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

    private AutoIncIdSegment segment;

    public BoundedSegmentSequenceGenerator(
            TablePath tablePath,
            String columnName,
            SequenceIDCounter sequenceIDCounter,
            TableConfig tableConf,
            long maxAllowedValue) {
        this.cacheSize = tableConf.getAutoIncrementCacheSize();
        this.columnName = columnName;
        this.tablePath = tablePath;
        this.sequenceIDCounter = sequenceIDCounter;
        this.segment = AutoIncIdSegment.EMPTY;
        this.maxAllowedValue = maxAllowedValue;
    }

    private void fetchSegment() {
        try {
            long start = sequenceIDCounter.getAndAdd(cacheSize);
            if (start >= maxAllowedValue) {
                throw new SequenceOverflowException(
                        String.format(
                                "Reached maximum value of sequence \"<%s>\" (%d).",
                                columnName, maxAllowedValue));
            }

            long actualEnd = Math.min(start + cacheSize, maxAllowedValue - 1);
            LOG.info(
                    "Successfully fetch auto-increment values range ({}, {}], table_path={}, column_name={}.",
                    start,
                    actualEnd,
                    tablePath,
                    columnName);
            segment = new AutoIncIdSegment(start, actualEnd - start);
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
        if (segment.remaining() <= 0) {
            fetchSegment();
        }
        return segment.tryNextVal();
    }

    private static class AutoIncIdSegment {
        private static final AutoIncIdSegment EMPTY = new AutoIncIdSegment(0, 0);
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
            long id = ++current;
            if (id > end) {
                throw new IllegalStateException("No more IDs available in current segment.");
            }
            return id;
        }
    }
}
