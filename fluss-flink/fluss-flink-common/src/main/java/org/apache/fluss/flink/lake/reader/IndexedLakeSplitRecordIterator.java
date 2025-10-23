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

package org.apache.fluss.flink.lake.reader;

import org.apache.fluss.record.LogRecord;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.utils.CloseableIterator;

/**
 * An iterator wrapper that converts LogRecord objects to InternalRow objects while tracking the
 * current LakeSplit index being processed.
 *
 * <p>This class serves as an adapter between the underlying LogRecord iterator and the InternalRow
 * interface expected by consumers. It maintains reference to the specific LakeSplit index that is
 * currently being iterated.
 *
 * <p>Primary responsibilities:
 *
 * <ul>
 *   <li>Wraps a LogRecord iterator and exposes InternalRow objects
 *   <li>Preserves the index of the LakeSplit being processed
 *   <li>Provides clean resource management through Closeable interface
 *   <li>Maintains iterator semantics for sequential data access
 * </ul>
 */
public class IndexedLakeSplitRecordIterator implements CloseableIterator<InternalRow> {
    private final CloseableIterator<LogRecord> logRecordIterators;
    private final int currentLakeSplitIndex;

    public IndexedLakeSplitRecordIterator(
            CloseableIterator<LogRecord> logRecordIterators, int currentLakeSplitIndex) {
        this.logRecordIterators = logRecordIterators;
        this.currentLakeSplitIndex = currentLakeSplitIndex;
    }

    public int getCurrentLakeSplitIndex() {
        return currentLakeSplitIndex;
    }

    @Override
    public void close() {
        logRecordIterators.close();
    }

    @Override
    public boolean hasNext() {
        return logRecordIterators.hasNext();
    }

    @Override
    public InternalRow next() {
        return logRecordIterators.next().getRow();
    }
}
