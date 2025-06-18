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

package com.alibaba.fluss.testutils;

import com.alibaba.fluss.record.LogRecordBatch;
import com.alibaba.fluss.record.LogRecords;
import com.alibaba.fluss.utils.AbstractIterator;

import java.util.Iterator;
import java.util.List;

/** Wraps a List of {@link LogRecords} as a {@link LogRecords}. */
public class ListLogRecords implements LogRecords {

    private final List<? extends LogRecords> logs;

    public ListLogRecords(List<? extends LogRecords> logs) {
        this.logs = logs;
    }

    @Override
    public int sizeInBytes() {
        int size = 0;
        for (LogRecords records : logs) {
            size += records.sizeInBytes();
        }
        return size;
    }

    @Override
    public Iterable<LogRecordBatch> batches() {
        return this::batchIterator;
    }

    private Iterator<LogRecordBatch> batchIterator() {
        return new AbstractIterator<LogRecordBatch>() {
            final Iterator<? extends LogRecords> iterator = logs.iterator();
            Iterator<LogRecordBatch> currentBatches;

            @Override
            protected LogRecordBatch makeNext() {
                if (currentBatches != null && currentBatches.hasNext()) {
                    return currentBatches.next();
                } else {
                    while (iterator.hasNext()) {
                        currentBatches = iterator.next().batches().iterator();
                        if (currentBatches.hasNext()) {
                            return currentBatches.next();
                        }
                    }
                }
                return allDone();
            }
        };
    }
}
