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

package org.apache.fluss.trino;

import org.apache.fluss.client.table.scanner.batch.BatchScanner;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.utils.CloseableIterator;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Iterator;

/** Supplies exact batch boundaries for yield, buffering and cleanup contracts. */
class TestingBatchScanner implements BatchScanner {
    private final Iterator<CloseableIterator<InternalRow>> batches;
    int polls;
    int closes;

    @SafeVarargs
    TestingBatchScanner(CloseableIterator<InternalRow>... batches) {
        this.batches = Arrays.asList(batches).iterator();
    }

    @Override
    public CloseableIterator<InternalRow> pollBatch(Duration timeout) throws IOException {
        polls++;
        return batches.hasNext() ? batches.next() : null;
    }

    @Override
    public void close() throws IOException {
        closes++;
    }
}
