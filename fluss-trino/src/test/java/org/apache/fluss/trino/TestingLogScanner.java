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

import org.apache.fluss.client.table.scanner.log.LogScanner;
import org.apache.fluss.client.table.scanner.log.ScanRecords;

import java.time.Duration;
import java.util.Arrays;
import java.util.Iterator;

/** Supplies sparse progress and failures that a healthy log cannot produce on demand. */
class TestingLogScanner implements LogScanner {
    private final Iterator<ScanRecords> batches;
    int closes;

    TestingLogScanner(ScanRecords... batches) {
        this.batches = Arrays.asList(batches).iterator();
    }

    @Override
    public ScanRecords poll(Duration timeout) {
        return batches.hasNext() ? batches.next() : ScanRecords.EMPTY;
    }

    @Override
    public void subscribe(int bucket, long offset) {}

    @Override
    public void subscribe(long partition, int bucket, long offset) {}

    @Override
    public void unsubscribe(int bucket) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void unsubscribe(long partition, int bucket) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void wakeup() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() throws Exception {
        closes++;
    }
}
