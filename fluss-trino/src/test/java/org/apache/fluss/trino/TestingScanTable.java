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

import org.apache.fluss.client.lookup.Lookup;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.scanner.Scan;
import org.apache.fluss.client.table.scanner.batch.BatchScanner;
import org.apache.fluss.client.table.scanner.log.LogScanner;
import org.apache.fluss.client.table.scanner.log.TypedLogScanner;
import org.apache.fluss.client.table.writer.Append;
import org.apache.fluss.client.table.writer.Upsert;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.predicate.Predicate;

import java.io.IOException;
import java.util.List;

/** Forwards real table operations, allowing scanner failures at the client boundary. */
class TestingScanTable implements Table {
    private final Table delegate;

    TestingScanTable(Table delegate) {
        this.delegate = delegate;
    }

    @Override
    public TableInfo getTableInfo() {
        return delegate.getTableInfo();
    }

    @Override
    public Scan newScan() {
        Scan scan = delegate.newScan();
        return new Scan() {
            @Override
            public Scan project(int[] columns) {
                return scan.project(columns);
            }

            @Override
            public Scan project(List<String> columns) {
                return scan.project(columns);
            }

            @Override
            public Scan limit(int rows) {
                return scan.limit(rows);
            }

            @Override
            public Scan filter(Predicate predicate) {
                return scan.filter(predicate);
            }

            @Override
            public LogScanner createLogScanner() {
                return TestingScanTable.this.createLogScanner(scan);
            }

            @Override
            public <T> TypedLogScanner<T> createTypedLogScanner(Class<T> type) {
                return scan.createTypedLogScanner(type);
            }

            @Override
            public BatchScanner createBatchScanner(TableBucket bucket) {
                return TestingScanTable.this.createBatchScanner(scan, bucket);
            }

            @Override
            public BatchScanner createBatchScanner(TableBucket bucket, long snapshotId) {
                return scan.createBatchScanner(bucket, snapshotId);
            }

            @Override
            public BatchScanner createBatchScanner() throws IOException {
                return scan.createBatchScanner();
            }
        };
    }

    BatchScanner createBatchScanner(Scan scan, TableBucket bucket) {
        return scan.createBatchScanner(bucket);
    }

    LogScanner createLogScanner(Scan scan) {
        return scan.createLogScanner();
    }

    @Override
    public Lookup newLookup() {
        return delegate.newLookup();
    }

    @Override
    public Append newAppend() {
        return delegate.newAppend();
    }

    @Override
    public Upsert newUpsert() {
        return delegate.newUpsert();
    }

    @Override
    public void close() throws Exception {
        delegate.close();
    }
}
