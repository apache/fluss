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

package org.apache.fluss.flink.utils;

import org.apache.fluss.client.table.writer.DeleteResult;
import org.apache.fluss.client.table.writer.UpsertResult;
import org.apache.fluss.client.table.writer.UpsertWriter;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.row.InternalRow;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Test implementation of {@link UpsertWriter} for testing.
 *
 * <p>Tracks all upsert/delete operations and supports failure injection.
 */
public class TestUpsertWriter implements UpsertWriter {
    private int upsertCount = 0;
    private int deleteCount = 0;
    private boolean flushCalled = false;
    private boolean shouldFail = false;
    private InternalRow lastUpsertedRow;
    private InternalRow lastDeletedRow;
    private final List<InternalRow> allUpsertedRows = new ArrayList<>();
    private final List<InternalRow> allDeletedRows = new ArrayList<>();
    private int retractCount = 0;
    private InternalRow lastRetractedRow;
    private final List<InternalRow> allRetractedRows = new ArrayList<>();

    @Override
    public CompletableFuture<UpsertResult> upsert(InternalRow record) {
        if (shouldFail) {
            CompletableFuture<UpsertResult> future = new CompletableFuture<>();
            future.completeExceptionally(new RuntimeException("Simulated write failure"));
            return future;
        }
        upsertCount++;
        lastUpsertedRow = record;
        allUpsertedRows.add(record);
        return CompletableFuture.completedFuture(new UpsertResult(new TableBucket(1L, 0), 0L));
    }

    @Override
    public CompletableFuture<UpsertResult> retract(InternalRow row) {
        if (shouldFail) {
            CompletableFuture<UpsertResult> future = new CompletableFuture<>();
            future.completeExceptionally(new RuntimeException("Simulated write failure"));
            return future;
        }
        retractCount++;
        lastRetractedRow = row;
        allRetractedRows.add(row);
        return CompletableFuture.completedFuture(new UpsertResult(new TableBucket(1L, 0), 0L));
    }

    @Override
    public CompletableFuture<DeleteResult> delete(InternalRow record) {
        if (shouldFail) {
            CompletableFuture<DeleteResult> future = new CompletableFuture<>();
            future.completeExceptionally(new RuntimeException("Simulated write failure"));
            return future;
        }
        deleteCount++;
        lastDeletedRow = record;
        allDeletedRows.add(record);
        return CompletableFuture.completedFuture(new DeleteResult(new TableBucket(1L, 0), 0L));
    }

    @Override
    public void flush() {
        flushCalled = true;
    }

    public int getUpsertCount() {
        return upsertCount;
    }

    public int getDeleteCount() {
        return deleteCount;
    }

    public boolean isFlushCalled() {
        return flushCalled;
    }

    public InternalRow getLastUpsertedRow() {
        return lastUpsertedRow;
    }

    public InternalRow getLastDeletedRow() {
        return lastDeletedRow;
    }

    public List<InternalRow> getAllUpsertedRows() {
        return allUpsertedRows;
    }

    public List<InternalRow> getAllDeletedRows() {
        return allDeletedRows;
    }

    public int getRetractCount() {
        return retractCount;
    }

    public InternalRow getLastRetractedRow() {
        return lastRetractedRow;
    }

    public List<InternalRow> getAllRetractedRows() {
        return allRetractedRows;
    }

    public void setShouldFail(boolean shouldFail) {
        this.shouldFail = shouldFail;
    }

    public void reset() {
        upsertCount = 0;
        deleteCount = 0;
        retractCount = 0;
        flushCalled = false;
        shouldFail = false;
        lastUpsertedRow = null;
        lastDeletedRow = null;
        lastRetractedRow = null;
        allUpsertedRows.clear();
        allDeletedRows.clear();
        allRetractedRows.clear();
    }
}
