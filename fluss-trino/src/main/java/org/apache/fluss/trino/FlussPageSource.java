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

package org.apache.fluss.trino;

import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.scanner.ScanRecord;
import org.apache.fluss.client.table.scanner.log.LogScanner;
import org.apache.fluss.client.table.scanner.log.ScanRecords;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.shaded.guava32.com.google.common.collect.ImmutableList;
import org.apache.fluss.utils.IOUtils;

import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.MemoryContext;
import io.trino.spi.connector.SourcePage;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.OptionalLong;

import static io.trino.spi.StandardErrorCode.EXCEEDED_LOCAL_MEMORY_LIMIT;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static org.apache.fluss.trino.FlussTableScanValidator.validateIdentity;
import static org.apache.fluss.trino.FlussTableScanValidator.validateSupportedTable;
import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

/**
 * Synchronous, exclusive-end reader for one Fluss bucket range.
 *
 * <p>Each page request performs at most one poll; its timeout bounds the data wait, not
 * initialization or metadata RPCs.
 */
final class FlussPageSource implements ConnectorPageSource {

    private static final Duration POLL_TIMEOUT = Duration.ofMillis(100);

    private static final int MAX_PAGE_ROWS = 1024;
    private static final int MAX_PAGE_BYTES = 1024 * 1024;
    private static final long MAX_ESTIMATED_RETAINED_BYTES = 64L * 1024 * 1024;

    private final FlussClientManager clients;
    private final FlussTableHandle handle;
    private final FlussSplit split;
    private final List<FlussColumnHandle> columns;
    private final MemoryContext memory;
    private final TableBucket bucket;

    private Table table;
    private LogScanner scanner;
    private FlussRowDecoder decoder;

    private List<ScanRecord> records = Collections.emptyList();
    private int recordIndex;
    private long batchBytes;

    private boolean stopReached;
    private boolean closed;

    private long completedBytes;
    private long completedPositions;
    private long readNanos;

    FlussPageSource(
            FlussClientManager clients,
            FlussTableHandle handle,
            FlussSplit split,
            List<FlussColumnHandle> columns,
            MemoryContext memory) {
        this.clients = checkNotNull(clients, "clients is null");
        this.handle = checkNotNull(handle, "handle is null");
        this.split = checkNotNull(split, "split is null");
        this.columns = ImmutableList.copyOf(checkNotNull(columns, "columns is null"));
        this.memory = checkNotNull(memory, "memory is null");

        checkArgument(
                split.getBucketId() < handle.getBucketCount(),
                "bucketId exceeds table bucket count");

        this.bucket = new TableBucket(handle.getTableId(), split.getBucketId());

        if (split.getStartOffset() == split.getStoppingOffset()) {
            closed = true;
        }
    }

    @Override
    public SourcePage getNextSourcePage() {
        if (closed) {
            return null;
        }

        try {
            initialize();

            if (records.isEmpty() && !stopReached) {
                pollNextBatch();
            }

            if (records.isEmpty()) {
                if (stopReached) {
                    close();
                }
                return null;
            }

            PageBuilder builder = PageBuilder.withMaxPageSize(MAX_PAGE_BYTES, decoder.getTypes());

            while (recordIndex < records.size()
                    && !builder.isFull()
                    && builder.getPositionCount() < MAX_PAGE_ROWS) {
                appendRecord(records.get(recordIndex++), builder);
            }

            if (recordIndex == records.size()) {
                clearBatch();
            }
            updateMemory(builder);

            if (builder.isEmpty()) {
                if (stopReached && records.isEmpty()) {
                    close();
                }
                return null;
            }

            Page page = builder.build();
            completedPositions += page.getPositionCount();

            // The returned page is owned by Trino. The page source only retains
            // the unconsumed Fluss batch from this point.
            memory.setBytes(batchBytes);

            if (stopReached && records.isEmpty()) {
                close();
            }

            return SourcePage.create(page);
        } catch (RuntimeException | Error e) {
            closeWithSuppression(e);
            throw e;
        }
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }

        closed = true;

        LogScanner scannerToClose = scanner;
        Table tableToClose = table;

        scanner = null;
        table = null;
        decoder = null;
        records = Collections.emptyList();
        recordIndex = 0;
        batchBytes = 0;

        Throwable failure = null;
        try {
            IOUtils.closeAll(scannerToClose, tableToClose);
        } catch (Exception | Error e) {
            failure = e;
        }
        try {
            memory.setBytes(0);
        } catch (RuntimeException | Error e) {
            if (failure == null) {
                failure = e;
            } else if (failure != e) {
                failure.addSuppressed(e);
            }
        }
        if (failure instanceof Error) {
            throw (Error) failure;
        }
        if (failure != null) {
            throw new TrinoException(
                    GENERIC_INTERNAL_ERROR,
                    "Failed closing Fluss reader for bucket " + split.getBucketId(),
                    failure);
        }
    }

    @Override
    public boolean isFinished() {
        return closed;
    }

    @Override
    public long getCompletedBytes() {
        return completedBytes;
    }

    @Override
    public OptionalLong getCompletedPositions() {
        return OptionalLong.of(completedPositions);
    }

    @Override
    public long getReadTimeNanos() {
        return readNanos;
    }

    private void initialize() {
        if (scanner != null) {
            return;
        }

        table =
                clients.openTable(
                        TablePath.of(handle.getFlussDatabaseName(), handle.getFlussTableName()));

        TableInfo tableInfo = table.getTableInfo();

        validateIdentity(handle, tableInfo);
        validateSupportedTable(tableInfo);

        decoder = new FlussRowDecoder(tableInfo.getSchema(), columns);

        scanner = table.newScan().createLogScanner();
        scanner.subscribe(split.getBucketId(), split.getStartOffset());
    }

    private void pollNextBatch() {
        long started = System.nanoTime();

        try {
            ScanRecords batch = scanner.poll(POLL_TIMEOUT);
            List<ScanRecord> batchRecords = batch.records(bucket);

            long retainedBytes = 0;

            for (ScanRecord record : batchRecords) {
                long recordBytes = Math.max(0, record.getSizeInBytes());

                // Encoded size plus object overhead is only an estimate. Client prefetch and
                // decoded buffers are not exposed by LogScanner and are not included here.
                retainedBytes += 128L + recordBytes;

                if (retainedBytes > MAX_ESTIMATED_RETAINED_BYTES) {
                    throw new TrinoException(
                            EXCEEDED_LOCAL_MEMORY_LIMIT,
                            "Fluss scan batch exceeds estimated memory budget");
                }
            }

            records = batchRecords;
            recordIndex = 0;
            batchBytes = retainedBytes;

            Long progress = batch.consumedUpToOffset(bucket);
            if (progress != null && progress >= split.getStoppingOffset()) {
                stopReached = true;
            }

            memory.setBytes(batchBytes);
        } finally {
            readNanos += System.nanoTime() - started;
        }
    }

    private void appendRecord(ScanRecord record, PageBuilder builder) {
        long offset = record.logOffset();

        if (offset < split.getStartOffset()) {
            throw new TrinoException(
                    GENERIC_INTERNAL_ERROR,
                    "Fluss offset invariant failed for bucket "
                            + split.getBucketId()
                            + ": "
                            + offset
                            + " outside "
                            + split);
        }

        if (offset >= split.getStoppingOffset()) {
            stopReached = true;
            return;
        }

        if (record.getTableId() != handle.getTableId()) {
            throw new TrinoException(
                    GENERIC_INTERNAL_ERROR,
                    "Fluss record table identity changed for bucket " + split.getBucketId());
        }

        decoder.append(record.getRow(), builder);
        completedBytes += Math.max(0, record.getSizeInBytes());

        if (offset == split.getStoppingOffset() - 1) {
            stopReached = true;
        }
    }

    private void clearBatch() {
        records = Collections.emptyList();
        recordIndex = 0;
        batchBytes = 0;
    }

    private void updateMemory(PageBuilder builder) {
        long retainedBytes = batchBytes + builder.getRetainedSizeInBytes();

        if (retainedBytes > MAX_ESTIMATED_RETAINED_BYTES) {
            throw new TrinoException(
                    EXCEEDED_LOCAL_MEMORY_LIMIT,
                    "Fluss page source exceeds estimated memory budget");
        }

        memory.setBytes(retainedBytes);
    }

    private void closeWithSuppression(Throwable failure) {
        try {
            close();
        } catch (RuntimeException | Error closeFailure) {
            if (closeFailure != failure) {
                failure.addSuppressed(closeFailure);
            }
        }
    }
}
