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
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.shaded.guava32.com.google.common.collect.ImmutableList;

import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.TrinoException;
import io.trino.spi.block.DuplicateMapKeyException;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.MemoryContext;
import io.trino.spi.connector.SourcePage;

import java.time.Duration;
import java.util.List;
import java.util.OptionalLong;

import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static org.apache.fluss.trino.FlussErrorCode.FLUSS_READ_ERROR;
import static org.apache.fluss.trino.FlussTableScanValidator.validateSplit;
import static org.apache.fluss.trino.FlussTableScanValidator.validateTable;
import static org.apache.fluss.utils.ExceptionUtils.firstOrSuppressed;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

/**
 * Synchronous bounded page source for one physical Fluss split.
 *
 * <p>The page source owns table and reader lifecycle, converts Fluss rows into Trino pages, and
 * reports reader and page-builder memory. Storage-specific LOG/KV semantics are encapsulated by
 * {@link FlussSplitReader}.
 *
 * <p>Each page request polls the underlying Fluss scanner at most once. A poll may yield without
 * completing the split.
 */
final class FlussPageSource implements ConnectorPageSource {

    private static final Duration POLL_TIMEOUT = Duration.ofMillis(100);

    private static final int MAX_PAGE_ROWS = 1024;
    private static final int MAX_PAGE_BYTES = 1024 * 1024;

    private final FlussClientManager clients;
    private final FlussTableHandle handle;
    private final FlussSplit split;
    private final List<FlussColumnHandle> columns;
    private final MemoryContext memory;

    private Table table;
    private FlussRowDecoder decoder;
    private FlussSplitReader reader;

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
    }

    @Override
    public SourcePage getNextSourcePage() {
        if (closed) {
            return null;
        }
        try {
            initialize();
            if (!reader.hasNext()) {
                if (reader.isFinished()) {
                    close();
                    return null;
                }
                FlussSplitReader.PollResult pollResult = pollReader();
                // Polling may acquire or release reader-owned input buffers.
                memory.setBytes(reader.getRetainedSizeInBytes());
                switch (pollResult) {
                    case AVAILABLE:
                        if (!reader.hasNext()) {
                            throw new TrinoException(
                                    GENERIC_INTERNAL_ERROR,
                                    "Fluss reader reported available data without a buffered row");
                        }
                        break;
                    case YIELD:
                        if (reader.hasNext() || reader.isFinished()) {
                            throw new TrinoException(
                                    GENERIC_INTERNAL_ERROR,
                                    "Fluss reader returned an invalid yield state");
                        }
                        return null;
                    case FINISHED:
                        if (!reader.isFinished()) {
                            throw new TrinoException(
                                    GENERIC_INTERNAL_ERROR,
                                    "Fluss reader reported completion without being finished");
                        }
                        close();
                        return null;
                    default:
                        throw new TrinoException(
                                GENERIC_INTERNAL_ERROR,
                                "Unknown Fluss reader poll result: " + pollResult);
                }
            }

            PageBuilder builder = PageBuilder.withMaxPageSize(MAX_PAGE_BYTES, decoder.getTypes());
            reportMemory(builder);
            /*
             * Consume only the currently buffered Fluss scanner batch. If the batch is exhausted
             * before the page is full, the next scanner poll happens on the next Trino page-source
             * request.
             */
            while (!builder.isFull()
                    && builder.getPositionCount() < MAX_PAGE_ROWS
                    && reader.hasNext()) {
                decoder.append(reader.next(), builder);
                // Report allocation growth as rows are appended instead of only after page build.
                reportMemory(builder);
            }
            // reader.next() may have released the exhausted scanner batch.
            reportMemory(builder);

            if (builder.isEmpty()) {
                // The PageBuilder becomes unreachable when this method returns.
                memory.setBytes(reader.getRetainedSizeInBytes());
                if (reader.isFinished()) {
                    close();
                }
                return null;
            }
            Page page;
            try {
                page = builder.build();
            } catch (DuplicateMapKeyException e) {
                throw new TrinoException(
                        FLUSS_READ_ERROR,
                        "Fluss map contains duplicate keys under Trino semantics for "
                                + handle
                                + ", split "
                                + split,
                        e);
            }
            completedPositions += page.getPositionCount();
            /*
             * Ownership of the built page transfers to Trino. Only reader-owned input memory
             * remains attributable to this page source.
             */
            memory.setBytes(reader.getRetainedSizeInBytes());
            if (reader.isFinished()) {
                close();
            }
            return SourcePage.create(page);
        } catch (RuntimeException | Error failure) {
            closeWithSuppression(failure);
            throw failure;
        }
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }

        closed = true;

        FlussSplitReader readerToClose = reader;
        Table tableToClose = table;

        reader = null;
        table = null;
        decoder = null;

        Throwable failure = null;

        if (readerToClose != null) {
            try {
                completedBytes = readerToClose.getCompletedBytes();
                readerToClose.close();
            } catch (Exception | Error e) {
                failure = firstOrSuppressed(e, failure);
            }
        }

        if (tableToClose != null) {
            try {
                tableToClose.close();
            } catch (Exception | Error e) {
                failure = firstOrSuppressed(e, failure);
            }
        }

        try {
            memory.setBytes(0);
        } catch (RuntimeException | Error e) {
            failure = firstOrSuppressed(e, failure);
        }

        if (failure instanceof Error) {
            throw (Error) failure;
        }

        if (failure != null) {
            throw new TrinoException(
                    GENERIC_INTERNAL_ERROR,
                    "Failed closing Fluss reader for " + handle + ", split " + split,
                    failure);
        }
    }

    @Override
    public boolean isFinished() {
        return closed;
    }

    @Override
    public long getCompletedBytes() {
        if (reader != null) {
            return reader.getCompletedBytes();
        }

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
        if (reader != null) {
            return;
        }

        table =
                clients.openTable(
                        TablePath.of(handle.getFlussDatabaseName(), handle.getFlussTableName()));

        TableInfo tableInfo = table.getTableInfo();

        validateTable(handle, tableInfo);
        validateSplit(split, tableInfo);

        decoder = new FlussRowDecoder(tableInfo.getSchema(), columns);

        reader = FlussSplitReaderFactory.create(table, split);
    }

    private FlussSplitReader.PollResult pollReader() {
        long started = System.nanoTime();

        try {
            return reader.poll(POLL_TIMEOUT);
        } finally {
            readNanos += System.nanoTime() - started;
        }
    }

    private void reportMemory(PageBuilder builder) {
        long retainedBytes =
                Math.addExact(reader.getRetainedSizeInBytes(), builder.getRetainedSizeInBytes());

        memory.setBytes(retainedBytes);
    }

    private void closeWithSuppression(Throwable failure) {
        try {
            close();
        } catch (RuntimeException | Error closeFailure) {
            if (failure != closeFailure) {
                failure.addSuppressed(closeFailure);
            }
        }
    }
}
