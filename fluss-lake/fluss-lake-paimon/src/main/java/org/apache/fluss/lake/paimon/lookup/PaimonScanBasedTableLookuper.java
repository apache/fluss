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

package org.apache.fluss.lake.paimon.lookup;

import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.TableConfig;
import org.apache.fluss.exception.KvStorageException;
import org.apache.fluss.lake.lakestorage.LakeTableLookuper;
import org.apache.fluss.lake.paimon.source.FlussRowAsPaimonRow;
import org.apache.fluss.metadata.DataLakeFormat;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.decode.KeyDecoder;
import org.apache.fluss.utils.IOUtils;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.options.Options;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.reader.RecordReader.RecordIterator;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.RowPartitionKeyExtractor;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.types.RowType;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.apache.fluss.lake.paimon.utils.PaimonConversions.toFlussValue;
import static org.apache.fluss.lake.paimon.utils.PaimonConversions.toPaimon;
import static org.apache.fluss.lake.paimon.utils.PaimonConversions.toPaimonPartition;
import static org.apache.fluss.utils.Preconditions.checkNotNull;
import static org.apache.fluss.utils.Preconditions.checkState;
import static org.apache.fluss.utils.concurrent.LockUtils.inReadLock;
import static org.apache.fluss.utils.concurrent.LockUtils.inWriteLock;

/**
 * Looks up a primary key by scanning the latest Paimon snapshot with a limit of one row.
 *
 * <p>Each scan is restricted to the requested partition, bucket, and complete primary key. It does
 * not create local lookup files. Lookups use independent readers and encoders and may run in
 * parallel. The catalog and table are initialized once, and close waits for active lookups to
 * finish.
 */
public class PaimonScanBasedTableLookuper implements LakeTableLookuper {

    private final Configuration paimonConfig;
    private final TablePath tablePath;
    private final TableConfig tableConfig;
    private final ReadWriteLock lifecycleLock = new ReentrantReadWriteLock();
    private final Object initializationLock = new Object();

    private @Nullable Catalog catalog;
    private @Nullable FileStoreTable fileStoreTable;
    private boolean closed;

    /** Creates a scan-based lookuper for the specified Paimon table. */
    public PaimonScanBasedTableLookuper(
            Configuration paimonConfig, TablePath tablePath, TableConfig tableConfig) {
        this.paimonConfig = checkNotNull(paimonConfig, "paimonConfig must not be null.");
        this.tablePath = checkNotNull(tablePath, "tablePath must not be null.");
        this.tableConfig = checkNotNull(tableConfig, "tableConfig must not be null.");
    }

    @Override
    public @Nullable byte[] lookup(byte[] key, LookupContext context) throws Exception {
        checkNotNull(key, "key must not be null.");
        checkNotNull(context, "context must not be null.");
        return inReadLock(
                lifecycleLock,
                () -> {
                    checkState(!closed, "Paimon scan-based lookuper has been closed.");
                    long lookupStartNanos = System.nanoTime();
                    try {
                        FileStoreTable table = table();
                        // Paimon tables contain mutable lazy store state; isolate it per lookup.
                        return scanLookup(table.copy(table.schema()), key, context);
                    } catch (IOException | UncheckedIOException e) {
                        // The next RPC retry plans a fresh scan after compaction or expiration.
                        throw new KvStorageException(
                                "Failed to scan historical data from Paimon for " + tablePath + ".",
                                e);
                    } finally {
                        context.lookupMetricRecorder()
                                .recordLookup(System.nanoTime() - lookupStartNanos, false);
                    }
                });
    }

    @Override
    public void close() {
        inWriteLock(
                lifecycleLock,
                () -> {
                    if (!closed) {
                        closed = true;
                        IOUtils.closeQuietly(catalog, "Paimon catalog");
                    }
                });
    }

    private FileStoreTable table() throws Exception {
        synchronized (initializationLock) {
            if (fileStoreTable == null) {
                Catalog newCatalog =
                        CatalogFactory.createCatalog(
                                CatalogContext.create(Options.fromMap(paimonConfig.toMap())));
                try {
                    FileStoreTable table =
                            (FileStoreTable) newCatalog.getTable(toPaimon(tablePath));
                    if (table.primaryKeys().isEmpty()) {
                        throw new UnsupportedOperationException(
                                "Point lookup is only supported for primary-key Paimon tables.");
                    }
                    catalog = newCatalog;
                    fileStoreTable = table;
                } finally {
                    if (fileStoreTable == null) {
                        IOUtils.closeQuietly(newCatalog, "Paimon catalog");
                    }
                }
            }
            return fileStoreTable;
        }
    }

    private @Nullable byte[] scanLookup(FileStoreTable table, byte[] key, LookupContext context)
            throws Exception {
        RowType rowType = table.rowType();
        List<String> primaryKeys = table.schema().trimmedPrimaryKeys();
        KeyDecoder keyDecoder =
                KeyDecoder.ofPrimaryKeyDecoder(
                        context.valueRowType(),
                        primaryKeys,
                        tableConfig.getKvFormatVersion().orElse(1).shortValue(),
                        DataLakeFormat.PAIMON,
                        table.schema().bucketKeys().equals(primaryKeys));
        FlussRowAsPaimonRow keyRow =
                new FlussRowAsPaimonRow(keyDecoder.decodeKey(key), rowType.project(primaryKeys));
        PredicateBuilder predicateBuilder = new PredicateBuilder(rowType);
        List<Predicate> predicates = new ArrayList<>(primaryKeys.size());
        for (int i = 0; i < primaryKeys.size(); i++) {
            int fieldIndex = predicateBuilder.indexOf(primaryKeys.get(i));
            Object value =
                    org.apache.paimon.data.InternalRow.createFieldGetter(
                                    rowType.getTypeAt(fieldIndex), i)
                            .getFieldOrNull(keyRow);
            predicates.add(predicateBuilder.equal(fieldIndex, value));
        }

        RowPartitionKeyExtractor partitionKeyExtractor =
                new RowPartitionKeyExtractor(table.schema());
        BinaryRow partition =
                toPaimonPartition(
                        context.partitionSpec(),
                        context.valueRowType(),
                        rowType,
                        partitionKeyExtractor::partition);
        ReadBuilder readBuilder =
                table.newReadBuilder()
                        .withFilter(predicates)
                        .withPartitionFilter(
                                PartitionPredicate.fromMultiple(
                                        rowType.project(table.partitionKeys()),
                                        Collections.singletonList(partition)))
                        .withBucket(context.bucketId())
                        .withReadType(rowType.project(context.valueRowType().getFieldNames()))
                        .withLimit(1);
        // Pushdown alone may only prune files. Filter each row before applying the limit.
        try (RecordReader<org.apache.paimon.data.InternalRow> reader =
                readBuilder.newRead().executeFilter().createReader(readBuilder.newScan().plan())) {
            RecordIterator<org.apache.paimon.data.InternalRow> batch;
            while ((batch = reader.readBatch()) != null) {
                try {
                    org.apache.paimon.data.InternalRow row = batch.next();
                    if (row != null) {
                        // Encode while the batch still owns the row's backing storage.
                        return toFlussValue(
                                row,
                                context.schemaId(),
                                context.valueRowType(),
                                tableConfig.getKvFormat());
                    }
                } finally {
                    batch.releaseBatch();
                }
            }
        }
        return null;
    }
}
