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

package org.apache.fluss.client.table.scanner.log;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.metadata.ClientSchemaGetter;
import org.apache.fluss.client.metadata.MetadataUpdater;
import org.apache.fluss.client.metrics.ScannerMetricGroup;
import org.apache.fluss.client.table.scanner.MultiTableRecord;
import org.apache.fluss.client.table.scanner.RemoteFileDownloader;
import org.apache.fluss.client.table.scanner.ScanRecord;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.SchemaGetter;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.LogRecordReadContext;
import org.apache.fluss.rpc.metrics.ClientMetricGroup;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;

/**
 * Default implementation of {@link MultiTableLogScanner}.
 *
 * <p>This scanner subscribes to log data from multiple tables simultaneously. Tables are registered
 * dynamically on the first {@code subscribe(...)} call to a given {@link TablePath}: the scanner
 * resolves the {@link TableInfo} via the admin client and registers the table on the underlying
 * {@link LogFetcher} with no projection / filter (phase 1 surface).
 *
 * <p>Internally, it delegates to a single shared {@link LogFetcher} that manages per-table read
 * contexts.
 *
 * <p>{@code MultiTableLogScannerImpl} is NOT thread-safe. It is the responsibility of the user to
 * ensure that multithreaded access is properly synchronized. Un-synchronized access will result in
 * {@link ConcurrentModificationException}.
 *
 * @since 0.7
 */
@Internal
public class MultiTableLogScannerImpl extends AbstractLogScanner<MultiTableRecords>
        implements MultiTableLogScanner {

    private static final Logger LOG = LoggerFactory.getLogger(MultiTableLogScannerImpl.class);

    private final MetadataUpdater metadataUpdater;
    private final Admin admin;

    /** Per-table {@link TableInfo}, populated lazily on first subscribe to the table. */
    private final Map<TablePath, TableInfo> registeredByPath = new HashMap<>();

    /** tableId -> tablePath, for poll-time record enrichment. */
    private final Map<Long, TablePath> pathByTableId = new HashMap<>();

    private final Map<Long, SchemaGetter> schemaGetters = new HashMap<>();

    /**
     * Currently subscribed buckets per table. When the last bucket of a table is unsubscribed, the
     * table registration is evicted so that a subsequent {@code subscribe(...)} re-resolves a fresh
     * {@link TableInfo}. This is what lets callers recover from a drop+recreate (tableId change) by
     * unsubscribing all buckets of the stale table and subscribing again.
     */
    private final Map<TablePath, Set<TableBucket>> subscribedBucketsByPath = new HashMap<>();

    public MultiTableLogScannerImpl(
            String scannerName,
            MetadataUpdater metadataUpdater,
            Configuration conf,
            ClientMetricGroup clientMetricGroup,
            RemoteFileDownloader remoteFileDownloader,
            Admin admin) {
        this(
                scannerName,
                metadataUpdater,
                conf,
                remoteFileDownloader,
                admin,
                new LogScannerStatus(),
                // Use a synthetic TablePath for scanner-level metrics scoping.
                new ScannerMetricGroup(
                        clientMetricGroup, TablePath.of("multi-table", scannerName)));
    }

    private MultiTableLogScannerImpl(
            String scannerName,
            MetadataUpdater metadataUpdater,
            Configuration conf,
            RemoteFileDownloader remoteFileDownloader,
            Admin admin,
            LogScannerStatus logScannerStatus,
            ScannerMetricGroup scannerMetricGroup) {
        super(
                scannerName,
                logScannerStatus,
                new LogFetcher(
                        scannerName,
                        logScannerStatus,
                        conf,
                        metadataUpdater,
                        scannerMetricGroup,
                        remoteFileDownloader,
                        LogRecordReadContext.SchemaResolution.DYNAMIC),
                scannerMetricGroup);
        this.metadataUpdater = metadataUpdater;
        this.admin = admin;
    }

    @Override
    protected MultiTableRecords emptyResult() {
        return MultiTableRecords.EMPTY;
    }

    @Override
    protected MultiTableRecords toResult(ScanRecords scanRecords) {
        return enrich(scanRecords);
    }

    @Override
    protected String notSubscribedMessage() {
        return "MultiTableLogScanner is not subscribed to any buckets.";
    }

    @Override
    public void subscribe(TablePath tablePath, int bucket, long offset) {
        acquireAndEnsureOpen();
        try {
            TableInfo tableInfo = registerIfAbsent(tablePath);
            if (tableInfo.isPartitioned()) {
                throw new IllegalStateException(
                        "Table "
                                + tablePath
                                + " is a partitioned table, please use "
                                + "\"subscribe(TablePath, long partitionId, int bucket, long offset)\" "
                                + "to subscribe a partitioned bucket instead.");
            }
            TableBucket tableBucket = new TableBucket(tableInfo.getTableId(), bucket);
            metadataUpdater.checkAndUpdateTableMetadata(Collections.singleton(tablePath));
            logScannerStatus.assignScanBuckets(Collections.singletonMap(tableBucket, offset));
            trackSubscription(tablePath, tableBucket);
        } finally {
            release();
        }
    }

    @Override
    public void unsubscribe(TablePath tablePath, int bucket) {
        acquireAndEnsureOpen();
        try {
            TableInfo tableInfo = requireRegistered(tablePath);
            if (tableInfo.isPartitioned()) {
                throw new IllegalStateException(
                        "Table "
                                + tablePath
                                + " is a partitioned table, please use "
                                + "\"unsubscribe(TablePath, long partitionId, int bucket)\" "
                                + "to unsubscribe a partitioned bucket instead.");
            }
            TableBucket tableBucket = new TableBucket(tableInfo.getTableId(), bucket);
            logScannerStatus.unassignScanBuckets(Collections.singletonList(tableBucket));
            untrackSubscription(tablePath, tableBucket);
        } finally {
            release();
        }
    }

    @Override
    public void subscribe(TablePath tablePath, long partitionId, int bucket, long offset) {
        acquireAndEnsureOpen();
        try {
            TableInfo tableInfo = registerIfAbsent(tablePath);
            if (!tableInfo.isPartitioned()) {
                throw new IllegalStateException(
                        "Table "
                                + tablePath
                                + " is not a partitioned table, please use "
                                + "\"subscribe(TablePath, int bucket, long offset)\" "
                                + "to subscribe a non-partitioned bucket instead.");
            }
            TableBucket tableBucket = new TableBucket(tableInfo.getTableId(), partitionId, bucket);
            metadataUpdater.checkAndUpdateTableMetadata(Collections.singleton(tablePath));
            metadataUpdater.checkAndUpdatePartitionMetadata(
                    tablePath, Collections.singleton(partitionId));
            logScannerStatus.assignScanBuckets(Collections.singletonMap(tableBucket, offset));
            trackSubscription(tablePath, tableBucket);
        } finally {
            release();
        }
    }

    @Override
    public void unsubscribe(TablePath tablePath, long partitionId, int bucket) {
        acquireAndEnsureOpen();
        try {
            TableInfo tableInfo = requireRegistered(tablePath);
            if (!tableInfo.isPartitioned()) {
                throw new IllegalStateException(
                        "Table " + tablePath + " is not a partitioned table.");
            }
            TableBucket tableBucket = new TableBucket(tableInfo.getTableId(), partitionId, bucket);
            logScannerStatus.unassignScanBuckets(Collections.singletonList(tableBucket));
            untrackSubscription(tablePath, tableBucket);
        } finally {
            release();
        }
    }

    /**
     * Register the table on the underlying {@link LogFetcher} if not yet registered. Must be called
     * under the acquire-lock. Phase 1 always registers with no projection / filter.
     */
    private TableInfo registerIfAbsent(TablePath tablePath) {
        TableInfo cached = registeredByPath.get(tablePath);
        if (cached != null) {
            checkTableIdNotChanged(tablePath, cached);
            return cached;
        }

        TableInfo tableInfo = resolveTableInfo(tablePath);
        TableScanSpec spec = new TableScanSpec(tableInfo, null, null);
        SchemaGetter schemaGetter =
                new ClientSchemaGetter(tablePath, tableInfo.getSchemaInfo(), admin);
        logFetcher.registerTable(spec, schemaGetter);
        registeredByPath.put(tablePath, tableInfo);
        pathByTableId.put(tableInfo.getTableId(), tablePath);
        schemaGetters.put(tableInfo.getTableId(), schemaGetter);
        return tableInfo;
    }

    /**
     * Record a freshly assigned bucket against its table. Must be called under the acquire-lock.
     */
    private void trackSubscription(TablePath tablePath, TableBucket tableBucket) {
        subscribedBucketsByPath.computeIfAbsent(tablePath, k -> new HashSet<>()).add(tableBucket);
    }

    /**
     * Drop a bucket from its table's subscription set. When the table has no remaining subscribed
     * buckets, evict its cached registration so a later {@code subscribe(...)} re-resolves the
     * table (picking up a new tableId after a drop+recreate). Must be called under the
     * acquire-lock.
     */
    private void untrackSubscription(TablePath tablePath, TableBucket tableBucket) {
        Set<TableBucket> buckets = subscribedBucketsByPath.get(tablePath);
        if (buckets == null) {
            return;
        }
        buckets.remove(tableBucket);
        if (buckets.isEmpty()) {
            subscribedBucketsByPath.remove(tablePath);
            TableInfo evicted = registeredByPath.remove(tablePath);
            if (evicted != null) {
                pathByTableId.remove(evicted.getTableId());
                schemaGetters.remove(evicted.getTableId());
            }
        }
    }

    private void checkTableIdNotChanged(TablePath tablePath, TableInfo cached) {
        Optional<Long> currentTableId = metadataUpdater.getCluster().getTableId(tablePath);
        if (currentTableId.isPresent() && currentTableId.get() != cached.getTableId()) {
            throw new IllegalStateException(
                    "Table "
                            + tablePath
                            + " has been recreated (tableId changed from "
                            + cached.getTableId()
                            + " to "
                            + currentTableId.get()
                            + "). Please unsubscribe the stale table and subscribe again.");
        }
    }

    private TableInfo requireRegistered(TablePath tablePath) {
        TableInfo tableInfo = registeredByPath.get(tablePath);
        if (tableInfo == null) {
            throw new IllegalArgumentException(
                    "Table "
                            + tablePath
                            + " has not been subscribed on this MultiTableLogScanner.");
        }
        return tableInfo;
    }

    private TableInfo resolveTableInfo(TablePath tablePath) {
        try {
            return admin.getTableInfo(tablePath).get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while resolving table info: " + tablePath, e);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause() != null ? e.getCause() : e;
            if (cause instanceof RuntimeException) {
                throw (RuntimeException) cause;
            }
            throw new RuntimeException("Failed to resolve table info: " + tablePath, cause);
        }
    }

    private MultiTableRecords enrich(ScanRecords scanRecords) {
        MultiTableRecords.Builder builder = new MultiTableRecords.Builder();
        for (TableBucket bucket : scanRecords.buckets()) {
            long tableId = bucket.getTableId();
            TablePath tablePath = pathByTableId.get(tableId);
            SchemaGetter schemaGetter = schemaGetters.get(tableId);
            if (tablePath == null || schemaGetter == null) {
                // Should not happen: only registered tables can produce records.
                throw new IllegalStateException(
                        String.format(
                                "Dropping records for unknown tableId %s in bucket %s",
                                tableId, bucket));
            }
            List<ScanRecord> records = scanRecords.records(bucket);
            for (ScanRecord scanRecord : records) {
                // The SchemaGetter is consumed by LogFetcher while decoding records: it resolves
                // the schema for each record's schemaId. ClientSchemaGetter is seeded with the
                // latest
                // SchemaInfo and caches every schema it ever resolves (by schemaId), so during the
                // records loading path lookups are served from the in-memory cache; The per-record
                // overhead is therefore negligible.
                int schemaId = scanRecord.getSchemaId();
                Schema schema = schemaGetter.getSchema(schemaId);
                builder.add(
                        tablePath,
                        bucket,
                        new MultiTableRecord(tablePath, tableId, schemaId, schema, scanRecord));
            }
        }
        return builder.build();
    }
}
