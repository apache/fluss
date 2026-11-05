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

package org.apache.fluss.flink.source.lookup;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.scanner.ScanRecord;
import org.apache.fluss.client.table.scanner.log.LogScanner;
import org.apache.fluss.client.table.scanner.log.ScanRecords;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.KvFormat;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.row.BinaryRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.encode.KeyEncoder;
import org.apache.fluss.row.serializer.RowSerializer;
import org.apache.fluss.types.DataType;
import org.apache.fluss.utils.IOUtils;

import io.github.fluss_contrib.rocksdb.RocksDBException;
import org.apache.flink.metrics.MetricGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;

import static org.apache.fluss.utils.Preconditions.checkState;

/**
 * Owns the bucket-sharded lookup cache, its snapshot bootstrap and changelog worker.
 *
 * <p>Reads wait for bootstrap and are synchronized with changelog batches and closure. A failed
 * batch makes the cache unreadable before releasing the write lock. Returned values are independent
 * byte arrays and can be decoded after releasing the read lock.
 */
@Internal
final class FullCacheLookupTable implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(FullCacheLookupTable.class);

    private final Configuration flussConfig;
    private final TablePath tablePath;
    private final int numBuckets;
    private final FullCacheLookupOptions cacheOptions;
    private final Function<Table, LogScanner> scannerFactory;
    private final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
    private final Object stateLock = new Object();
    private final CountDownLatch bootstrapReady = new CountDownLatch(1);

    private Connection connection;
    private Table table;
    private volatile LogScanner logScanner;
    private RocksDBLookupStore cacheStore;
    private Thread workerThread;
    private KeyEncoder primaryKeyEncoder;
    private RowSerializer rowSerializer;
    private volatile Throwable workerFailure;
    private volatile long snapshotRows;
    private volatile long bootstrapDurationMs;
    private volatile boolean ready;
    private volatile boolean closed;

    FullCacheLookupTable(
            Configuration flussConfig,
            TablePath tablePath,
            int numBuckets,
            FullCacheLookupOptions cacheOptions) {
        this(
                flussConfig,
                tablePath,
                numBuckets,
                cacheOptions,
                table -> table.newScan().createLogScanner());
    }

    @VisibleForTesting
    FullCacheLookupTable(
            Configuration flussConfig,
            TablePath tablePath,
            int numBuckets,
            FullCacheLookupOptions cacheOptions,
            Function<Table, LogScanner> scannerFactory) {
        this.flussConfig = flussConfig;
        this.cacheOptions = cacheOptions;
        this.scannerFactory = scannerFactory;
        this.tablePath = tablePath;
        this.numBuckets = numBuckets;
    }

    /**
     * Opens the store and starts bootstrap in the background, returning the current table metadata.
     */
    TableInfo open(int subtaskIndex, int parallelism, MetricGroup metricGroup) throws Exception {
        try {
            connection = ConnectionFactory.createConnection(flussConfig);
            table = connection.getTable(tablePath);
            TableInfo tableInfo = table.getTableInfo();
            checkState(
                    !tableInfo.isPartitioned(),
                    "Bucket-sharded full lookup cache does not yet support partitioned tables.");
            checkState(
                    !tableInfo.getTableConfig().getKvTTL().isPresent(),
                    "Full lookup cache does not support row-level TTL.");
            checkState(
                    tableInfo.getNumBuckets() == numBuckets,
                    "Resolved bucket count %s does not match current table bucket count %s.",
                    numBuckets,
                    tableInfo.getNumBuckets());

            org.apache.fluss.types.RowType rowType = tableInfo.getRowType();
            primaryKeyEncoder =
                    KeyEncoder.ofPrimaryKeyEncoder(
                            rowType,
                            tableInfo.getPhysicalPrimaryKeys(),
                            tableInfo.getTableConfig(),
                            tableInfo.isDefaultBucketKey());
            BinaryRow.BinaryRowFormat format =
                    tableInfo.getTableConfig().getKvFormat() == KvFormat.COMPACTED
                            ? BinaryRow.BinaryRowFormat.COMPACTED
                            : BinaryRow.BinaryRowFormat.INDEXED;
            rowSerializer =
                    new RowSerializer(rowType.getChildren().toArray(new DataType[0]), format);
            Set<Integer> ownedBuckets =
                    new FullCacheBucketAssignment(numBuckets)
                            .ownedBuckets(subtaskIndex, parallelism);
            cacheStore = new RocksDBLookupStore(createCacheDirectory(subtaskIndex), cacheOptions);
            logScanner = scannerFactory.apply(table);

            metricGroup.gauge("ready", () -> ready ? 1 : 0);
            metricGroup.gauge("ownedBuckets", ownedBuckets::size);
            metricGroup.gauge("snapshotRows", () -> snapshotRows);
            metricGroup.gauge("bootstrapDurationMs", () -> bootstrapDurationMs);
            metricGroup.gauge("rocksdbDiskBytes", () -> storeSizeBytes(false));
            metricGroup.gauge("rocksdbEstimatedMemoryBytes", () -> storeSizeBytes(true));
            metricGroup.gauge("workerFailed", () -> workerFailure == null ? 0 : 1);

            startWorker(
                    new FullCacheLoader(
                            connection.getAdmin(),
                            flussConfig.get(ConfigOptions.CLIENT_SCANNER_KV_BATCH_STRATEGY),
                            table,
                            tableInfo,
                            ownedBuckets));
            LOG.info(
                    "Full lookup cache for {} opened with buckets {}, bootstrapping in background.",
                    tablePath,
                    ownedBuckets);
            return tableInfo;
        } catch (Throwable t) {
            close();
            throw t;
        }
    }

    /**
     * Reads a complete lookup result after bootstrap, under the cache lock.
     *
     * <p>A null key represents a SQL lookup containing null and returns no matches. It still checks
     * the cache state so a stream of null keys cannot hide a failed worker.
     */
    List<byte[]> lookup(@Nullable byte[] key, boolean prefixLookup) throws RocksDBException {
        awaitReady();
        rwLock.readLock().lock();
        try {
            checkReadable();
            if (key == null) {
                return Collections.emptyList();
            }
            if (prefixLookup) {
                return cacheStore.prefixLookup(key);
            }
            byte[] value = cacheStore.get(key);
            return value == null ? Collections.emptyList() : Collections.singletonList(value);
        } finally {
            rwLock.readLock().unlock();
        }
    }

    /** Waits until bootstrap completes, the worker fails, or the cache closes. */
    private void awaitReady() {
        try {
            bootstrapReady.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(
                    "Interrupted while waiting for the full lookup cache to be ready.", e);
        }
    }

    private void checkReadable() {
        synchronized (stateLock) {
            Throwable failure = workerFailure;
            if (failure != null) {
                throw new RuntimeException(
                        "Full-cache worker failed; lookup cache is unavailable.", failure);
            }
            checkState(ready && !closed, "Full lookup cache is not ready.");
        }
    }

    @Override
    public void close() {
        synchronized (stateLock) {
            if (closed) {
                return;
            }
            closed = true;
            ready = false;
            bootstrapReady.countDown();
        }
        // Cancellation must not wait for the cache lock or hold the state lock.
        LogScanner scanner = logScanner;
        if (scanner != null) {
            scanner.wakeup();
        }
        if (workerThread != null) {
            workerThread.interrupt();
            boolean interrupted = false;
            while (workerThread.isAlive()) {
                try {
                    workerThread.join(1000L);
                } catch (InterruptedException e) {
                    interrupted = true;
                }
            }
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
        IOUtils.closeQuietly(logScanner);
        if (cacheStore != null) {
            rwLock.writeLock().lock();
            try {
                IOUtils.closeQuietly(cacheStore);
            } finally {
                rwLock.writeLock().unlock();
            }
        }
        IOUtils.closeQuietly(table);
        if (connection != null) {
            IOUtils.closeQuietly(() -> connection.close(Duration.ZERO), "Fluss connection");
        }
    }

    private long storeSizeBytes(boolean memory) {
        rwLock.readLock().lock();
        try {
            // Reporters may retain the gauge after close. Do not access a released native handle.
            if (closed) {
                return 0L;
            }
            return memory ? cacheStore.estimatedMemoryBytes() : cacheStore.diskSizeBytes();
        } catch (RocksDBException e) {
            LOG.warn("Failed to read full lookup cache size for {}.", tablePath, e);
            return 0L;
        } finally {
            rwLock.readLock().unlock();
        }
    }

    private void startWorker(FullCacheLoader loader) {
        workerThread = new Thread(() -> runWorker(loader), "fluss-full-cache-worker-" + tablePath);
        workerThread.setDaemon(true);
        workerThread.start();
    }

    /**
     * Bootstraps the cache from KV snapshots and then keeps it up to date by consuming the
     * changelog of all owned buckets. Runs on the background worker thread.
     */
    private void runWorker(FullCacheLoader loader) {
        try {
            long startNanos = System.nanoTime();
            // Load the snapshot. No lock is needed, lookups are blocked on the latch.
            FullCacheLoader.Result result = loader.runSnapshot(this::putRow);
            snapshotRows = result.getSnapshotRows();

            // Apply CDC to the same store before serving lookups. Reuse the scanner afterwards;
            // a poll may already have applied records beyond the fixed readiness targets.
            loader.replayChangelog(
                    logScanner, result.getBucketSnapshotOffsets(), this::writeLogRecords);
            bootstrapDurationMs = (System.nanoTime() - startNanos) / 1_000_000L;
            if (!completeBootstrap()) {
                return;
            }
            LOG.info(
                    "Full lookup cache for {} is ready in {} ms with {} rows.",
                    tablePath,
                    bootstrapDurationMs,
                    snapshotRows);

            loader.consumeChangelog(logScanner, this::applyLogRecords, () -> closed);
        } catch (Throwable t) {
            failWorker(t);
        }
    }

    /** Publishes bootstrap completion only while the cache is neither closed nor failed. */
    @VisibleForTesting
    boolean completeBootstrap() {
        synchronized (stateLock) {
            if (closed || workerFailure != null) {
                return false;
            }
            ready = true;
            bootstrapReady.countDown();
            return true;
        }
    }

    private void failWorker(Throwable failure) {
        synchronized (stateLock) {
            if (closed || workerFailure != null) {
                return;
            }
            workerFailure = failure;
            ready = false;
            bootstrapReady.countDown();
        }
        LOG.error("Full-cache worker for {} failed.", tablePath, failure);
    }

    /** Applies a batch, publishing any failure before readers can acquire the lock. */
    @VisibleForTesting
    void applyLogRecords(ScanRecords records) throws Exception {
        rwLock.writeLock().lock();
        try {
            checkReadable();
            writeLogRecords(records);
        } catch (Throwable t) {
            failWorker(t);
            throw t;
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    private void writeLogRecords(ScanRecords records) throws Exception {
        for (TableBucket bucket : records.buckets()) {
            for (ScanRecord record : records.records(bucket)) {
                ChangeType changeType = record.getChangeType();
                if (changeType == ChangeType.INSERT || changeType == ChangeType.UPDATE_AFTER) {
                    putRow(record.getRow());
                } else if (changeType == ChangeType.DELETE) {
                    cacheStore.delete(primaryKeyEncoder.encodeKey(record.getRow()));
                } else if (changeType != ChangeType.UPDATE_BEFORE) {
                    // UPDATE_BEFORE carries the previous image, which is already replaced by the
                    // following UPDATE_AFTER record.
                    throw new IllegalStateException(
                            "Unexpected full-cache changelog kind: " + changeType);
                }
            }
        }
    }

    private void putRow(InternalRow row) throws RocksDBException {
        byte[] key = primaryKeyEncoder.encodeKey(row);
        BinaryRow binaryRow = rowSerializer.toBinaryRow(row);
        byte[] value = new byte[binaryRow.getSizeInBytes()];
        binaryRow.copyTo(value, 0);
        cacheStore.put(key, value);
    }

    private File createCacheDirectory(int subtaskIndex) throws IOException {
        File root = new File(flussConfig.get(ConfigOptions.CLIENT_SCANNER_IO_TMP_DIR));
        if (!root.mkdirs() && !root.isDirectory()) {
            throw new IOException("Unable to create scanner temporary directory " + root);
        }
        String name =
                "lookup-full-"
                        + tablePath.toString().replaceAll("[^a-zA-Z0-9._-]", "_")
                        + '-'
                        + subtaskIndex
                        + '-'
                        + System.nanoTime();
        return new File(root, name);
    }
}
