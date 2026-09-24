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

import org.apache.flink.metrics.MetricGroup;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.apache.fluss.utils.Preconditions.checkState;

/**
 * Owns the local lookup cache, its snapshot bootstrap and changelog worker.
 *
 * <p>Reads wait for bootstrap and are synchronized with changelog batches and closure. A failed
 * batch makes the cache unreadable before releasing the write lock. Returned values are independent
 * byte arrays and can be decoded after releasing the read lock.
 */
final class FullCacheLookupRuntime implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(FullCacheLookupRuntime.class);
    private static final Duration LOG_POLL_TIMEOUT = Duration.ofSeconds(1);

    private final Configuration flussConfig;
    private final TablePath tablePath;
    private final int numBuckets;
    private final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
    private final CountDownLatch bootstrapReady = new CountDownLatch(1);
    private final AtomicBoolean closed = new AtomicBoolean(false);

    private Connection connection;
    private Table table;
    private LogScanner logScanner;
    private RocksDBLookupStore cacheStore;
    private Thread workerThread;
    private KeyEncoder primaryKeyEncoder;
    private RowSerializer rowSerializer;
    private volatile Throwable workerFailure;
    private volatile long snapshotRows;
    private volatile long bootstrapDurationMs;
    private volatile boolean ready;

    FullCacheLookupRuntime(Configuration flussConfig, TablePath tablePath, int numBuckets) {
        this.flussConfig = flussConfig;
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
                    "Full lookup cache does not support partitioned tables.");
            checkState(
                    !tableInfo.getTableConfig().getDataLakeFormat().isPresent(),
                    "Full lookup cache does not support datalake-enabled tables.");
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
            cacheStore = new RocksDBLookupStore(createCacheDirectory(subtaskIndex));
            logScanner = table.newScan().createLogScanner();

            metricGroup.gauge("ready", () -> ready ? 1 : 0);
            metricGroup.gauge("ownedBuckets", ownedBuckets::size);
            metricGroup.gauge("snapshotRows", () -> snapshotRows);
            metricGroup.gauge("bootstrapDurationMs", () -> bootstrapDurationMs);
            metricGroup.gauge("rocksdbDiskBytes", cacheStore::diskSizeBytes);

            startWorker(
                    new FullCacheBootstrapper(
                            table, tableInfo, ownedBuckets, primaryKeyEncoder, rowSerializer));
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

    /** Reads a complete lookup result under the cache lock. */
    List<byte[]> lookup(byte[] key, boolean prefixLookup) throws RocksDBException {
        awaitReady();
        rwLock.readLock().lock();
        try {
            checkReadable();
            if (prefixLookup) {
                return cacheStore.prefixLookup(key);
            }
            byte[] value = cacheStore.get(key);
            return value == null ? Collections.emptyList() : Collections.singletonList(value);
        } finally {
            rwLock.readLock().unlock();
        }
    }

    /** Waits for bootstrap and rejects failed or closed caches, including for null-key lookups. */
    void awaitReady() {
        try {
            bootstrapReady.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(
                    "Interrupted while waiting for the full lookup cache to be ready.", e);
        }
        checkReadable();
    }

    private void checkReadable() {
        Throwable failure = workerFailure;
        if (failure != null) {
            throw new RuntimeException("Full-cache worker failed; refusing stale lookup.", failure);
        }
        checkState(ready && !closed.get(), "Full lookup cache is not ready.");
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        ready = false;
        // Unblock any lookup that is still waiting for the snapshot phase.
        bootstrapReady.countDown();
        if (logScanner != null) {
            logScanner.wakeup();
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

    private void startWorker(FullCacheBootstrapper bootstrapper) {
        workerThread =
                new Thread(() -> runWorker(bootstrapper), "fluss-full-cache-worker-" + tablePath);
        workerThread.setDaemon(true);
        workerThread.start();
    }

    /**
     * Bootstraps the cache from KV snapshots and then keeps it up to date by consuming the
     * changelog of all owned buckets. Runs on the background worker thread.
     */
    private void runWorker(FullCacheBootstrapper bootstrapper) {
        try {
            // Load the snapshot. No lock is needed, lookups are blocked on the latch.
            FullCacheBootstrapper.Result result = bootstrapper.run(cacheStore::put);
            snapshotRows = result.getSnapshotRows();
            bootstrapDurationMs = result.getDurationMs();

            for (Map.Entry<Integer, Long> entry : result.getBucketSnapshotOffsets().entrySet()) {
                logScanner.subscribe(entry.getKey(), entry.getValue());
            }
            if (closed.get()) {
                return;
            }
            ready = true;
            bootstrapReady.countDown();
            LOG.info(
                    "Full lookup cache for {} is ready in {} ms with {} rows.",
                    tablePath,
                    bootstrapDurationMs,
                    snapshotRows);

            // Continuously consume the changelog.
            while (!closed.get()) {
                ScanRecords records = logScanner.poll(LOG_POLL_TIMEOUT);
                if (records.hasProgress()) {
                    applyLogRecords(records);
                }
            }
        } catch (Throwable t) {
            if (!closed.get()) {
                workerFailure = t;
                ready = false;
                LOG.error("Full-cache worker for {} failed.", tablePath, t);
            }
            // Wake up lookups so they can observe the failure (or the closure) instead of
            // blocking forever.
            bootstrapReady.countDown();
        }
    }

    /** Applies a batch, publishing any failure before readers can acquire the lock. */
    @VisibleForTesting
    void applyLogRecords(ScanRecords records) throws Exception {
        rwLock.writeLock().lock();
        try {
            checkReadable();
            writeLogRecords(records);
        } catch (Throwable t) {
            workerFailure = t;
            ready = false;
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
                    byte[] key = primaryKeyEncoder.encodeKey(record.getRow());
                    cacheStore.put(key, serializeRow(record.getRow(), rowSerializer));
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

    private static byte[] serializeRow(InternalRow row, RowSerializer serializer) {
        BinaryRow binaryRow = serializer.toBinaryRow(row);
        byte[] value = new byte[binaryRow.getSizeInBytes()];
        binaryRow.copyTo(value, 0);
        return value;
    }
}
