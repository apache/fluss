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

package org.apache.fluss.server.kv.scan;

import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.TooManyScannersException;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.server.replica.Replica;
import org.apache.fluss.utils.AutoCloseableAsync;
import org.apache.fluss.utils.clock.Clock;
import org.apache.fluss.utils.clock.SystemClock;
import org.apache.fluss.utils.concurrent.FutureUtils;
import org.apache.fluss.utils.concurrent.Scheduler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Manages server-side KV full-scan sessions ({@link ScannerContext}). Sessions are keyed by a
 * server-assigned UUID and persist across multiple batched-fetch RPCs.
 *
 * <p>Concurrency limits are enforced both per-bucket and per-server, with a pre-check followed by
 * an atomic increment + re-check (rolled back on contention) so the configured caps cannot be
 * permanently breached. Sessions idle longer than {@code kv.scanner.ttl} are evicted by a
 * background task; their IDs are retained for {@code 2 × ttl} so that {@link
 * #isRecentlyExpired(byte[])} can distinguish expired sessions from unknown ones.
 *
 * <p>{@link #closeScannersForBucket(TableBucket)} must be called when a bucket loses leadership to
 * release RocksDB resources promptly.
 */
@ThreadSafe
public class ScannerManager implements AutoCloseableAsync {

    private static final Logger LOG = LoggerFactory.getLogger(ScannerManager.class);

    /** Sentinel for read paths so a missing bucket entry does not allocate a counter. */
    private static final AtomicInteger ZERO = new AtomicInteger(0);

    private final Map<String, ScannerContext> scanners = new ConcurrentHashMap<>();
    private final Map<String, Long> recentlyExpiredIds = new ConcurrentHashMap<>();
    private final Map<TableBucket, AtomicInteger> perBucketCount = new ConcurrentHashMap<>();
    private final AtomicInteger totalScanners = new AtomicInteger(0);

    private final AtomicBoolean closed = new AtomicBoolean(false);

    private final Clock clock;
    private final long scannerTtlMs;
    private final long recentlyExpiredRetentionMs;
    private final int maxPerBucket;
    private final int maxPerServer;
    private final int maxBatchSizeBytes;

    @Nullable private ScheduledFuture<?> evictorTask;

    public ScannerManager(Configuration conf, Scheduler scheduler) {
        this(conf, scheduler, SystemClock.getInstance());
    }

    @VisibleForTesting
    ScannerManager(Configuration conf, Scheduler scheduler, Clock clock) {
        this.clock = clock;
        this.scannerTtlMs = conf.get(ConfigOptions.KV_SCANNER_TTL).toMillis();
        this.recentlyExpiredRetentionMs = 2 * scannerTtlMs;
        this.maxPerBucket = conf.get(ConfigOptions.KV_SCANNER_MAX_PER_BUCKET);
        this.maxPerServer = conf.get(ConfigOptions.KV_SCANNER_MAX_PER_SERVER);
        long configuredMaxBatch = conf.get(ConfigOptions.KV_SCANNER_MAX_BATCH_SIZE).getBytes();
        this.maxBatchSizeBytes = (int) Math.min(Integer.MAX_VALUE, configuredMaxBatch);

        long expirationIntervalMs =
                conf.get(ConfigOptions.KV_SCANNER_EXPIRATION_INTERVAL).toMillis();
        this.evictorTask =
                scheduler.schedule(
                        "scanner-expiration",
                        this::evictExpiredScanners,
                        expirationIntervalMs,
                        expirationIntervalMs);

        LOG.info(
                "Started ScannerManager: ttl={}ms, expirationInterval={}ms, "
                        + "maxPerBucket={}, maxPerServer={}",
                scannerTtlMs,
                expirationIntervalMs,
                maxPerBucket,
                maxPerServer);
    }

    /**
     * Opens a new scan session against the given leader replica. The snapshot is taken under the
     * replica's leader-ISR read lock; on {@link TooManyScannersException} the just-opened context
     * is closed by {@link Replica#openScan} before the exception propagates. Returns an empty-
     * bucket result (context = {@code null}) without consuming a slot when the bucket has no rows.
     *
     * @param limit optional row-count cap ({@code null} or ≤ 0 means unlimited)
     * @throws TooManyScannersException if the per-bucket or per-server limit is exceeded
     * @throws IOException if RocksDB is shutting down
     */
    public OpenScanResult createScanner(Replica replica, @Nullable Long limit) throws IOException {
        checkLimits(replica.getTableBucket());
        String scannerId = generateScannerId();
        return replica.openScan(this, scannerId, limit != null ? limit : -1L, clock.milliseconds());
    }

    /**
     * Registers an already-opened {@link ScannerContext}, enforcing the per-bucket and per-server
     * limits. The caller must close the context if {@link TooManyScannersException} is thrown.
     */
    public void register(ScannerContext context) {
        registerContext(context);
    }

    /**
     * Looks up a scanner session. <em>Does not</em> refresh the last-access timestamp — call {@link
     * #markAccessed(ScannerContext)} only after the request has been fully validated, so malformed
     * requests cannot extend the idle TTL of an orphan session.
     */
    @Nullable
    public ScannerContext getScanner(byte[] scannerId) {
        return scanners.get(new String(scannerId, StandardCharsets.UTF_8));
    }

    /** Refreshes the last-access timestamp; call only when about to do real work. */
    public void markAccessed(ScannerContext context) {
        context.updateLastAccessTime(clock.milliseconds());
    }

    /**
     * Returns {@code true} if the ID belongs to a session evicted within the last {@code 2 ×
     * ttlMs}; lets callers distinguish "expired" from "unknown".
     */
    public boolean isRecentlyExpired(byte[] scannerId) {
        return recentlyExpiredIds.containsKey(new String(scannerId, StandardCharsets.UTF_8));
    }

    /**
     * Removes and closes the given scanner. Uses a conditional remove so the TTL evictor and an
     * explicit close cannot both run the close path.
     */
    public void removeScanner(ScannerContext context) {
        if (scanners.remove(context.getIdString(), context)) {
            decrementCounts(context.getTableBucket());
            closeScannerContext(context);
        }
    }

    /** Looks up and removes a scanner by raw ID bytes; no-op if not found. */
    public void removeScanner(byte[] scannerId) {
        String key = new String(scannerId, StandardCharsets.UTF_8);
        ScannerContext context = scanners.get(key);
        if (context != null) {
            removeScanner(context);
        }
    }

    /** Returns the max batch size in bytes allowed for a kv scan response. */
    public int getMaxBatchSizeBytes() {
        return maxBatchSizeBytes;
    }

    @VisibleForTesting
    public int activeScannerCount() {
        return totalScanners.get();
    }

    @VisibleForTesting
    public int activeScannerCountForBucket(TableBucket tableBucket) {
        return perBucketCount.getOrDefault(tableBucket, ZERO).get();
    }

    /**
     * Closes all active scanner sessions for the bucket on a leadership change. Records the IDs in
     * {@link #recentlyExpiredIds} so continuation RPCs after the close surface SCANNER_EXPIRED
     * (recoverable) instead of UNKNOWN_SCANNER_ID.
     */
    public void closeScannersForBucket(TableBucket tableBucket) {
        List<ScannerContext> toRemove = new ArrayList<>();
        for (Map.Entry<String, ScannerContext> entry : scanners.entrySet()) {
            if (tableBucket.equals(entry.getValue().getTableBucket())) {
                toRemove.add(entry.getValue());
            }
        }
        long now = clock.milliseconds();
        for (ScannerContext context : toRemove) {
            LOG.info(
                    "Closing scanner {} for bucket {} due to leadership change.",
                    context.getIdString(),
                    tableBucket);
            // Record before removal so a racing continuation never sees the ID as both unknown
            // and not-recently-expired.
            recentlyExpiredIds.put(context.getIdString(), now);
            removeScanner(context);
        }
        perBucketCount.remove(tableBucket);
    }

    /** Fast best-effort pre-check; the atomic re-check in {@link #registerContext} is the gate. */
    private void checkLimits(TableBucket tableBucket) {
        if (totalScanners.get() >= maxPerServer) {
            throw new TooManyScannersException(
                    String.format(
                            "Cannot create scanner for bucket %s: server-wide limit of %d reached.",
                            tableBucket, maxPerServer));
        }
        if (perBucketCount.getOrDefault(tableBucket, ZERO).get() >= maxPerBucket) {
            throw new TooManyScannersException(
                    String.format(
                            "Cannot create scanner for bucket %s: per-bucket limit of %d reached.",
                            tableBucket, maxPerBucket));
        }
    }

    private void registerContext(ScannerContext context) {
        TableBucket tableBucket = context.getTableBucket();

        int newTotal = totalScanners.incrementAndGet();
        if (newTotal > maxPerServer) {
            totalScanners.decrementAndGet();
            throw new TooManyScannersException(
                    String.format(
                            "Cannot create scanner for bucket %s: server-wide limit of %d reached.",
                            tableBucket, maxPerServer));
        }

        AtomicInteger bucketCount =
                perBucketCount.computeIfAbsent(tableBucket, k -> new AtomicInteger(0));
        int newBucketCount = bucketCount.incrementAndGet();
        if (newBucketCount > maxPerBucket) {
            bucketCount.decrementAndGet();
            totalScanners.decrementAndGet();
            throw new TooManyScannersException(
                    String.format(
                            "Cannot create scanner for bucket %s: per-bucket limit of %d reached.",
                            tableBucket, maxPerBucket));
        }

        scanners.put(context.getIdString(), context);

        LOG.debug(
                "Registered scanner {} for bucket {} (total={}, perBucket={})",
                context.getIdString(),
                tableBucket,
                newTotal,
                newBucketCount);
    }

    private void evictExpiredScanners() {
        if (closed.get()) {
            return;
        }
        long now = clock.milliseconds();

        recentlyExpiredIds
                .entrySet()
                .removeIf(e -> now - e.getValue() > recentlyExpiredRetentionMs);

        for (Map.Entry<String, ScannerContext> entry : scanners.entrySet()) {
            ScannerContext context = entry.getValue();
            if (context.isExpired(scannerTtlMs, now)) {
                if (scanners.remove(entry.getKey(), context)) {
                    LOG.info(
                            "Evicted idle scanner {} for bucket {} (idle > {}ms).",
                            entry.getKey(),
                            context.getTableBucket(),
                            scannerTtlMs);
                    recentlyExpiredIds.put(entry.getKey(), now);
                    decrementCounts(context.getTableBucket());
                    closeScannerContext(context);
                }
            }
        }
    }

    private void decrementCounts(TableBucket bucket) {
        totalScanners.decrementAndGet();
        perBucketCount.computeIfPresent(
                bucket,
                (k, count) -> {
                    int remaining = count.decrementAndGet();
                    return remaining <= 0 ? null : count;
                });
    }

    private void closeScannerContext(ScannerContext context) {
        try {
            context.close();
        } catch (Exception e) {
            LOG.warn(
                    "Error closing scanner {} for bucket {}.",
                    context.getIdString(),
                    context.getTableBucket(),
                    e);
        }
    }

    private static String generateScannerId() {
        return UUID.randomUUID().toString().replace("-", "");
    }

    @Override
    public CompletableFuture<Void> closeAsync() {
        try {
            close();
            return CompletableFuture.completedFuture(null);
        } catch (Exception e) {
            return FutureUtils.completedExceptionally(e);
        }
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }

        if (evictorTask != null) {
            evictorTask.cancel(false);
            evictorTask = null;
        }

        for (Map.Entry<String, ScannerContext> entry : scanners.entrySet()) {
            if (scanners.remove(entry.getKey(), entry.getValue())) {
                decrementCounts(entry.getValue().getTableBucket());
                closeScannerContext(entry.getValue());
            }
        }
        recentlyExpiredIds.clear();
    }
}
