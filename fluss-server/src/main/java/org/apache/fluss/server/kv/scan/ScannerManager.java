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
import org.apache.fluss.server.kv.KvTablet;
import org.apache.fluss.utils.AutoCloseableAsync;
import org.apache.fluss.utils.MapUtils;
import org.apache.fluss.utils.clock.Clock;
import org.apache.fluss.utils.clock.SystemClock;
import org.apache.fluss.utils.concurrent.FutureUtils;
import org.apache.fluss.utils.concurrent.Scheduler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Manages server-side KV full-scan sessions ({@link ScannerContext}).
 *
 * <p>Each KV full scan opens a persistent server-side session that holds a point-in-time RocksDB
 * snapshot and a cursor. Sessions are keyed by a server-assigned UUID-based scanner ID and persist
 * across multiple batched-fetch RPCs from the same client.
 *
 * <h3>Concurrency limits</h3>
 *
 * <ul>
 *   <li><b>Per-bucket:</b> at most {@code maxPerBucket} concurrent sessions on any single bucket.
 *   <li><b>Per-server:</b> at most {@code maxPerServer} concurrent sessions across all buckets.
 * </ul>
 *
 * <p>Limit enforcement is two-phase: a fast pre-check guards the common case; the subsequent atomic
 * increment with re-check and rollback prevents the TOCTOU race from permanently breaching the
 * configured limits. Exceeding either limit causes {@link TooManyScannersException}.
 *
 * <h3>Empty bucket handling</h3>
 *
 * <p>If the target bucket contains no rows at the time the scan is opened, {@link
 * #createScanner(KvTablet, TableBucket, Long)} returns {@code null} without consuming a limit slot.
 * The caller should return an empty response immediately.
 *
 * <h3>TTL eviction</h3>
 *
 * <p>A background reaper task runs every {@code server.scanner.expiration-interval} and evicts
 * sessions idle longer than {@code server.scanner.ttl}. Recently evicted IDs are retained for
 * {@code 2 × ttl} so callers can distinguish "expired" from "never existed."
 *
 * <h3>Leadership change</h3>
 *
 * {@link #closeScannersForBucket(TableBucket)} must be called when a bucket loses leadership to
 * release all RocksDB snapshot/iterator resources for that bucket promptly.
 */
public class ScannerManager implements AutoCloseableAsync {

    private static final Logger LOG = LoggerFactory.getLogger(ScannerManager.class);

    private final Map<String, ScannerContext> scanners = MapUtils.newConcurrentHashMap();
    private final Map<String, Long> recentlyExpiredIds = MapUtils.newConcurrentHashMap();

    /** Per-bucket active scanner count, used for O(1) per-bucket limit enforcement. */
    private final Map<TableBucket, AtomicInteger> perBucketCount = MapUtils.newConcurrentHashMap();

    /** Total active scanner count across all buckets on this tablet server. */
    private final AtomicInteger totalScanners = new AtomicInteger(0);

    private final Clock clock;
    private final long scannerTtlMs;
    private final long recentlyExpiredRetentionMs;
    private final int maxPerBucket;
    private final int maxPerServer;

    @Nullable private ScheduledFuture<?> cleanupTask;

    public ScannerManager(Configuration conf, Scheduler scheduler) {
        this(conf, scheduler, SystemClock.getInstance());
    }

    @VisibleForTesting
    ScannerManager(Configuration conf, Scheduler scheduler, Clock clock) {
        this.clock = clock;
        this.scannerTtlMs = conf.get(ConfigOptions.SERVER_SCANNER_TTL).toMillis();
        this.recentlyExpiredRetentionMs = 2 * scannerTtlMs;
        this.maxPerBucket = conf.get(ConfigOptions.SERVER_SCANNER_MAX_PER_BUCKET);
        this.maxPerServer = conf.get(ConfigOptions.SERVER_SCANNER_MAX_PER_SERVER);

        long expirationIntervalMs =
                conf.get(ConfigOptions.SERVER_SCANNER_EXPIRATION_INTERVAL).toMillis();
        this.cleanupTask =
                scheduler.schedule(
                        "scanner-expiration",
                        this::cleanupExpiredScanners,
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
     * Creates a new scan session for the given bucket, taking a point-in-time RocksDB snapshot.
     *
     * <p>Returns {@code null} if the bucket is empty (no rows to scan). In that case no session
     * slot is consumed and the caller should return an empty response immediately.
     *
     * <p><b>Limit enforcement is two-phase:</b> a fast pre-check guards the common case; the
     * subsequent atomic increment + re-check prevents the TOCTOU race from permanently breaching
     * configured limits. If registration fails after the snapshot is already opened, the context is
     * closed and the exception is re-thrown to avoid leaking resources.
     *
     * @param kvTablet the {@link KvTablet} for the bucket; used to open the snapshot
     * @param tableBucket the bucket being scanned
     * @param limit optional row-count limit ({@code null} or ≤ 0 means unlimited)
     * @return the newly registered {@link ScannerContext}, or {@code null} if the bucket is empty
     * @throws TooManyScannersException if the per-bucket or per-server limit is exceeded
     * @throws IOException if the underlying {@link org.apache.fluss.server.utils.ResourceGuard} is
     *     already closed (the bucket is shutting down)
     */
    @Nullable
    public ScannerContext createScanner(
            KvTablet kvTablet, TableBucket tableBucket, @Nullable Long limit) throws IOException {
        checkLimits(tableBucket);

        String scannerId = generateScannerId();
        ScannerContext context =
                kvTablet.openScan(scannerId, limit != null ? limit : -1L, clock.milliseconds());
        if (context == null) {
            // Bucket is empty — no session slot consumed.
            return null;
        }

        try {
            registerContext(context, tableBucket);
        } catch (TooManyScannersException e) {
            // Limit was exceeded between the initial check and registration (race window).
            // Close the already-opened context to avoid leaking the snapshot and lease.
            closeScannerContext(context);
            throw e;
        }
        return context;
    }

    /**
     * Looks up an existing scanner session by its raw ID bytes and refreshes its last-access
     * timestamp.
     *
     * @return the {@link ScannerContext}, or {@code null} if not found (may have expired or never
     *     existed)
     */
    @Nullable
    public ScannerContext getScanner(byte[] scannerId) {
        ScannerContext context = scanners.get(new String(scannerId, StandardCharsets.UTF_8));
        if (context != null) {
            context.updateLastAccessTime(clock.milliseconds());
        }
        return context;
    }

    /**
     * Returns {@code true} if the given scanner ID belongs to a session that was recently evicted
     * by the TTL reaper (within the last {@code 2 × ttlMs}).
     *
     * <p>Callers can use this to distinguish "scanner expired" from "unknown scanner ID."
     */
    public boolean isRecentlyExpired(byte[] scannerId) {
        return recentlyExpiredIds.containsKey(new String(scannerId, StandardCharsets.UTF_8));
    }

    /**
     * Removes and closes a known scanner context directly, avoiding a map lookup.
     *
     * <p>Uses a conditional remove ({@link java.util.concurrent.ConcurrentHashMap#remove(Object,
     * Object)}) so that concurrent calls — e.g. from the TTL reaper and a close-scanner RPC
     * arriving simultaneously — result in exactly one winner closing the context, preventing
     * double-release of the non-idempotent {@link
     * org.apache.fluss.server.utils.ResourceGuard.Lease}.
     */
    public void removeScanner(ScannerContext context) {
        if (scanners.remove(context.getId(), context)) {
            decrementCounts(context.getTableBucket());
            closeScannerContext(context);
        }
    }

    /**
     * Looks up and removes a scanner session by its raw ID bytes.
     *
     * <p>No-op if the ID is not found (already removed or expired).
     */
    public void removeScanner(byte[] scannerId) {
        String key = new String(scannerId, StandardCharsets.UTF_8);
        ScannerContext context = scanners.remove(key);
        if (context != null) {
            decrementCounts(context.getTableBucket());
            closeScannerContext(context);
        }
    }

    /** Returns the total number of active scanner sessions on this tablet server. */
    @VisibleForTesting
    public int activeScannerCount() {
        return totalScanners.get();
    }

    /** Returns the number of active scanner sessions for the given bucket. */
    @VisibleForTesting
    public int activeScannerCountForBucket(TableBucket tableBucket) {
        AtomicInteger count = perBucketCount.get(tableBucket);
        return count == null ? 0 : count.get();
    }

    /**
     * Closes and removes all active scanner sessions for the given bucket. Must be called when a
     * bucket loses leadership to prevent stale RocksDB snapshot/iterator leaks.
     */
    public void closeScannersForBucket(TableBucket tableBucket) {
        List<String> toRemove = new ArrayList<>();
        for (Map.Entry<String, ScannerContext> entry : scanners.entrySet()) {
            if (tableBucket.equals(entry.getValue().getTableBucket())) {
                toRemove.add(entry.getKey());
            }
        }
        for (String key : toRemove) {
            ScannerContext context = scanners.remove(key);
            if (context != null) {
                decrementCounts(tableBucket);
                LOG.info(
                        "Closing scanner {} for bucket {} due to leadership change.",
                        key,
                        tableBucket);
                closeScannerContext(context);
            }
        }
    }

    /**
     * Fast pre-check of per-server and per-bucket limits before opening the snapshot. This is a
     * best-effort check; a small race window exists between the check and {@link #registerContext}.
     * The race is handled by the atomic re-check inside {@link #registerContext}.
     */
    private void checkLimits(TableBucket tableBucket) {
        if (totalScanners.get() >= maxPerServer) {
            throw new TooManyScannersException(
                    String.format(
                            "Cannot create scanner for bucket %s: server-wide limit of %d reached.",
                            tableBucket, maxPerServer));
        }
        AtomicInteger bucketCount =
                perBucketCount.computeIfAbsent(tableBucket, k -> new AtomicInteger(0));
        if (bucketCount.get() >= maxPerBucket) {
            throw new TooManyScannersException(
                    String.format(
                            "Cannot create scanner for bucket %s: per-bucket limit of %d reached.",
                            tableBucket, maxPerBucket));
        }
    }

    /**
     * Atomically increments the counters and puts the context in the map. Throws {@link
     * TooManyScannersException} and rolls back the increments if a concurrent create caused either
     * limit to be exceeded between the initial check and this call.
     */
    private void registerContext(ScannerContext context, TableBucket tableBucket) {
        AtomicInteger bucketCount =
                perBucketCount.computeIfAbsent(tableBucket, k -> new AtomicInteger(0));

        int newTotal = totalScanners.incrementAndGet();
        if (newTotal > maxPerServer) {
            totalScanners.decrementAndGet();
            throw new TooManyScannersException(
                    String.format(
                            "Cannot create scanner for bucket %s: server-wide limit of %d reached.",
                            tableBucket, maxPerServer));
        }

        int newBucketCount = bucketCount.incrementAndGet();
        if (newBucketCount > maxPerBucket) {
            bucketCount.decrementAndGet();
            totalScanners.decrementAndGet();
            throw new TooManyScannersException(
                    String.format(
                            "Cannot create scanner for bucket %s: per-bucket limit of %d reached.",
                            tableBucket, maxPerBucket));
        }

        scanners.put(context.getId(), context);

        LOG.debug(
                "Registered scanner {} for bucket {} (total={}, perBucket={})",
                context.getId(),
                tableBucket,
                newTotal,
                newBucketCount);
    }

    /** TTL reaper — invoked periodically by the background scheduler. */
    private void cleanupExpiredScanners() {
        long now = clock.milliseconds();

        // Prune stale entries from the recently-expired cache to bound memory usage.
        recentlyExpiredIds
                .entrySet()
                .removeIf(e -> now - e.getValue() > recentlyExpiredRetentionMs);

        for (Map.Entry<String, ScannerContext> entry : scanners.entrySet()) {
            ScannerContext context = entry.getValue();
            if (context.isExpired(scannerTtlMs, now)) {
                // Conditional remove prevents double-close if removeScanner() fires concurrently.
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
                    context.getId(),
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
        if (cleanupTask != null) {
            cleanupTask.cancel(false);
            cleanupTask = null;
        }

        for (Map.Entry<String, ScannerContext> entry : scanners.entrySet()) {
            if (scanners.remove(entry.getKey(), entry.getValue())) {
                decrementCounts(entry.getValue().getTableBucket());
                closeScannerContext(entry.getValue());
            }
        }

        recentlyExpiredIds.clear();
        totalScanners.set(0);
    }
}
