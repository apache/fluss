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

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.rpc.protocol.Errors;
import org.apache.fluss.server.kv.KvTablet;
import org.apache.fluss.server.kv.rocksdb.RocksDBKv;
import org.apache.fluss.utils.AutoCloseableAsync;
import org.apache.fluss.utils.MapUtils;
import org.apache.fluss.utils.clock.Clock;
import org.apache.fluss.utils.clock.SystemClock;
import org.apache.fluss.utils.concurrent.ExecutorThreadFactory;
import org.apache.fluss.utils.concurrent.FutureUtils;

import org.rocksdb.ReadOptions;
import org.rocksdb.RocksIterator;
import org.rocksdb.Snapshot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/** A manager for scanners. */
public class ScannerManager implements AutoCloseableAsync {
    private static final Logger LOG = LoggerFactory.getLogger(ScannerManager.class);

    private final Map<ByteBuffer, ScannerContext> scanners = MapUtils.newConcurrentHashMap();
    private final ScheduledExecutorService cleanupExecutor;
    private final Clock clock;
    private final long scannerTtlMs;

    public ScannerManager(Configuration conf) {
        this(conf, SystemClock.getInstance());
    }

    public ScannerManager(Configuration conf, Clock clock) {
        this.clock = clock;
        this.scannerTtlMs = conf.get(ConfigOptions.SERVER_SCANNER_TTL).toMillis();
        this.cleanupExecutor =
                Executors.newSingleThreadScheduledExecutor(
                        new ExecutorThreadFactory("scanner-cleanup-thread"));
        this.cleanupExecutor.scheduleWithFixedDelay(
                this::cleanupExpiredScanners,
                scannerTtlMs / 2,
                scannerTtlMs / 2,
                TimeUnit.MILLISECONDS);
    }

    public byte[] createScanner(KvTablet kvTablet, long limit) {
        RocksDBKv rocksDBKv = kvTablet.getRocksDBKv();
        Snapshot snapshot = rocksDBKv.getSnapshot();
        ReadOptions readOptions = new ReadOptions().setSnapshot(snapshot);
        RocksIterator iterator = rocksDBKv.newIterator(readOptions);
        iterator.seekToFirst();

        ScannerContext context =
                new ScannerContext(rocksDBKv, iterator, readOptions, snapshot, limit, clock);
        byte[] scannerId = generateScannerId();
        scanners.put(ByteBuffer.wrap(scannerId), context);
        return scannerId;
    }

    public ScannerContext getScanner(byte[] scannerId) {
        ScannerContext context = scanners.get(ByteBuffer.wrap(scannerId));
        if (context != null) {
            context.updateLastAccessTime(clock.milliseconds());
        }
        return context;
    }

    public void keepAlive(byte[] scannerId) {
        ScannerContext context = scanners.get(ByteBuffer.wrap(scannerId));
        if (context != null) {
            context.updateLastAccessTime(clock.milliseconds());
        } else {
            throw Errors.SCANNER_NOT_FOUND_EXCEPTION.exception(
                    "Scanner not found: " + scannerIdToString(scannerId));
        }
    }

    public void removeScanner(byte[] scannerId) {
        ScannerContext context = scanners.remove(ByteBuffer.wrap(scannerId));
        if (context != null) {
            closeScannerContext(context);
        }
    }

    private void cleanupExpiredScanners() {
        long now = clock.milliseconds();
        for (Map.Entry<ByteBuffer, ScannerContext> entry : scanners.entrySet()) {
            if (now - entry.getValue().getLastAccessTime() > scannerTtlMs) {
                ScannerContext context = scanners.remove(entry.getKey());
                if (context != null) {
                    LOG.info(
                            "Scanner {} expired, closing it.",
                            scannerIdToString(entry.getKey().array()));
                    closeScannerContext(context);
                }
            }
        }
    }

    private void closeScannerContext(ScannerContext context) {
        try {
            context.close();
        } catch (Exception e) {
            LOG.error("Fail to close scanner context.", e);
        }
    }

    private byte[] generateScannerId() {
        return UUID.randomUUID().toString().replace("-", "").getBytes(StandardCharsets.UTF_8);
    }

    private String scannerIdToString(byte[] scannerId) {
        return new String(scannerId, StandardCharsets.UTF_8);
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
    public void close() throws Exception {
        cleanupExecutor.shutdownNow();
        for (ScannerContext context : scanners.values()) {
            closeScannerContext(context);
        }
        scanners.clear();
    }
}
