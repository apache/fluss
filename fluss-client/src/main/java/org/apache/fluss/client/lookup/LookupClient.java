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

package org.apache.fluss.client.lookup;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.client.metadata.MetadataUpdater;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.utils.ExponentialBackoff;
import org.apache.fluss.utils.concurrent.ExecutorThreadFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.concurrent.FutureUtils.completedExceptionally;

/**
 * A client that lookups value of keys from server.
 *
 * <p>The lookup client contains of a queue of pending lookup operations and background I/O threads
 * that is responsible for turning these lookup operations into network requests and transmitting
 * them to the cluster.
 *
 * <p>The {@link #lookup(TablePath, TableBucket, byte[], boolean, String)} method is asynchronous,
 * when called, it adds the lookup operation to a queue of pending lookup operations and immediately
 * returns. This allows the lookup operations to batch together individual lookup operations for
 * efficiency.
 */
@ThreadSafe
@Internal
public class LookupClient {

    private static final Logger LOG = LoggerFactory.getLogger(LookupClient.class);

    public static final String LOOKUP_THREAD_PREFIX = "fluss-lookup-sender";

    private final LookupQueue lookupQueue;

    private final ExecutorService lookupSenderThreadPool;
    private final LookupSender lookupSender;
    private final Set<CompletableFuture<?>> pendingPartitionRouting = new HashSet<>();
    private CompletableFuture<Void> lastPartitionRouting = CompletableFuture.completedFuture(null);
    private volatile Thread partitionRoutingThread;
    private boolean closing;
    private volatile boolean partitionRoutingClosed;

    public LookupClient(Configuration conf, MetadataUpdater metadataUpdater) {
        ExponentialBackoff retryBackoff = createRetryBackoff(conf);
        this.lookupQueue = new LookupQueue(conf);
        this.lookupSenderThreadPool = createThreadPool();
        short acks = configureAcks(conf);
        this.lookupSender =
                new LookupSender(
                        metadataUpdater,
                        lookupQueue,
                        conf.getInt(ConfigOptions.CLIENT_LOOKUP_MAX_INFLIGHT_SIZE),
                        conf.getInt(ConfigOptions.CLIENT_LOOKUP_MAX_RETRIES),
                        retryBackoff,
                        acks,
                        (int) conf.get(ConfigOptions.CLIENT_REQUEST_TIMEOUT).toMillis());
        lookupSenderThreadPool.submit(lookupSender);
    }

    private static ExponentialBackoff createRetryBackoff(Configuration conf) {
        Duration initial = conf.get(ConfigOptions.CLIENT_LOOKUP_RETRY_BACKOFF);
        Duration maximum = conf.get(ConfigOptions.CLIENT_LOOKUP_RETRY_BACKOFF_MAX);
        checkArgument(
                !initial.isNegative()
                        && initial.compareTo(maximum) <= 0
                        && maximum.compareTo(Duration.ofMillis(Integer.MAX_VALUE)) <= 0,
                "%s and %s must satisfy 0ms <= initial <= maximum <= %sms.",
                ConfigOptions.CLIENT_LOOKUP_RETRY_BACKOFF.key(),
                ConfigOptions.CLIENT_LOOKUP_RETRY_BACKOFF_MAX.key(),
                Integer.MAX_VALUE);
        return new ExponentialBackoff(initial.toMillis(), 2, maximum.toMillis(), 0.2);
    }

    private short configureAcks(Configuration conf) {
        String acks = conf.get(ConfigOptions.CLIENT_WRITER_ACKS);
        short ack;
        if (acks.equals("all")) {
            ack = -1;
        } else {
            ack = Short.parseShort(acks);
        }

        return ack;
    }

    private ExecutorService createThreadPool() {
        // according to benchmark, increase the thread pool size improve not so much
        // performance, so we always use 1 thread for simplicity.
        return Executors.newFixedThreadPool(1, new ExecutorThreadFactory(LOOKUP_THREAD_PREFIX));
    }

    /**
     * Looks up a key from the specified table bucket.
     *
     * @param tablePath path of the table to look up
     * @param tableBucket bucket to look up
     * @param keyBytes serialized key to look up
     * @param insertIfNotExists whether to insert the key when it does not exist
     * @param originalPartitionName null for a regular lookup against an active Fluss partition; the
     *     original partition name for a historical lookup against a partition whose Fluss data has
     *     expired and is looked up from lake storage
     * @return a future containing the serialized value, or null if the key does not exist
     */
    public CompletableFuture<byte[]> lookup(
            TablePath tablePath,
            TableBucket tableBucket,
            byte[] keyBytes,
            boolean insertIfNotExists,
            @Nullable String originalPartitionName,
            int bucketCount) {
        LookupQuery lookup =
                new LookupQuery(
                        tablePath,
                        tableBucket,
                        keyBytes,
                        insertIfNotExists,
                        originalPartitionName,
                        bucketCount);
        lookupQueue.appendLookup(lookup);
        return lookup.future();
    }

    public CompletableFuture<List<byte[]>> prefixLookup(
            TablePath tablePath, TableBucket tableBucket, byte[] keyBytes, int bucketCount) {
        PrefixLookupQuery prefixLookup =
                new PrefixLookupQuery(tablePath, tableBucket, keyBytes, bucketCount);
        lookupQueue.appendLookup(prefixLookup);
        return prefixLookup.future();
    }

    synchronized CompletableFuture<LookupResult> resolvePartitionLookup(
            Supplier<CompletableFuture<LookupResult>> resolveAndSend) {
        // An accepted lookup may need to resolve the historical partition during close.
        if (partitionRoutingClosed
                || (closing && Thread.currentThread() != partitionRoutingThread)) {
            return completedExceptionally(
                    new IllegalStateException("The lookup client is closed."));
        }
        // Serialize cache misses so later lookups can use the first refresh. Data requests
        // can batch as soon as routing finishes, without waiting for earlier data responses.
        CompletableFuture<CompletableFuture<LookupResult>> routed =
                lastPartitionRouting.thenApplyAsync(
                        ignored -> {
                            if (partitionRoutingClosed) {
                                throw new IllegalStateException("The lookup client is closed.");
                            }
                            partitionRoutingThread = Thread.currentThread();
                            try {
                                return resolveAndSend.get();
                            } finally {
                                partitionRoutingThread = null;
                            }
                        });
        pendingPartitionRouting.add(routed);
        lastPartitionRouting =
                routed.handle(
                        (result, error) -> {
                            synchronized (this) {
                                pendingPartitionRouting.remove(routed);
                                if (pendingPartitionRouting.isEmpty()) {
                                    notifyAll();
                                }
                            }
                            return null;
                        });
        // Aborting routing on close must not run user completion callbacks on the closing thread.
        return lastPartitionRouting.thenComposeAsync(ignored -> routed.join());
    }

    private long initiateClose(long timeoutMs) {
        long startNanos = System.nanoTime();
        long remainingMs = timeoutMs;
        CompletableFuture<?>[] abandoned;
        synchronized (this) {
            closing = true;
            while (!pendingPartitionRouting.isEmpty() && remainingMs > 0) {
                try {
                    wait(remainingMs);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
                remainingMs =
                        Math.max(
                                0L,
                                timeoutMs
                                        - TimeUnit.NANOSECONDS.toMillis(
                                                System.nanoTime() - startNanos));
            }
            partitionRoutingClosed = true;
            abandoned = pendingPartitionRouting.toArray(new CompletableFuture<?>[0]);
        }
        lookupSender.initiateClose();
        for (CompletableFuture<?> routing : abandoned) {
            routing.completeExceptionally(
                    new IllegalStateException("The lookup client is closed."));
        }
        return remainingMs;
    }

    /** Closes the sender after draining pending partition routing or reaching the timeout. */
    public void close(Duration timeout) {
        LOG.info("Closing lookup client and lookup sender.");
        long remainingMs = initiateClose(Math.max(0L, timeout.toMillis()));

        if (lookupSenderThreadPool != null) {
            lookupSenderThreadPool.shutdown();
            try {
                if (!lookupSenderThreadPool.awaitTermination(remainingMs, TimeUnit.MILLISECONDS)) {
                    lookupSenderThreadPool.shutdownNow();

                    if (!lookupSenderThreadPool.awaitTermination(
                            remainingMs, TimeUnit.MILLISECONDS)) {
                        LOG.error("Failed to shutdown lookup client.");
                    }
                }
            } catch (InterruptedException e) {
                lookupSenderThreadPool.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }

        if (lookupSender != null) {
            lookupSender.forceClose();
        }
        LOG.info("Lookup client closed.");
    }
}
