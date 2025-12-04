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
import org.apache.fluss.utils.concurrent.ExecutorThreadFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * A client that lookups value of keys from server.
 *
 * <p>The lookup client contains of a queue of pending lookup operations and background I/O threads
 * that is responsible for turning these lookup operations into network requests and transmitting
 * them to the cluster.
 *
 * <p>The {@link #lookup(TablePath, TableBucket, byte[])} method is asynchronous, when called, it
 * adds the lookup operation to a queue of pending lookup operations and immediately returns. This
 * allows the lookup operations to batch together individual lookup operations for efficiency.
 */
@ThreadSafe
@Internal
public class LookupClient {

    private static final Logger LOG = LoggerFactory.getLogger(LookupClient.class);

    public static final String LOOKUP_THREAD_PREFIX = "fluss-lookup-sender";

    private final LookupQueue lookupQueue;

    private final ExecutorService lookupSenderThreadPool;
    private final LookupSender lookupSender;

    public LookupClient(Configuration conf, MetadataUpdater metadataUpdater) {
        this.lookupQueue = new LookupQueue(conf);
        this.lookupSenderThreadPool = createThreadPool();
        this.lookupSender =
                new LookupSender(
                        metadataUpdater,
                        lookupQueue,
                        conf.getInt(ConfigOptions.CLIENT_LOOKUP_MAX_INFLIGHT_SIZE),
                        conf.getInt(ConfigOptions.CLIENT_LOOKUP_MAX_RETRIES));
        lookupSenderThreadPool.submit(lookupSender);
    }

    private ExecutorService createThreadPool() {
        // according to benchmark, increase the thread pool size improve not so much
        // performance, so we always use 1 thread for simplicity.
        return Executors.newFixedThreadPool(1, new ExecutorThreadFactory(LOOKUP_THREAD_PREFIX));
    }

    public CompletableFuture<byte[]> lookup(
            TablePath tablePath, TableBucket tableBucket, byte[] keyBytes) {
        LookupQuery lookup = new LookupQuery(tablePath, tableBucket, keyBytes);
        lookupQueue.appendLookup(lookup);
        return lookup.future();
    }

    public CompletableFuture<List<byte[]>> prefixLookup(
            TablePath tablePath, TableBucket tableBucket, byte[] keyBytes) {
        PrefixLookupQuery prefixLookup = new PrefixLookupQuery(tablePath, tableBucket, keyBytes);
        lookupQueue.appendLookup(prefixLookup);
        return prefixLookup.future();
    }

    public void close(Duration timeout) {
        LOG.info("Closing lookup client and lookup sender.");

        if (lookupSender != null) {
            lookupSender.initiateClose();
        }

        if (lookupSenderThreadPool != null) {
            lookupSenderThreadPool.shutdown();
            try {
                if (lookupSenderThreadPool.awaitTermination(
                        timeout.toMillis(), TimeUnit.MILLISECONDS)) {
                    lookupSenderThreadPool.shutdownNow();

                    if (!lookupSenderThreadPool.awaitTermination(
                            timeout.toMillis(), TimeUnit.MILLISECONDS)) {
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
