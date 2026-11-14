/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.flink.sink;

import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.FlussRuntimeException;
import org.apache.fluss.metadata.PartitionInfo;
import org.apache.fluss.metadata.TablePath;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Resolves the actual bucket count of a partition at runtime for the pre-write bucket shuffle.
 *
 * <p>After a bucket num rescale, partitions of the same table may have different bucket counts:
 * existing partitions keep their counts while partitions created afterwards use the new table-level
 * default. A sharding decision based on the table-level value captured at job submission time
 * therefore scatters the records of one bucket across multiple writer subtasks, which eventually
 * breaks sink recovery (conflicting per-bucket offsets in the WriterState).
 *
 * <p>This resolver lazily fetches the authoritative bucket count of a partition from cluster
 * metadata and caches it locally. A partition's bucket count is immutable once the partition is
 * created, so a cached entry is valid forever and no invalidation is ever needed. When a partition
 * is not found in the metadata (a dynamically created partition whose creation is triggered by the
 * downstream writer, i.e. after this sharding step), the resolver falls back to the current
 * table-level bucket number, which is exactly what the partition will be created with.
 */
public class PartitionBucketCountResolver implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(PartitionBucketCountResolver.class);

    private static final int MAX_RETRIES = 3;
    private static final long RETRY_INTERVAL_MS = 100L;

    private final TablePath tablePath;
    private final Configuration flussConfig;
    private final int defaultBucketCount;

    private transient Connection connection;
    private transient Admin admin;
    private transient ConcurrentHashMap<String, Integer> bucketCounts;

    /** Fetches partition metadata from the cluster. Implementations must be thread-safe. */
    interface PartitionMetadataFetcher extends Serializable {

        /**
         * Returns all partitions of the table, or {@code null} if the table itself is not
         * retrievable (e.g. concurrently dropped).
         */
        @Nullable
        List<PartitionInfo> listPartitionInfos(TablePath tablePath) throws Exception;

        /** Returns the current table-level bucket number. */
        int tableBucketCount(TablePath tablePath) throws Exception;
    }

    public PartitionBucketCountResolver(
            TablePath tablePath, Configuration flussConfig, int defaultBucketCount) {
        this.tablePath = tablePath;
        this.flussConfig = flussConfig;
        this.defaultBucketCount = defaultBucketCount;
    }

    /**
     * Returns the actual bucket count of the given partition, fetching and caching it on the first
     * access. Cached entries are valid forever because a partition's bucket count never changes.
     */
    public int bucketCountOf(String partitionName) {
        Integer cached = bucketCounts().get(partitionName);
        if (cached != null) {
            return cached;
        }
        return resolveAndCache(partitionName);
    }

    private int resolveAndCache(String partitionName) {
        Integer cached = bucketCounts().get(partitionName);
        if (cached != null) {
            return cached;
        }

        List<PartitionInfo> infos = fetchPartitionInfosWithRetry();
        if (infos != null) {
            // Cache every partition carried by the response: one RPC warms up the whole table.
            for (PartitionInfo info : infos) {
                bucketCounts().putIfAbsent(info.getPartitionName(), info.getBucketCount());
            }
        }

        Integer resolved = bucketCounts().get(partitionName);
        if (resolved != null) {
            return resolved;
        }

        // The partition doesn't exist in the metadata yet: its creation is triggered by the
        // downstream writer after the sharding step, so blocking here would deadlock. Fall back
        // to the current table-level bucket count, which is exactly what the partition will be
        // created with; the fallback value is cached for the job's lifetime. The only problematic
        // window is a rescale landing between this fallback and the actual creation: the
        // partition is then created with the new table-level count while the cached value
        // remains the old one, so this partition keeps sharding with a wrong count until
        // restart.
        int fallbackBucketCount = tableBucketCountWithRetry();
        LOG.debug(
                "Partition {} not found for table {}, fall back to the table-level bucket count {}.",
                partitionName,
                tablePath,
                fallbackBucketCount);
        bucketCounts().put(partitionName, fallbackBucketCount);
        return fallbackBucketCount;
    }

    private List<PartitionInfo> fetchPartitionInfosWithRetry() {
        return fetchWithRetry(
                "list partition infos", () -> fetcher().listPartitionInfos(tablePath));
    }

    private int tableBucketCountWithRetry() {
        return fetchWithRetry(
                "get the table-level bucket count", () -> fetcher().tableBucketCount(tablePath));
    }

    /**
     * Invokes the metadata call with bounded retries: up to {@link #MAX_RETRIES} attempts with a
     * short interval in between, failing fast with a {@link FlussRuntimeException} once exhausted.
     */
    private <T> T fetchWithRetry(String operation, Callable<T> call) {
        Exception lastException = null;
        for (int attempt = 1; attempt <= MAX_RETRIES; attempt++) {
            try {
                return call.call();
            } catch (Exception e) {
                lastException = e;
                LOG.warn(
                        "Failed to {} for table {} (attempt {}/{}).",
                        operation,
                        tablePath,
                        attempt,
                        MAX_RETRIES,
                        e);
                sleepBeforeRetry();
            }
        }
        throw new FlussRuntimeException(
                String.format(
                        "Failed to %s for table %s after %d retries.",
                        operation, tablePath, MAX_RETRIES),
                lastException);
    }

    private void sleepBeforeRetry() {
        try {
            Thread.sleep(RETRY_INTERVAL_MS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new FlussRuntimeException("Interrupted while retrying metadata fetch.", e);
        }
    }

    private ConcurrentHashMap<String, Integer> bucketCounts() {
        if (bucketCounts == null) {
            bucketCounts = new ConcurrentHashMap<>();
        }
        return bucketCounts;
    }

    protected PartitionMetadataFetcher fetcher() {
        // Static sharding mode: never touch metadata, always fall back to the default count.
        if (flussConfig == null) {
            return new StaticPartitionMetadataFetcher(defaultBucketCount);
        }
        return new AdminPartitionMetadataFetcher(this);
    }

    /** Production fetcher backed by a lazily created Fluss {@link Admin}. */
    private static class AdminPartitionMetadataFetcher implements PartitionMetadataFetcher {

        private static final long serialVersionUID = 1L;

        private final PartitionBucketCountResolver owner;

        private AdminPartitionMetadataFetcher(PartitionBucketCountResolver owner) {
            this.owner = owner;
        }

        @Nullable
        @Override
        public List<PartitionInfo> listPartitionInfos(TablePath tablePath) throws Exception {
            return owner.admin().listPartitionInfos(tablePath).get();
        }

        @Override
        public int tableBucketCount(TablePath tablePath) throws Exception {
            return owner.admin().getTableInfo(tablePath).get().getNumBuckets();
        }
    }

    /** Fallback fetcher used when no Fluss config is provided: always reports the default count. */
    private static class StaticPartitionMetadataFetcher implements PartitionMetadataFetcher {

        private static final long serialVersionUID = 1L;

        private final int defaultBucketCount;

        private StaticPartitionMetadataFetcher(int defaultBucketCount) {
            this.defaultBucketCount = defaultBucketCount;
        }

        @Override
        public List<PartitionInfo> listPartitionInfos(TablePath tablePath) {
            return null;
        }

        @Override
        public int tableBucketCount(TablePath tablePath) {
            return defaultBucketCount;
        }
    }

    private Admin admin() {
        if (admin == null) {
            connection = ConnectionFactory.createConnection(flussConfig);
            admin = connection.getAdmin();
        }
        return admin;
    }
}
