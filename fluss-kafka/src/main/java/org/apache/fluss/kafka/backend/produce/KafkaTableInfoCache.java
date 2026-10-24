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

package org.apache.fluss.kafka.backend.produce;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.rpc.messages.GetTableInfoResponse;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

import javax.annotation.concurrent.ThreadSafe;

import java.util.Arrays;
import java.util.Objects;
import java.util.function.Supplier;

import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

/**
 * Bounded cache for parsed Kafka topic {@link TableInfo} snapshots.
 *
 * <p>The cache never replaces the authoritative {@code GetTableInfo} call. Every Produce request
 * still obtains a fresh response (and therefore retains its authorization and freshness semantics).
 * This cache only avoids parsing an unchanged descriptor JSON response again.
 *
 * <p>An entry is reused only when all response identity fields and the complete descriptor bytes
 * match. Comparing the complete descriptor is intentional: a {@code kafka.*} property change may
 * alter decoding semantics without changing the schema ID. Atomic updates serialize parsing for one
 * table, while independent topics remain concurrent.
 */
@Internal
@ThreadSafe
final class KafkaTableInfoCache {

    static final long DEFAULT_MAXIMUM_SIZE = 1_000L;

    private final Cache<TablePath, CacheEntry> entries;

    KafkaTableInfoCache() {
        this(DEFAULT_MAXIMUM_SIZE);
    }

    KafkaTableInfoCache(long maximumSize) {
        checkArgument(maximumSize > 0, "Kafka TableInfo cache size must be greater than zero.");
        this.entries = Caffeine.newBuilder().maximumSize(maximumSize).build();
    }

    TableInfo getOrLoad(
            TablePath tablePath,
            GetTableInfoResponse response,
            Supplier<TableInfo> tableInfoLoader) {
        checkNotNull(tablePath);
        checkNotNull(response);
        checkNotNull(tableInfoLoader);

        return entries.asMap()
                .compute(
                        tablePath,
                        (path, entry) ->
                                entry != null && entry.matches(response)
                                        ? entry
                                        : new CacheEntry(
                                                response, checkNotNull(tableInfoLoader.get())))
                .tableInfo;
    }

    void invalidate(TablePath tablePath) {
        entries.invalidate(checkNotNull(tablePath));
    }

    long estimatedSize() {
        entries.cleanUp();
        return entries.estimatedSize();
    }

    private static final class CacheEntry {
        private final long tableId;
        private final int schemaId;
        private final byte[] tableJson;
        private final long createdTime;
        private final long modifiedTime;
        private final long bucketCountEpoch;
        private final String remoteDataDir;
        private final TableInfo tableInfo;

        private CacheEntry(GetTableInfoResponse response, TableInfo tableInfo) {
            this.tableId = response.getTableId();
            this.schemaId = response.getSchemaId();
            this.tableJson = response.getTableJson().clone();
            this.createdTime = response.getCreatedTime();
            this.modifiedTime = response.getModifiedTime();
            this.bucketCountEpoch =
                    response.hasBucketCountEpoch() ? response.getBucketCountEpoch() : 0L;
            this.remoteDataDir = response.hasRemoteDataDir() ? response.getRemoteDataDir() : null;
            this.tableInfo = tableInfo;
        }

        private boolean matches(GetTableInfoResponse response) {
            return tableId == response.getTableId()
                    && schemaId == response.getSchemaId()
                    && createdTime == response.getCreatedTime()
                    && modifiedTime == response.getModifiedTime()
                    && bucketCountEpoch
                            == (response.hasBucketCountEpoch()
                                    ? response.getBucketCountEpoch()
                                    : 0L)
                    && Objects.equals(
                            remoteDataDir,
                            response.hasRemoteDataDir() ? response.getRemoteDataDir() : null)
                    && Arrays.equals(tableJson, response.getTableJson());
        }
    }
}
