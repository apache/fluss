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

package org.apache.fluss.server.metadata;

import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.exception.SchemaNotExistException;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.SchemaGetter;
import org.apache.fluss.metadata.SchemaInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.server.coordinator.MetadataManager;
import org.apache.fluss.shaded.guava32.com.google.common.cache.Cache;
import org.apache.fluss.shaded.guava32.com.google.common.cache.CacheBuilder;
import org.apache.fluss.shaded.guava32.com.google.common.util.concurrent.UncheckedExecutionException;
import org.apache.fluss.utils.MapUtils;

import javax.annotation.concurrent.ThreadSafe;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

/**
 * Caches and manages schema metadata for tables, providing methods to update and query schema
 * information. Unlike other metadata cached all in {@link ServerMetadataCache}, only required
 * schemas are cached here due to {@link Schema} being a large object. Including two kind of schema:
 * 1. latest schema of each subscribed table, updated by UpdateMetadata request. 2. history schemas
 * of each table, updated by lookup from tablet server.
 */
@ThreadSafe
public class ServerSchemaCache {

    private final MetadataManager metadataManager;
    private final Map<Long, SchemaInfo> latestSchemaByTableId;
    // table_id -> subscriber count
    private final Map<Long, Integer> subscriberCounters;
    private final Cache<TableSchemaKey, Schema> schemaCache;

    public ServerSchemaCache(MetadataManager metadataManager) {
        this.metadataManager = metadataManager;
        // thread safe is guaranteed by subscriberCounters.
        this.subscriberCounters = MapUtils.newConcurrentHashMap();
        this.latestSchemaByTableId = MapUtils.newConcurrentHashMap();
        this.schemaCache = CacheBuilder.newBuilder().maximumSize(100).build();
    }

    public SchemaGetter subscribeWithInitialSchema(
            long tableId, TablePath tablePath, int initialSchemaId, Schema initialSchema) {
        subscriberCounters.compute(
                tableId,
                (key, oldValue) -> {
                    updateSchema(tableId, initialSchemaId, initialSchema);
                    if (oldValue == null) {
                        return 1;
                    } else {
                        return oldValue + 1;
                    }
                });
        return new SchemaMetadataSubscriberImpl(tableId, tablePath);
    }

    public void updateLatestSchema(long tableId, short schemaId, Schema schema) {
        // only update if tablePath is subscribed.
        subscriberCounters.computeIfPresent(
                tableId,
                (key, oldValue) -> {
                    updateSchema(tableId, schemaId, schema);
                    return oldValue;
                });
    }

    private void unsubscribe(long tableId) {
        subscriberCounters.compute(
                tableId,
                (key, oldValue) -> {
                    if (oldValue != null && oldValue > 1) {
                        return oldValue - 1;
                    }
                    latestSchemaByTableId.remove(tableId);
                    return null;
                });
    }

    private Schema getFlussSchema(long tableId, TablePath tablePath, int schemaId)
            throws ExecutionException {
        return schemaCache.get(
                new TableSchemaKey(tableId, schemaId),
                () -> {
                    SchemaInfo schemaInfo = latestSchemaByTableId.get(tableId);
                    if (schemaInfo != null && schemaInfo.getSchemaId() == schemaId) {
                        return latestSchemaByTableId.get(tableId).getSchema();
                    } else {
                        return metadataManager.getSchemaById(tablePath, schemaId).getSchema();
                    }
                });
    }

    private void updateSchema(long tableId, int schemaId, Schema schema) {
        latestSchemaByTableId.compute(
                tableId,
                (key, oldValue) -> {
                    if (oldValue == null || oldValue.getSchemaId() < schemaId) {
                        return new SchemaInfo(schema, schemaId);
                    } else {
                        return oldValue;
                    }
                });

        schemaCache.put(new TableSchemaKey(tableId, schemaId), schema);
    }

    @VisibleForTesting
    public Map<Long, SchemaInfo> getLatestSchemaByTableId() {
        return latestSchemaByTableId;
    }

    @VisibleForTesting
    Map<Long, Integer> getSubscriberCounters() {
        return subscriberCounters;
    }

    private static class TableSchemaKey {
        private final long tableId;
        private final int schemaId;

        TableSchemaKey(long tableId, int schemaId) {
            this.tableId = tableId;
            this.schemaId = schemaId;
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof TableSchemaKey)) {
                return false;
            }
            TableSchemaKey that = (TableSchemaKey) o;
            return schemaId == that.schemaId && Objects.equals(tableId, that.tableId);
        }

        @Override
        public int hashCode() {
            return Objects.hash(tableId, schemaId);
        }
    }

    private class SchemaMetadataSubscriberImpl implements SchemaGetter {
        private final long tableId;
        private final TablePath tablePath;
        private volatile boolean released;

        public SchemaMetadataSubscriberImpl(long tableId, TablePath tablePath) {
            this.tableId = tableId;
            this.tablePath = tablePath;
            this.released = false;
        }

        @Override
        public SchemaInfo getLatestSchemaInfo() {
            return latestSchemaByTableId.get(tableId);
        }

        @Override
        public Schema getSchema(int schemaId) {
            try {
                return getFlussSchema(tableId, tablePath, schemaId);
            } catch (ExecutionException | UncheckedExecutionException executionException) {
                Throwable cause = executionException.getCause();
                if (cause instanceof SchemaNotExistException) {
                    throw (SchemaNotExistException) cause;
                } else {
                    throw new RuntimeException(cause);
                }
            }
        }

        @Override
        public CompletableFuture<SchemaInfo> getSchemaInfoAsync(int schemaId) {
            // TODO: make it async using async zookeeper API.
            return CompletableFuture.completedFuture(new SchemaInfo(getSchema(schemaId), schemaId));
        }

        @Override
        public void release() {
            if (!released) {
                unsubscribe(tableId);
                released = true;
            }
        }
    }
}
