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

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;

/**
 * Caches and manages schema metadata for tables, providing methods to update and query schema
 * information. Unlike other metadata cached all in {@link ServerMetadataCache}, only required
 * schemas are cached here due to {@link Schema} being a large object. Including two kind of schema:
 * 1. latest schema of each subscribed table, updated by UpdateMetadata request. 2. history schemas
 * of each table, updated by lookup from tablet server.
 */
public class SchemaMetadataManager {

    private MetadataManager metadataManager;
    private final Map<TablePath, SchemaInfo> latestSchemaByTablePath;
    private final Map<TablePath, Integer> subscriberCounters;
    private final Cache<TableSchemaKey, Schema> schemaCache;

    public SchemaMetadataManager(MetadataManager metadataManager) {
        this.metadataManager = metadataManager;
        // thread safe is guaranteed by subscriberCounters.
        this.subscriberCounters = MapUtils.newConcurrentHashMap();
        this.latestSchemaByTablePath = MapUtils.newConcurrentHashMap();
        this.schemaCache = CacheBuilder.newBuilder().maximumSize(100).build();
    }

    public SchemaGetter subscribeWithInitialSchema(
            TablePath tablePath, int initialSchemaId, Schema initialSchema) {
        subscriberCounters.compute(
                tablePath,
                (key, oldValue) -> {
                    updateSchema(tablePath, initialSchemaId, initialSchema);
                    if (oldValue == null) {
                        return 1;
                    } else {
                        return oldValue + 1;
                    }
                });
        return new SchemaMetadataSubscriberImpl(tablePath);
    }

    public void updateLatestSchema(TablePath tablePath, short schemaId, Schema schema) {
        // only update if tablePath is subscribed.
        subscriberCounters.computeIfPresent(
                tablePath,
                (key, oldValue) -> {
                    updateSchema(tablePath, schemaId, schema);
                    return oldValue;
                });
    }

    private void unsubscribe(TablePath tablePath) {
        subscriberCounters.compute(
                tablePath,
                (key, oldValue) -> {
                    if (oldValue != null && oldValue > 1) {
                        return oldValue - 1;
                    }
                    latestSchemaByTablePath.remove(tablePath);
                    return null;
                });
    }

    private Schema getFlussSchema(TablePath tablePath, int schemaId) throws ExecutionException {
        return schemaCache.get(
                new TableSchemaKey(tablePath, schemaId),
                () -> {
                    SchemaInfo schemaInfo = latestSchemaByTablePath.get(tablePath);
                    if (schemaInfo != null && schemaInfo.getSchemaId() == schemaId) {
                        return latestSchemaByTablePath.get(tablePath).getSchema();
                    } else {
                        return metadataManager.getSchemaById(tablePath, schemaId).getSchema();
                    }
                });
    }

    private void updateSchema(TablePath tablePath, int schemaId, Schema schema) {
        latestSchemaByTablePath.compute(
                tablePath,
                (key, oldValue) -> {
                    if (oldValue == null || oldValue.getSchemaId() < schemaId) {
                        return new SchemaInfo(schema, schemaId);
                    } else {
                        return oldValue;
                    }
                });

        schemaCache.put(new TableSchemaKey(tablePath, schemaId), schema);
    }

    @VisibleForTesting
    public Map<TablePath, SchemaInfo> getLatestSchemaByTablePath() {
        return latestSchemaByTablePath;
    }

    @VisibleForTesting
    Map<TablePath, Integer> getSubscriberCounters() {
        return subscriberCounters;
    }

    private static class TableSchemaKey {
        private final TablePath tablePath;
        private final int schemaId;

        TableSchemaKey(TablePath tablePath, int schemaId) {
            this.tablePath = tablePath;
            this.schemaId = schemaId;
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof TableSchemaKey)) {
                return false;
            }
            TableSchemaKey that = (TableSchemaKey) o;
            return schemaId == that.schemaId && Objects.equals(tablePath, that.tablePath);
        }

        @Override
        public int hashCode() {
            return Objects.hash(tablePath, schemaId);
        }
    }

    private class SchemaMetadataSubscriberImpl implements SchemaGetter {
        private final TablePath tablePath;
        private volatile boolean released;

        public SchemaMetadataSubscriberImpl(TablePath tablePath) {
            this.tablePath = tablePath;
            this.released = false;
        }

        @Override
        public SchemaInfo getLatestSchemaInfo() {
            return latestSchemaByTablePath.get(tablePath);
        }

        @Override
        public Schema getSchema(int schemaId) {
            // todo:  support with get table.
            try {
                return getFlussSchema(tablePath, schemaId);
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
        public void release() {
            if (!released) {
                unsubscribe(tablePath);
                released = true;
            }
        }
    }
}
