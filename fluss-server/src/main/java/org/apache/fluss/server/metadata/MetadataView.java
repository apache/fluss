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

import org.apache.fluss.cluster.ServerNode;
import org.apache.fluss.metadata.TablePath;

import java.util.Collection;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.function.Predicate;

/** A listener-scoped metadata-cache view that authorizes each table lookup. */
public final class MetadataView {
    private final ServerMetadataSnapshot snapshot;
    private final ServerTablePropertyCache tablePropertyCache;
    private final String listenerName;
    private final Predicate<TablePath> authorized;

    /** Creates a view from routing and properties captured under the metadata update lock. */
    MetadataView(
            ServerMetadataSnapshot snapshot,
            ServerTablePropertyCache tablePropertyCache,
            String listenerName,
            Predicate<TablePath> authorized) {
        this.snapshot = snapshot;
        this.tablePropertyCache = tablePropertyCache;
        this.listenerName = listenerName;
        this.authorized = authorized;
    }

    /** Returns the broker endpoints for the request listener. */
    public Collection<ServerNode> getTabletServers() {
        return snapshot.getAliveTabletServers(listenerName).values();
    }

    /** Returns the paths in this snapshot; {@link #getTable(TablePath)} authorizes each lookup. */
    public Collection<TablePath> getTablePaths() {
        return snapshot.getTableIdByPath().keySet();
    }

    /** Looks up one table without catalog enumeration or a schema lookup. */
    public Optional<Table> getTable(TablePath path) {
        if (!authorized.test(path)) {
            return Optional.empty();
        }
        OptionalLong id = snapshot.getTableId(path);
        if (!id.isPresent()) {
            return Optional.empty();
        }
        Boolean kafkaEnabled = tablePropertyCache.isKafkaEnabled(id.getAsLong());
        return kafkaEnabled == null
                ? Optional.empty()
                : Optional.of(
                        new Table(
                                path,
                                id.getAsLong(),
                                kafkaEnabled,
                                snapshot.getBucketMetadataForTable(id.getAsLong()).values()));
    }

    /** Resolves a native table ID through the same snapshot's reverse index. */
    public Optional<Table> getTable(long tableId) {
        return snapshot.getTablePath(tableId).flatMap(this::getTable);
    }

    /** Minimal table information required for routing; it never retains a schema. */
    public static final class Table {
        private final TablePath tablePath;
        private final long tableId;
        private final boolean kafkaEnabled;
        private final Collection<BucketMetadata> buckets;

        private Table(
                TablePath tablePath,
                long tableId,
                boolean kafkaEnabled,
                Collection<BucketMetadata> buckets) {
            this.tablePath = tablePath;
            this.tableId = tableId;
            this.kafkaEnabled = kafkaEnabled;
            this.buckets = buckets;
        }

        /** Returns the native table path. */
        public TablePath getTablePath() {
            return tablePath;
        }

        /** Returns the native table ID. */
        public long getTableId() {
            return tableId;
        }

        /** Returns the cached Kafka enablement flag. */
        public boolean isKafkaEnabled() {
            return kafkaEnabled;
        }

        /** Returns this snapshot's immutable bucket metadata. */
        public Collection<BucketMetadata> getBuckets() {
            return buckets;
        }
    }
}
