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

package org.apache.fluss.lake.iceberg.tiering;

import org.apache.fluss.config.Configuration;
import org.apache.fluss.lake.committer.LakeCommitter;
import org.apache.fluss.metadata.TablePath;

import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.events.CreateSnapshotEvent;
import org.apache.iceberg.events.Listener;
import org.apache.iceberg.events.Listeners;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.lang.reflect.Field;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;

import static org.apache.fluss.lake.iceberg.utils.IcebergConversions.toIceberg;
import static org.assertj.core.api.Assertions.assertThat;

class IcebergLakeCommitterListenerLeakTest {

    private @TempDir File tempWarehouseDir;
    private Catalog icebergCatalog;
    private IcebergCatalogProvider provider;

    @BeforeEach
    void beforeEach() {
        Configuration configuration = new Configuration();
        configuration.setString("warehouse", "file://" + tempWarehouseDir);
        configuration.setString("type", "hadoop");
        configuration.setString("name", "test");
        provider = new IcebergCatalogProvider(configuration);
        icebergCatalog = provider.get();
    }

    @Test
    void testListenerRegisteredOnceAcrossMultipleCommitters() throws Exception {
        TablePath tablePath = TablePath.of("iceberg", "listener_leak_table");
        createSimpleTable(tablePath);

        // Trigger class initialization first so the static {} block runs once
        // before we take the baseline snapshot of the registry.
        Class.forName(IcebergLakeCommitter.class.getName());

        int baseline = createSnapshotEventListeners().size();
        long flussListenersBaseline = countFlussListeners(createSnapshotEventListeners());

        int newCommitters = 5;
        for (int i = 0; i < newCommitters; i++) {
            try (LakeCommitter<IcebergWriteResult, IcebergCommittable> committer =
                    new IcebergLakeCommitter(provider, tablePath)) {
                assertThat(committer).isNotNull();
            }
        }

        Queue<Listener<?>> after = createSnapshotEventListeners();

        assertThat(after.size() - baseline)
                .as(
                        "IcebergSnapshotCreateListener is registered via a static {} block, "
                                + "so it must only be registered once for the whole JVM, "
                                + "no matter how many IcebergLakeCommitter instances are created.")
                .isZero();

        long flussListenerCount = countFlussListeners(after);
        assertThat(flussListenerCount - flussListenersBaseline)
                .as(
                        "No additional IcebergSnapshotCreateListener should be appended to the "
                                + "JVM-wide org.apache.iceberg.events.Listeners registry when "
                                + "instantiating new IcebergLakeCommitter instances.")
                .isZero();
        assertThat(flussListenerCount)
                .as("The Fluss snapshot create listener should be registered exactly once.")
                .isEqualTo(1L);
    }

    private static long countFlussListeners(Queue<Listener<?>> listeners) {
        return listeners.stream()
                .filter(
                        l ->
                                l.getClass()
                                        == IcebergLakeCommitter.IcebergSnapshotCreateListener.class)
                .count();
    }

    @SuppressWarnings("unchecked")
    private static Queue<Listener<?>> createSnapshotEventListeners() throws Exception {
        Field listenersField = Listeners.class.getDeclaredField("LISTENERS");
        listenersField.setAccessible(true);
        Map<Class<?>, Queue<Listener<?>>> registry =
                (Map<Class<?>, Queue<Listener<?>>>) listenersField.get(null);
        Queue<Listener<?>> queue = registry.get(CreateSnapshotEvent.class);
        return queue == null ? new LinkedList<>() : queue;
    }

    private void createSimpleTable(TablePath tablePath) {
        Namespace namespace = Namespace.of(tablePath.getDatabaseName());
        if (icebergCatalog instanceof SupportsNamespaces) {
            SupportsNamespaces ns = (SupportsNamespaces) icebergCatalog;
            if (!ns.namespaceExists(namespace)) {
                ns.createNamespace(namespace);
            }
        }

        Schema schema =
                new Schema(
                        Types.NestedField.required(1, "c1", Types.IntegerType.get()),
                        Types.NestedField.optional(2, "c2", Types.StringType.get()));

        TableIdentifier id = toIceberg(tablePath);
        if (!icebergCatalog.tableExists(id)) {
            icebergCatalog.createTable(id, schema, PartitionSpec.unpartitioned());
        }
    }
}
