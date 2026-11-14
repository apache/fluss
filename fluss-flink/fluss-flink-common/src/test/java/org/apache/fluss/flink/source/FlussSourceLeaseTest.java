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

package org.apache.fluss.flink.source;

import org.apache.fluss.client.initializer.OffsetsInitializer;
import org.apache.fluss.flink.source.deserializer.RowDataDeserializationSchema;
import org.apache.fluss.flink.source.split.SourceSplitBase;
import org.apache.fluss.flink.source.state.SourceEnumeratorState;
import org.apache.fluss.flink.utils.FlinkTestBase;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.utils.InstantiationUtils;

import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.mocks.MockSplitEnumeratorContext;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.table.data.RowData;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Collections;
import java.util.Optional;

import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.apache.fluss.testutils.common.CommonTestUtils.waitValue;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests that a DataStream {@link FlussSource} owns an isolated KV snapshot lease identity: stable
 * for one logical source, distinct for every other source in the same JVM (as in a Flink session
 * cluster).
 */
class FlussSourceLeaseTest extends FlinkTestBase {

    @Test
    void testIndependentlyBuiltSourcesHaveDistinctLeaseIds() throws Exception {
        TablePath tablePath = TablePath.of(DEFAULT_DB, "lease-distinct-sources-table");
        createTable(tablePath, DEFAULT_PK_TABLE_DESCRIPTOR);

        FlinkSource<RowData> firstSource = buildSource(tablePath);
        FlinkSource<RowData> secondSource = buildSource(tablePath);

        String firstLeaseId = leaseIdOf(firstSource);
        String secondLeaseId = leaseIdOf(secondSource);

        assertThat(firstLeaseId).isNotNull().isNotEmpty();
        assertThat(secondLeaseId).isNotNull().isNotEmpty();
        assertThat(firstLeaseId)
                .as("two independently built sources must use distinct kv snapshot lease ids")
                .isNotEqualTo(secondLeaseId);

        // Recreating an enumerator for the same source must retain the lease identity.
        assertThat(leaseIdOf(firstSource))
                .as("the lease id of one source instance must be stable")
                .isEqualTo(firstLeaseId);
    }

    @Test
    void testBuilderReuseYieldsDistinctLeaseIds() throws Exception {
        TablePath tablePath = TablePath.of(DEFAULT_DB, "lease-builder-reuse-table");
        createTable(tablePath, DEFAULT_PK_TABLE_DESCRIPTOR);

        FlussSourceBuilder<RowData> builder =
                FlussSource.<RowData>builder()
                        .setBootstrapServers(bootstrapServers)
                        .setDatabase(tablePath.getDatabaseName())
                        .setTable(tablePath.getTableName())
                        .setStartingOffsets(OffsetsInitializer.full())
                        .setDeserializationSchema(new RowDataDeserializationSchema());

        String firstLeaseId = leaseIdOf(builder.build());
        String secondLeaseId = leaseIdOf(builder.build());

        assertThat(firstLeaseId)
                .as("reusing a builder must not reuse the kv snapshot lease id")
                .isNotEqualTo(secondLeaseId);
    }

    @Test
    void testSerializationPreservesLeaseId() throws Exception {
        TablePath tablePath = TablePath.of(DEFAULT_DB, "lease-serialization-table");
        createTable(tablePath, DEFAULT_PK_TABLE_DESCRIPTOR);

        FlinkSource<RowData> source = buildSource(tablePath);
        String leaseId = leaseIdOf(source);

        byte[] serialized = InstantiationUtils.serializeObject(source);
        FlinkSource<RowData> copy =
                InstantiationUtils.deserializeObject(
                        serialized, FlussSourceLeaseTest.class.getClassLoader());

        assertThat(leaseIdOf(copy))
                .as("a serialized source must keep its kv snapshot lease id")
                .isEqualTo(leaseId);
    }

    /** A restore must reuse the lease id persisted in the checkpoint, not the restoring one. */
    @Test
    void testRestoreEnumeratorUsesPersistedLeaseIdOfAnotherSource() throws Exception {
        TablePath tablePath = TablePath.of(DEFAULT_DB, "lease-restore-table");
        createTable(tablePath, DEFAULT_PK_TABLE_DESCRIPTOR);

        FlinkSource<RowData> checkpointedSource = buildSource(tablePath);
        FlinkSource<RowData> restoringSource = buildSource(tablePath);

        // Persist the enumerator state the way a checkpoint does.
        SourceEnumeratorState persistedState;
        try (MockSplitEnumeratorContext<SourceSplitBase> context =
                        new MockSplitEnumeratorContext<>(DEFAULT_BUCKET_NUM);
                SplitEnumerator<SourceSplitBase, SourceEnumeratorState> enumerator =
                        checkpointedSource.createEnumerator(context)) {
            SimpleVersionedSerializer<SourceEnumeratorState> serializer =
                    checkpointedSource.getEnumeratorCheckpointSerializer();
            persistedState =
                    serializer.deserialize(
                            serializer.getVersion(),
                            serializer.serialize(enumerator.snapshotState(1L)));
        }

        assertThat(persistedState.getLeaseId()).isNotNull().isNotEmpty();
        assertThat(leaseIdOf(restoringSource))
                .as("the restoring source is a different source, so it has its own lease id")
                .isNotEqualTo(persistedState.getLeaseId());

        try (MockSplitEnumeratorContext<SourceSplitBase> context =
                        new MockSplitEnumeratorContext<>(DEFAULT_BUCKET_NUM);
                SplitEnumerator<SourceSplitBase, SourceEnumeratorState> restoredEnumerator =
                        restoringSource.restoreEnumerator(context, persistedState)) {
            assertThat(restoredEnumerator.snapshotState(1L).getLeaseId())
                    .as("a restored enumerator must keep writing under the persisted lease id")
                    .isEqualTo(persistedState.getLeaseId());
        }
    }

    /**
     * Server-side regression: a source that never checkpointed drops its lease on {@code close()}.
     * That drop must not remove the lease another running source registered on the same table.
     */
    @Test
    void testCloseWithoutCheckpointOfOtherSourceKeepsLease() throws Throwable {
        TablePath tablePath = TablePath.of(DEFAULT_DB, "lease-close-isolation-table");
        createTable(tablePath, DEFAULT_PK_TABLE_DESCRIPTOR);
        // A kv snapshot is required before a source can pin anything on the server.
        writeRows(conn, tablePath, Collections.singletonList(row(1, "v1")), false);
        FLUSS_CLUSTER_EXTENSION.triggerAndWaitSnapshot(tablePath);

        ZooKeeperClient zooKeeperClient = FLUSS_CLUSTER_EXTENSION.getZooKeeperClient();
        FlinkSource<RowData> firstSource = buildSource(tablePath);
        FlinkSource<RowData> secondSource = buildSource(tablePath);

        // Read both lease ids up front: snapshotState() on an enumerator marks its checkpoint as
        // already triggered, which would suppress the drop in close() and make the second source
        // exit without exercising the drop path at all.
        String firstLeaseId = leaseIdOf(firstSource);
        String secondLeaseId = leaseIdOf(secondSource);

        try (MockSplitEnumeratorContext<SourceSplitBase> firstContext =
                        new MockSplitEnumeratorContext<>(DEFAULT_BUCKET_NUM);
                SplitEnumerator<SourceSplitBase, SourceEnumeratorState> firstEnumerator =
                        firstSource.createEnumerator(firstContext)) {
            // The first source runs like a streaming job and pins the current snapshot.
            firstEnumerator.start();
            firstContext.runNextOneTimeCallable();
            waitUntilLeaseRegistered(zooKeeperClient, firstLeaseId);

            // The second source is never snapshotted, so closing it drops its lease in close().
            try (MockSplitEnumeratorContext<SourceSplitBase> secondContext =
                            new MockSplitEnumeratorContext<>(DEFAULT_BUCKET_NUM);
                    SplitEnumerator<SourceSplitBase, SourceEnumeratorState> secondEnumerator =
                            secondSource.createEnumerator(secondContext)) {
                secondEnumerator.start();
                secondContext.runNextOneTimeCallable();
                waitUntilLeaseRegistered(zooKeeperClient, secondLeaseId);
            }

            assertThat(zooKeeperClient.getKvSnapshotLeaseMetadata(secondLeaseId))
                    .as("closing source %s must drop its own kv snapshot lease", secondLeaseId)
                    .isEmpty();
            assertThat(zooKeeperClient.getKvSnapshotLeaseMetadata(firstLeaseId))
                    .as(
                            "closing source %s must not drop the kv snapshot lease %s of another "
                                    + "running source",
                            secondLeaseId, firstLeaseId)
                    .isPresent();
        }
    }

    private FlinkSource<RowData> buildSource(TablePath tablePath) {
        return FlussSource.<RowData>builder()
                .setBootstrapServers(bootstrapServers)
                .setDatabase(tablePath.getDatabaseName())
                .setTable(tablePath.getTableName())
                .setStartingOffsets(OffsetsInitializer.full())
                .setDeserializationSchema(new RowDataDeserializationSchema())
                .build();
    }

    /** Returns the lease id of a source without starting its enumerator. */
    private static String leaseIdOf(FlinkSource<RowData> source) throws Exception {
        try (MockSplitEnumeratorContext<SourceSplitBase> context =
                        new MockSplitEnumeratorContext<>(DEFAULT_BUCKET_NUM);
                SplitEnumerator<SourceSplitBase, SourceEnumeratorState> enumerator =
                        source.createEnumerator(context)) {
            return enumerator.snapshotState(1L).getLeaseId();
        }
    }

    private static void waitUntilLeaseRegistered(ZooKeeperClient zooKeeperClient, String leaseId)
            throws Exception {
        waitValue(
                () ->
                        zooKeeperClient.getKvSnapshotLeaseMetadata(leaseId).isPresent()
                                ? Optional.of(leaseId)
                                : Optional.<String>empty(),
                Duration.ofMinutes(1),
                String.format("kv snapshot lease %s was not registered on the server", leaseId));
    }
}
