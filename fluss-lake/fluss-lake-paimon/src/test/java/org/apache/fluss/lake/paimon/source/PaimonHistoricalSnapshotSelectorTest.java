/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.lake.paimon.source;

import org.apache.fluss.lake.source.LakeSource.HistoricalSnapshot;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.utils.json.TableBucketOffsets;

import org.apache.paimon.Snapshot;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.SnapshotManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.apache.fluss.lake.committer.LakeCommitter.FLUSS_LAKE_SNAP_BUCKET_OFFSET_PROPERTY;
import static org.apache.fluss.lake.paimon.tiering.PaimonLakeTieringFactory.FLUSS_LAKE_TIERING_COMMIT_USER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Tests selection from historical metadata, including corrupt and expired offset files. */
class PaimonHistoricalSnapshotSelectorTest {
    @TempDir private Path temporary;
    private final TableBucket bucket = new TableBucket(42, 0);
    private final FileStoreTable table = mock(FileStoreTable.class, RETURNS_DEEP_STUBS);
    private final SnapshotManager manager = mock(SnapshotManager.class);
    private TableInfo info;

    @BeforeEach
    void setup() {
        Schema schema = Schema.newBuilder().column("id", DataTypes.INT()).primaryKey("id").build();
        info =
                TableInfo.of(
                        TablePath.of("db", "t"),
                        42,
                        0,
                        TableDescriptor.builder().schema(schema).distributedBy(1).build(),
                        "unused",
                        0,
                        0);
        when(table.rowType())
                .thenReturn(
                        RowType.of(
                                new org.apache.paimon.types.DataType[] {
                                    org.apache.paimon.types.DataTypes.INT().notNull()
                                },
                                new String[] {"id"}));
        when(table.primaryKeys()).thenReturn(Collections.singletonList("id"));
        when(table.partitionKeys()).thenReturn(Collections.emptyList());
        when(table.options()).thenReturn(Collections.singletonMap("bucket", "1"));
        when(table.snapshotManager()).thenReturn(manager);
        TableSchema tableSchema = mock(TableSchema.class);
        when(tableSchema.id()).thenReturn(0L);
        when(table.schema()).thenReturn(tableSchema);
    }

    @Test
    void testSelectsOlderBaseUsingOffsetsRatherThanCommitTime() throws Exception {
        when(manager.latestSnapshotId()).thenReturn(3L);
        when(manager.earliestSnapshotId()).thenReturn(1L);
        snapshot(3, writeOffsets("new", 42, 12), Snapshot.CommitKind.APPEND);
        snapshot(2, null, Snapshot.CommitKind.COMPACT);
        snapshot(1, writeOffsets("old", 42, 4), Snapshot.CommitKind.APPEND);
        Optional<HistoricalSnapshot> selected = select(2, 8);
        assertThat(selected).isPresent();
        assertThat(selected.get().getSnapshotId()).isEqualTo(1L);
        assertThat(selected.get().getReadableOffsets()).containsEntry(bucket, 4L);
    }

    @Test
    void testMissingReferencedOffsetFileFailsEvenWhenOlderBaseIsAvailable() throws Exception {
        when(manager.latestSnapshotId()).thenReturn(2L);
        when(manager.earliestSnapshotId()).thenReturn(1L);
        String missingPath = temporary.resolve("deleted").toUri().toString();
        snapshot(2, missingPath, Snapshot.CommitKind.APPEND);
        snapshot(1, writeOffsets("old", 42, 3), Snapshot.CommitKind.APPEND);
        assertThatThrownBy(() -> select(3, 8))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Missing fluss-offsets file")
                .hasMessageContaining("table=db.t")
                .hasMessageContaining("tableId=42")
                .hasMessageContaining("snapshotId=2")
                .hasMessageContaining(missingPath);
    }

    @Test
    void testExactlyAlignedBaseDoesNotNeedExpiredLog() throws Exception {
        when(manager.latestSnapshotId()).thenReturn(1L);
        when(manager.earliestSnapshotId()).thenReturn(1L);
        snapshot(1, writeOffsets("aligned", 42, 8), Snapshot.CommitKind.APPEND);
        assertThat(select(10, 8)).isPresent();
    }

    @Test
    void testWrongTableIdentityAndCorruptOffsetsFail() throws Exception {
        when(manager.latestSnapshotId()).thenReturn(1L);
        when(manager.earliestSnapshotId()).thenReturn(1L);
        Snapshot snapshot = snapshot(1, writeOffsets("wrong", 43, 4), Snapshot.CommitKind.APPEND);
        assertThatThrownBy(() -> select(0, 8)).hasMessageContaining("identity mismatch");
        Path corrupt = temporary.resolve("corrupt");
        Files.write(corrupt, new byte[] {1, 2, 3});
        when(snapshot.properties())
                .thenReturn(
                        Collections.singletonMap(
                                FLUSS_LAKE_SNAP_BUCKET_OFFSET_PROPERTY,
                                corrupt.toUri().toString()));
        assertThatThrownBy(() -> select(0, 8)).hasMessageContaining("Corrupt historical offsets");
    }

    @Test
    void testForeignWriterInEarlierLineageInvalidatesLaterFlussBase() throws Exception {
        when(manager.latestSnapshotId()).thenReturn(2L);
        when(manager.earliestSnapshotId()).thenReturn(1L);
        snapshot(2, writeOffsets("new", 42, 4), Snapshot.CommitKind.APPEND);
        Snapshot foreign = snapshot(1, null, Snapshot.CommitKind.APPEND);
        when(foreign.commitUser()).thenReturn("external-writer");
        assertThatThrownBy(() -> select(0, 8))
                .hasMessageContaining("Unsupported historical lineage");
    }

    @Test
    void testMissingBucketAndTooNewBaseAreNotCompatible() {
        assertThat(
                        PaimonHistoricalSnapshotSelector.compatible(
                                Collections.emptyMap(), offsets(0), offsets(8)))
                .isFalse();
        assertThat(PaimonHistoricalSnapshotSelector.compatible(offsets(9), offsets(0), offsets(8)))
                .isFalse();
    }

    @Test
    void testDeletionVectorBaseRequiresNoL0Files() throws Exception {
        when(manager.latestSnapshotId()).thenReturn(1L);
        when(manager.earliestSnapshotId()).thenReturn(1L);
        snapshot(1, writeOffsets("dv", 42, 4), Snapshot.CommitKind.APPEND);
        Map<String, String> options = new HashMap<>();
        options.put("bucket", "1");
        options.put("deletion-vectors.enabled", "true");
        when(table.options()).thenReturn(options);
        ManifestEntry l0 = mock(ManifestEntry.class);
        when(l0.level()).thenReturn(0);
        when(table.store().newScan().withSnapshot(1L).plan().files(FileKind.ADD))
                .thenReturn(Collections.singletonList(l0));
        assertThat(select(0, 8)).isEmpty();
        when(table.store().newScan().withSnapshot(1L).plan().files(FileKind.ADD))
                .thenReturn(Collections.emptyList());
        assertThat(select(0, 8)).isPresent();
    }

    @Test
    void testSchemaMismatchFailsBeforeReadingOffsets() {
        when(table.rowType())
                .thenReturn(
                        RowType.of(
                                new org.apache.paimon.types.DataType[] {
                                    org.apache.paimon.types.DataTypes.BIGINT().notNull()
                                },
                                new String[] {"id"}));
        assertThatThrownBy(() -> select(0, 8)).hasMessageContaining("schema or keys differ");
    }

    private Optional<HistoricalSnapshot> select(long earliest, long target) throws Exception {
        return new PaimonHistoricalSnapshotSelector(table)
                .select(info, offsets(earliest), offsets(target));
    }

    private Map<TableBucket, Long> offsets(long offset) {
        return Collections.singletonMap(bucket, offset);
    }

    private String writeOffsets(String name, long tableId, long offset) throws Exception {
        Path path = temporary.resolve(name);
        Files.write(
                path,
                new TableBucketOffsets(
                                tableId,
                                Collections.singletonMap(new TableBucket(tableId, 0), offset))
                        .toJsonBytes());
        return path.toUri().toString();
    }

    private Snapshot snapshot(long id, String path, Snapshot.CommitKind kind) throws Exception {
        Snapshot snapshot = mock(Snapshot.class);
        when(snapshot.id()).thenReturn(id);
        when(snapshot.schemaId()).thenReturn(0L);
        when(snapshot.commitKind()).thenReturn(kind);
        when(snapshot.commitUser()).thenReturn(FLUSS_LAKE_TIERING_COMMIT_USER);
        when(snapshot.properties())
                .thenReturn(
                        path == null
                                ? Collections.emptyMap()
                                : Collections.singletonMap(
                                        FLUSS_LAKE_SNAP_BUCKET_OFFSET_PROPERTY, path));
        when(manager.tryGetSnapshot(id)).thenReturn(snapshot);
        return snapshot;
    }
}
