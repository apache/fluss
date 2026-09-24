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

package org.apache.fluss.server.kv.snapshot;

import org.apache.fluss.fs.FsPath;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.server.kv.rocksdb.RocksDBKvBuilder;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link LocalKvSnapshotUtils}. */
class LocalKvSnapshotUtilsTest {

    @Test
    void testRestoreMissingTabletDirectory(@TempDir Path dataDir) {
        Path missingTabletDir = dataDir.resolve("missing-tablet");

        assertThat(
                        LocalKvSnapshotUtils.restore(
                                missingTabletDir.toFile(), completedSnapshot(1L, 1L, 1L)))
                .isFalse();
        assertThat(missingTabletDir).doesNotExist();
    }

    @Test
    void testRestoreValidSnapshotAndRetainCheckpoint(@TempDir Path tabletDir) throws Exception {
        long snapshotId = 2L;
        Path staleSnapshot =
                LocalKvSnapshotUtils.getSnapshotDirectory(tabletDir.toFile(), 1L).toPath();
        Files.createDirectories(staleSnapshot);
        Files.write(staleSnapshot.resolve("stale"), new byte[] {1});

        Path snapshotDirectory =
                LocalKvSnapshotUtils.getSnapshotDirectory(tabletDir.toFile(), snapshotId).toPath();
        Files.createDirectories(snapshotDirectory);
        byte[] sstBytes = "sst-data".getBytes(StandardCharsets.UTF_8);
        byte[] currentBytes = "MANIFEST-1".getBytes(StandardCharsets.UTF_8);
        Files.write(snapshotDirectory.resolve("000001.sst"), sstBytes);
        Files.write(snapshotDirectory.resolve("CURRENT"), currentBytes);

        Path activeDb = RocksDBKvBuilder.getInstanceRocksDBPath(tabletDir.toFile()).toPath();
        Files.createDirectories(activeDb);
        Files.write(activeDb.resolve("old"), new byte[] {1});

        CompletedSnapshot completedSnapshot =
                completedSnapshot(snapshotId, sstBytes.length, currentBytes.length);
        assertThat(LocalKvSnapshotUtils.restore(tabletDir.toFile(), completedSnapshot)).isTrue();

        assertThat(activeDb.resolve("old")).doesNotExist();
        assertThat(activeDb.resolve("000001.sst")).hasBinaryContent(sstBytes);
        assertThat(activeDb.resolve("CURRENT")).hasBinaryContent(currentBytes);
        assertThat(snapshotDirectory).isDirectory();
        assertThat(staleSnapshot).doesNotExist();

        // Mutable RocksDB metadata must be copied rather than linked back into the checkpoint.
        Files.write(activeDb.resolve("CURRENT"), "MANIFEST-2".getBytes(StandardCharsets.UTF_8));
        assertThat(snapshotDirectory.resolve("CURRENT")).hasBinaryContent(currentBytes);
    }

    @Test
    void testInvalidSnapshotFallsBackWithoutChangingActiveDb(@TempDir Path tabletDir)
            throws Exception {
        long snapshotId = 3L;
        Path snapshotDirectory =
                LocalKvSnapshotUtils.getSnapshotDirectory(tabletDir.toFile(), snapshotId).toPath();
        Files.createDirectories(snapshotDirectory);
        Files.write(
                snapshotDirectory.resolve("000001.sst"),
                "wrong-size".getBytes(StandardCharsets.UTF_8));
        Files.write(snapshotDirectory.resolve("CURRENT"), new byte[] {1});

        Path activeDb = RocksDBKvBuilder.getInstanceRocksDBPath(tabletDir.toFile()).toPath();
        Files.createDirectories(activeDb);
        Path activeMarker = activeDb.resolve("active");
        Files.write(activeMarker, new byte[] {1});

        assertThat(
                        LocalKvSnapshotUtils.restore(
                                tabletDir.toFile(), completedSnapshot(snapshotId, 1, 1)))
                .isFalse();
        assertThat(activeMarker).exists();
        assertThat(snapshotDirectory).isDirectory();
    }

    private static CompletedSnapshot completedSnapshot(
            long snapshotId, long sstSize, long currentSize) {
        KvSnapshotHandle snapshotHandle =
                KvSnapshotHandle.create(
                        Collections.singletonList(
                                KvFileHandleAndLocalPath.of(
                                        new KvFileHandle(
                                                "file:///remote/shared/000001.sst", sstSize),
                                        "000001.sst")),
                        Collections.singletonList(
                                KvFileHandleAndLocalPath.of(
                                        new KvFileHandle(
                                                "file:///remote/snapshot/CURRENT", currentSize),
                                        "CURRENT")),
                        sstSize + currentSize);
        return new CompletedSnapshot(
                new TableBucket(1L, 0),
                snapshotId,
                new FsPath(new File("remote-snapshot").toURI()),
                snapshotHandle);
    }
}
