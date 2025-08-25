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

package org.apache.fluss.server.log;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.OutOfOrderSequenceException;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.utils.clock.ManualClock;
import org.apache.fluss.utils.types.Tuple2;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.apache.fluss.server.log.WriterStateManager.listSnapshotFiles;
import static org.apache.fluss.utils.FlussPaths.offsetFromFile;
import static org.apache.fluss.utils.FlussPaths.writerSnapshotFile;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link WriterStateManager}. */
public class WriterStateManagerTest {

    private @TempDir File tempDir;
    private final long writerId = 1L;
    private File logDir;
    private TableBucket tableBucket;
    private Configuration conf;
    private WriterStateManager stateManager;

    @BeforeEach
    public void setup() throws Exception {
        long tableId = 1001;
        logDir = LogTestUtils.makeRandomLogTabletDir(tempDir, "testDb", tableId, "testTable");
        tableBucket = new TableBucket(tableId, 0);
        conf = new Configuration();
        stateManager =
                new WriterStateManager(
                        tableBucket,
                        logDir,
                        (int) conf.get(ConfigOptions.WRITER_ID_EXPIRATION_TIME).toMillis());
    }

    @Test
    void testBasicWriterIdMapping() {
        // First entry for id 0 added.
        append(stateManager, writerId, 0, 0L);

        // Second entry for id 1 added.
        append(stateManager, writerId, 1, 0L);

        // Duplicates are checked separately and should result in OutOfOrderSequence if appended
        assertThatThrownBy(() -> append(stateManager, writerId, 1, 0L))
                .isInstanceOf(OutOfOrderSequenceException.class)
                .hasMessageContaining(
                        "Out of order batch sequence for writer 1 at offset 0 in "
                                + "table-bucket TableBucket{tableId=1001, bucket=0} "
                                + ": 1 (incoming batch seq.), 1 (current batch seq.)");

        // Invalid batch sequence (greater than next expected batch sequence).
        assertThatThrownBy(() -> append(stateManager, writerId, 5, 0L))
                .isInstanceOf(OutOfOrderSequenceException.class)
                .hasMessageContaining(
                        "Out of order batch sequence for writer 1 at offset 0 in"
                                + " table-bucket TableBucket{tableId=1001, bucket=0} "
                                + ": 5 (incoming batch seq.), 1 (current batch seq.)");
    }

    @Test
    void testValidationOnFirstEntryWhenLoadingLog() {
        // When the first entry is added, the batch sequence should only be 0.
        int batchSequence = 16;
        long offset = 735L;
        assertThatThrownBy(() -> append(stateManager, writerId, batchSequence, offset))
                .isInstanceOf(OutOfOrderSequenceException.class)
                .hasMessageContaining(
                        "Out of order batch sequence for writer 1 at offset 735 in "
                                + "table-bucket TableBucket{tableId=1001, bucket=0}"
                                + " : 16 (incoming batch seq.), -1 (current batch seq.)");

        append(stateManager, writerId, 0, offset);
        Optional<WriterStateEntry> maybeLastEntry = stateManager.lastEntry(writerId);
        assertThat(maybeLastEntry).isPresent();

        WriterStateEntry lastEntry = maybeLastEntry.get();
        assertThat(lastEntry.firstBatchSequence()).isEqualTo(0);
        assertThat(lastEntry.lastBatchSequence()).isEqualTo(0);
        assertThat(lastEntry.firstDataOffset()).isEqualTo(offset);
        assertThat(lastEntry.lastDataOffset()).isEqualTo(offset);
    }

    @Test
    void testPrepareUpdateDoesNotMutate() {
        WriterAppendInfo appendInfo = stateManager.prepareUpdate(writerId);
        appendInfo.appendDataBatch(
                0, new LogOffsetMetadata(15L), 20L, false, System.currentTimeMillis());
        assertThat(stateManager.lastEntry(writerId)).isNotPresent();
        stateManager.update(appendInfo);
        assertThat(stateManager.lastEntry(writerId)).isPresent();

        WriterAppendInfo nextAppendInfo = stateManager.prepareUpdate(writerId);
        nextAppendInfo.appendDataBatch(
                1, new LogOffsetMetadata(26L), 30L, false, System.currentTimeMillis());
        assertThat(stateManager.lastEntry(writerId)).isPresent();

        WriterStateEntry lastEntry = stateManager.lastEntry(writerId).get();
        assertThat(lastEntry.lastBatchSequence()).isEqualTo(0);
        assertThat(lastEntry.lastDataOffset()).isEqualTo(20L);

        stateManager.update(nextAppendInfo);
        lastEntry = stateManager.lastEntry(writerId).get();
        assertThat(lastEntry.lastBatchSequence()).isEqualTo(1);
        assertThat(lastEntry.lastDataOffset()).isEqualTo(30L);
    }

    @Test
    void testTruncateAndReloadRemovesOutOfRangeSnapshots() throws IOException {
        for (int i = 0; i < 5; i++) {
            append(stateManager, writerId, i, i);
            stateManager.takeSnapshot();
        }

        stateManager.truncateAndReload(1L, 3L, System.currentTimeMillis());
        assertThat(stateManager.oldestSnapshotOffset()).isPresent();
        assertThat(stateManager.oldestSnapshotOffset().get()).isEqualTo(2L);
        assertThat(stateManager.latestSnapshotOffset()).isPresent();
        assertThat(stateManager.latestSnapshotOffset().get()).isEqualTo(3L);
    }

    @Test
    void testTakeSnapshot() throws IOException {
        append(stateManager, writerId, 0, 0L);
        append(stateManager, writerId, 1, 1L);

        // Take snapshot.
        stateManager.takeSnapshot();

        String[] fileList = logDir.list();
        assertThat(fileList).isNotNull();
        assertThat(fileList.length).isEqualTo(1);
        assertThat(new File(logDir, fileList[0]).length() > 0).isTrue();
    }

    @Test
    void testFetchSnapshotEmptySnapshot() {
        assertThat(stateManager.fetchSnapshot(1)).isEmpty();
    }

    @Test
    void testRemoveExpiredWritersOnReload() throws IOException {
        append(stateManager, writerId, 0, 0L, false, 0);
        append(stateManager, writerId, 1, 1L, false, 1);

        stateManager.takeSnapshot();
        WriterStateManager recoveredMapping =
                new WriterStateManager(
                        tableBucket,
                        logDir,
                        (int) conf.get(ConfigOptions.WRITER_ID_EXPIRATION_TIME).toMillis());
        recoveredMapping.truncateAndReload(0L, 1L, 70000);

        // Entry added after recovery. The writer id should be expired now, and would not exist in
        // the writer mapping. If writing with the same writerId and non-zero batch sequence, the
        // OutOfOrderSequenceException will throw. If you want to continue to write, you need to get
        // a new writer id.
        assertThatThrownBy(() -> append(recoveredMapping, writerId, 2, 2L, false, 3000L))
                .isInstanceOf(OutOfOrderSequenceException.class)
                .hasMessageContaining(
                        "Out of order batch sequence for writer 1 at offset 2 in "
                                + "table-bucket TableBucket{tableId=1001, bucket=0}"
                                + " : 2 (incoming batch seq.), -1 (current batch seq.)");

        append(recoveredMapping, 2L, 0, 2L, false, 70002);

        assertThat(recoveredMapping.activeWriters().size()).isEqualTo(1);
        assertThat(recoveredMapping.activeWriters().values().iterator().next().lastBatchSequence())
                .isEqualTo(0);
        assertThat(recoveredMapping.mapEndOffset()).isEqualTo(3L);
    }

    @Test
    void testAppendAnExpiredBatchWithEmptyWriterStatus() throws Exception {
        ManualClock clock = new ManualClock(5000L);

        // 2 seconds to expire the writer.
        conf.set(ConfigOptions.WRITER_ID_EXPIRATION_TIME, Duration.ofSeconds(2));
        WriterStateManager stateManager1 =
                new WriterStateManager(
                        tableBucket,
                        logDir,
                        (int) conf.get(ConfigOptions.WRITER_ID_EXPIRATION_TIME).toMillis());

        // If we try to append an expired batch with none zero batch sequence, the
        // OutOfOrderSequenceException will not been throw.
        append(stateManager1, 1L, 10, 10L, true, clock.milliseconds());
        assertThat(stateManager1.activeWriters().size()).isEqualTo(1);
        assertThat(stateManager1.activeWriters().values().iterator().next().lastBatchSequence())
                .isEqualTo(10);

        // If we try to append a none-expired batch with none zero batch sequence, the
        // OutOfOrderSequenceException will throw.
        assertThatThrownBy(() -> append(stateManager1, 2L, 10, 10L, false, 1000L))
                .isInstanceOf(OutOfOrderSequenceException.class)
                .hasMessageContaining(
                        "Out of order batch sequence for writer 2 at offset 10 in table-bucket "
                                + "TableBucket{tableId=1001, bucket=0} : 10 (incoming batch seq.), -1 (current batch seq.)");
    }

    @Test
    void testDeleteSnapshotsBefore() throws IOException {
        append(stateManager, writerId, 0, 0L);
        append(stateManager, writerId, 1, 1L);
        stateManager.takeSnapshot();
        assertThat(Objects.requireNonNull(logDir.listFiles()).length).isEqualTo(1);
        assertThat(currentSnapshotOffsets()).isEqualTo(Collections.singleton(2L));

        append(stateManager, writerId, 2, 2L);
        stateManager.takeSnapshot();
        assertThat(Objects.requireNonNull(logDir.listFiles()).length).isEqualTo(2);
        assertThat(currentSnapshotOffsets()).isEqualTo(new HashSet<>(Arrays.asList(2L, 3L)));

        stateManager.deleteSnapshotsBefore(3L);
        assertThat(Objects.requireNonNull(logDir.listFiles()).length).isEqualTo(1);
        assertThat(currentSnapshotOffsets()).isEqualTo(Collections.singleton(3L));

        stateManager.deleteSnapshotsBefore(4L);
        assertThat(Objects.requireNonNull(logDir.listFiles()).length).isEqualTo(0);
        assertThat(currentSnapshotOffsets()).isEqualTo(Collections.emptySet());
    }

    @Test
    void testTruncateFullyAndStartAt() throws IOException {
        append(stateManager, writerId, 0, 0L);
        append(stateManager, writerId, 1, 1L);
        stateManager.takeSnapshot();
        assertThat(Objects.requireNonNull(logDir.listFiles()).length).isEqualTo(1);
        assertThat(currentSnapshotOffsets()).isEqualTo(Collections.singleton(2L));

        append(stateManager, writerId, 2, 2L);
        stateManager.takeSnapshot();
        assertThat(Objects.requireNonNull(logDir.listFiles()).length).isEqualTo(2);
        assertThat(currentSnapshotOffsets()).isEqualTo(new HashSet<>(Arrays.asList(2L, 3L)));

        stateManager.truncateFullyAndStartAt(0L);
        assertThat(Objects.requireNonNull(logDir.listFiles()).length).isEqualTo(0);
        assertThat(currentSnapshotOffsets()).isEqualTo(Collections.emptySet());

        append(stateManager, writerId, 0, 0L);
        stateManager.takeSnapshot();
        assertThat(Objects.requireNonNull(logDir.listFiles()).length).isEqualTo(1);
        assertThat(currentSnapshotOffsets()).isEqualTo(Collections.singleton(1L));
    }

    @Test
    void testReloadSnapshots() throws Exception {
        append(stateManager, writerId, 0, 1L);
        append(stateManager, writerId, 1, 2L);
        stateManager.takeSnapshot();

        Set<Tuple2<Path, byte[]>> pathAndDataList =
                Arrays.stream(Objects.requireNonNull(logDir.listFiles()))
                        .map(
                                file -> {
                                    try {
                                        return Tuple2.of(
                                                file.toPath(), Files.readAllBytes(file.toPath()));
                                    } catch (IOException e) {
                                        throw new RuntimeException(e);
                                    }
                                })
                        .collect(Collectors.toSet());

        append(stateManager, writerId, 2, 3L);
        append(stateManager, writerId, 3, 4L);
        stateManager.takeSnapshot();
        assertThat(Objects.requireNonNull(logDir.listFiles()).length).isEqualTo(2);
        assertThat(currentSnapshotOffsets()).isEqualTo(new HashSet<>(Arrays.asList(3L, 5L)));

        // Truncate to the range (3, 5), this will delete the earlier snapshot until offset 3.
        stateManager.truncateAndReload(3L, 5L, System.currentTimeMillis());
        assertThat(Objects.requireNonNull(logDir.listFiles()).length).isEqualTo(1);
        assertThat(currentSnapshotOffsets()).isEqualTo(Collections.singleton(5L));

        // Add the snapshot files until offset 3 to the log dir.
        for (Tuple2<Path, byte[]> pathAndData : pathAndDataList) {
            Files.write(pathAndData.f0, pathAndData.f1);
        }
        // Cleanup the in-memory snapshots and reload the snapshots from log dir.
        // It loads the earlier written snapshot files from log dir.
        stateManager.truncateFullyAndReloadSnapshots();

        assertThat(stateManager.latestSnapshotOffset().get()).isEqualTo(3);
        assertThat(currentSnapshotOffsets()).isEqualTo(Collections.singleton(3L));
    }

    @Test
    void testLoadFromSnapshotRetainsNonExpiredWriters() throws IOException {
        long writerId1 = 1L;
        long writerId2 = 2L;

        append(stateManager, writerId1, 0, 0L);
        append(stateManager, writerId2, 0, 1L);
        stateManager.takeSnapshot();
        assertThat(stateManager.activeWriters().size()).isEqualTo(2);

        stateManager.truncateAndReload(1L, 2L, System.currentTimeMillis());
        assertThat(stateManager.activeWriters().size()).isEqualTo(2);

        Optional<WriterStateEntry> entry1 = stateManager.lastEntry(writerId1);
        assertThat(entry1).isPresent();
        assertThat(entry1.get().lastBatchSequence()).isEqualTo(0);
        assertThat(entry1.get().lastDataOffset()).isEqualTo(0L);

        Optional<WriterStateEntry> entry2 = stateManager.lastEntry(writerId2);
        assertThat(entry2).isPresent();
        assertThat(entry2.get().lastBatchSequence()).isEqualTo(0);
        assertThat(entry2.get().lastDataOffset()).isEqualTo(1L);
    }

    @Test
    void testSkipSnapshotIfOffsetUnchanged() throws IOException {
        append(stateManager, writerId, 0, 0L, false, 0L);

        stateManager.takeSnapshot();
        assertThat(Objects.requireNonNull(logDir.listFiles()).length).isEqualTo(1);
        assertThat(currentSnapshotOffsets()).isEqualTo(Collections.singleton(1L));

        // nothing changed so there should be no new snapshot.
        stateManager.takeSnapshot();
        assertThat(Objects.requireNonNull(logDir.listFiles()).length).isEqualTo(1);
        assertThat(currentSnapshotOffsets()).isEqualTo(Collections.singleton(1L));
    }

    @Test
    void testWriterExpirationTimeout() throws Exception {
        conf.set(ConfigOptions.WRITER_ID_EXPIRATION_TIME, Duration.ofSeconds(3));
        WriterStateManager stateManager1 =
                new WriterStateManager(
                        tableBucket,
                        logDir,
                        (int) conf.get(ConfigOptions.WRITER_ID_EXPIRATION_TIME).toMillis());
        append(stateManager1, writerId, 0, 1L);
        stateManager1.removeExpiredWriters(System.currentTimeMillis() + 4000L);

        assertThatThrownBy(() -> append(stateManager1, writerId, 2, 2L))
                .isInstanceOf(OutOfOrderSequenceException.class)
                .hasMessageContaining(
                        "Out of order batch sequence for writer 1 at offset 2 in "
                                + "table-bucket TableBucket{tableId=1001, bucket=0}"
                                + " : 2 (incoming batch seq.), -1 (current batch seq.)");

        append(stateManager1, writerId, 0, 2L);
        assertThat(stateManager1.activeWriters().size()).isEqualTo(1);
        assertThat(stateManager1.activeWriters().values().iterator().next().lastBatchSequence())
                .isEqualTo(0);
        assertThat(stateManager1.mapEndOffset()).isEqualTo(3L);
    }

    @Test
    void testLoadFromEmptySnapshotFile() throws IOException {
        testLoadFromCorruptSnapshot(
                fileChannel -> {
                    try {
                        fileChannel.truncate(0L);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                });
    }

    @Test
    void testLoadFromTruncatedSnapshotFile() throws IOException {
        testLoadFromCorruptSnapshot(
                fileChannel -> {
                    try {
                        // truncate to some arbitrary point in the middle of the snapshot.
                        assertThat(fileChannel.size()).isGreaterThan(2);
                        fileChannel.truncate(fileChannel.size() / 2);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                });
    }

    @Test
    void testLoadFromCorruptSnapshotFile() throws IOException {
        testLoadFromCorruptSnapshot(
                fileChannel -> {
                    try {
                        // write some garbage somewhere in the file.
                        assertThat(fileChannel.size()).isGreaterThan(2);
                        fileChannel.write(
                                ByteBuffer.wrap(new byte[] {1, 2, 3}), fileChannel.size() / 2);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                });
    }

    @Test
    void testRemoveStraySnapshotsKeepCleanShutdownSnapshot() throws IOException {
        // Test that when stray snapshots are removed, the largest stray snapshot is kept around.
        // This covers the case where the tablet server shutdown cleanly and emitted a snapshot file
        // larger than the base offset of the active segment.

        // Create 3 snapshot files at different offsets.
        Files.createFile(writerSnapshotFile(logDir, 5).toPath()); // not stray.
        Files.createFile(writerSnapshotFile(logDir, 2).toPath()); // stray.
        Files.createFile(writerSnapshotFile(logDir, 42).toPath()); // not stray.

        // claim that we only have one segment with a base offset of 5.
        stateManager.removeStraySnapshots(Collections.singleton(5L));

        // The snapshot file at offset 2 should be considered a stray, but the snapshot at 42 should
        // be kept around because it is the largest snapshot.
        assertThat(stateManager.latestSnapshotOffset()).isEqualTo(Optional.of(42L));
        assertThat(stateManager.oldestSnapshotOffset()).isEqualTo(Optional.of(5L));
        assertThat(listSnapshotFiles(logDir).stream().map(snapshotFile -> snapshotFile.offset))
                .containsExactlyInAnyOrderElementsOf(new HashSet<>(Arrays.asList(5L, 42L)));
    }

    @Test
    void testRemoveAllStraySnapshots() throws IOException {
        // Test that when stray snapshots are removed, we remove only the stray snapshots below the
        // largest segment base offset. Snapshots associated with an offset in the list of segment
        // base offsets should remain.

        // Create 3 snapshot files at different offsets.
        Files.createFile(writerSnapshotFile(logDir, 5).toPath()); // stray.
        Files.createFile(writerSnapshotFile(logDir, 2).toPath()); // stray.
        Files.createFile(writerSnapshotFile(logDir, 42).toPath()); // not stray.

        stateManager.removeStraySnapshots(Collections.singleton(42L));
        assertThat(listSnapshotFiles(logDir).stream().map(snapshotFile -> snapshotFile.offset))
                .containsExactlyInAnyOrderElementsOf(Collections.singleton(42L));
    }

    private void testLoadFromCorruptSnapshot(Consumer<FileChannel> makeFileCorrupt)
            throws IOException {
        long writerId = 1L;

        append(stateManager, writerId, 0, 0L);
        stateManager.takeSnapshot();
        append(stateManager, writerId, 1, 1L);
        stateManager.takeSnapshot();

        // Truncate the last snapshot.
        Optional<Long> latestSnapshotOffset = stateManager.latestSnapshotOffset();
        assertThat(latestSnapshotOffset.get()).isEqualTo(2L);

        File snapshotToTruncate = writerSnapshotFile(logDir, latestSnapshotOffset.get());

        try (FileChannel channel =
                FileChannel.open(snapshotToTruncate.toPath(), StandardOpenOption.WRITE)) {
            makeFileCorrupt.accept(channel);
        }

        // Ensure that the truncated snapshot is deleted and writer state is loaded from the
        // previous snapshot.
        WriterStateManager reloadedStateManager =
                new WriterStateManager(
                        tableBucket,
                        logDir,
                        (int) conf.get(ConfigOptions.WRITER_ID_EXPIRATION_TIME).toMillis());
        reloadedStateManager.truncateAndReload(0L, 20L, System.currentTimeMillis());
        assertThat(snapshotToTruncate.exists()).isFalse();

        WriterStateEntry loadedWriterState = reloadedStateManager.activeWriters().get(writerId);
        assertThat(loadedWriterState).isNotNull();
        assertThat(loadedWriterState.lastDataOffset()).isEqualTo(0L);
    }

    private void append(
            WriterStateManager stateManager, long writerId, int batchSequence, long offset) {
        append(stateManager, writerId, batchSequence, offset, false, System.currentTimeMillis());
    }

    private void append(
            WriterStateManager stateManager,
            long writerId,
            int batchSequence,
            long offset,
            boolean isWriterInBatchExpired,
            long lastTimestamp) {
        WriterAppendInfo appendInfo = stateManager.prepareUpdate(writerId);
        appendInfo.appendDataBatch(
                batchSequence,
                new LogOffsetMetadata(offset),
                offset,
                isWriterInBatchExpired,
                lastTimestamp);
        stateManager.update(appendInfo);
        stateManager.updateMapEndOffset(offset + 1);
    }

    private Set<Long> currentSnapshotOffsets() {
        Set<Long> offsets = new HashSet<>();
        for (File file : Objects.requireNonNull(logDir.listFiles())) {
            offsets.add(offsetFromFile(file));
        }

        return offsets;
    }
}
