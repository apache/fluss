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

package com.alibaba.fluss.server.log;

import com.alibaba.fluss.annotation.VisibleForTesting;
import com.alibaba.fluss.exception.CorruptSnapshotException;
import com.alibaba.fluss.exception.UnknownWriterIdException;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.record.LogRecordBatch;
import com.alibaba.fluss.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;
import com.alibaba.fluss.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import com.alibaba.fluss.utils.json.JsonDeserializer;
import com.alibaba.fluss.utils.json.JsonSerdeUtils;
import com.alibaba.fluss.utils.json.JsonSerializer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.NotThreadSafe;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.alibaba.fluss.utils.FlussPaths.WRITER_SNAPSHOT_FILE_SUFFIX;
import static com.alibaba.fluss.utils.FlussPaths.writerSnapshotFile;

/* This file is based on source code of Apache Kafka Project (https://kafka.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/**
 * Maintains a mapping from Writer Ids to metadata about the last appended entries (e.g. batch
 * sequence.)
 *
 * <p>The batch sequence is the last number successfully appended to the bucket for given
 * identifier.
 *
 * <p>As long as a writer id is contained in the map, the corresponding writer can continue to write
 * data. However, writer ids can be expired due to lack of recent use or if the last written entry
 * has been deleted from the log (e.g. if the retention policy is "delete").
 */
@NotThreadSafe
public class WriterStateManager {
    private static final Logger LOG = LoggerFactory.getLogger(WriterStateManager.class);

    private final TableBucket tableBucket;
    private final int writerExpirationMs;
    private final Map<Long, WriterStateEntry> writers = new HashMap<>();

    private final File logTabletDir;
    /** The same as writers#size, but for lock-free access. */
    private volatile int writerIdCount = 0;

    private ConcurrentSkipListMap<Long, SnapshotFile> snapshots;
    private long lastMapOffset = 0L;
    private long lastSnapOffset = 0L;

    public WriterStateManager(TableBucket tableBucket, File logTabletDir, int writerExpirationMs)
            throws IOException {
        this.tableBucket = tableBucket;
        this.writerExpirationMs = writerExpirationMs;
        this.logTabletDir = logTabletDir;
        this.snapshots = loadSnapshots();
    }

    public int writerIdCount() {
        return writerIdCount;
    }

    /** Returns the last offset of this map. */
    public long mapEndOffset() {
        return lastMapOffset;
    }

    public void updateMapEndOffset(long lastOffset) {
        lastMapOffset = lastOffset;
    }

    /** Get the last written entry for the given writer id. */
    public Optional<WriterStateEntry> lastEntry(long writerId) {
        return Optional.ofNullable(writers.get(writerId));
    }

    /** Get a copy of the active writers. */
    public Map<Long, WriterStateEntry> activeWriters() {
        return Collections.unmodifiableMap(writers);
    }

    public boolean isEmpty() {
        return writers.isEmpty();
    }

    public void removeExpiredWriters(long currentTimeMs) {
        List<Long> keys =
                writers.entrySet().stream()
                        .filter(entry -> isWriterExpired(currentTimeMs, entry.getValue()))
                        .map(Map.Entry::getKey)
                        .collect(Collectors.toList());
        removeWriterIds(keys);
    }

    /**
     * Truncate the writer id mapping to the given offset range and reload the entries from the most
     * recent snapshot in range (if there is one). We delete snapshot files prior to the
     * logStartOffset but do not remove writer state from the map. This means that in-memory and
     * on-disk state can diverge, and in the case of tablet server failover or unclean shutdown, any
     * in-memory state not persisted in the snapshots will be lost, which would lead to {@link
     * UnknownWriterIdException} errors. Note that the log end offset is assumed to be less than or
     * equal to the high watermark.
     */
    public void truncateAndReload(long logStartOffset, long logEndOffset, long currentTimeMs)
            throws IOException {
        // remove all out of range snapshots.
        for (SnapshotFile snapshot : snapshots.values()) {
            if (snapshot.offset > logEndOffset || snapshot.offset <= logStartOffset) {
                removeAndDeleteSnapshot(snapshot.offset);
            }
        }

        if (logEndOffset != mapEndOffset()) {
            clearWriterIds();
            loadFromSnapshot(logStartOffset, currentTimeMs);
        } else {
            if (lastMapOffset < logStartOffset) {
                lastMapOffset = logStartOffset;
            }
            lastSnapOffset = latestSnapshotOffset().orElse(logStartOffset);
        }
    }

    public void truncateFullyAndStartAt(long offset) throws IOException {
        clearWriterIds();
        for (SnapshotFile snapshot : snapshots.values()) {
            removeAndDeleteSnapshot(snapshot.offset);
        }
        lastSnapOffset = 0L;
        lastMapOffset = offset;
    }

    public void reloadSnapshots() throws IOException {
        LOG.info("Reloading the writer state snapshots");
        snapshots = loadSnapshots();
    }

    public void truncateFullyAndReloadSnapshots() throws IOException {
        LOG.info("Reloading the writer state snapshots");
        truncateFullyAndStartAt(0L);
        snapshots = loadSnapshots();
    }

    /**
     * Take a snapshot at the current end offset if one does not already exist with syncing the
     * change to the device.
     */
    public void takeSnapshot() throws IOException {
        // If not a new offset, then it is not worth taking another snapshot
        if (lastMapOffset > lastSnapOffset) {
            SnapshotFile snapshotFile =
                    new SnapshotFile(writerSnapshotFile(logTabletDir, lastMapOffset));
            long start = System.currentTimeMillis();
            writeSnapshot(snapshotFile.file(), writers);
            LOG.info(
                    "Wrote writer snapshot at offset {} with {} producer ids for table bucket {} in {} ms.",
                    lastMapOffset,
                    writers.size(),
                    tableBucket,
                    System.currentTimeMillis() - start);

            snapshots.put(snapshotFile.offset, snapshotFile);

            // Update the last snap offset according to the serialized map
            lastSnapOffset = lastMapOffset;
        }
    }

    /**
     * Deletes the writer snapshot files until the given offset (exclusive) in a thread safe manner.
     */
    @VisibleForTesting
    public void deleteSnapshotsBefore(long offset) throws IOException {
        for (SnapshotFile snapshot : snapshots.subMap(0L, offset).values()) {
            removeAndDeleteSnapshot(snapshot.offset);
        }
    }

    /** Fetch the snapshot file for the end offset of the log segment. */
    public Optional<File> fetchSnapshot(long offset) {
        return Optional.ofNullable(snapshots.get(offset)).map(SnapshotFile::file);
    }

    public WriterAppendInfo prepareUpdate(long writerId) {
        WriterStateEntry currentEntry =
                lastEntry(writerId).orElse(WriterStateEntry.empty(writerId));
        return new WriterAppendInfo(writerId, tableBucket, currentEntry);
    }

    /** Update the mapping with the given append information. */
    public void update(WriterAppendInfo appendInfo) {
        long writerId = appendInfo.writerId();
        if (writerId == LogRecordBatch.NO_WRITER_ID) {
            throw new IllegalArgumentException(
                    "Invalid writer id "
                            + writerId
                            + " passed to update "
                            + "for bucket "
                            + tableBucket);
        }

        LOG.trace("Updated writer id {} state to {}", writerId, appendInfo);
        WriterStateEntry updatedEntry = appendInfo.toEntry();
        WriterStateEntry currentEntry = writers.get(writerId);
        if (currentEntry != null) {
            currentEntry.update(updatedEntry);
        } else {
            addWriterId(writerId, updatedEntry);
        }
    }

    /**
     * Scans the log directory, gathering all writer snapshot files. Snapshot files which do not
     * have an offset corresponding to one of the provided offsets in segmentBaseOffsets will be
     * removed, except in the case that there is a snapshot file at a higher offset than any offset
     * in segmentBaseOffsets.
     *
     * <p>The goal here is to remove any snapshot files which do not have an associated segment
     * file, but not to remove the largest stray snapshot file which was emitted during clean
     * shutdown.
     */
    public void removeStraySnapshots(Collection<Long> segmentBaseOffsets) throws IOException {
        OptionalLong maxSegmentBaseOffset =
                segmentBaseOffsets.isEmpty()
                        ? OptionalLong.empty()
                        : OptionalLong.of(segmentBaseOffsets.stream().max(Long::compare).get());

        HashSet<Long> baseOffsets = new HashSet<>(segmentBaseOffsets);
        Optional<SnapshotFile> latestStraySnapshot = Optional.empty();

        ConcurrentSkipListMap<Long, SnapshotFile> snapshots = loadSnapshots();
        for (SnapshotFile snapshot : snapshots.values()) {
            long key = snapshot.offset;
            if (latestStraySnapshot.isPresent()) {
                SnapshotFile prev = latestStraySnapshot.get();
                if (!baseOffsets.contains(key)) {
                    // this snapshot is now the largest stray snapshot.
                    prev.deleteIfExists();
                    snapshots.remove(prev.offset);
                    latestStraySnapshot = Optional.of(snapshot);
                }
            } else {
                if (!baseOffsets.contains(key)) {
                    latestStraySnapshot = Optional.of(snapshot);
                }
            }
        }

        // Check to see if the latestStraySnapshot is larger than the largest segment base offset,
        // if it is not, delete the largestStraySnapshot.
        if (latestStraySnapshot.isPresent() && maxSegmentBaseOffset.isPresent()) {
            long strayOffset = latestStraySnapshot.get().offset;
            long maxOffset = maxSegmentBaseOffset.getAsLong();
            if (strayOffset < maxOffset) {
                SnapshotFile removedSnapshot = snapshots.remove(strayOffset);
                if (removedSnapshot != null) {
                    removedSnapshot.deleteIfExists();
                }
            }
        }

        this.snapshots = snapshots;
    }

    private void loadFromSnapshot(long logStartOffset, long currentTime) throws IOException {
        while (true) {
            Optional<SnapshotFile> latestSnapshotFileOptional = latestSnapshotFile();
            if (latestSnapshotFileOptional.isPresent()) {
                SnapshotFile snapshot = latestSnapshotFileOptional.get();
                try {
                    LOG.info("Loading writer state from snapshot file '{}'", snapshot);
                    Stream<WriterStateEntry> loadedWriters =
                            readSnapshot(snapshot.file()).stream()
                                    .filter(
                                            writerStateEntry ->
                                                    !isWriterExpired(
                                                            currentTime, writerStateEntry));
                    loadedWriters.forEach(this::loadWriterEntry);
                    lastSnapOffset = snapshot.offset;
                    lastMapOffset = lastSnapOffset;
                    return;
                } catch (CorruptSnapshotException e) {
                    LOG.warn(
                            "Failed to load writer snapshot from '{}': {}",
                            snapshot.file(),
                            e.getMessage());
                    removeAndDeleteSnapshot(snapshot.offset);
                }
            } else {
                lastSnapOffset = logStartOffset;
                lastMapOffset = logStartOffset;
                return;
            }
        }
    }

    /** Load writer state snapshots by scanning the logDir. */
    private ConcurrentSkipListMap<Long, SnapshotFile> loadSnapshots() throws IOException {
        ConcurrentSkipListMap<Long, SnapshotFile> offsetToSnapshots = new ConcurrentSkipListMap<>();
        List<SnapshotFile> snapshotFiles = listSnapshotFiles(logTabletDir);
        for (SnapshotFile snapshotFile : snapshotFiles) {
            offsetToSnapshots.put(snapshotFile.offset, snapshotFile);
        }
        return offsetToSnapshots;
    }

    private void addWriterId(long writerId, WriterStateEntry entry) {
        writers.put(writerId, entry);
        writerIdCount = writers.size();
    }

    private void removeWriterIds(List<Long> keys) {
        keys.forEach(writers::remove);
        writerIdCount = writers.size();
    }

    private void clearWriterIds() {
        writers.clear();
        writerIdCount = 0;
    }

    private Optional<SnapshotFile> latestSnapshotFile() {
        return Optional.ofNullable(snapshots.lastEntry()).map(Map.Entry::getValue);
    }

    /** Get the last offset (exclusive) of the latest snapshot file. */
    public Optional<Long> latestSnapshotOffset() {
        Optional<SnapshotFile> snapshotFileOptional = latestSnapshotFile();
        return snapshotFileOptional.map(snapshotFile -> snapshotFile.offset);
    }

    public Optional<Long> oldestSnapshotOffset() {
        Optional<SnapshotFile> snapshotFileOptional = oldestSnapshotFile();
        return snapshotFileOptional.map(snapshotFile -> snapshotFile.offset);
    }

    @VisibleForTesting
    public static List<SnapshotFile> listSnapshotFiles(File dir) throws IOException {
        if (dir.exists() && dir.isDirectory()) {
            try (Stream<Path> paths = Files.list(dir.toPath())) {
                return paths.filter(WriterStateManager::isSnapshotFile)
                        .map(path -> new SnapshotFile(path.toFile()))
                        .collect(Collectors.toList());
            }
        } else {
            return Collections.emptyList();
        }
    }

    private Optional<SnapshotFile> oldestSnapshotFile() {
        return Optional.ofNullable(snapshots.firstEntry()).map(Map.Entry::getValue);
    }

    /**
     * Removes the writer state snapshot file metadata corresponding to the provided offset if it
     * exists from this WriterStateManager, and deletes the backing snapshot file.
     */
    public void removeAndDeleteSnapshot(long snapshotOffset) throws IOException {
        SnapshotFile snapshotFile = snapshots.remove(snapshotOffset);
        if (snapshotFile != null) {
            snapshotFile.deleteIfExists();
        }
    }

    private static boolean isSnapshotFile(Path path) {
        return Files.isRegularFile(path)
                && path.getFileName().toString().endsWith(WRITER_SNAPSHOT_FILE_SUFFIX);
    }

    @VisibleForTesting
    public void loadWriterEntry(WriterStateEntry entry) {
        long writerId = entry.writerId();
        addWriterId(writerId, entry);
    }

    private boolean isWriterExpired(long currentTimeMs, WriterStateEntry writerStateEntry) {
        return currentTimeMs - writerStateEntry.lastBatchTimestamp() > writerExpirationMs;
    }

    public boolean isWriterInBatchExpired(long currentTimeMs, LogRecordBatch recordBatch) {
        return currentTimeMs - recordBatch.commitTimestamp() > writerExpirationMs;
    }

    private static List<WriterStateEntry> readSnapshot(File file) {
        try {
            byte[] json = Files.readAllBytes(file.toPath());
            WriterSnapshotMap writerSnapshotMap = WriterSnapshotMap.fromJsonBytes(json);

            List<WriterStateEntry> writerIdEntries = new ArrayList<>();
            writerSnapshotMap.snapshotEntries.forEach(
                    snapshotEntry ->
                            writerIdEntries.add(
                                    new WriterStateEntry(
                                            snapshotEntry.writerId,
                                            snapshotEntry.lastBatchTimestamp,
                                            new WriterStateEntry.BatchMetadata(
                                                    snapshotEntry.writerId,
                                                    snapshotEntry.lastBatchSequence,
                                                    snapshotEntry.lastBatchBaseOffset,
                                                    snapshotEntry.lastBatchOffsetDelta,
                                                    snapshotEntry.lastBatchTimestamp))));
            return writerIdEntries;
        } catch (IOException | UncheckedIOException e) {
            throw new CorruptSnapshotException("Failed to read snapshot file " + file, e);
        }
    }

    private static void writeSnapshot(File file, Map<Long, WriterStateEntry> entries)
            throws IOException {
        List<WriterSnapshotEntry> snapshotEntries = new ArrayList<>();
        entries.forEach(
                (writerId, writerStateEntry) ->
                        snapshotEntries.add(
                                new WriterSnapshotEntry(
                                        writerId,
                                        writerStateEntry.lastBatchSequence(),
                                        writerStateEntry.lastDataOffset(),
                                        writerStateEntry.lastOffsetDelta(),
                                        writerStateEntry.lastBatchTimestamp())));
        byte[] jsonBytes = new WriterSnapshotMap(snapshotEntries).toJsonBytes();

        ByteBuffer buffer = ByteBuffer.allocate(jsonBytes.length);
        buffer.put(jsonBytes);
        buffer.flip();

        try (FileChannel fileChannel =
                FileChannel.open(
                        file.toPath(), StandardOpenOption.CREATE, StandardOpenOption.WRITE)) {
            fileChannel.write(buffer);
            fileChannel.force(true);
        }
    }

    /** Writer snapshot map json serde. */
    public static class WriterSnapshotMapJsonSerde
            implements JsonSerializer<WriterSnapshotMap>, JsonDeserializer<WriterSnapshotMap> {
        public static final WriterSnapshotMapJsonSerde INSTANCE = new WriterSnapshotMapJsonSerde();

        private static final String VERSION_KEY = "version";
        private static final String WRITER_ID_ENTRIES_FILED = "writer_id_entries";
        private static final String WRITER_ID_FILED = "writer_id";
        private static final String LAST_BATCH_SEQUENCE_FILED = "last_batch_sequence";
        private static final String LAST_BATCH_BASE_OFFSET_FILED = "last_batch_base_offset";
        private static final String LAST_BATCH_OFFSET_DELTA_FILED = "offset_delta";
        private static final String LAST_BATCH_TIMESTAMP_FILED = "last_batch_timestamp";
        private static final int WRITER_ID_SNAPSHOT_VERSION = 1;

        @Override
        public void serialize(WriterSnapshotMap writerSnapshotMap, JsonGenerator generator)
                throws IOException {
            generator.writeStartObject();

            // serialize data version.
            generator.writeNumberField(VERSION_KEY, WRITER_ID_SNAPSHOT_VERSION);

            // serialize writer id entries.
            generator.writeArrayFieldStart(WRITER_ID_ENTRIES_FILED);
            for (WriterSnapshotEntry entry : writerSnapshotMap.snapshotEntries) {
                generator.writeStartObject();
                generator.writeNumberField(WRITER_ID_FILED, entry.writerId);
                generator.writeNumberField(LAST_BATCH_SEQUENCE_FILED, entry.lastBatchSequence);
                generator.writeNumberField(LAST_BATCH_BASE_OFFSET_FILED, entry.lastBatchBaseOffset);
                generator.writeNumberField(
                        LAST_BATCH_OFFSET_DELTA_FILED, entry.lastBatchOffsetDelta);
                generator.writeNumberField(LAST_BATCH_TIMESTAMP_FILED, entry.lastBatchTimestamp);
                generator.writeEndObject();
            }
            generator.writeEndArray();

            generator.writeEndObject();
        }

        @Override
        public WriterSnapshotMap deserialize(JsonNode node) {
            Iterator<JsonNode> entriesJson = node.get(WRITER_ID_ENTRIES_FILED).elements();
            List<WriterSnapshotEntry> snapshotEntries = new ArrayList<>();
            while (entriesJson.hasNext()) {
                JsonNode entryJson = entriesJson.next();
                long writerId = entryJson.get(WRITER_ID_FILED).asLong();
                int batchSequenceNumber = entryJson.get(LAST_BATCH_SEQUENCE_FILED).asInt();
                long lastBatchBaseOffset = entryJson.get(LAST_BATCH_BASE_OFFSET_FILED).asLong();
                int lastBatchOffsetDelta = entryJson.get(LAST_BATCH_OFFSET_DELTA_FILED).asInt();
                long lastBatchTimestamp = entryJson.get(LAST_BATCH_TIMESTAMP_FILED).asLong();
                snapshotEntries.add(
                        new WriterSnapshotEntry(
                                writerId,
                                batchSequenceNumber,
                                lastBatchBaseOffset,
                                lastBatchOffsetDelta,
                                lastBatchTimestamp));
            }

            return new WriterSnapshotMap(snapshotEntries);
        }
    }

    /** Writer snapshot entry. */
    public static class WriterSnapshotEntry {
        public final long writerId;
        public final int lastBatchSequence;
        public final long lastBatchBaseOffset;
        public final int lastBatchOffsetDelta;
        public final long lastBatchTimestamp;

        public WriterSnapshotEntry(
                long writerId,
                int lastBatchSequence,
                long lastBatchBaseOffset,
                int lastBatchOffsetDelta,
                long lastBatchTimestamp) {
            this.writerId = writerId;
            this.lastBatchSequence = lastBatchSequence;
            this.lastBatchBaseOffset = lastBatchBaseOffset;
            this.lastBatchOffsetDelta = lastBatchOffsetDelta;
            this.lastBatchTimestamp = lastBatchTimestamp;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            WriterSnapshotEntry that = (WriterSnapshotEntry) o;
            return writerId == that.writerId
                    && lastBatchSequence == that.lastBatchSequence
                    && lastBatchBaseOffset == that.lastBatchBaseOffset
                    && lastBatchOffsetDelta == that.lastBatchOffsetDelta
                    && lastBatchTimestamp == that.lastBatchTimestamp;
        }

        @Override
        public int hashCode() {
            return Objects.hash(
                    writerId,
                    lastBatchSequence,
                    lastBatchBaseOffset,
                    lastBatchOffsetDelta,
                    lastBatchTimestamp);
        }

        @Override
        public String toString() {
            return "WriterSnapshotEntry{"
                    + "writerId="
                    + writerId
                    + ", lastBatchSequence="
                    + lastBatchSequence
                    + ", lastBatchBaseOffset="
                    + lastBatchBaseOffset
                    + ", lastBatchOffsetDelta="
                    + lastBatchOffsetDelta
                    + ", lastBatchTimestamp="
                    + lastBatchTimestamp
                    + '}';
        }
    }

    /** Writer snapshot map. */
    public static class WriterSnapshotMap {
        // Version of the snapshot file.
        private final List<WriterSnapshotEntry> snapshotEntries;

        public WriterSnapshotMap(List<WriterSnapshotEntry> snapshotEntries) {
            this.snapshotEntries = snapshotEntries;
        }

        private static WriterSnapshotMap fromJsonBytes(byte[] json) {
            return JsonSerdeUtils.readValue(json, WriterSnapshotMapJsonSerde.INSTANCE);
        }

        private byte[] toJsonBytes() {
            return JsonSerdeUtils.writeValueAsBytes(this, WriterSnapshotMapJsonSerde.INSTANCE);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            WriterSnapshotMap that = (WriterSnapshotMap) o;
            return Objects.equals(snapshotEntries, that.snapshotEntries);
        }

        @Override
        public int hashCode() {
            return Objects.hash(snapshotEntries);
        }

        @Override
        public String toString() {
            return "WriterSnapshotMap{" + "snapshotEntries=" + snapshotEntries + '}';
        }
    }
}
