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

import org.apache.fluss.exception.LogStorageException;
import org.apache.fluss.metadata.LeaderEpochOffset;
import org.apache.fluss.server.log.checkpoint.CheckpointFile;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.TreeMap;

import static org.apache.fluss.utils.Preconditions.checkArgument;

/**
 * Persistent epoch boundaries for a tablet's log. Epochs may be skipped, and an epoch may contain
 * no records. Offsets before the first boundary have unknown history.
 *
 * <p>The caller serializes all reads and mutations with log appends and truncation. Boundaries must
 * be persisted before their records are appended. Log truncation must be durable before boundaries
 * are removed.
 */
final class LeaderEpochHistory {
    private final CheckpointFile<LeaderEpochOffset> checkpoint;
    private final NavigableMap<Integer, Long> epochs = new TreeMap<>();
    private IOException persistenceFailure;

    LeaderEpochHistory(File file) throws IOException {
        checkpoint = new CheckpointFile<>(file, 0, new EntryFormatter());
        for (LeaderEpochOffset entry : checkpoint.read()) {
            if (entry.epoch() < 0
                    || entry.offset() < 0
                    || (!epochs.isEmpty()
                            && (entry.epoch() <= epochs.lastKey()
                                    || entry.offset() < epochs.lastEntry().getValue()))) {
                throw new IOException("Invalid leader epoch history in " + file);
            }
            epochs.put(entry.epoch(), entry.offset());
        }
    }

    /** Persists an epoch boundary before publishing it to readers. */
    void assign(int epoch, long startOffset) throws IOException {
        ensureUsable();
        checkArgument(epoch >= 0 && startOffset >= 0, "Negative epoch or offset.");
        Long existing = epochs.get(epoch);
        if (existing != null) {
            // A restarted leader can resume the latest epoch at a later log end.
            checkArgument(epoch == epochs.lastKey(), "Epoch must not decrease.");
            checkArgument(startOffset >= existing, "Epoch already starts at %s", existing);
            return;
        }
        checkArgument(epochs.isEmpty() || epoch > epochs.lastKey(), "Epoch must increase.");
        checkArgument(
                epochs.isEmpty() || startOffset >= epochs.lastEntry().getValue(),
                "Epoch offsets must not decrease.");
        NavigableMap<Integer, Long> updated = new TreeMap<>(epochs);
        updated.put(epoch, startOffset);
        persist(updated);
    }

    /** Returns the epoch of an actual record, excluding empty epochs at the log end. */
    int epochForOffset(long offset, long logEndOffset) {
        ensureUsable();
        if (offset < 0 || offset >= logEndOffset) {
            return -1;
        }
        int result = -1;
        for (Map.Entry<Integer, Long> entry : epochs.entrySet()) {
            if (entry.getValue() > offset) {
                break;
            }
            result = entry.getKey();
        }
        return result;
    }

    /** Returns the end of the greatest known epoch no greater than the requested epoch. */
    Optional<LeaderEpochOffset> endOffsetFor(int requestedEpoch, long logEndOffset) {
        ensureUsable();
        Map.Entry<Integer, Long> entry = epochs.floorEntry(requestedEpoch);
        if (entry == null || entry.getValue() > logEndOffset) {
            return Optional.empty();
        }
        Map.Entry<Integer, Long> next = epochs.higherEntry(entry.getKey());
        return Optional.of(
                new LeaderEpochOffset(
                        entry.getKey(),
                        next == null ? logEndOffset : Math.min(next.getValue(), logEndOffset)));
    }

    /** Removes boundaries for records discarded by a durable log truncation. */
    void truncateFromEnd(long endOffset) throws IOException {
        ensureUsable();
        checkArgument(endOffset >= 0, "Negative log end offset.");
        NavigableMap<Integer, Long> updated = new TreeMap<>(epochs);
        updated.entrySet().removeIf(entry -> entry.getValue() >= endOffset);
        if (!updated.equals(epochs)) {
            persist(updated);
        }
    }

    /** Retains the epoch covering the first retained record and all later boundaries. */
    void truncateFromStart(long startOffset) throws IOException {
        ensureUsable();
        int retainedEpoch = epochForOffset(startOffset, Long.MAX_VALUE);
        if (retainedEpoch < 0) {
            return;
        }
        NavigableMap<Integer, Long> updated = new TreeMap<>(epochs.tailMap(retainedEpoch, true));
        if (!updated.equals(epochs)) {
            persist(updated);
        }
    }

    /**
     * Discards epoch knowledge without changing WAL bytes, before an untracked append or restore.
     */
    void invalidate() throws IOException {
        ensureUsable();
        if (!epochs.isEmpty()) {
            persist(new TreeMap<>());
        }
    }

    /** Returns boundaries covering a fetched range; the first may precede its start. */
    List<LeaderEpochOffset> entries(long startOffset, long endOffset) {
        ensureUsable();
        List<LeaderEpochOffset> result = new ArrayList<>();
        if (endOffset <= startOffset) {
            return result;
        }
        int first = epochForOffset(startOffset, Long.MAX_VALUE);
        for (Map.Entry<Integer, Long> entry : epochs.entrySet()) {
            if (entry.getValue() >= endOffset) {
                break;
            }
            if (entry.getKey() >= first) {
                result.add(new LeaderEpochOffset(entry.getKey(), entry.getValue()));
            }
        }
        return result;
    }

    /** Persists the epoch boundaries accompanying a contiguous replication append. */
    void append(List<LeaderEpochOffset> entries, long startOffset, long endOffset)
            throws IOException {
        ensureUsable();
        if (endOffset <= startOffset) {
            return;
        }
        NavigableMap<Integer, Long> updated = new TreeMap<>(epochs);
        for (LeaderEpochOffset entry : entries) {
            if (entry.offset() >= endOffset) {
                break;
            }
            Long existing = updated.get(entry.epoch());
            if (existing != null) {
                checkArgument(
                        existing == entry.offset(),
                        "Conflicting start offset for epoch %s",
                        entry.epoch());
                continue;
            }
            // A source boundary can precede this fetch, but it cannot establish the identity
            // of bytes we did not copy. Preserve unknown history until a new boundary is fetched.
            if (entry.offset() < startOffset) {
                continue;
            }
            checkArgument(entry.epoch() >= 0 && entry.offset() >= 0, "Negative epoch boundary.");
            checkArgument(
                    updated.isEmpty()
                            || (entry.epoch() > updated.lastKey()
                                    && entry.offset() >= updated.lastEntry().getValue()),
                    "Non-monotonic replication epoch history.");
            updated.put(entry.epoch(), entry.offset());
        }
        if (!updated.equals(epochs)) {
            persist(updated);
        }
    }

    /** Prevents further access after WAL or checkpoint persistence becomes uncertain. */
    void markFailed(IOException cause) {
        persistenceFailure = cause;
    }

    void ensureUsable() {
        if (persistenceFailure != null) {
            throw new LogStorageException(
                    "Leader epoch checkpoint failed; the log must be reloaded before further use.",
                    persistenceFailure);
        }
    }

    private void persist(NavigableMap<Integer, Long> updated) throws IOException {
        List<LeaderEpochOffset> entries = new ArrayList<>();
        updated.forEach((epoch, offset) -> entries.add(new LeaderEpochOffset(epoch, offset)));
        try {
            checkpoint.write(entries);
        } catch (IOException e) {
            // A failed directory sync can follow a successful atomic replacement. Neither the
            // old in-memory history nor a retry is safe until the log has been recovered.
            persistenceFailure = e;
            throw e;
        }
        epochs.clear();
        epochs.putAll(updated);
    }

    private static final class EntryFormatter
            implements CheckpointFile.EntryFormatter<LeaderEpochOffset> {
        @Override
        public String toString(LeaderEpochOffset entry) {
            return entry.epoch() + " " + entry.offset();
        }

        @Override
        public Optional<LeaderEpochOffset> fromString(String line) {
            String[] fields = line.split(" ");
            if (fields.length != 2) {
                return Optional.empty();
            }
            try {
                return Optional.of(
                        new LeaderEpochOffset(
                                Integer.parseInt(fields[0]), Long.parseLong(fields[1])));
            } catch (NumberFormatException e) {
                return Optional.empty();
            }
        }
    }
}
