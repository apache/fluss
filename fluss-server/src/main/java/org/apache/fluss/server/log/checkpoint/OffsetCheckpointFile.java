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

package org.apache.fluss.server.log.checkpoint;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.exception.LogStorageException;
import org.apache.fluss.metadata.TableBucket;

import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;

/* This file is based on source code of Apache Kafka Project (https://kafka.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/**
 * this class persists a map of (Bucket => Offsets) to a file (for a certain replica).
 *
 * <pre>
 * The format in the offset checkpoint file is like this:
 * ------ checkpoint file begin -----
 * 0              <- OffsetCheckpointFile.currentVersion
 * 2              <- following entries size
 * 150001 [20241121] 0 10    <- (TableBucket.tableId, [TableBucket.partitionId], TableBucket.bucket, Offset)
 * 150001 [20241121] 1 5
 * ----- checkpoint file end  ------
 * </pre>
 */
@Internal
public final class OffsetCheckpointFile {
    private static final Pattern WHITE_SPACES_PATTERN = Pattern.compile("\\s+");
    static final int CURRENT_VERSION = 0;

    private final CheckpointFile<Pair<TableBucket, Long>> checkpoint;

    public OffsetCheckpointFile(File file) throws IOException {
        this.checkpoint =
                new CheckpointFile<>(
                        file,
                        OffsetCheckpointFile.CURRENT_VERSION,
                        new OffsetCheckpointFile.Formatter());
    }

    public void write(Map<TableBucket, Long> offsets) {
        List<Pair<TableBucket, Long>> list = new ArrayList<>(offsets.size());
        for (Map.Entry<TableBucket, Long> entry : offsets.entrySet()) {
            list.add(new MutablePair<>(entry.getKey(), entry.getValue()));
        }
        try {
            checkpoint.write(list);
        } catch (IOException e) {
            String msg = "Error while writing to checkpoint file " + checkpoint.getAbsolutePath();
            throw new LogStorageException(msg, e);
        }
    }

    public Map<TableBucket, Long> read() {
        List<Pair<TableBucket, Long>> list;
        try {
            list = checkpoint.read();
        } catch (IOException e) {
            String msg = "Error while reading checkpoint file " + checkpoint.getAbsolutePath();
            throw new LogStorageException(msg, e);
        }

        Map<TableBucket, Long> result = new HashMap<>();
        for (Pair<TableBucket, Long> pair : list) {
            result.put(pair.getKey(), pair.getValue());
        }
        return result;
    }

    /** Formatter for offset checkpoint file. */
    public static class Formatter
            implements CheckpointFile.EntryFormatter<Pair<TableBucket, Long>> {

        @Override
        public String toString(Pair<TableBucket, Long> entry) {
            TableBucket tableBucket = entry.getLeft();
            long offset = entry.getRight();
            if (tableBucket.getPartitionId() == null) {
                return tableBucket.getTableId() + " " + tableBucket.getBucket() + " " + offset;
            } else {
                return tableBucket.getTableId()
                        + " "
                        + tableBucket.getPartitionId()
                        + " "
                        + tableBucket.getBucket()
                        + " "
                        + offset;
            }
        }

        @Override
        public Optional<Pair<TableBucket, Long>> fromString(String line) {
            String[] parts = WHITE_SPACES_PATTERN.split(line);
            if (parts.length == 3) {
                int tableId = Integer.parseInt(parts[0]);
                int bucketId = Integer.parseInt(parts[1]);
                long offset = Long.parseLong(parts[2]);
                return Optional.of(new MutablePair<>(new TableBucket(tableId, bucketId), offset));
            } else if (parts.length == 4) {
                int tableId = Integer.parseInt(parts[0]);
                long partitionId = Long.parseLong(parts[1]);
                int bucketId = Integer.parseInt(parts[2]);
                long offset = Long.parseLong(parts[3]);
                return Optional.of(
                        new MutablePair<>(new TableBucket(tableId, partitionId, bucketId), offset));
            } else {
                return Optional.empty();
            }
        }
    }

    /** Loads checkpoint file on demand and caches the offsets for reuse. */
    public static class LazyOffsetCheckpoints {
        private final OffsetCheckpointFile checkpoint;
        private Map<TableBucket, Long> offsets;

        public LazyOffsetCheckpoints(OffsetCheckpointFile checkpoint) {
            this.checkpoint = checkpoint;
            this.offsets = null;
        }

        private Map<TableBucket, Long> getOffsets() {
            if (offsets == null) {
                offsets = checkpoint.read();
            }
            return offsets;
        }

        public Optional<Long> fetch(TableBucket tableBucket) {
            return Optional.ofNullable(getOffsets().get(tableBucket));
        }
    }
}
