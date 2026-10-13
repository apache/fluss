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

package org.apache.fluss.lake.paimon.utils;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessage;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Utility class for Paimon test helpers that can be shared across test classes.
 *
 * <p>This class provides common methods for creating tables, writing data, and triggering
 * compactions in Paimon tests.
 */
public class PaimonTestUtils {

    /**
     * Write data to multiple buckets in a single commit, creating one snapshot.
     *
     * @param fileStoreTable the FileStoreTable instance
     * @param bucketRows map of bucket -> rows to append to the bucket
     * @return the snapshot ID after commit
     */
    public static long writeAndCommitData(
            FileStoreTable fileStoreTable, Map<Integer, List<GenericRow>> bucketRows)
            throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.WRITE_ONLY.key(), "true");
        options.put(CoreOptions.SNAPSHOT_IGNORE_EMPTY_COMMIT.key(), "false");
        FileStoreTable writeTable = fileStoreTable.copy(options);
        BatchWriteBuilder writeBuilder = writeTable.newBatchWriteBuilder();
        try (BatchTableWrite write = writeBuilder.newWrite();
                BatchTableCommit commit = writeBuilder.newCommit()) {
            for (Map.Entry<Integer, List<GenericRow>> entry : bucketRows.entrySet()) {
                int bucket = entry.getKey();
                for (GenericRow row : entry.getValue()) {
                    write.write(row, bucket);
                }
            }
            List<CommitMessage> messages = write.prepareCommit();
            commit.commit(messages);
        }
        return fileStoreTable.snapshotManager().latestSnapshot().id();
    }

    /** Helper class for compacting buckets in tests. */
    public static class CompactHelper {
        private final FileStoreTable fileStoreTable;
        private final File compactionTempDir;

        public CompactHelper(FileStoreTable fileStoreTable, File compactionTempDir) {
            this.fileStoreTable = fileStoreTable;
            this.compactionTempDir = compactionTempDir;
        }

        public CompactCommitter compactBucket(BinaryRow partition, int bucket) throws Exception {
            Map<String, String> options = new HashMap<>();
            options.put(CoreOptions.WRITE_ONLY.key(), String.valueOf(false));
            FileStoreTable compactTable = fileStoreTable.copy(options);
            BatchWriteBuilder writeBuilder = compactTable.newBatchWriteBuilder();
            try (BatchTableWrite write =
                    (BatchTableWrite)
                            writeBuilder
                                    .newWrite()
                                    .withIOManager(
                                            IOManager.create(compactionTempDir.toString()))) {
                BatchTableCommit batchTableCommit = writeBuilder.newCommit();
                write.compact(partition, bucket, false);
                // All compaction operations are prepared together and will be committed atomically
                List<CommitMessage> messages = write.prepareCommit();
                return new CompactCommitter(batchTableCommit, messages);
            }
        }

        public CompactCommitter compactBucket(int bucket) throws Exception {
            return compactBucket(BinaryRow.EMPTY_ROW, bucket);
        }
    }

    /**
     * A helper class to commit compaction operations later.
     *
     * <p>This allows preparing compaction operations and committing them at a specific point in the
     * test, which is useful for testing scenarios where multiple compactions are prepared but
     * committed in a specific order.
     */
    public static class CompactCommitter {
        private final BatchTableCommit tableCommit;
        private final List<CommitMessage> messages;

        private CompactCommitter(BatchTableCommit batchTableCommit, List<CommitMessage> messages) {
            this.tableCommit = batchTableCommit;
            this.messages = messages;
        }

        /**
         * Commit the compaction operation.
         *
         * @throws Exception if the commit fails
         */
        public void commit() throws Exception {
            tableCommit.commit(messages);
            tableCommit.close();
        }
    }
}
