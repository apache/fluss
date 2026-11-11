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

package org.apache.fluss.lake.lance.tiering;

import org.apache.fluss.config.Configuration;
import org.apache.fluss.lake.committer.LakeCommitter.CommitContext;
import org.apache.fluss.metadata.TablePath;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for {@link LanceLakeCommitter} focused on close-time resource semantics and error
 * wrapping.
 *
 * <p>The end-to-end tiering path is already exercised by {@code LanceTieringTest}; here we cover
 * the failure-side branches that are hard to reach from a happy-path IT.
 */
class LanceLakeCommitterTest {

    @TempDir File tempWarehouse;

    @Test
    void testCloseReleasesAllocatorEvenIfCommitNeverCalled() throws Exception {
        LanceLakeCommitter committer = newCommitter("db", "t");
        // Simply constructing and closing must not leak memory.
        committer.close();
    }

    @Test
    void testCloseWrapsLeakedChildBufferAsIOException() throws Exception {
        LanceLakeCommitter committer = newCommitter("db", "t");
        BufferAllocator child =
                committer.getAllocator().newChildAllocator("leaked-child", 0, Long.MAX_VALUE);
        ArrowBuf leaked = child.buffer(64);
        try {
            assertThatThrownBy(committer::close)
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("LanceLakeCommitter allocator reported leaked memory")
                    .cause()
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("Memory was leaked");
        } finally {
            // The parent allocator is already closed. Releasing the buffer frees the native
            // memory; closing the child afterwards fails because its parent is closed.
            leaked.close();
        }
    }

    @Test
    void testCommitOnMissingDatasetIsWrappedAsIOException() throws Exception {
        // Point warehouse at a subdirectory that has never been created as a Lance dataset;
        // Dataset.open (invoked by commitAppend) is expected to raise, and we assert that
        // LanceLakeCommitter#commit surfaces this as an IOException with the cause preserved.
        LanceLakeCommitter committer = newCommitter("db", "does_not_exist");
        try {
            LanceCommittable committable = new LanceCommittable(Collections.emptyList());
            CommitContext context =
                    new CommitContext(Collections.emptyMap(), Collections.emptyMap());
            assertThatThrownBy(() -> committer.commit(committable, context))
                    .isInstanceOf(java.io.IOException.class)
                    .hasCauseInstanceOf(RuntimeException.class);
        } finally {
            // Regardless of the failed commit, close must still release the allocator without
            // reporting a memory-leak error.
            committer.close();
        }
    }

    @Test
    void testGetMissingLakeSnapshotReturnsNullWhenDatasetAbsent() throws Exception {
        // A dataset that has not been created has no committed snapshot. The LakeCommitter
        // contract is to return null so callers can start a fresh tiering commit.
        LanceLakeCommitter committer = newCommitter("db", "no_such_table");
        try {
            assertThat(committer.getMissingLakeSnapshot(null)).isNull();
        } finally {
            committer.close();
        }
    }

    private LanceLakeCommitter newCommitter(String database, String table) {
        Configuration options = new Configuration();
        options.setString("warehouse", "file://" + tempWarehouse.getAbsolutePath());
        return new LanceLakeCommitter(options, TablePath.of(database, table));
    }
}
