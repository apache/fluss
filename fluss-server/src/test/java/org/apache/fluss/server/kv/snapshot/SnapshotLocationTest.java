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

import org.apache.fluss.fs.FileSystem;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.fs.local.LocalFileSystem;
import org.apache.fluss.server.kv.snapshot.SnapshotLocation.FsSnapshotOutputStream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for the {@link org.apache.fluss.server.kv.snapshot.SnapshotLocation}. */
class SnapshotLocationTest {

    @TempDir private Path exclusiveSnapshotDir;
    @TempDir private Path sharedSnapshotDir;

    // ------------------------------------------------------------------------
    //  tests
    // ------------------------------------------------------------------------

    @Test
    @SuppressWarnings("ConstantConditions")
    void testWriteFlushesIfAboveThreshold() throws IOException {
        int writeBufferSize = 100;
        final SnapshotLocation snapshotLocation =
                createSnapshotLocation(LocalFileSystem.getSharedInstance(), writeBufferSize);

        // test write not-shared
        verifyWriteFlushesIfAboveThreshold(
                snapshotLocation, writeBufferSize, SnapshotFileScope.EXCLUSIVE);
        // test write shared
        verifyWriteFlushesIfAboveThreshold(
                snapshotLocation, writeBufferSize, SnapshotFileScope.SHARED);
    }

    private void verifyWriteFlushesIfAboveThreshold(
            SnapshotLocation snapshotLocation,
            int fileSizeThreshold,
            SnapshotFileScope snapshotFileScope)
            throws IOException {
        try (FsSnapshotOutputStream stream =
                snapshotLocation.createSnapshotOutputStream(snapshotFileScope)) {
            stream.write(new byte[fileSizeThreshold]);
            Path pathToCheck =
                    snapshotFileScope == SnapshotFileScope.SHARED
                            ? sharedSnapshotDir
                            : exclusiveSnapshotDir;
            File[] files = new File(pathToCheck.toUri()).listFiles();
            assertThat(files).hasSize(1);
            File file = files[0];
            assertThat(file).hasSize(fileSizeThreshold);
            stream.write(new byte[fileSizeThreshold - 1]); // should buffer without flushing
            stream.write(127); // should buffer without flushing
            assertThat(file).hasSize(fileSizeThreshold);
        }
    }

    @Test
    void testFlushUnderThreshold() throws IOException {
        flushAndVerify(11, 10, true);
    }

    @Test
    void testFlushAboveThreshold() throws IOException {
        flushAndVerify(10, 11, false);
    }

    private void flushAndVerify(int minFileSize, int bytesToFlush, boolean expectEmpty)
            throws IOException {
        try (FsSnapshotOutputStream stream =
                createSnapshotLocation(LocalFileSystem.getSharedInstance(), minFileSize)
                        .createSnapshotOutputStream(SnapshotFileScope.EXCLUSIVE)) {
            stream.write(new byte[bytesToFlush]);
            stream.flush();
            assertThat(new File(exclusiveSnapshotDir.toUri()).listFiles())
                    .hasSize(expectEmpty ? 0 : 1);
        }
    }

    // ------------------------------------------------------------------------
    //  test utils
    // ------------------------------------------------------------------------

    private SnapshotLocation createSnapshotLocation(FileSystem fs, int bufferSize) {
        return new SnapshotLocation(
                fs,
                new FsPath(exclusiveSnapshotDir.toUri()),
                new FsPath(sharedSnapshotDir.toUri()),
                bufferSize);
    }
}
