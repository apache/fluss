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

package com.alibaba.fluss.utils;

import com.alibaba.fluss.testutils.common.CheckedThread;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.FileSystemException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assumptions.assumeThat;

/** Tests for the {@link FileUtils}. */
class FileUtilsTest {

    @TempDir private Path temporaryFolder;

    @Test
    void testDeleteQuietly() throws Exception {
        // should ignore the call
        FileUtils.deleteDirectoryQuietly(null);
        File doesNotExist = Files.createDirectory(temporaryFolder.resolve("abc")).toFile();
        FileUtils.deleteDirectoryQuietly(doesNotExist);

        File cannotDeleteParent = temporaryFolder.toFile();
        File cannotDeleteChild = new File(cannotDeleteParent, "child");

        try {
            assertThat(cannotDeleteChild.createNewFile()).isTrue();
            assertThat(cannotDeleteParent.setWritable(false)).isTrue();
            assertThat(cannotDeleteChild.setWritable(false)).isTrue();

            FileUtils.deleteDirectoryQuietly(cannotDeleteParent);
        } finally {
            //noinspection ResultOfMethodCallIgnored
            cannotDeleteParent.setWritable(true);
            //noinspection ResultOfMethodCallIgnored
            cannotDeleteChild.setWritable(true);
        }
    }

    @Test
    void testDeleteDirectoryWhichIsAFile() throws Exception {
        File file = Files.createFile(temporaryFolder.resolve("test")).toFile();
        // deleting a directory that is actually a file should fails
        assertThatThrownBy(() -> FileUtils.deleteDirectory(file))
                .withFailMessage("this should fail with an exception")
                .isInstanceOf(IOException.class);
    }

    /** Deleting a symbolic link directory should not delete the files in it. */
    @Test
    void testDeleteSymbolicLinkDirectory() throws Exception {
        // creating a directory to which the test creates a symbolic link
        File linkedDirectory = temporaryFolder.toFile();
        File fileInLinkedDirectory = new File(linkedDirectory, "child");
        assertThat(fileInLinkedDirectory.createNewFile()).isTrue();

        File symbolicLink = new File(temporaryFolder.toString(), "symLink");
        try {
            Files.createSymbolicLink(symbolicLink.toPath(), linkedDirectory.toPath());
        } catch (FileSystemException e) {
            // this operation can fail under Windows due to: "A required privilege is not held by
            // the client."
            assumeThat(OperatingSystem.isWindows())
                    .withFailMessage("This test does not work properly under Windows")
                    .isFalse();
            throw e;
        }

        FileUtils.deleteDirectory(symbolicLink);
        assertThat(fileInLinkedDirectory.exists()).isTrue();
    }

    @Test
    void testDeleteDirectoryConcurrently() throws Exception {
        final File parent = temporaryFolder.toFile();

        generateRandomDirs(parent, 20, 5, 3);

        // start three concurrent threads that delete the contents
        CheckedThread t1 = new Deleter(parent);
        CheckedThread t2 = new Deleter(parent);
        CheckedThread t3 = new Deleter(parent);
        t1.start();
        t2.start();
        t3.start();
        t1.sync();
        t2.sync();
        t3.sync();

        // assert is empty
        assertThat(parent.exists()).isFalse();
    }

    private static void generateRandomDirs(File dir, int numFiles, int numDirs, int depth)
            throws IOException {
        // generate the random files
        for (int i = 0; i < numFiles; i++) {
            File file = new File(dir, UUID.randomUUID().toString());
            try (FileOutputStream out = new FileOutputStream(file)) {
                out.write(1);
            }
        }

        if (depth > 0) {
            // generate the directories
            for (int i = 0; i < numDirs; i++) {
                File subdir = new File(dir, UUID.randomUUID().toString());
                assertThat(subdir.mkdir()).isTrue();
                generateRandomDirs(subdir, numFiles, numDirs, depth - 1);
            }
        }
    }

    // ------------------------------------------------------------------------

    private static class Deleter extends CheckedThread {

        private final File target;

        Deleter(File target) {
            this.target = target;
        }

        @Override
        public void go() throws Exception {
            FileUtils.deleteDirectory(target);
        }
    }
}
