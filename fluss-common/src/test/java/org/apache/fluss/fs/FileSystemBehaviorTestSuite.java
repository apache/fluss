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

package org.apache.fluss.fs;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;

/** Common tests for the behavior of {@link org.apache.fluss.fs.FileSystem} methods. */
public abstract class FileSystemBehaviorTestSuite {

    /** The cached file system instance. */
    protected FileSystem fs;

    /** The cached base path. */
    private FsPath basePath;

    // ------------------------------------------------------------------------
    //  FileSystem-specific methods
    // ------------------------------------------------------------------------

    /** Gets an instance of the {@code FileSystem} to be tested. */
    protected abstract FileSystem getFileSystem() throws Exception;

    /** Gets the base path in the file system under which tests will place their temporary files. */
    protected abstract FsPath getBasePath() throws Exception;

    protected long getConsistencyToleranceNS() {
        return 0;
    }

    // ------------------------------------------------------------------------
    //  Init / Cleanup
    // ------------------------------------------------------------------------

    @BeforeEach
    void prepare() throws Exception {
        fs = getFileSystem();
        basePath = new FsPath(getBasePath(), randomName());
        fs.mkdirs(basePath);
    }

    @AfterEach
    void cleanup() throws Exception {
        fs.delete(basePath, true);
    }

    // ------------------------------------------------------------------------
    //  Suite of Tests
    // ------------------------------------------------------------------------

    // --- access and scheme

    @Test
    void testPathAndScheme() throws Exception {
        assertThat(fs.getUri()).isEqualTo(getBasePath().getFileSystem().getUri());
        assertThat(fs.getUri().getScheme()).isEqualTo(getBasePath().toUri().getScheme());
    }

    // --- exists

    @Test
    void testFileExists() throws IOException {
        final FsPath filePath = createRandomFileInDirectory(basePath);
        assertThat(fs.exists(filePath)).isTrue();
    }

    @Test
    void testFileDoesNotExist() throws IOException {
        assertThat(fs.exists(new FsPath(basePath, randomName()))).isFalse();
    }

    // --- delete

    @Test
    void testExistingFileDeletion() throws IOException {
        testSuccessfulDeletion(createRandomFileInDirectory(basePath), false);
    }

    @Test
    void testExistingFileRecursiveDeletion() throws IOException {
        testSuccessfulDeletion(createRandomFileInDirectory(basePath), true);
    }

    @Test
    void testNotExistingFileDeletion() throws IOException {
        testSuccessfulDeletion(new FsPath(basePath, randomName()), false);
    }

    @Test
    void testNotExistingFileRecursiveDeletion() throws IOException {
        testSuccessfulDeletion(new FsPath(basePath, randomName()), true);
    }

    @Test
    void testExistingEmptyDirectoryDeletion() throws IOException {
        final FsPath path = new FsPath(basePath, randomName());
        fs.mkdirs(path);
        testSuccessfulDeletion(path, false);
    }

    @Test
    void testExistingEmptyDirectoryRecursiveDeletion() throws IOException {
        final FsPath path = new FsPath(basePath, randomName());
        fs.mkdirs(path);
        testSuccessfulDeletion(path, true);
    }

    private void testSuccessfulDeletion(FsPath path, boolean recursionEnabled) throws IOException {
        fs.delete(path, recursionEnabled);
        assertThat(fs.exists(path)).isFalse();
    }

    @Test
    void testExistingNonEmptyDirectoryDeletion() throws IOException {
        final FsPath directoryPath = new FsPath(basePath, randomName());
        final FsPath filePath = createRandomFileInDirectory(directoryPath);

        assertThatThrownBy(() -> fs.delete(directoryPath, false)).isInstanceOf(IOException.class);
        assertThat(fs.exists(directoryPath)).isTrue();
        assertThat(fs.exists(filePath)).isTrue();
    }

    @Test
    void testExistingNonEmptyDirectoryRecursiveDeletion() throws IOException {
        final FsPath directoryPath = new FsPath(basePath, randomName());
        final FsPath filePath = createRandomFileInDirectory(directoryPath);

        fs.delete(directoryPath, true);
        assertThat(fs.exists(directoryPath)).isFalse();
        assertThat(fs.exists(filePath)).isFalse();
    }

    @Test
    void testExistingNonEmptyDirectoryWithSubDirRecursiveDeletion() throws IOException {
        final FsPath level1SubDirWithFile = new FsPath(basePath, randomName());
        final FsPath fileInLevel1Subdir = createRandomFileInDirectory(level1SubDirWithFile);
        final FsPath level2SubDirWithFile = new FsPath(level1SubDirWithFile, randomName());
        final FsPath fileInLevel2Subdir = createRandomFileInDirectory(level2SubDirWithFile);

        testSuccessfulDeletion(level1SubDirWithFile, true);
        assertThat(fs.exists(fileInLevel1Subdir)).isFalse();
        assertThat(fs.exists(level2SubDirWithFile)).isFalse();
        assertThat(fs.exists(fileInLevel2Subdir)).isFalse();
    }

    // --- mkdirs

    @Test
    void testMkdirsReturnsTrueWhenCreatingDirectory() throws Exception {
        // this test applies to object stores as well, as rely on the fact that they
        // return true when things are not bad

        final FsPath directory = new FsPath(basePath, randomName());
        assertThat(fs.mkdirs(directory)).isTrue();
    }

    @Test
    void testMkdirsCreatesParentDirectories() throws Exception {
        // this test applies to object stores as well, as rely on the fact that they
        // return true when things are not bad

        final FsPath directory =
                new FsPath(
                        new FsPath(new FsPath(basePath, randomName()), randomName()), randomName());
        assertThat(fs.mkdirs(directory)).isTrue();
    }

    @Test
    void testMkdirsReturnsTrueForExistingDirectory() throws Exception {
        // this test applies to object stores as well, as rely on the fact that they
        // return true when things are not bad

        final FsPath directory = new FsPath(basePath, randomName());

        // make sure the directory exists
        createRandomFileInDirectory(directory);
        assertThat(fs.listStatus(directory).length).isGreaterThan(0);

        assertThat(fs.mkdirs(directory)).isTrue();
    }

    @Test
    protected void testMkdirsFailsForExistingFile() throws Exception {
        final FsPath file = new FsPath(getBasePath(), randomName());
        createFile(file);

        try {
            fs.mkdirs(file);
            fail("should fail with an IOException");
        } catch (IOException e) {
            // good!
        }
    }

    @Test
    void testMkdirsFailsWithExistingParentFile() throws Exception {
        final FsPath file = new FsPath(getBasePath(), randomName());
        createFile(file);

        final FsPath dirUnderFile = new FsPath(file, randomName());
        try {
            fs.mkdirs(dirUnderFile);
            fail("should fail with an IOException");
        } catch (IOException e) {
            // good!
        }
    }

    @Test
    void testSimpleFileWriteAndRead() throws Exception {
        final String testLine = "Hello Upload!";
        final FsPath path = new FsPath(basePath, "test.txt");
        try {
            try (FSDataOutputStream out = fs.create(path, FileSystem.WriteMode.OVERWRITE);
                    OutputStreamWriter writer =
                            new OutputStreamWriter(out, StandardCharsets.UTF_8)) {
                writer.write(testLine);
            }
            // just in case, wait for the path to exist
            checkPathExistence(path, true, getConsistencyToleranceNS());
            try (FSDataInputStream in = fs.open(path);
                    InputStreamReader ir = new InputStreamReader(in, StandardCharsets.UTF_8);
                    BufferedReader reader = new BufferedReader(ir)) {
                String line = reader.readLine();
                assertThat(line).isEqualTo(testLine);
            }
        } finally {
            fs.delete(path, false);
        }
        checkPathExistence(path, false, getConsistencyToleranceNS());
    }

    @Test
    void testDirectoryListing() throws Exception {
        final FsPath directory = new FsPath(basePath, "testdir/");
        // directory must not yet exist
        assertThat(fs.exists(directory)).isFalse();
        try {
            // create directory
            assertThat(fs.mkdirs(directory)).isTrue();
            checkEmptyDirectory(directory);
            // directory empty
            assertThat(fs.listStatus(directory)).isEmpty();
            // create some files
            final int numFiles = 3;
            for (int i = 0; i < numFiles; i++) {
                FsPath file = new FsPath(directory, "/file-" + i);
                try (FSDataOutputStream out = fs.create(file, FileSystem.WriteMode.OVERWRITE);
                        OutputStreamWriter writer =
                                new OutputStreamWriter(out, StandardCharsets.UTF_8)) {
                    writer.write("hello-" + i + "\n");
                }
                // just in case, wait for the file to exist (should then also be reflected in the
                // directory's file list below)
                checkPathExistence(file, true, getConsistencyToleranceNS());
            }
            FileStatus[] files = fs.listStatus(directory);
            assertThat(files).isNotNull();
            assertThat(files).hasSize(3);
            for (FileStatus status : files) {
                assertThat(status.isDir()).isFalse();
            }
            // now that there are files, the directory must exist
            assertThat(fs.exists(directory)).isTrue();
            assertThat(fs.getFileStatus(directory).getPath()).isEqualTo(directory);
        } finally {
            // clean up
            cleanupDirectoryWithRetry(fs, directory, getConsistencyToleranceNS());
        }
    }

    // ------------------------------------------------------------------------
    //  Utilities
    // ------------------------------------------------------------------------

    protected static String randomName() {
        return UUID.randomUUID().toString();
    }

    private void createFile(FsPath file) throws IOException {
        try (FSDataOutputStream out = fs.create(file, FileSystem.WriteMode.NO_OVERWRITE)) {
            out.write(new byte[] {1, 2, 3, 4, 5, 6, 7, 8});
        }
    }

    private FsPath createRandomFileInDirectory(FsPath directory) throws IOException {
        fs.mkdirs(directory);
        final FsPath filePath = new FsPath(directory, randomName());
        createFile(filePath);

        return filePath;
    }

    void checkEmptyDirectory(FsPath path) throws IOException, InterruptedException {
        checkPathExistence(path, true, getConsistencyToleranceNS());
    }

    private void checkPathExistence(
            FsPath path, boolean expectedExists, long consistencyToleranceNS)
            throws IOException, InterruptedException {
        if (consistencyToleranceNS == 0) {
            // strongly consistency
            assertThat(fs.exists(path)).isEqualTo(expectedExists);
        } else {
            // eventually consistency
            checkPathEventualExistence(fs, path, expectedExists, consistencyToleranceNS);
        }
    }

    /**
     * Verifies that the given path eventually appears on / disappears from <tt>fs</tt> within
     * <tt>consistencyToleranceNS</tt> nanoseconds.
     */
    private static void checkPathEventualExistence(
            FileSystem fs, FsPath path, boolean expectedExists, long consistencyToleranceNS)
            throws IOException, InterruptedException {
        boolean dirExists;
        long deadline = System.nanoTime() + consistencyToleranceNS;
        while ((dirExists = fs.exists(path)) != expectedExists
                && System.nanoTime() - deadline < 0) {
            Thread.sleep(10);
        }
        assertThat(dirExists).isEqualTo(expectedExists);
    }

    private static void cleanupDirectoryWithRetry(
            FileSystem fs, FsPath path, long consistencyToleranceNS)
            throws IOException, InterruptedException {
        fs.delete(path, true);
        long deadline = System.nanoTime() + consistencyToleranceNS;
        while (fs.exists(path) && System.nanoTime() - deadline < 0) {
            fs.delete(path, true);
            Thread.sleep(50L);
        }
        assertThat(fs.exists(path)).isFalse();
    }
}
