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

package org.apache.fluss.fs.local;

import org.apache.fluss.fs.FSDataInputStream;
import org.apache.fluss.fs.FSDataOutputStream;
import org.apache.fluss.fs.FileStatus;
import org.apache.fluss.fs.FileSystem;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.utils.ExecutorUtils;
import org.apache.fluss.utils.function.ThrowingConsumer;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.ClosedChannelException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

/**
 * This class tests the functionality of the {@link org.apache.fluss.fs.local.LocalFileSystem} class
 * in its components. In particular, file/directory access, creation, deletion, read, write is
 * tested.
 */
class LocalFileSystemTest {

    @TempDir private Path temporaryFolder;

    /**
     * This test checks the functionality of the {@link org.apache.fluss.fs.local.LocalFileSystem}
     * class.
     */
    @Test
    void testLocalFilesystem() throws Exception {
        final File tempdir = new File(temporaryFolder.toString(), UUID.randomUUID().toString());

        final File testfile1 = new File(tempdir, UUID.randomUUID().toString());
        final File testfile2 = new File(tempdir, UUID.randomUUID().toString());

        final FsPath pathtotestfile1 = new FsPath(testfile1.toURI().getPath());
        final FsPath pathtotestfile2 = new FsPath(testfile2.toURI().getPath());

        final LocalFileSystem lfs = new LocalFileSystem();

        final FsPath pathtotmpdir = new FsPath(tempdir.toURI().getPath());

        /*
         * check that lfs can see/create/delete/read directories
         */

        // check that dir is not existent yet
        assertThat(lfs.exists(pathtotmpdir)).isFalse();
        assertThat(tempdir.mkdirs()).isTrue();

        // check that local file system recognizes file..
        assertThat(lfs.exists(pathtotmpdir)).isTrue();
        final FileStatus localstatus1 = lfs.getFileStatus(pathtotmpdir);

        // check that lfs recognizes directory..
        assertThat(localstatus1.isDir()).isTrue();
        // check the gotten path
        FsPath expectedPath = new LocalFileStatus(new File(pathtotmpdir.getPath()), lfs).getPath();
        assertThat(localstatus1.getPath()).isEqualTo(expectedPath);

        // get status for files in this (empty) directory..
        final FileStatus[] statusforfiles = lfs.listStatus(pathtotmpdir);

        // no files in there.. hence, must be zero
        assertThat(statusforfiles.length).isEqualTo(0);

        // check that lfs can delete directory..
        lfs.delete(pathtotmpdir, true);

        // double check that directory is not existent anymore..
        assertThat(lfs.exists(pathtotmpdir)).isFalse();
        assertThat(tempdir.exists()).isFalse();

        // re-create directory..
        lfs.mkdirs(pathtotmpdir);

        // creation successful?
        assertThat(tempdir.exists()).isTrue();

        /*
         * check that lfs can create/read/write from/to files properly and read meta information..
         */

        // create files.. one ""natively"", one using lfs
        final FSDataOutputStream lfsoutput1 =
                lfs.create(pathtotestfile1, FileSystem.WriteMode.NO_OVERWRITE);
        assertThat(testfile2.createNewFile()).isTrue();

        // does lfs create files? does lfs recognize created files?
        assertThat(testfile1.exists()).isTrue();
        assertThat(lfs.exists(pathtotestfile2)).isTrue();

        // test that lfs can write to files properly
        final byte[] testbytes = {1, 2, 3, 4, 5};
        lfsoutput1.write(testbytes);
        lfsoutput1.close();

        assertThat(5L).isEqualTo(testfile1.length());

        byte[] testbytestest = new byte[5];
        try (FileInputStream fisfile1 = new FileInputStream(testfile1)) {
            assertThat(fisfile1.read(testbytestest)).isEqualTo(testbytestest.length);
        }

        assertThat(testbytestest).isEqualTo(testbytes);

        // does lfs see the correct file length?
        assertThat(testfile1.length()).isEqualTo(lfs.getFileStatus(pathtotestfile1).getLen());

        // as well, when we call the listStatus (that is intended for directories?)
        assertThat(testfile1.length()).isEqualTo(lfs.listStatus(pathtotestfile1)[0].getLen());

        // test that lfs can read files properly
        final FileOutputStream fosfile2 = new FileOutputStream(testfile2);
        fosfile2.write(testbytes);
        fosfile2.close();

        testbytestest = new byte[5];
        final FSDataInputStream lfsinput2 = lfs.open(pathtotestfile2);
        assertThat(5).isEqualTo(lfsinput2.read(testbytestest));
        lfsinput2.close();
        assertThat(testbytestest).isEqualTo(testbytes);

        // does lfs see two files?
        assertThat(2).isEqualTo(lfs.listStatus(pathtotmpdir).length);

        /*
         * can lfs delete files / directories?
         */
        assertThat(lfs.delete(pathtotestfile1, false)).isTrue();

        // and can lfs also delete directories recursively?
        assertThat(lfs.delete(pathtotmpdir, true)).isTrue();

        assertThat(tempdir.exists()).isFalse();
    }

    @Test
    void testRenamePath() throws IOException {
        final File rootDirectory = temporaryFolder.toFile();

        // create a file /root/src/B/test.csv
        final File srcDirectory = new File(new File(rootDirectory, "src"), "B");
        assertThat(srcDirectory.mkdirs()).isTrue();

        final File srcFile = new File(srcDirectory, "test.csv");
        assertThat(srcFile.createNewFile()).isTrue();

        // Move/rename B and its content to /root/dst/A
        final File destDirectory = new File(new File(rootDirectory, "dst"), "B");
        final File destFile = new File(destDirectory, "test.csv");

        final FsPath srcDirPath = new FsPath(srcDirectory.toURI());
        final FsPath srcFilePath = new FsPath(srcFile.toURI());
        final FsPath destDirPath = new FsPath(destDirectory.toURI());
        final FsPath destFilePath = new FsPath(destFile.toURI());

        FileSystem fs = FileSystem.get(LocalFileSystem.getLocalFsURI());

        // pre-conditions: /root/src/B exists but /root/dst/B does not
        assertThat(fs.exists(srcDirPath)).isTrue();
        assertThat(fs.exists(destDirPath)).isFalse();

        // do the move/rename: /root/src/B -> /root/dst/
        assertThat(fs.rename(srcDirPath, destDirPath)).isTrue();

        // post-conditions: /root/src/B doesn't exists, /root/dst/B/test.csv has been created
        assertThat(fs.exists(destFilePath)).isTrue();
        assertThat(fs.exists(srcDirPath)).isFalse();

        // re-create source file and test overwrite
        assertThat(srcDirectory.mkdirs()).isTrue();
        assertThat(srcFile.createNewFile()).isTrue();

        // overwrite the destination file
        assertThat(fs.rename(srcFilePath, destFilePath)).isTrue();

        // post-conditions: now only the src file has been moved
        assertThat(fs.exists(srcFilePath)).isFalse();
        assertThat(fs.exists(srcDirPath)).isTrue();
        assertThat(fs.exists(destFilePath)).isTrue();
    }

    @Test
    void testRenameNonExistingFile() throws IOException {
        final FileSystem fs = FileSystem.get(LocalFileSystem.getLocalFsURI());

        final File srcFile = new File(temporaryFolder.toString(), "someFile.txt");
        final File destFile = new File(temporaryFolder.toString(), "target");

        final FsPath srcFilePath = new FsPath(srcFile.toURI());
        final FsPath destFilePath = new FsPath(destFile.toURI());

        // this cannot succeed because the source file does not exist
        assertThat(fs.rename(srcFilePath, destFilePath)).isFalse();
    }

    @Test
    void testRenameToNonEmptyTargetDir() throws IOException {
        final FileSystem fs = FileSystem.get(LocalFileSystem.getLocalFsURI());

        // a source folder with a file
        final File srcFolder =
                Files.createTempDirectory(temporaryFolder, UUID.randomUUID().toString()).toFile();
        final File srcFile = new File(srcFolder, "someFile.txt");
        assertThat(srcFile.createNewFile()).isTrue();

        // a non-empty destination folder
        final File dstFolder =
                Files.createTempDirectory(temporaryFolder, UUID.randomUUID().toString()).toFile();
        final File dstFile = new File(dstFolder, "target");
        assertThat(dstFile.createNewFile()).isTrue();

        // this cannot succeed because the destination folder is not empty
        assertThat(fs.rename(new FsPath(srcFolder.toURI()), new FsPath(dstFolder.toURI())))
                .isFalse();

        // retry after deleting the occupying target file
        assertThat(dstFile.delete()).isTrue();
        assertThat(fs.rename(new FsPath(srcFolder.toURI()), new FsPath(dstFolder.toURI())))
                .isTrue();
        assertThat(new File(dstFolder, srcFile.getName()).exists()).isTrue();
    }

    @Test
    void testConcurrentMkdirs() throws Exception {
        final FileSystem fs = FileSystem.get(LocalFileSystem.getLocalFsURI());
        final File root = temporaryFolder.toFile();
        final int directoryDepth = 10;
        final int concurrentOperations = 10;

        final Collection<File> targetDirectories =
                createTargetDirectories(root, directoryDepth, concurrentOperations);

        final ExecutorService executor = Executors.newFixedThreadPool(concurrentOperations);
        final CyclicBarrier cyclicBarrier = new CyclicBarrier(concurrentOperations);

        try {
            final Collection<CompletableFuture<Void>> mkdirsFutures =
                    new ArrayList<>(concurrentOperations);
            for (File targetDirectory : targetDirectories) {
                final CompletableFuture<Void> mkdirsFuture =
                        CompletableFuture.runAsync(
                                () -> {
                                    try {
                                        cyclicBarrier.await();
                                        assertThat(fs.mkdirs(FsPath.fromLocalFile(targetDirectory)))
                                                .isTrue();
                                    } catch (Exception e) {
                                        throw new CompletionException(e);
                                    }
                                },
                                executor);

                mkdirsFutures.add(mkdirsFuture);
            }

            final CompletableFuture<Void> allFutures =
                    CompletableFuture.allOf(
                            mkdirsFutures.toArray(new CompletableFuture[concurrentOperations]));

            allFutures.get();
        } finally {
            final long timeout = 10000L;
            ExecutorUtils.gracefulShutdown(timeout, TimeUnit.MILLISECONDS, executor);
        }
    }

    @Test
    void testCreatingFileInCurrentDirectoryWithRelativePath() throws IOException {
        FileSystem fs = FileSystem.get(LocalFileSystem.getLocalFsURI());

        FsPath filePath = new FsPath("local_fs_test_" + UUID.randomUUID());
        try (FSDataOutputStream outputStream =
                fs.create(filePath, FileSystem.WriteMode.OVERWRITE)) {
            // Do nothing.
        } finally {
            for (int i = 0; i < 10 && fs.exists(filePath); ++i) {
                fs.delete(filePath, true);
            }
        }
    }

    @Test
    void testFlushMethodFailsOnClosedOutputStream() throws IOException {
        assertThatExceptionOfType(ClosedChannelException.class)
                .isThrownBy(() -> testMethodCallFailureOnClosedStream(FSDataOutputStream::flush));
    }

    @Test
    void testWriteIntegerMethodFailsOnClosedOutputStream() throws IOException {
        assertThatExceptionOfType(ClosedChannelException.class)
                .isThrownBy(() -> testMethodCallFailureOnClosedStream(os -> os.write(0)));
    }

    @Test
    void testWriteBytesMethodFailsOnClosedOutputStream() throws IOException {
        assertThatExceptionOfType(ClosedChannelException.class)
                .isThrownBy(() -> testMethodCallFailureOnClosedStream(os -> os.write(new byte[0])));
    }

    @Test
    void testWriteBytesSubArrayMethodFailsOnClosedOutputStream() throws IOException {
        assertThatExceptionOfType(ClosedChannelException.class)
                .isThrownBy(
                        () ->
                                testMethodCallFailureOnClosedStream(
                                        os -> os.write(new byte[0], 0, 0)));
    }

    @Test
    void testGetPosMethodFailsOnClosedOutputStream() throws IOException {
        assertThatExceptionOfType(ClosedChannelException.class)
                .isThrownBy(() -> testMethodCallFailureOnClosedStream(FSDataOutputStream::getPos));
    }

    private void testMethodCallFailureOnClosedStream(
            ThrowingConsumer<FSDataOutputStream, IOException> callback) throws IOException {
        final FileSystem fs = FileSystem.get(LocalFileSystem.getLocalFsURI());
        final FSDataOutputStream outputStream =
                fs.create(
                        new FsPath(
                                temporaryFolder.toString(), "close_fs_test_" + UUID.randomUUID()),
                        FileSystem.WriteMode.OVERWRITE);
        outputStream.close();
        callback.accept(outputStream);
    }

    private Collection<File> createTargetDirectories(
            File root, int directoryDepth, int numberDirectories) {
        final StringBuilder stringBuilder = new StringBuilder();

        for (int i = 0; i < directoryDepth; i++) {
            stringBuilder.append('/').append(i);
        }

        final Collection<File> targetDirectories = new ArrayList<>(numberDirectories);

        for (int i = 0; i < numberDirectories; i++) {
            targetDirectories.add(new File(root, stringBuilder.toString() + '/' + i));
        }

        return targetDirectories;
    }
}
