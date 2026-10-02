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

package org.apache.fluss.fs.s3;

import org.apache.fluss.config.Configuration;
import org.apache.fluss.fs.FSDataInputStream;
import org.apache.fluss.fs.FSDataOutputStream;
import org.apache.fluss.fs.FileStatus;
import org.apache.fluss.fs.FileSystem;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.utils.IOUtils;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests the Hadoop S3A and AWS SDK v2 path against a hermetic S3-compatible backend. */
@Testcontainers(disabledWithoutDocker = true)
class SeaweedFsS3FileSystemBehaviorITCase {

    @Container private static final SeaweedFsTestContainer SEAWEEDFS = new SeaweedFsTestContainer();

    private static final Duration CONSISTENCY_TIMEOUT = Duration.ofSeconds(10);

    private static FileSystem fileSystem;
    private static FsPath bucketPath;

    @BeforeAll
    static void initializeFileSystem() throws Exception {
        FileSystem.initialize(SEAWEEDFS.createS3Configuration(), null);
        bucketPath = new FsPath(SEAWEEDFS.getBucketUri());
        fileSystem = bucketPath.getFileSystem();
    }

    @AfterAll
    static void resetFileSystem() {
        FileSystem.initialize(new Configuration(), null);
    }

    @Test
    void testWriteReadListAndDelete() throws Exception {
        FsPath directory = new FsPath(bucketPath, "write-read-list-delete");
        FsPath file = new FsPath(directory, "data.txt");
        byte[] expected = "fluss-through-s3a".getBytes(StandardCharsets.UTF_8);

        try {
            assertThat(fileSystem.mkdirs(directory)).isTrue();
            try (FSDataOutputStream output =
                    fileSystem.create(file, FileSystem.WriteMode.NO_OVERWRITE)) {
                output.write(expected);
            }

            awaitPathState(file, true);
            awaitDirectoryContains(directory, file);
            assertFileContent(file, expected);

            assertThat(fileSystem.delete(file, false)).isTrue();
            awaitPathState(file, false);
        } finally {
            fileSystem.delete(directory, true);
            awaitPathState(directory, false);
        }
    }

    @Test
    void testRenameUsesS3CopyAndDeletePath() throws Exception {
        FsPath directory = new FsPath(bucketPath, "rename");
        FsPath source = new FsPath(directory, "source.txt");
        FsPath destination = new FsPath(directory, "destination.txt");
        byte[] expected = "rename-through-s3a".getBytes(StandardCharsets.UTF_8);

        try {
            assertThat(fileSystem.mkdirs(directory)).isTrue();
            try (FSDataOutputStream output =
                    fileSystem.create(source, FileSystem.WriteMode.NO_OVERWRITE)) {
                output.write(expected);
            }
            awaitPathState(source, true);

            assertThat(fileSystem.rename(source, destination)).isTrue();
            awaitPathState(source, false);
            awaitPathState(destination, true);
            assertFileContent(destination, expected);
        } finally {
            fileSystem.delete(directory, true);
            awaitPathState(directory, false);
        }
    }

    private static void assertFileContent(FsPath file, byte[] expected) throws Exception {
        byte[] actual = new byte[expected.length];
        try (FSDataInputStream input = fileSystem.open(file)) {
            IOUtils.readFully(input, actual);
        }
        assertThat(actual).isEqualTo(expected);
    }

    private static void awaitDirectoryContains(FsPath directory, FsPath expectedFile)
            throws Exception {
        long deadline = System.nanoTime() + CONSISTENCY_TIMEOUT.toNanos();
        boolean found;
        do {
            FileStatus[] statuses = fileSystem.listStatus(directory);
            found =
                    Arrays.stream(statuses)
                            .anyMatch(status -> status.getPath().equals(expectedFile));
            if (!found) {
                Thread.sleep(50L);
            }
        } while (!found && System.nanoTime() - deadline < 0);
        assertThat(found).as("directory %s contains %s", directory, expectedFile).isTrue();
    }

    private static void awaitPathState(FsPath path, boolean expectedExists) throws Exception {
        long deadline = System.nanoTime() + CONSISTENCY_TIMEOUT.toNanos();
        boolean exists;
        do {
            exists = fileSystem.exists(path);
            if (exists != expectedExists) {
                Thread.sleep(50L);
            }
        } while (exists != expectedExists && System.nanoTime() - deadline < 0);
        assertThat(exists).as("path %s existence", path).isEqualTo(expectedExists);
    }
}
