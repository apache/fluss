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

package org.apache.fluss.fs.hdfs;

import org.apache.fluss.fs.FSDataInputStream;
import org.apache.fluss.fs.FSDataOutputStream;
import org.apache.fluss.fs.FileSystem;
import org.apache.fluss.fs.FileSystemBehaviorTestSuite;
import org.apache.fluss.fs.FileSystemFailure;
import org.apache.fluss.fs.FileSystemOperationException;
import org.apache.fluss.fs.FileSystemPathNotFoundException;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.utils.OperatingSystem;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.security.PrivilegedExceptionAction;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assumptions.assumeThat;

/** Behavior tests for HDFS. */
class HdfsBehaviorTest extends FileSystemBehaviorTestSuite {

    private static MiniDFSCluster hdfsCluster;

    private static FileSystem fs;

    private static FsPath basePath;

    // ------------------------------------------------------------------------

    @BeforeAll
    static void verifyOS() {
        assumeThat(OperatingSystem.isWindows())
                .describedAs("HDFS cluster cannot be started on Windows without extensions.")
                .isFalse();
    }

    @BeforeAll
    static void createHDFS(@TempDir File tmp) throws Exception {
        Configuration hdConf = new Configuration();
        hdConf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, tmp.getAbsolutePath());
        MiniDFSCluster.Builder builder = new MiniDFSCluster.Builder(hdConf);
        hdfsCluster = builder.build();

        org.apache.hadoop.fs.FileSystem hdfs = hdfsCluster.getFileSystem();
        fs = new HdfsFileSystem(hdfs);

        basePath = new FsPath(hdfs.getUri().toString() + "/tests");
    }

    @AfterAll
    static void destroyHDFS() throws Exception {
        if (hdfsCluster != null) {
            hdfsCluster
                    .getFileSystem()
                    .delete(new org.apache.hadoop.fs.Path(basePath.toUri()), true);
            hdfsCluster.shutdown();
        }
    }

    @Test
    void testHDFSOutputStream() throws Exception {
        final FsPath file = new FsPath(getBasePath(), randomName());
        try (FSDataOutputStream out = fs.create(file, FileSystem.WriteMode.NO_OVERWRITE)) {
            byte[] writtenBytes = new byte[] {1, 2, 3, 4};
            out.write(writtenBytes);
            assertThat(out.getPos()).isEqualTo(writtenBytes.length);
            out.flush();
            // now, we should read the data
            byte[] readBytes = new byte[4];
            try (FSDataInputStream in = fs.open(file)) {
                assertThat(in.read(readBytes)).isEqualTo(writtenBytes.length);
            }
            assertThat(readBytes).isEqualTo(writtenBytes);
        }
    }

    @Test
    void missingDirectoryIsReportedAsAConfirmedMissingPath() {
        FsPath missing = new FsPath(basePath, randomName());

        assertThatThrownBy(() -> fs.listStatus(missing))
                .isInstanceOf(FileSystemPathNotFoundException.class)
                .satisfies(
                        failure -> {
                            FileSystemFailure normalized = (FileSystemFailure) failure;
                            assertThat(normalized.kind())
                                    .isEqualTo(FileSystemFailure.Kind.NOT_FOUND);
                            assertThat(normalized.resource())
                                    .isEqualTo(FileSystemFailure.Resource.PATH);
                            assertThat(normalized.operation()).isEqualTo("list_status");
                            assertThat(normalized.isTemporary()).isFalse();
                            assertThat(normalized.serviceCode()).isNull();
                            assertThat(normalized.requestId()).isNull();
                            assertThat(failure.getCause()).isNotNull();
                        });
    }

    @Test
    void restrictedUserGetsPermissionFailureInsteadOfMissingPath() throws Exception {
        Path privateDirectory = new Path(basePath.toUri().toString(), randomName());
        org.apache.hadoop.fs.FileSystem owner = hdfsCluster.getFileSystem();
        owner.mkdirs(privateDirectory);
        owner.setPermission(privateDirectory, new FsPermission((short) 0700));

        UserGroupInformation restricted =
                UserGroupInformation.createUserForTesting(
                        "restricted-user", new String[] {"users"});
        restricted.doAs(
                (PrivilegedExceptionAction<Void>)
                        () -> {
                            try (org.apache.hadoop.fs.FileSystem hdfs =
                                    org.apache.hadoop.fs.FileSystem.newInstance(
                                            owner.getUri(), hdfsCluster.getConfiguration(0))) {
                                HdfsFileSystem restrictedFs = new HdfsFileSystem(hdfs);
                                assertThatThrownBy(
                                                () ->
                                                        restrictedFs.listStatus(
                                                                new FsPath(
                                                                        privateDirectory.toUri())))
                                        .isInstanceOf(FileSystemOperationException.class)
                                        .satisfies(
                                                failure -> {
                                                    FileSystemFailure normalized =
                                                            (FileSystemFailure) failure;
                                                    assertThat(normalized.kind())
                                                            .isEqualTo(
                                                                    FileSystemFailure.Kind
                                                                            .PERMISSION_DENIED);
                                                    assertThat(normalized.resource())
                                                            .isEqualTo(
                                                                    FileSystemFailure.Resource
                                                                            .UNKNOWN);
                                                });
                            }
                            return null;
                        });
    }

    // ------------------------------------------------------------------------

    @Override
    protected FileSystem getFileSystem() {
        return fs;
    }

    @Override
    protected FsPath getBasePath() {
        return basePath;
    }
}
