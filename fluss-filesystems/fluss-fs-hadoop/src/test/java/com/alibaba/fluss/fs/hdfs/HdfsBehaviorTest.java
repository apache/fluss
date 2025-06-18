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

package com.alibaba.fluss.fs.hdfs;

import com.alibaba.fluss.fs.FSDataInputStream;
import com.alibaba.fluss.fs.FSDataOutputStream;
import com.alibaba.fluss.fs.FileSystem;
import com.alibaba.fluss.fs.FileSystemBehaviorTestSuite;
import com.alibaba.fluss.fs.FsPath;
import com.alibaba.fluss.utils.OperatingSystem;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;

import static org.assertj.core.api.Assertions.assertThat;
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
        fs = new HadoopFileSystem(hdfs);

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
