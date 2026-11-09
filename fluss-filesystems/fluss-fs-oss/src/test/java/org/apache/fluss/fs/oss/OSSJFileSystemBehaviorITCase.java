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

package org.apache.fluss.fs.oss;

import org.apache.fluss.fs.FSDataOutputStream;
import org.apache.fluss.fs.FileStatus;
import org.apache.fluss.fs.FileSystem;
import org.apache.fluss.fs.FsPath;

import org.junit.jupiter.api.Test;

import java.io.DataInputStream;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/** OSS filesystem behavior tests for the ossj fallback scheme. */
class OSSJFileSystemBehaviorITCase extends OSSFileSystemBehaviorITCase {

    private static final String TEST_DATA_DIR = "tests-ossj-" + UUID.randomUUID();

    @Test
    void testRenameAndReadListedPathWithEscapedName() throws Exception {
        FsPath directory = new FsPath(getBasePath(), randomName());
        FsPath source = new FsPath(directory, "before space%.txt");
        FsPath target = new FsPath(directory, "after space%.txt");
        byte[] expected = {1, 2, 3};
        try {
            fs.mkdirs(directory);
            try (FSDataOutputStream output = fs.create(source, FileSystem.WriteMode.NO_OVERWRITE)) {
                output.write(expected);
            }
            assertThat(fs.rename(source, target)).isTrue();
            assertThat(fs.exists(source)).isFalse();
            FileStatus[] statuses = fs.listStatus(directory);
            assertThat(statuses).hasSize(1);
            FsPath listedPath = statuses[0].getPath();
            assertThat(listedPath).isEqualTo(target);
            assertThat(listedPath.toUri().getScheme()).isEqualTo("ossj");
            try (DataInputStream input =
                    new DataInputStream(listedPath.getFileSystem().open(listedPath))) {
                byte[] actual = new byte[expected.length];
                input.readFully(actual);
                assertThat(actual).isEqualTo(expected);
            }
        } finally {
            fs.delete(directory, true);
        }
    }

    @Override
    protected FsPath getBasePath() {
        return new FsPath(OSSTestCredentials.getTestBucketUriWithScheme("ossj") + TEST_DATA_DIR);
    }
}
