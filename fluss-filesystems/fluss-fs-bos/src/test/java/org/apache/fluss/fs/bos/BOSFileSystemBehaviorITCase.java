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

package org.apache.fluss.fs.bos;

import org.apache.fluss.config.Configuration;
import org.apache.fluss.fs.FileSystem;
import org.apache.fluss.fs.FileSystemBehaviorTestSuite;
import org.apache.fluss.fs.FsPath;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

import java.util.UUID;

/**
 * An implementation of the {@link FileSystemBehaviorTestSuite} for the BOS file system with the BOS
 * Java SDK, using a long-term access key.
 */
class BOSFileSystemBehaviorITCase extends FileSystemBehaviorTestSuite {

    private static final String TEST_DATA_DIR = "tests-" + UUID.randomUUID();

    @BeforeAll
    static void setup() {
        BOSTestCredentials.assumeCredentialsAvailable();

        final Configuration conf = new Configuration();
        conf.setString("bos.endpoint", BOSTestCredentials.getBOSEndpoint());
        conf.setString("bos.region", BOSTestCredentials.getBOSRegion());
        conf.setString("bos.access.key", BOSTestCredentials.getBOSAccessKey());
        conf.setString("bos.secret.key", BOSTestCredentials.getBOSSecretKey());
        FileSystem.initialize(conf, null);
    }

    @AfterAll
    static void clearFsConfig() throws Exception {
        if (BOSTestCredentials.credentialsAvailable()) {
            // some cases of the suite create files directly under the test dir and leave them
            FsPath testDir = new FsPath(BOSTestCredentials.getTestBucketUri() + TEST_DATA_DIR);
            testDir.getFileSystem().delete(testDir, true);
        }
        FileSystem.initialize(new Configuration(), null);
    }

    @Override
    protected FileSystem getFileSystem() throws Exception {
        return getBasePath().getFileSystem();
    }

    @Override
    protected FsPath getBasePath() {
        return new FsPath(BOSTestCredentials.getTestBucketUri() + TEST_DATA_DIR);
    }
}
