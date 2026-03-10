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

package org.apache.fluss.fs.cos;

import org.apache.fluss.config.Configuration;
import org.apache.fluss.fs.FileSystem;
import org.apache.fluss.fs.FileSystemBehaviorTestSuite;
import org.apache.fluss.fs.FsPath;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

import java.util.UUID;

/**
 * An implementation of the {@link FileSystemBehaviorTestSuite} for the COS file system with Hadoop
 * cos sdk.
 */
class COSFileSystemBehaviorITCase extends FileSystemBehaviorTestSuite {

    private static final String TEST_DATA_DIR = "tests-" + UUID.randomUUID();

    @BeforeAll
    static void setup() {
        COSTestCredentials.assumeCredentialsAvailable();

        final Configuration conf = new Configuration();
        conf.setString("fs.cosn.endpoint", COSTestCredentials.getCOSEndpoint());
        conf.setString("fs.cosn.userinfo.secretId", COSTestCredentials.getCOSSecretId());
        conf.setString("fs.cosn.userinfo.secretKey", COSTestCredentials.getCOSSecretKey());
        FileSystem.initialize(conf, null);
    }

    @Override
    protected FileSystem getFileSystem() throws Exception {
        return getBasePath().getFileSystem();
    }

    @Override
    protected FsPath getBasePath() {
        return new FsPath(COSTestCredentials.getTestBucketUri() + TEST_DATA_DIR);
    }

    @AfterAll
    static void clearFsConfig() {
        FileSystem.initialize(new Configuration(), null);
    }
}
