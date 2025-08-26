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
import org.apache.fluss.fs.FileSystem;
import org.apache.fluss.fs.FileSystemBehaviorTestSuite;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.fs.s3.token.S3DelegationTokenReceiver;
import org.apache.fluss.fs.token.ObtainedSecurityToken;

import org.junit.jupiter.api.BeforeAll;

import java.io.IOException;
import java.util.UUID;

/** Base case for access s3 with token. */
class S3WithTokenFileSystemBehaviorITCase extends FileSystemBehaviorTestSuite {

    private static final String TEST_DATA_DIR = "tests-" + UUID.randomUUID();

    @BeforeAll
    static void setup() throws Exception {
        S3TestCredentials.assumeCredentialsAvailable();

        // first init filesystem with ak/sk
        Configuration conf = new Configuration();
        conf.setString("s3.access.key", S3TestCredentials.getS3AccessKey());
        conf.setString("s3.secret.key", S3TestCredentials.getS3SecretKey());
        conf.setString("s3.region", S3TestCredentials.getS3Region());

        FileSystem.initialize(conf, null);

        // then init with token
        initFileSystemWithToken();
    }

    @Override
    protected FileSystem getFileSystem() throws IOException {
        return getBasePath().getFileSystem();
    }

    @Override
    protected FsPath getBasePath() {
        return new FsPath(S3TestCredentials.getTestBucketUri() + TEST_DATA_DIR);
    }

    protected static void initFileSystemWithToken() throws Exception {
        Configuration configuration = new Configuration();
        FsPath fsPath = new FsPath(S3TestCredentials.getTestBucketUri() + TEST_DATA_DIR);
        // obtain a security token and call onNewTokensObtained
        ObtainedSecurityToken obtainedSecurityToken = fsPath.getFileSystem().obtainSecurityToken();

        S3DelegationTokenReceiver s3DelegationTokenReceiver = new S3DelegationTokenReceiver();
        s3DelegationTokenReceiver.onNewTokensObtained(obtainedSecurityToken);

        FileSystem.initialize(configuration, null);
    }
}
