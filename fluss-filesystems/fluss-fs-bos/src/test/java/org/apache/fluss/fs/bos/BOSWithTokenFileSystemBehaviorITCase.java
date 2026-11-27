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
import org.apache.fluss.fs.bos.token.BOSSecurityTokenReceiver;
import org.apache.fluss.fs.token.ObtainedSecurityToken;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

import java.util.UUID;

/**
 * IT case for access BOS with a security token issued by the server side, i.e. without any
 * long-term access key on the client.
 */
class BOSWithTokenFileSystemBehaviorITCase extends FileSystemBehaviorTestSuite {

    private static final String TEST_DATA_DIR = "tests-" + UUID.randomUUID();

    @BeforeAll
    static void setup() throws Exception {
        BOSTestCredentials.assumeCredentialsAvailable();

        // first init a filesystem with the access key so that it can issue a token. The suite
        // writes, so the token needs write permission on top of the read-only default.
        final Configuration conf = new Configuration();
        conf.setString("bos.endpoint", BOSTestCredentials.getBOSEndpoint());
        conf.setString("bos.region", BOSTestCredentials.getBOSRegion());
        conf.setString("bos.access.key", BOSTestCredentials.getBOSAccessKey());
        conf.setString("bos.secret.key", BOSTestCredentials.getBOSSecretKey());
        conf.setString("bos.sts.permissions", "READ,WRITE,LIST");
        // the issued credential is valid for the whole bucket, keep it short lived
        conf.setString("bos.sts.duration.seconds", "1800");
        if (BOSTestCredentials.getBOSStsEndpoint() != null) {
            conf.setString("bos.sts.endpoint", BOSTestCredentials.getBOSStsEndpoint());
        }
        FileSystem.initialize(conf, null);

        // obtain a security token and hand it to the receiver, like a client would
        ObtainedSecurityToken token = getFsPath().getFileSystem().obtainSecurityToken();
        new BOSSecurityTokenReceiver().onNewTokensObtained(token);

        // now init without any key: the file system has to sign with the token
        FileSystem.initialize(new Configuration(), null);
    }

    @AfterAll
    static void clearFsConfig() throws Exception {
        if (BOSTestCredentials.credentialsAvailable()) {
            // some cases of the suite create files directly under the test dir and leave them
            // behind
            getFsPath().getFileSystem().delete(getFsPath(), true);
        }
        FileSystem.initialize(new Configuration(), null);
    }

    @Override
    protected FileSystem getFileSystem() throws Exception {
        return getFsPath().getFileSystem();
    }

    @Override
    protected FsPath getBasePath() {
        return getFsPath();
    }

    private static FsPath getFsPath() {
        return new FsPath(BOSTestCredentials.getTestBucketUri() + TEST_DATA_DIR);
    }
}
