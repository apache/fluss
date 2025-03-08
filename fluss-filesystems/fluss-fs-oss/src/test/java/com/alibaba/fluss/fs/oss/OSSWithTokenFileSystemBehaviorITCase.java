/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.fs.oss;

import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.fs.FileSystem;
import com.alibaba.fluss.fs.FsPath;
import com.alibaba.fluss.fs.oss.token.OSSSecurityTokenReceiver;
import com.alibaba.fluss.fs.token.ObtainedSecurityToken;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

import java.util.UUID;

/** IT case for access oss with sts token in hadoop sdk as FileSystem. */
class OSSWithTokenFileSystemBehaviorITCase extends OSSWithTokenFileSystemBehaviorBaseITCase {

    private static final String TEST_DATA_DIR = "tests-" + UUID.randomUUID();

    @BeforeAll
    static void setup() throws Exception {
        // init a filesystem with ak/sk so that it can generate sts token
        initFileSystemWithSecretKey();
        // now, we can init with sts token
        initFileSystemWithToken(getFsPath());
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
        return new FsPath(OSSTestCredentials.getTestBucketUri() + TEST_DATA_DIR);
    }

    @AfterAll
    static void clearFsConfig() {
        FileSystem.initialize(new Configuration(), null);
    }

    private static void initFileSystemWithToken(FsPath fsPath) throws Exception {
        Configuration configuration = new Configuration();
        // obtain a security token and call onNewTokensObtained
        ObtainedSecurityToken obtainedSecurityToken = fsPath.getFileSystem().obtainSecurityToken();
        OSSSecurityTokenReceiver ossSecurityTokenReceiver = new OSSSecurityTokenReceiver();
        ossSecurityTokenReceiver.onNewTokensObtained(obtainedSecurityToken);

        FileSystem.initialize(configuration, null);
    }
}
