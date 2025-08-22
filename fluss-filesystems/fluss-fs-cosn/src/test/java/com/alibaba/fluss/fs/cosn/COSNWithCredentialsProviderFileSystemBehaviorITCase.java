/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.fs.cosn;

import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.fs.FileSystem;
import com.alibaba.fluss.fs.FileSystemBehaviorTestSuite;
import com.alibaba.fluss.fs.FsPath;

import org.apache.hadoop.fs.cosn.auth.EnvironmentVariableCredentialsProvider;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

import java.util.UUID;

import static org.apache.hadoop.fs.cosn.CosNConfigKeys.COSN_CREDENTIALS_PROVIDER;
import static org.apache.hadoop.fs.cosn.CosNConfigKeys.COSN_REGION_KEY;
import static org.apache.hadoop.fs.cosn.CosNConfigKeys.COSN_SECRET_ID_KEY;
import static org.apache.hadoop.fs.cosn.CosNConfigKeys.COSN_SECRET_KEY_KEY;

/**
 * IT case for access cosn via set {@link org.apache.hadoop.fs.cosn.auth.SimpleCredentialsProvider}.
 */
class COSNWithCredentialsProviderFileSystemBehaviorITCase extends FileSystemBehaviorTestSuite {

    private static final String TEST_DATA_DIR = "tests-" + UUID.randomUUID();

    @BeforeAll
    static void setup() {
        COSTestCredentials.assumeCredentialsAvailable();

        // use SystemPropertiesCredentialProvider
        final Configuration conf = new Configuration();
        conf.setString(
                COSN_CREDENTIALS_PROVIDER, EnvironmentVariableCredentialsProvider.class.getName());
        conf.setString(COSN_REGION_KEY, COSTestCredentials.getCOSNRegion());

        // now, we need to set cosn config to system properties
        System.setProperty(COSN_SECRET_ID_KEY, COSTestCredentials.getCOSNAccessKey());
        System.setProperty(COSN_SECRET_KEY_KEY, COSTestCredentials.getCOSNSecretKey());
        FileSystem.initialize(conf, null);
    }

    @AfterAll
    static void cleanup() {
        // clean up system properties
        System.clearProperty(COSN_SECRET_ID_KEY);
        System.clearProperty(COSN_SECRET_KEY_KEY);
        FileSystem.initialize(new Configuration(), null);
    }

    @Override
    protected FileSystem getFileSystem() throws Exception {
        return getBasePath().getFileSystem();
    }

    @Override
    protected FsPath getBasePath() throws Exception {
        return new FsPath(COSTestCredentials.getTestBucketUri() + TEST_DATA_DIR);
    }
}
