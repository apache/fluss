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

package com.alibaba.fluss.fs.oss;

import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.fs.FileSystem;
import com.alibaba.fluss.fs.FileSystemBehaviorTestSuite;
import com.alibaba.fluss.fs.FsPath;

import com.aliyun.oss.common.auth.CredentialsProvider;
import com.aliyun.oss.common.auth.SystemPropertiesCredentialsProvider;
import org.junit.jupiter.api.BeforeAll;

import java.util.UUID;

import static org.apache.hadoop.fs.aliyun.oss.Constants.CREDENTIALS_PROVIDER_KEY;

/** IT case for access oss via set {@link CredentialsProvider}. */
class OSSWithCredentialsProviderFileSystemBehaviorITCase extends FileSystemBehaviorTestSuite {

    private static final String TEST_DATA_DIR = "tests-" + UUID.randomUUID();

    @BeforeAll
    static void setup() {
        OSSTestCredentials.assumeCredentialsAvailable();

        // use SystemPropertiesCredentialsProvider
        final Configuration conf = new Configuration();
        conf.setString(
                CREDENTIALS_PROVIDER_KEY,
                SystemPropertiesCredentialsProvider.class.getCanonicalName());
        conf.setString("fs.oss.endpoint", OSSTestCredentials.getOSSEndpoint());
        conf.setString("fs.oss.region", OSSTestCredentials.getOSSRegion());

        // now, we need to set oss config to system properties
        System.setProperty("oss.accessKeyId", OSSTestCredentials.getOSSAccessKey());
        System.setProperty("oss.accessKeySecret", OSSTestCredentials.getOSSSecretKey());
        FileSystem.initialize(conf, null);
    }

    @Override
    protected FileSystem getFileSystem() throws Exception {
        return getBasePath().getFileSystem();
    }

    @Override
    protected FsPath getBasePath() throws Exception {
        return new FsPath(OSSTestCredentials.getTestBucketUri() + TEST_DATA_DIR);
    }
}
