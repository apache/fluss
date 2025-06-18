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

/** Base case for access oss with sts token in hadoop/jindo sdk as OSS FileSystem implementation. */
abstract class OSSWithTokenFileSystemBehaviorBaseITCase extends FileSystemBehaviorTestSuite {

    static void initFileSystemWithSecretKey() {
        OSSTestCredentials.assumeCredentialsAvailable();

        // first init filesystem with ak/sk
        Configuration conf = new Configuration();
        conf.setString("fs.oss.endpoint", OSSTestCredentials.getOSSEndpoint());
        conf.setString("fs.oss.region", OSSTestCredentials.getOSSRegion());
        conf.setString("fs.oss.accessKeyId", OSSTestCredentials.getOSSAccessKey());
        conf.setString("fs.oss.accessKeySecret", OSSTestCredentials.getOSSSecretKey());
        // need to set sts endpoint and roleArn to make it can generate token
        conf.setString("fs.oss.sts.endpoint", OSSTestCredentials.getOSSStsEndpoint());
        conf.setString("fs.oss.roleArn", OSSTestCredentials.getOSSRoleArn());
        FileSystem.initialize(conf, null);
    }
}
