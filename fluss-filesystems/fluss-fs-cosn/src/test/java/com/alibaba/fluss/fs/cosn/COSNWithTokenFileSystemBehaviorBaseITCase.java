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

import static com.alibaba.fluss.fs.cosn.token.COSNSecurityTokenProvider.STS_REGION_KEY;
import static org.apache.hadoop.fs.cosn.CosNConfigKeys.COSN_REGION_KEY;
import static org.apache.hadoop.fs.cosn.CosNConfigKeys.COSN_SECRET_ID_KEY;
import static org.apache.hadoop.fs.cosn.CosNConfigKeys.COSN_SECRET_KEY_KEY;

/** Base case for access cosn with sts token in hadoop sdk as COSN FileSystem implementation. */
abstract class COSNWithTokenFileSystemBehaviorBaseITCase extends FileSystemBehaviorTestSuite {

    static void initFileSystemWithSecretKey() {
        COSTestCredentials.assumeCredentialsAvailable();

        // first init filesystem with ak/sk to be able to generate token
        Configuration conf = new Configuration();
        conf.setString(COSN_REGION_KEY, COSTestCredentials.getCOSNRegion());
        conf.setString(COSN_SECRET_ID_KEY, COSTestCredentials.getCOSNAccessKey());
        conf.setString(COSN_SECRET_KEY_KEY, COSTestCredentials.getCOSNSecretKey());
        conf.setString("fs.cosn.sts.endpoint", COSTestCredentials.getCOSNStsEndpoint());
        conf.setString(STS_REGION_KEY, COSTestCredentials.getCOSNStsRegion());
        conf.setString("fs.cosn.roleArn", COSTestCredentials.getCOSNRoleArn());
        FileSystem.initialize(conf, null);
    }
}
