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

/** Base IT case for access COS with temporary credentials in hadoop sdk as COS FileSystem. */
abstract class COSWithTokenFileSystemBehaviorBaseITCase extends FileSystemBehaviorTestSuite {

    static void initFileSystemWithSecretKey() {
        COSTestCredentials.assumeCredentialsAvailable();

        // first init filesystem with secretId/secretKey
        final Configuration conf = new Configuration();
        conf.setString("fs.cosn.endpoint", COSTestCredentials.getCOSEndpoint());
        conf.setString("fs.cosn.userinfo.secretId", COSTestCredentials.getCOSSecretId());
        conf.setString("fs.cosn.userinfo.secretKey", COSTestCredentials.getCOSSecretKey());
        FileSystem.initialize(conf, null);
    }
}
