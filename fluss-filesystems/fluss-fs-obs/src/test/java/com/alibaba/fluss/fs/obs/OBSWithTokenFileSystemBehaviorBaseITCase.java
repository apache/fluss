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

package com.alibaba.fluss.fs.obs;

import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.fs.FileSystem;
import com.alibaba.fluss.fs.FileSystemBehaviorTestSuite;

/** Base IT case for access obs with temporary credentials in hadoop sdk as OBS FileSystem. */
abstract class OBSWithTokenFileSystemBehaviorBaseITCase extends FileSystemBehaviorTestSuite {

    static void initFileSystemWithSecretKey() {
        OBSTestCredentials.assumeCredentialsAvailable();

        // first init filesystem with ak/sk
        final Configuration conf = new Configuration();
        conf.setString("fs.obs.endpoint", OBSTestCredentials.getOBSEndpoint());
        conf.setString("fs.obs.region", OBSTestCredentials.getOBSRegion());
        conf.setString("fs.obs.access.key", OBSTestCredentials.getOBSAccessKey());
        conf.setString("fs.obs.secret.key", OBSTestCredentials.getOBSSecretKey());
        FileSystem.initialize(conf, null);
    }
}
