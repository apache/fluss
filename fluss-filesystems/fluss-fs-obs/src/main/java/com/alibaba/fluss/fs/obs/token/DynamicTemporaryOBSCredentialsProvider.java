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

package com.alibaba.fluss.fs.obs.token;

import com.alibaba.fluss.annotation.Internal;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.obs.BasicSessionCredential;
import org.apache.hadoop.fs.obs.OBSFileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;

/**
 * Support dynamic session credentials for authenticating with HuaweiCloud OBS. It'll get
 * credentials from {@link OBSSecurityTokenReceiver}. It implements obs native {@link
 * BasicSessionCredential} to work with {@link OBSFileSystem}.
 */
@Internal
public class DynamicTemporaryOBSCredentialsProvider implements BasicSessionCredential {

    private static final Logger LOG =
            LoggerFactory.getLogger(DynamicTemporaryOBSCredentialsProvider.class);

    public static final String NAME = DynamicTemporaryOBSCredentialsProvider.class.getName();

    public DynamicTemporaryOBSCredentialsProvider(URI name, Configuration conf) {
        // do nothing
        // OBSFileSystem need this Constructor
    }

    @Override
    public String getOBSAccessKeyId() {
        return OBSSecurityTokenReceiver.getCredentials().getAccess();
    }

    @Override
    public String getOBSSecretKey() {
        return OBSSecurityTokenReceiver.getCredentials().getSecret();
    }

    @Override
    public String getSessionToken() {
        return OBSSecurityTokenReceiver.getCredentials().getSecuritytoken();
    }
}
