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

package org.apache.fluss.fs.oss.token;

import org.apache.fluss.annotation.Internal;

import com.aliyun.oss.common.auth.BasicCredentials;
import com.aliyun.oss.common.auth.Credentials;
import com.aliyun.oss.common.auth.CredentialsProvider;
import com.aliyun.oss.common.auth.InvalidCredentialsException;
import org.apache.hadoop.fs.aliyun.oss.AliyunOSSFileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Support dynamic session credentials for authenticating with OSS. It'll get credentials from
 * {@link OSSSecurityTokenReceiver}. It implements oss native {@link CredentialsProvider} to work
 * with {@link AliyunOSSFileSystem}.
 */
@Internal
public class DynamicTemporaryOssCredentialsProvider implements CredentialsProvider {

    private static final Logger LOG =
            LoggerFactory.getLogger(DynamicTemporaryOssCredentialsProvider.class);

    public static final String NAME = DynamicTemporaryOssCredentialsProvider.class.getName();

    @Override
    public void setCredentials(Credentials credentials) {
        // do nothing
    }

    @Override
    public Credentials getCredentials() {
        Credentials credentials = OSSSecurityTokenReceiver.getCredentials();
        if (credentials == null) {
            throw new InvalidCredentialsException("Credentials is not ready.");
        }
        LOG.debug("Providing session credentials");
        return new BasicCredentials(
                credentials.getAccessKeyId(),
                credentials.getSecretAccessKey(),
                credentials.getSecurityToken());
    }
}
