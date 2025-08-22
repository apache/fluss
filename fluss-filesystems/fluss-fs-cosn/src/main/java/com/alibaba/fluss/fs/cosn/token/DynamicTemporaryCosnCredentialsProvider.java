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

package com.alibaba.fluss.fs.cosn.token;

import com.alibaba.fluss.annotation.Internal;

import com.qcloud.cos.auth.COSCredentials;
import com.qcloud.cos.auth.COSCredentialsProvider;
import com.qcloud.cos.exception.CosClientException;
import org.apache.hadoop.fs.cosn.CosNFileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Support dynamic session credentials for authenticating with COSN. It'll get credentials from
 * {@link COSNSecurityTokenReceiver}. It implements cosn native {@link COSCredentialsProvider} to
 * work with {@link CosNFileSystem}.
 */
@Internal
public class DynamicTemporaryCosnCredentialsProvider implements COSCredentialsProvider {

    private static final Logger LOG =
            LoggerFactory.getLogger(DynamicTemporaryCosnCredentialsProvider.class);

    public static final String NAME = DynamicTemporaryCosnCredentialsProvider.class.getName();

    @Override
    public COSCredentials getCredentials() {
        COSCredentials credentials = COSNSecurityTokenReceiver.getCredentials();
        if (credentials == null) {
            throw new CosClientException("Credentials is not ready.");
        }
        LOG.debug("Providing session credentials");

        return credentials;
    }

    @Override
    public void refresh() {
        // The token is refreshed and pushed externally via COSNSecurityTokenReceiver.
        // This provider just retrieves the latest one. So do nothing here.
    }
}
