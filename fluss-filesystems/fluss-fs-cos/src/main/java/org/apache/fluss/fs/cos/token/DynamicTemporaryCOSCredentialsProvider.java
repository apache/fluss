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

package org.apache.fluss.fs.cos.token;

import org.apache.fluss.annotation.Internal;

import com.qcloud.cos.auth.BasicSessionCredentials;
import com.qcloud.cos.auth.COSCredentials;
import com.qcloud.cos.auth.COSCredentialsProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Support dynamic session credentials for authenticating with Tencent Cloud COS. It'll get
 * credentials from {@link COSSecurityTokenReceiver}. It implements COS native {@link
 * COSCredentialsProvider} to work with CosNFileSystem.
 */
@Internal
public class DynamicTemporaryCOSCredentialsProvider implements COSCredentialsProvider {

    private static final Logger LOG =
            LoggerFactory.getLogger(DynamicTemporaryCOSCredentialsProvider.class);

    public static final String NAME = DynamicTemporaryCOSCredentialsProvider.class.getName();

    @Override
    public COSCredentials getCredentials() {
        COSCredentials credentials = COSSecurityTokenReceiver.getCredentials();
        if (credentials == null) {
            throw new RuntimeException("Credentials is not ready.");
        }
        LOG.debug("Providing session credentials");
        return new BasicSessionCredentials(
                credentials.getCOSAccessKeyId(),
                credentials.getCOSSecretKey(),
                ((BasicSessionCredentials) credentials).getSessionToken());
    }

    @Override
    public void refresh() {
        // do nothing, credentials are updated by COSSecurityTokenReceiver
    }
}
