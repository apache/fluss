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

package org.apache.fluss.fs.bos.token;

import org.apache.fluss.fs.token.Credentials;

import com.baidubce.auth.BceSessionCredentials;

import static org.apache.fluss.utils.Preconditions.checkState;

/**
 * Credentials handed to the BOS SDK on clients that were not configured with an access key. The SDK
 * calls the getters for every request, so this always reflects the latest token received through
 * {@link BOSSecurityTokenReceiver}.
 */
public class DynamicTemporaryBOSCredentialsProvider implements BceSessionCredentials {

    private static Credentials current() {
        Credentials c = BOSSecurityTokenReceiver.getCredentials();
        checkState(
                c != null,
                "Dynamic session credentials for Fluss: No BOS credentials (no security token received yet)");
        return c;
    }

    @Override
    public String getAccessKeyId() {
        return current().getAccessKeyId();
    }

    @Override
    public String getSecretKey() {
        return current().getSecretAccessKey();
    }

    @Override
    public String getSessionToken() {
        return current().getSecurityToken();
    }
}
