/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.fs.s3.token;

import org.apache.fluss.fs.token.Credentials;
import org.apache.fluss.fs.token.CredentialsJsonSerde;
import org.apache.fluss.fs.token.ObtainedSecurityToken;
import org.apache.fluss.utils.StringUtils;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

class S3DelegationTokenReceiverMaskingTest {

    @Test
    void receiverUpdatesCredentialsAndMaskedValueDisplaysSafely() {
        String accessKeyId = "AS7124FSDFER";
        String secretAccessKey = "testSecret";
        String sessionToken = "testToken";

        Credentials creds = new Credentials(accessKeyId, secretAccessKey, sessionToken);
        byte[] json = CredentialsJsonSerde.toJson(creds);

        ObtainedSecurityToken token =
                new ObtainedSecurityToken(
                        "s3", json, System.currentTimeMillis(), Collections.emptyMap());

        S3DelegationTokenReceiver receiver = new S3DelegationTokenReceiver();
        receiver.onNewTokensObtained(token);

        Credentials updated = S3DelegationTokenReceiver.getCredentials();
        assertThat(updated.getAccessKeyId()).isEqualTo(accessKeyId);

        String masked = StringUtils.maskSecret(updated.getAccessKeyId());
        assertThat(masked).isEqualTo("AS7******");
    }
}
