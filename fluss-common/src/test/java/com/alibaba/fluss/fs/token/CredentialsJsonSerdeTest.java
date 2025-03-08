/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.fs.token;

import com.alibaba.fluss.utils.json.JsonSerdeTestBase;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link CredentialsJsonSerde}. */
class CredentialsJsonSerdeTest extends JsonSerdeTestBase<Credentials> {

    CredentialsJsonSerdeTest() {
        super(CredentialsJsonSerde.INSTANCE);
    }

    @Override
    protected Credentials[] createObjects() {
        Credentials credentials1 =
                new Credentials(
                        "myrandomaccessid111", "myrandomaccesssecret111", "myrandomsecuritytoken");
        Credentials credentials2 =
                new Credentials(
                        "myrandomaccessid222",
                        "myrandomaccesssecret222",
                        "myrandomsecuritytoken222");
        return new Credentials[] {credentials1, credentials2};
    }

    @Override
    protected String[] expectedJsons() {
        return new String[] {
            "{\"version\":1,\"access_key_id\":\"myrandomaccessid111\",\"access_key_secret\":\"myrandomaccesssecret111\",\"security_token\":\"myrandomsecuritytoken\"}",
            "{\"version\":1,\"access_key_id\":\"myrandomaccessid222\",\"access_key_secret\":\"myrandomaccesssecret222\","
                    + "\"security_token\":\"myrandomsecuritytoken222\""
                    + "}"
        };
    }

    @Override
    protected void assertEquals(Credentials actual, Credentials expected) {
        assertThat(actual.getAccessKeyId()).isEqualTo(expected.getAccessKeyId());
        assertThat(actual.getSecretAccessKey()).isEqualTo(expected.getSecretAccessKey());
        assertThat(actual.getSecurityToken()).isEqualTo(expected.getSecurityToken());
    }
}
