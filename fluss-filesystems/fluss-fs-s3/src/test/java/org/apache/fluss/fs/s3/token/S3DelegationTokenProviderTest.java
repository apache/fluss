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

package org.apache.fluss.fs.s3.token;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link S3DelegationTokenProvider} constructor validation. */
class S3DelegationTokenProviderTest {

    @Test
    void testDefaultChainWithRoleArn() {
        Configuration conf = new Configuration();
        conf.set("fs.s3a.region", "us-east-1");
        conf.set("fs.s3a.assumed.role.arn", "arn:aws:iam::123456789012:role/test-role");

        assertThatCode(() -> new S3DelegationTokenProvider("s3", conf)).doesNotThrowAnyException();
    }

    @Test
    void testDefaultChainWithoutRoleArnThrows() {
        Configuration conf = new Configuration();
        conf.set("fs.s3a.region", "us-east-1");

        assertThatThrownBy(() -> new S3DelegationTokenProvider("s3", conf))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Role ARN must be set");
    }

    @Test
    void testPartialStaticCredentialsThrows() {
        Configuration conf = new Configuration();
        conf.set("fs.s3a.region", "us-east-1");
        conf.set("fs.s3a.access.key", "testAccessKey");

        assertThatThrownBy(() -> new S3DelegationTokenProvider("s3", conf))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must both be set or both be unset");
    }
}
