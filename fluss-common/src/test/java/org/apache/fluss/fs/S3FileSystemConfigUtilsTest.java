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

package org.apache.fluss.fs;

import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link S3FileSystemConfigUtils}. */
class S3FileSystemConfigUtilsTest {

    @Test
    void testConvertFlussConfigKeyToHadoopConfigKey() {
        assertThat(S3FileSystemConfigUtils.toHadoopConfigKey("s3.endpoint"))
                .isEqualTo(S3FileSystemConfigUtils.ENDPOINT);
        assertThat(S3FileSystemConfigUtils.toHadoopConfigKey("s3a.region"))
                .isEqualTo(S3FileSystemConfigUtils.REGION);
        assertThat(S3FileSystemConfigUtils.toHadoopConfigKey("fs.s3a.path-style-access"))
                .isEqualTo(S3FileSystemConfigUtils.PATH_STYLE_ACCESS_ALIAS);
        assertThat(S3FileSystemConfigUtils.toHadoopConfigKey("oss.endpoint")).isNull();
    }

    @Test
    void testDetectCredentialConfigKey() {
        assertThat(
                        Arrays.asList(
                                "s3a.aws.credentials.provider",
                                "s3a.access.key",
                                "s3a.secret.key",
                                "s3a.access-key",
                                "s3a.secret-key",
                                "s3a.session.token",
                                "s3a.assumed.role.arn",
                                "s3a.assumed.role.sts.endpoint",
                                "s3.aws.credentials.provider",
                                "s3.access.key",
                                "s3.secret.key",
                                "s3.secret-key",
                                "fs.s3a.aws.credentials.provider",
                                "fs.s3a.access.key",
                                "fs.s3a.secret-key",
                                "fs.s3a.assumed.role.arn"))
                .allMatch(S3FileSystemConfigUtils::isCredentialConfigKey);
    }

    @Test
    void testAllowNonCredentialConfigKey() {
        assertThat(
                        Arrays.asList(
                                "s3a.endpoint",
                                "s3a.region",
                                "s3a.path-style-access",
                                "s3a.path.style.access",
                                "oss.access.key"))
                .noneMatch(S3FileSystemConfigUtils::isCredentialConfigKey);
    }
}
