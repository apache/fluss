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

import org.apache.fluss.annotation.Internal;

import javax.annotation.Nullable;

/** Utilities for converting and classifying S3 filesystem configuration keys. */
@Internal
public final class S3FileSystemConfigUtils {

    public static final String ACCESS_KEY = "fs.s3a.access.key";
    public static final String ACCESS_KEY_ALIAS = "fs.s3a.access-key";
    public static final String SECRET_KEY = "fs.s3a.secret.key";
    public static final String SECRET_KEY_ALIAS = "fs.s3a.secret-key";
    public static final String SESSION_TOKEN = "fs.s3a.session.token";
    public static final String AWS_CREDENTIALS_PROVIDER = "fs.s3a.aws.credentials.provider";
    public static final String ROLE_ARN = "fs.s3a.assumed.role.arn";
    public static final String STS_ENDPOINT = "fs.s3a.assumed.role.sts.endpoint";
    public static final String ENDPOINT = "fs.s3a.endpoint";
    public static final String REGION = "fs.s3a.region";
    public static final String PATH_STYLE_ACCESS = "fs.s3a.path.style.access";
    public static final String PATH_STYLE_ACCESS_ALIAS = "fs.s3a.path-style-access";

    private static final String HADOOP_CONFIG_PREFIX = "fs.s3a.";
    private static final String ASSUMED_ROLE_PREFIX = "fs.s3a.assumed.role.";
    private static final String[] FLUSS_CONFIG_PREFIXES = {"s3.", "s3a.", HADOOP_CONFIG_PREFIX};

    /**
     * Converts a Fluss S3 configuration key to the corresponding Hadoop S3A configuration key.
     *
     * <p>Supported Fluss prefixes are {@code s3.}, {@code s3a.}, and {@code fs.s3a.}. Unknown
     * prefixes return {@code null}.
     */
    @Nullable
    public static String toHadoopConfigKey(String key) {
        for (String prefix : FLUSS_CONFIG_PREFIXES) {
            if (key.startsWith(prefix)) {
                return HADOOP_CONFIG_PREFIX + key.substring(prefix.length());
            }
        }
        return null;
    }

    /**
     * Returns whether the given Fluss S3 configuration key carries client-side S3 credentials or
     * credential provider settings.
     */
    public static boolean isCredentialConfigKey(String key) {
        String hadoopConfigKey = toHadoopConfigKey(key);
        if (hadoopConfigKey == null) {
            return false;
        }

        return hadoopConfigKey.equals(ACCESS_KEY)
                || hadoopConfigKey.equals(ACCESS_KEY_ALIAS)
                || hadoopConfigKey.equals(SECRET_KEY)
                || hadoopConfigKey.equals(SECRET_KEY_ALIAS)
                || hadoopConfigKey.equals(SESSION_TOKEN)
                || hadoopConfigKey.equals(AWS_CREDENTIALS_PROVIDER)
                || hadoopConfigKey.startsWith(ASSUMED_ROLE_PREFIX);
    }

    private S3FileSystemConfigUtils() {}
}
