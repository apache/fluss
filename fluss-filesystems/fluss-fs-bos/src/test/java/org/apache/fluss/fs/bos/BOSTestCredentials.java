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

package org.apache.fluss.fs.bos;

import javax.annotation.Nullable;

import static org.junit.jupiter.api.Assumptions.assumeTrue;

/** Access to credentials to access BOS buckets during integration tests. */
public class BOSTestCredentials {

    @Nullable private static final String ENDPOINT = System.getenv("BOS_ENDPOINT");

    @Nullable private static final String BUCKET = System.getenv("BOS_BUCKET");

    @Nullable private static final String REGION = System.getenv("BOS_REGION");

    @Nullable private static final String ACCESS_KEY = System.getenv("BOS_ACCESS_KEY");

    @Nullable private static final String SECRET_KEY = System.getenv("BOS_SECRET_KEY");

    /** Optional, only needed when the default BCE STS endpoint is not the one to use. */
    @Nullable private static final String STS_ENDPOINT = System.getenv("BOS_STS_ENDPOINT");

    // ------------------------------------------------------------------------

    public static boolean credentialsAvailable() {
        return isNotEmpty(ENDPOINT)
                && isNotEmpty(BUCKET)
                && isNotEmpty(REGION)
                && isNotEmpty(ACCESS_KEY)
                && isNotEmpty(SECRET_KEY);
    }

    /** Checks if a String is not null and not empty. */
    private static boolean isNotEmpty(@Nullable String str) {
        return str != null && !str.isEmpty();
    }

    public static void assumeCredentialsAvailable() {
        assumeTrue(
                credentialsAvailable(), "No BOS credentials available in this test's environment");
    }

    /** Get BOS endpoint, e.g. {@code http://hkg.bcebos.com}. */
    public static String getBOSEndpoint() {
        return get(ENDPOINT, "endpoint");
    }

    /** Get BOS region, e.g. {@code hkg}. */
    public static String getBOSRegion() {
        return get(REGION, "region");
    }

    /** Get BOS access key. */
    public static String getBOSAccessKey() {
        return get(ACCESS_KEY, "access key");
    }

    /** Get BOS secret key. */
    public static String getBOSSecretKey() {
        return get(SECRET_KEY, "secret key");
    }

    /** Get the BCE STS endpoint override, or null if the default should be used. */
    @Nullable
    public static String getBOSStsEndpoint() {
        return isNotEmpty(STS_ENDPOINT) ? STS_ENDPOINT : null;
    }

    public static String getTestBucketUri() {
        return getTestBucketUriWithScheme("bos");
    }

    public static String getTestBucketUriWithScheme(String scheme) {
        return scheme + "://" + get(BUCKET, "test bucket") + "/";
    }

    private static String get(@Nullable String value, String what) {
        if (value != null) {
            return value;
        }
        throw new IllegalStateException("BOS " + what + " is not available");
    }
}
