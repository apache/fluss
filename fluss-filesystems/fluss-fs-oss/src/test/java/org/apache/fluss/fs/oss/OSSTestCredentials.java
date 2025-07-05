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

package org.apache.fluss.fs.oss;

import javax.annotation.Nullable;

import static org.junit.jupiter.api.Assumptions.assumeTrue;

/** Access to credentials to access OSS buckets during integration tests. */
public class OSSTestCredentials {
    @Nullable private static final String ENDPOINT = System.getenv("ARTIFACTS_OSS_ENDPOINT");

    @Nullable private static final String REGION = System.getenv("ARTIFACTS_OSS_REGION");

    @Nullable private static final String BUCKET = System.getenv("ARTIFACTS_OSS_BUCKET");

    @Nullable private static final String ACCESS_KEY = System.getenv("ARTIFACTS_OSS_ACCESS_KEY");

    @Nullable private static final String SECRET_KEY = System.getenv("ARTIFACTS_OSS_SECRET_KEY");

    @Nullable
    private static final String STS_ENDPOINT = System.getenv("ARTIFACTS_OSS_STS_ENDPOINT");

    @Nullable private static final String ROLE_ARN = System.getenv("ARTIFACTS_OSS_ROLE_ARN");

    // ------------------------------------------------------------------------

    public static boolean credentialsAvailable() {
        return isNotEmpty(ENDPOINT)
                && isNotEmpty(BUCKET)
                && isNotEmpty(ACCESS_KEY)
                && isNotEmpty(SECRET_KEY);
    }

    /** Checks if a String is not null and not empty. */
    private static boolean isNotEmpty(@Nullable String str) {
        return str != null && !str.isEmpty();
    }

    public static void assumeCredentialsAvailable() {
        assumeTrue(
                credentialsAvailable(), "No OSS credentials available in this test's environment");
    }

    /**
     * Get OSS endpoint used to connect.
     *
     * @return OSS endpoint
     */
    public static String getOSSEndpoint() {
        if (ENDPOINT != null) {
            return ENDPOINT;
        } else {
            throw new IllegalStateException("OSS endpoint is not available");
        }
    }

    /**
     * Get the region for the OSS.
     *
     * @return OSS region
     */
    public static String getOSSRegion() {
        if (REGION != null) {
            return REGION;
        } else {
            throw new IllegalStateException("OSS endpoint is not available");
        }
    }

    /**
     * Get OSS access key.
     *
     * @return OSS access key
     */
    public static String getOSSAccessKey() {
        if (ACCESS_KEY != null) {
            return ACCESS_KEY;
        } else {
            throw new IllegalStateException("OSS access key is not available");
        }
    }

    /**
     * Get OSS secret key.
     *
     * @return OSS secret key
     */
    public static String getOSSSecretKey() {
        if (SECRET_KEY != null) {
            return SECRET_KEY;
        } else {
            throw new IllegalStateException("OSS secret key is not available");
        }
    }

    public static String getOSSStsEndpoint() {
        if (STS_ENDPOINT != null) {
            return STS_ENDPOINT;
        } else {
            throw new IllegalStateException("OSS sts endpoint is not available");
        }
    }

    public static String getOSSRoleArn() {
        if (ROLE_ARN != null) {
            return ROLE_ARN;
        } else {
            throw new IllegalStateException("OSS role arn is not available");
        }
    }

    public static String getTestBucketUri() {
        return getTestBucketUriWithScheme("oss");
    }

    public static String getTestBucketUriWithScheme(String scheme) {
        if (BUCKET != null) {
            return scheme + "://" + BUCKET + "/";
        } else {
            throw new IllegalStateException("OSS test bucket is not available");
        }
    }
}
