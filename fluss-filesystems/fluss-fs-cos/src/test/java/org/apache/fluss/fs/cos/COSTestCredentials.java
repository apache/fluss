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

package org.apache.fluss.fs.cos;

import javax.annotation.Nullable;

import static org.junit.jupiter.api.Assumptions.assumeTrue;

/** Access to credentials to access COS buckets during integration tests. */
public class COSTestCredentials {
    @Nullable private static final String ENDPOINT_SUFFIX = System.getenv("COSN_ENDPOINT_SUFFIX");

    @Nullable private static final String BUCKET = System.getenv("COSN_BUCKET");

    @Nullable private static final String SECRET_ID = System.getenv("COSN_SECRET_ID");

    @Nullable private static final String SECRET_KEY = System.getenv("COSN_SECRET_KEY");

    @Nullable private static final String REGION = System.getenv("COSN_REGION");

    // ------------------------------------------------------------------------

    public static boolean credentialsAvailable() {
        return isNotEmpty(ENDPOINT_SUFFIX)
                && isNotEmpty(BUCKET)
                && isNotEmpty(SECRET_ID)
                && isNotEmpty(SECRET_KEY)
                && isNotEmpty(REGION);
    }

    /** Checks if a String is not null and not empty. */
    private static boolean isNotEmpty(@Nullable String str) {
        return str != null && !str.isEmpty();
    }

    public static void assumeCredentialsAvailable() {
        assumeTrue(
                credentialsAvailable(), "No COS credentials available in this test's environment");
    }

    /**
     * Get COS endpoint suffix used to connect.
     *
     * @return COS endpoint suffix
     */
    public static String getCOSEndpointSuffix() {
        if (ENDPOINT_SUFFIX != null) {
            return ENDPOINT_SUFFIX;
        } else {
            throw new IllegalStateException("COS endpoint suffix is not available");
        }
    }

    /**
     * Get COS secret id.
     *
     * @return COS secret id
     */
    public static String getCOSSecretId() {
        if (SECRET_ID != null) {
            return SECRET_ID;
        } else {
            throw new IllegalStateException("COS secret id is not available");
        }
    }

    /**
     * Get COS secret key.
     *
     * @return COS secret key
     */
    public static String getCOSSecretKey() {
        if (SECRET_KEY != null) {
            return SECRET_KEY;
        } else {
            throw new IllegalStateException("COS secret key is not available");
        }
    }

    /**
     * Get COS region.
     *
     * @return COS region
     */
    public static String getCOSRegion() {
        if (REGION != null) {
            return REGION;
        } else {
            throw new IllegalStateException("COS region is not available");
        }
    }

    public static String getTestBucketUri() {
        return getTestBucketUriWithScheme("cosn");
    }

    public static String getTestBucketUriWithScheme(String scheme) {
        if (BUCKET != null) {
            return scheme + "://" + BUCKET + "/";
        } else {
            throw new IllegalStateException("COS test bucket is not available");
        }
    }
}
