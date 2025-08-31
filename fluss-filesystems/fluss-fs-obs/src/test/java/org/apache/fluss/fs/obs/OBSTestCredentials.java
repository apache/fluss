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

package org.apache.fluss.fs.obs;

import javax.annotation.Nullable;

import static org.junit.jupiter.api.Assumptions.assumeTrue;

/** Access to credentials to access OBS buckets during integration tests. */
public class OBSTestCredentials {
    @Nullable private static final String ENDPOINT = System.getenv("ARTIFACTS_OBS_ENDPOINT");

    @Nullable private static final String REGION = System.getenv("ARTIFACTS_OBS_REGION");

    @Nullable private static final String BUCKET = System.getenv("ARTIFACTS_OBS_BUCKET");

    @Nullable private static final String ACCESS_KEY = System.getenv("ARTIFACTS_OBS_ACCESS_KEY");

    @Nullable private static final String SECRET_KEY = System.getenv("ARTIFACTS_OBS_SECRET_KEY");

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
                credentialsAvailable(), "No OBS credentials available in this test's environment");
    }

    /**
     * Get OBS endpoint used to connect.
     *
     * @return OBS endpoint
     */
    public static String getOBSEndpoint() {
        if (ENDPOINT != null) {
            return ENDPOINT;
        } else {
            throw new IllegalStateException("OBS endpoint is not available");
        }
    }

    /**
     * Get the region for the OBS.
     *
     * @return OBS region
     */
    public static String getOBSRegion() {
        if (REGION != null) {
            return REGION;
        } else {
            throw new IllegalStateException("OBS region is not available");
        }
    }

    /**
     * Get OBS access key.
     *
     * @return OBS access key
     */
    public static String getOBSAccessKey() {
        if (ACCESS_KEY != null) {
            return ACCESS_KEY;
        } else {
            throw new IllegalStateException("OBS access key is not available");
        }
    }

    /**
     * Get OBS secret key.
     *
     * @return OBS secret key
     */
    public static String getOBSSecretKey() {
        if (SECRET_KEY != null) {
            return SECRET_KEY;
        } else {
            throw new IllegalStateException("OBS secret key is not available");
        }
    }

    public static String getTestBucketUri() {
        return getTestBucketUriWithScheme("obs");
    }

    public static String getTestBucketUriWithScheme(String scheme) {
        if (BUCKET != null) {
            return scheme + "://" + BUCKET + "/";
        } else {
            throw new IllegalStateException("OBS test bucket is not available");
        }
    }
}
