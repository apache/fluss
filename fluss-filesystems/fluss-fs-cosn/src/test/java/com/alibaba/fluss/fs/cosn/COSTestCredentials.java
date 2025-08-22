/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.fs.cosn;

import javax.annotation.Nullable;

import static org.junit.jupiter.api.Assumptions.assumeTrue;

/** Access to credentials to access COSN buckets during integration tests. */
public class COSTestCredentials {
    @Nullable private static final String REGION = System.getenv("ARTIFACTS_COSN_REGION");
    @Nullable private static final String BUCKET = System.getenv("ARTIFACTS_COSN_BUCKET");
    @Nullable private static final String ACCESS_KEY = System.getenv("ARTIFACTS_COSN_ACCESS_KEY");
    @Nullable private static final String SECRET_KEY = System.getenv("ARTIFACTS_COSN_SECRET_KEY");

    @Nullable
    private static final String STS_ENDPOINT = System.getenv("ARTIFACTS_COSN_STS_ENDPOINT");

    @Nullable private static final String STS_REGION = System.getenv("ARTIFACTS_COSN_STS_REGION");
    @Nullable private static final String ROLE_ARN = System.getenv("ARTIFACTS_COSN_ROLE_ARN");

    // ------------------------------------------------------------------------

    public static boolean credentialsAvailable() {
        return isNotEmpty(REGION)
                && isNotEmpty(BUCKET)
                && isNotEmpty(ACCESS_KEY)
                && isNotEmpty(SECRET_KEY);
    }

    private static boolean isNotEmpty(@Nullable String str) {
        return str != null && !str.isEmpty();
    }

    public static void assumeCredentialsAvailable() {
        assumeTrue(
                credentialsAvailable(), "No COSN credentials available in this test's environment");
    }

    public static String getCOSNRegion() {
        if (isNotEmpty(REGION)) {
            return REGION;
        } else {
            throw new IllegalStateException("COSN region is not available");
        }
    }

    public static String getCOSNAccessKey() {
        if (isNotEmpty(ACCESS_KEY)) {
            return ACCESS_KEY;
        } else {
            throw new IllegalStateException("COSN access key is not available");
        }
    }

    public static String getCOSNSecretKey() {
        if (isNotEmpty(SECRET_KEY)) {
            return SECRET_KEY;
        } else {
            throw new IllegalStateException("COSN secret key is not available");
        }
    }

    public static String getCOSNStsEndpoint() {
        if (isNotEmpty(STS_ENDPOINT)) {
            return STS_ENDPOINT;
        } else {
            throw new IllegalStateException("COSN sts endpoint is not available");
        }
    }

    public static String getCOSNStsRegion() {
        if (isNotEmpty(STS_REGION)) {
            return STS_REGION;
        } else {
            throw new IllegalStateException("COSN sts region is not available");
        }
    }

    public static String getCOSNRoleArn() {
        if (isNotEmpty(ROLE_ARN)) {
            return ROLE_ARN;
        } else {
            throw new IllegalStateException("COSN role arn is not available");
        }
    }

    public static String getTestBucketUri() {
        if (isNotEmpty(BUCKET)) {
            return "cosn://" + BUCKET + "/";
        } else {
            throw new IllegalStateException("COSN test bucket is not available");
        }
    }
}
