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

package org.apache.fluss.fs.s3.minio;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;

/**
 * A test-only credentials provider that reads the access and secret keys from system properties.
 *
 * <p>It exists so that integration tests can exercise the {@code credentialsProviderClass != null} setup without
 * depending on the proper environment.
 */
public final class SystemPropertyCredentialsProvider implements AWSCredentialsProvider {

    public static final String ACCESS_KEY_PROPERTY = "fluss.test.minio.accessKey";
    public static final String SECRET_KEY_PROPERTY = "fluss.test.minio.secretKey";

    @Override
    public AWSCredentials getCredentials() {
        String ak = System.getProperty(ACCESS_KEY_PROPERTY);
        String sk = System.getProperty(SECRET_KEY_PROPERTY);
        if (ak == null || sk == null) {
            throw new IllegalStateException(
                    "System properties "
                            + ACCESS_KEY_PROPERTY
                            + " and "
                            + SECRET_KEY_PROPERTY
                            + " must be set.");
        }
        return new BasicAWSCredentials(ak, sk);
    }

    @Override
    public void refresh() {
        //
    }

}
