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

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import org.apache.hadoop.fs.s3a.auth.NoAwsCredentialsException;

/**
 * A credentials provider that disables delegation token generation.
 *
 * <p>Configure this provider via {@code fs.delegation.s3a.aws.credentials.provider} to signal that
 * delegation should be disabled. When {@link #getCredentials()} is called, it throws {@link
 * NoAwsCredentialsException}, causing {@link S3DelegationTokenProvider} to return an empty token.
 */
public class NoDelegationAWSCredentialsProvider implements AWSCredentialsProvider {

    @Override
    public AWSCredentials getCredentials() {
        throw new NoAwsCredentialsException(
                "NoDelegationAWSCredentialsProvider",
                "Delegation is disabled via NoDelegationAWSCredentialsProvider.");
    }

    @Override
    public void refresh() {}
}
