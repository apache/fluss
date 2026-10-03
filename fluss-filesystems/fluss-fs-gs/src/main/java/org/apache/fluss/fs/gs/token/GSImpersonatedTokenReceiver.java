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

package org.apache.fluss.fs.gs.token;

import org.apache.fluss.exception.FlussRuntimeException;
import org.apache.fluss.fs.token.Credentials;
import org.apache.fluss.fs.token.CredentialsJsonSerde;
import org.apache.fluss.fs.token.ObtainedSecurityToken;
import org.apache.fluss.fs.token.SecurityTokenReceiver;

import com.google.cloud.hadoop.util.AccessTokenProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Security token receiver for GCS filesystem. */
public class GSImpersonatedTokenReceiver implements SecurityTokenReceiver {

    public static final String PROVIDER_CONFIG_NAME = "fs.gs.auth.access.token.provider.impl";

    private static final Logger LOG = LoggerFactory.getLogger(GSImpersonatedTokenReceiver.class);

    static volatile AccessTokenProvider.AccessToken accessToken;

    public static void updateHadoopConfig(org.apache.hadoop.conf.Configuration hadoopConfig) {
        LOG.info("Updating Hadoop configuration");

        String providers = hadoopConfig.get(PROVIDER_CONFIG_NAME, "");

        if (!providers.contains(GSImperonatedAccessTokenProvider.NAME)) {
            if (providers.isEmpty()) {
                LOG.debug("Setting provider");
                providers = GSImperonatedAccessTokenProvider.NAME;
            } else {
                providers = GSImperonatedAccessTokenProvider.NAME + "," + providers;
                LOG.debug("Prepending provider, new providers value: {}", providers);
            }

            hadoopConfig.set(PROVIDER_CONFIG_NAME, providers);
        } else {
            LOG.debug("Provider already exists");
        }

        if (accessToken == null) {
            throw new FlussRuntimeException(
                    GSImperonatedAccessTokenProvider.COMPONENT + " not set");
        }

        LOG.info("Updated Hadoop configuration successfully");
    }

    @Override
    public String scheme() {
        return "gs";
    }

    @Override
    public void onNewTokensObtained(ObtainedSecurityToken token) throws Exception {
        LOG.info("Updating session credentials");

        byte[] tokenBytes = token.getToken();

        Credentials credentials = CredentialsJsonSerde.fromJson(tokenBytes);

        accessToken =
                new AccessTokenProvider.AccessToken(
                        credentials.getSecurityToken(), token.getValidUntil().get());

        LOG.info(
                "Session credentials updated successfully with access key: {}.",
                credentials.getAccessKeyId());
    }

    public static AccessTokenProvider.AccessToken getAccessToken() {
        return accessToken;
    }
}
