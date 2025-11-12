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

package org.apache.fluss.fs.abfs.token;

import org.apache.fluss.fs.token.Credentials;
import org.apache.fluss.fs.token.CredentialsJsonSerde;
import org.apache.fluss.fs.token.ObtainedSecurityToken;
import org.apache.fluss.fs.token.SecurityTokenReceiver;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/** Security token receiver for the abfs filesystem. */
public class AzureDelegationTokenReceiver implements SecurityTokenReceiver {
    public static final String PROVIDER_CONFIG_NAME = "fs.azure.account.oauth.provider.type";

    private static final Logger LOG = LoggerFactory.getLogger(AzureDelegationTokenReceiver.class);

    static volatile Credentials credentials;
    static volatile Map<String, String> additionInfos;

    public static void updateHadoopConfig(org.apache.hadoop.conf.Configuration hadoopConfig) {
        LOG.info("Updating Hadoop configuration");

        String providers = hadoopConfig.get(PROVIDER_CONFIG_NAME, "");

        if (!providers.contains(DynamicTemporaryAzureCredentialsProvider.NAME)) {
            if (providers.isEmpty()) {
                LOG.debug("Setting provider");
                providers = DynamicTemporaryAzureCredentialsProvider.NAME;
            } else {
                providers = DynamicTemporaryAzureCredentialsProvider.NAME + "," + providers;
                LOG.debug("Prepending provider, new providers value: {}", providers);
            }
            hadoopConfig.set(PROVIDER_CONFIG_NAME, providers);
        } else {
            LOG.debug("Provider already exists");
        }

        // then, set addition info
        if (additionInfos == null) {
            // if addition info is null, it also means we have not received any token,
            // we throw InvalidCredentialsException
            throw new IllegalStateException(DynamicTemporaryAzureCredentialsProvider.COMPONENT);
        } else {
            for (Map.Entry<String, String> entry : additionInfos.entrySet()) {
                hadoopConfig.set(entry.getKey(), entry.getValue());
            }
        }

        LOG.info("Updated Hadoop configuration successfully");
    }

    @Override
    public String scheme() {
        return "abfs";
    }

    @Override
    public void onNewTokensObtained(ObtainedSecurityToken token) {
        LOG.info("Updating session credentials");

        byte[] tokenBytes = token.getToken();

        credentials = CredentialsJsonSerde.fromJson(tokenBytes);
        additionInfos = token.getAdditionInfos();

        LOG.info(
                "Session credentials updated successfully with access key: {}.",
                credentials.getAccessKeyId());
    }

    public static Credentials getCredentials() {
        return credentials;
    }
}
