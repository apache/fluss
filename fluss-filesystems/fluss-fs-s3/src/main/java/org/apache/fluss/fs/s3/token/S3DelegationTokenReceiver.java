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

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.exception.FlussRuntimeException;
import org.apache.fluss.fs.s3.S3ConfigOptions;
import org.apache.fluss.fs.token.Credentials;
import org.apache.fluss.fs.token.CredentialsJsonSerde;
import org.apache.fluss.fs.token.ObtainedSecurityToken;
import org.apache.fluss.fs.token.SecurityTokenReceiver;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/* This file is based on source code of Apache Flink Project (https://flink.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/** Security token receiver for S3 filesystem. */
public class S3DelegationTokenReceiver implements SecurityTokenReceiver {

    private static final Logger LOG = LoggerFactory.getLogger(S3DelegationTokenReceiver.class);

    static volatile Credentials credentials;
    static volatile Map<String, String> additionalInfos;

    public static void updateHadoopConfigCredentialProviders(
            org.apache.hadoop.conf.Configuration hadoopConfig, List<String> credentialProvider) {
        LOG.info("Updating credential providers in Hadoop configuration");

        String providers = hadoopConfig.get(S3ConfigOptions.CREDENTIALS_PROVIDER_CONFIG_NAME, "");
        List<String> credentialProviderPrependOrder = new ArrayList<>(credentialProvider);
        Collections.reverse(credentialProviderPrependOrder);

        for (String credentialProviderName : credentialProviderPrependOrder) {
            if (!providers.contains(credentialProviderName)) {
                if (providers.isEmpty()) {
                    LOG.debug("Setting provider {}", credentialProviderName);
                    providers = credentialProviderName;
                } else {
                    providers = credentialProviderName + "," + providers;
                    LOG.debug("Prepending provider, new providers value: {}", providers);
                }
                hadoopConfig.set(S3ConfigOptions.CREDENTIALS_PROVIDER_CONFIG_NAME, providers);
            } else {
                LOG.debug("Provider {} already exists in chain", credentialProviderName);
            }
        }

        LOG.info("Updated credential providers in Hadoop configuration successfully");
    }

    public static void updateHadoopConfigAdditionalInfos(
            org.apache.hadoop.conf.Configuration hadoopConfig) {
        LOG.info("Updating additional infos in Hadoop configuration");

        if (additionalInfos == null) {
            LOG.error(
                    "{} has not received any additional infos.",
                    S3ADelegationTokenReceiver.class.getName());
            throw new FlussRuntimeException("Expected additionalInfos to be not null.");
        } else {
            for (Map.Entry<String, String> entry : additionalInfos.entrySet()) {
                LOG.debug("Setting configuration '{}' = '{}'", entry.getKey(), entry.getValue());
                hadoopConfig.set(entry.getKey(), entry.getValue());
            }
        }

        LOG.info("Updating additional infos in Hadoop configuration successfully");
    }

    @Override
    public String scheme() {
        return "s3";
    }

    @Override
    public void onNewTokensObtained(ObtainedSecurityToken token) {
        LOG.info("Trying to update session credentials and additional infos");

        byte[] tokenBytes = token.getToken();
        if (tokenBytes.length != 0) {
            credentials = CredentialsJsonSerde.fromJson(tokenBytes);
            additionalInfos = token.getAdditionInfos();

            LOG.info(
                    "Session credentials updated successfully with access key: {}. Updated additional infos: {}",
                    credentials.getAccessKeyId(),
                    additionalInfos);
        } else {
            additionalInfos = token.getAdditionInfos();
            LOG.info(
                    "Received an empty token. This indicates that {} has been disabled and clients will authenticate using their own credentials. Updated additional infos only: {}",
                    ConfigOptions.FILE_SYSTEM_S3_ENABLE_TOKEN_DELEGATION.key(),
                    additionalInfos);
        }
    }

    public static Credentials getCredentials() {
        return credentials;
    }
}
