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

package org.apache.fluss.fs.obs.token;

import org.apache.fluss.fs.obs.OBSFileSystemPlugin;
import org.apache.fluss.fs.token.CredentialsJsonSerde;
import org.apache.fluss.fs.token.ObtainedSecurityToken;
import org.apache.fluss.fs.token.SecurityTokenReceiver;

import com.obs.services.internal.security.BasicSecurityKey;
import com.obs.services.model.ISecurityKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static org.apache.fluss.fs.obs.OBSFileSystemPlugin.CREDENTIALS_PROVIDER;

/** Security token receiver for HuaweiCloud OBS filesystem. */
public class OBSSecurityTokenReceiver implements SecurityTokenReceiver {

    private static final Logger LOG = LoggerFactory.getLogger(OBSSecurityTokenReceiver.class);

    static volatile ISecurityKey credentials;
    static volatile Map<String, String> additionInfos;

    public static void updateHadoopConfig(org.apache.hadoop.conf.Configuration hadoopConfig) {
        updateHadoopConfig(hadoopConfig, DynamicTemporaryOBSCredentialsProvider.NAME);
    }

    protected static void updateHadoopConfig(
            org.apache.hadoop.conf.Configuration hadoopConfig, String credentialsProviderName) {
        LOG.info("Updating Hadoop configuration");

        String providers = hadoopConfig.get(CREDENTIALS_PROVIDER, "");

        if (!providers.contains(credentialsProviderName)) {
            if (providers.isEmpty()) {
                LOG.debug("Setting provider");
                providers = credentialsProviderName;
            } else {
                providers = credentialsProviderName + "," + providers;
                LOG.debug("Prepending provider, new providers value: {}", providers);
            }
            hadoopConfig.set(CREDENTIALS_PROVIDER, providers);
        } else {
            LOG.debug("Provider already exists");
        }

        // then, set addition info
        if (additionInfos == null) {
            // if addition info is null, it also means we have not received any token,
            throw new RuntimeException("Credentials is not ready.");
        } else {
            for (Map.Entry<String, String> entry : additionInfos.entrySet()) {
                hadoopConfig.set(entry.getKey(), entry.getValue());
            }
        }

        LOG.info("Updated Hadoop configuration successfully");
    }

    @Override
    public String scheme() {
        return OBSFileSystemPlugin.SCHEME;
    }

    @Override
    public void onNewTokensObtained(ObtainedSecurityToken token) {
        LOG.info("Updating session credentials");

        byte[] tokenBytes = token.getToken();

        org.apache.fluss.fs.token.Credentials flussCredentials =
                CredentialsJsonSerde.fromJson(tokenBytes);

        // Create Credential from fluss credentials
        credentials =
                new BasicSecurityKey(
                        flussCredentials.getAccessKeyId(),
                        flussCredentials.getSecretAccessKey(),
                        flussCredentials.getSecurityToken());
        additionInfos = token.getAdditionInfos();

        LOG.info(
                "Session credentials updated successfully with access key: {}.",
                credentials.getAccessKey());
    }

    public static ISecurityKey getCredentials() {
        return credentials;
    }
}
