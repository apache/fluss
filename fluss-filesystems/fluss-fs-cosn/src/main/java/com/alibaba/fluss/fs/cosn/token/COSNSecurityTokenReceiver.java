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

package com.alibaba.fluss.fs.cosn.token;

import com.alibaba.fluss.fs.cosn.COSNFileSystemPlugin;
import com.alibaba.fluss.fs.token.CredentialsJsonSerde;
import com.alibaba.fluss.fs.token.ObtainedSecurityToken;
import com.alibaba.fluss.fs.token.SecurityTokenReceiver;

import com.qcloud.cos.auth.BasicSessionCredentials;
import com.qcloud.cos.auth.COSCredentials;
import com.qcloud.cos.exception.CosClientException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static org.apache.hadoop.fs.cosn.CosNConfigKeys.COSN_CREDENTIALS_PROVIDER;

/** Security token receiver for COSN filesystem. */
public class COSNSecurityTokenReceiver implements SecurityTokenReceiver {

    private static final Logger LOG = LoggerFactory.getLogger(COSNSecurityTokenReceiver.class);

    static volatile COSCredentials credentials;
    static volatile Map<String, String> additionInfos;

    public static void updateHadoopConfig(org.apache.hadoop.conf.Configuration hadoopConfig) {
        updateHadoopConfig(hadoopConfig, DynamicTemporaryCosnCredentialsProvider.NAME);
    }

    protected static void updateHadoopConfig(
            org.apache.hadoop.conf.Configuration hadoopConfig, String credentialsProviderName) {
        LOG.info("Updating Hadoop configuration for COSN");

        String providers = hadoopConfig.get(COSN_CREDENTIALS_PROVIDER, "");

        if (!providers.contains(credentialsProviderName)) {
            if (providers.isEmpty()) {
                LOG.debug("Setting COSN provider");
                providers = credentialsProviderName;
            } else {
                providers = credentialsProviderName + "," + providers;
                LOG.debug("Prepending COSN provider, new providers value: {}", providers);
            }
            hadoopConfig.set(COSN_CREDENTIALS_PROVIDER, providers);
        } else {
            LOG.debug("COSN Provider already exists");
        }

        if (additionInfos == null) {
            // if addition info is null, it also means we have not received any token,
            // we throw CosClientException
            throw new CosClientException("COSN Credentials is not ready.");
        } else {
            for (Map.Entry<String, String> entry : additionInfos.entrySet()) {
                hadoopConfig.set(entry.getKey(), entry.getValue());
            }
        }

        LOG.info("Updated Hadoop configuration for COSN successfully");
    }

    @Override
    public String scheme() {
        return COSNFileSystemPlugin.SCHEME;
    }

    @Override
    public void onNewTokensObtained(ObtainedSecurityToken token) {
        LOG.info("Updating COSN session credentials");

        byte[] tokenBytes = token.getToken();

        com.alibaba.fluss.fs.token.Credentials flussCredentials =
                CredentialsJsonSerde.fromJson(tokenBytes);

        credentials =
                new BasicSessionCredentials(
                        flussCredentials.getAccessKeyId(),
                        flussCredentials.getSecretAccessKey(),
                        flussCredentials.getSecurityToken());
        additionInfos = token.getAdditionInfos();

        LOG.info(
                "COSN session credentials updated successfully with tmpSecretId: {}.",
                credentials.getCOSAccessKeyId());
    }

    public static COSCredentials getCredentials() {
        return credentials;
    }
}
