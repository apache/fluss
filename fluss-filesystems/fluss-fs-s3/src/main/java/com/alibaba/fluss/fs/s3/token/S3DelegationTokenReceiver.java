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

package com.alibaba.fluss.fs.s3.token;

import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.fs.token.Credentials;
import com.alibaba.fluss.fs.token.CredentialsJsonSerde;
import com.alibaba.fluss.fs.token.ObtainedSecurityToken;
import com.alibaba.fluss.fs.token.SecurityTokenReceiver;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/* This file is based on source code of Apache Flink Project (https://flink.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/** Security token receiver for S3 filesystem. */
public class S3DelegationTokenReceiver implements SecurityTokenReceiver {

    public static final String PROVIDER_CONFIG_NAME = "fs.s3a.aws.credentials.provider";

    private static final Logger LOG = LoggerFactory.getLogger(S3DelegationTokenReceiver.class);

    static volatile Credentials credentials;
    static volatile Map<String, String> additionalInfos;

    public static void updateHadoopConfig(org.apache.hadoop.conf.Configuration hadoopConfig) {
        LOG.info("Updating Hadoop configuration");

        // We always add the Fluss credential providers to the beginning of the provider chain.
        // Independently whether token delegation is enabled or disabled, the library will go
        // through the chain and
        // find a valid credential provider automatically.
        String providers = hadoopConfig.get(PROVIDER_CONFIG_NAME, "");
        if (!providers.contains(DynamicTemporaryAWSCredentialsProvider.NAME)) {
            if (providers.isEmpty()) {
                LOG.debug("Setting provider {}", DynamicTemporaryAWSCredentialsProvider.NAME);
                providers = DynamicTemporaryAWSCredentialsProvider.NAME;
            } else {
                providers = DynamicTemporaryAWSCredentialsProvider.NAME + "," + providers;
                LOG.debug("Prepending provider, new providers value: {}", providers);
            }
            hadoopConfig.set(PROVIDER_CONFIG_NAME, providers);
        } else {
            LOG.debug("Provider already exists");
        }

        // TODO
        if (additionalInfos != null) {
            for (Map.Entry<String, String> entry : additionalInfos.entrySet()) {
                hadoopConfig.set(entry.getKey(), entry.getValue());
            }
        }

        LOG.info("Updated Hadoop configuration successfully");
    }

    @Override
    public String scheme() {
        return "s3";
    }

    @Override
    public void onNewTokensObtained(ObtainedSecurityToken token) {
        LOG.info("Trying to update session credentials");

        byte[] tokenBytes = token.getToken();
        if (tokenBytes.length != 0) {
            credentials = CredentialsJsonSerde.fromJson(tokenBytes);
            additionalInfos = token.getAdditionInfos();

            LOG.info(
                    "Session credentials updated successfully with access key: {}.",
                    credentials.getAccessKeyId());
        } else {
            // When token delegation is deactivated, we still distribute the additional infos to the
            // client, so they do not need to be set in the client config.
            LOG.info(
                    "Received an empty token. This usually indicates that {} has been disabled. Updating additional infos only.",
                    ConfigOptions.FILE_SYSTEM_S3_ENABLE_TOKEN_DELEGATION.key());
            additionalInfos = token.getAdditionInfos();
            LOG.info(additionalInfos.toString());
        }
    }

    public static Credentials getCredentials() {
        return credentials;
    }
}
