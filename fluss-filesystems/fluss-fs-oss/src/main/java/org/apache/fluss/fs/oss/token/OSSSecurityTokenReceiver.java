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

package org.apache.fluss.fs.oss.token;

import org.apache.fluss.fs.oss.OSSFileSystemPlugin;
import org.apache.fluss.fs.token.CredentialsJsonSerde;
import org.apache.fluss.fs.token.ObtainedSecurityToken;
import org.apache.fluss.fs.token.SecurityTokenReceiver;
import org.apache.fluss.utils.MapUtils;

import com.aliyun.oss.common.auth.Credentials;
import com.aliyun.oss.common.auth.DefaultCredentials;
import com.aliyun.oss.common.auth.InvalidCredentialsException;
import org.apache.hadoop.fs.aliyun.oss.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.Map;

import static org.apache.fluss.fs.FileSystem.FSKey;

/** Security token receiver for OSS filesystem. */
public class OSSSecurityTokenReceiver implements SecurityTokenReceiver {

    private static final Logger LOG = LoggerFactory.getLogger(OSSSecurityTokenReceiver.class);

    static Map<FSKey, Credentials> credentialsCache = MapUtils.newConcurrentHashMap();
    static Map<FSKey, Map<String, String>> additionInfosCache = MapUtils.newConcurrentHashMap();

    public static void updateHadoopConfig(
            URI fsUri, org.apache.hadoop.conf.Configuration hadoopConfig) {
        updateHadoopConfig(fsUri, hadoopConfig, DynamicTemporaryOssCredentialsProvider.NAME);
    }

    protected static void updateHadoopConfig(
            URI fsUri,
            org.apache.hadoop.conf.Configuration hadoopConfig,
            String credentialsProviderName) {
        LOG.info("Updating Hadoop configuration");

        String providers = hadoopConfig.get(Constants.CREDENTIALS_PROVIDER_KEY, "");

        if (!providers.contains(credentialsProviderName)) {
            if (providers.isEmpty()) {
                LOG.debug("Setting provider");
                providers = credentialsProviderName;
            } else {
                providers = credentialsProviderName + "," + providers;
                LOG.debug("Prepending provider, new providers value: {}", providers);
            }
            hadoopConfig.set(Constants.CREDENTIALS_PROVIDER_KEY, providers);
        } else {
            LOG.debug("Provider already exists");
        }

        FSKey fsKey = new FSKey(fsUri.getScheme(), fsUri.getAuthority());
        Map<String, String> additionInfos = additionInfosCache.get(fsKey);
        // then, set addition info
        if (additionInfos == null) {
            // if addition info is null, it also means we have not received any token,
            // we throw InvalidCredentialsException
            throw new InvalidCredentialsException("Credentials is not ready.");
        } else {
            for (Map.Entry<String, String> entry : additionInfos.entrySet()) {
                hadoopConfig.set(entry.getKey(), entry.getValue());
            }
        }

        LOG.info("Updated Hadoop configuration successfully");
    }

    @Override
    public String scheme() {
        return OSSFileSystemPlugin.SCHEME;
    }

    @Override
    public void onNewTokensObtained(ObtainedSecurityToken token) {
        LOG.info("Updating session credentials");

        byte[] tokenBytes = token.getToken();

        org.apache.fluss.fs.token.Credentials flussCredentials =
                CredentialsJsonSerde.fromJson(tokenBytes);

        FSKey fsKey = new FSKey(token.getScheme(), token.getAuthority().orElse(null));

        Credentials credentials =
                new DefaultCredentials(
                        flussCredentials.getAccessKeyId(),
                        flussCredentials.getSecretAccessKey(),
                        flussCredentials.getSecurityToken());
        credentialsCache.put(fsKey, credentials);

        additionInfosCache.put(fsKey, token.getAdditionInfos());

        LOG.info(
                "Session credentials updated successfully with access key: {}.",
                credentials.getAccessKeyId());
    }

    public static Credentials getCredentials(URI uri) {
        return credentialsCache.get(new FSKey(uri.getScheme(), uri.getAuthority()));
    }
}
