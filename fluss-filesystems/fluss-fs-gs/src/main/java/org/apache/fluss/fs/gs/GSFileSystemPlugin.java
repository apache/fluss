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

package org.apache.fluss.fs.gs;

import org.apache.fluss.config.ConfigBuilder;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.fs.FileSystem;
import org.apache.fluss.fs.FileSystemPlugin;
import org.apache.fluss.fs.gs.token.GSSecurityTokenReceiver;

import com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem;
import com.google.cloud.hadoop.util.HadoopCredentialConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.List;

import static com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystemConfiguration.DELEGATION_TOKEN_BINDING_CLASS;
import static com.google.cloud.hadoop.util.HadoopCredentialConfiguration.SERVICE_ACCOUNT_JSON_KEYFILE_SUFFIX;
import static com.google.cloud.hadoop.util.HadoopCredentialConfiguration.SERVICE_ACCOUNT_KEYFILE_SUFFIX;
import static com.google.cloud.hadoop.util.HadoopCredentialConfiguration.SERVICE_ACCOUNT_PRIVATE_KEY_ID_SUFFIX;

/** Simple factory for the Google Cloud Storage file system. */
public class GSFileSystemPlugin implements FileSystemPlugin {
    private static final Logger LOG = LoggerFactory.getLogger(GSFileSystemPlugin.class);

    private static final String[] FLUSS_CONFIG_PREFIXES = {"gs.", "fs.gs."};

    private static final String HADOOP_CONFIG_PREFIX = "fs.gs.";

    private static final String GCS_CONFIG_PREFIX = "fs.gs";

    private static final List<String> CREDENTIAL_KEY_PREFIXES =
            HadoopCredentialConfiguration.getConfigKeyPrefixes(GCS_CONFIG_PREFIX);

    @Override
    public String getScheme() {
        return "gs";
    }

    @Override
    public FileSystem create(URI fsUri, Configuration flussConfig) throws IOException {
        org.apache.hadoop.conf.Configuration hadoopConfig = getHadoopConfiguration(flussConfig);
        // Use the token received from the Fluss server only when no credentials are configured.
        // The gate is on an already received token: servers never receive one, and a client
        // whose server cannot issue tokens keeps its ambient credentials as before.
        if (GSSecurityTokenReceiver.getAccessToken() != null
                && !hasConfiguredCredentials(hadoopConfig)) {
            GSSecurityTokenReceiver.updateHadoopConfig(hadoopConfig);
        }

        // create the Google Hadoop FileSystem
        org.apache.hadoop.fs.FileSystem fs = new GoogleHadoopFileSystem();
        fs.initialize(getInitURI(fsUri, hadoopConfig), hadoopConfig);
        return new GSFileSystem(getScheme(), fs, hadoopConfig);
    }

    org.apache.hadoop.conf.Configuration getHadoopConfiguration(Configuration flussConfig) {
        org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
        if (flussConfig == null) {
            return conf;
        }

        for (String key : flussConfig.keySet()) {
            for (String prefix : FLUSS_CONFIG_PREFIXES) {
                if (key.startsWith(prefix)) {
                    String newKey = HADOOP_CONFIG_PREFIX + key.substring(prefix.length());
                    String newValue =
                            flussConfig.getString(
                                    ConfigBuilder.key(key).stringType().noDefaultValue(), null);
                    conf.set(newKey, newValue);

                    LOG.debug(
                            "Adding Fluss config entry for {} as {} to Hadoop config", key, newKey);
                }
            }
        }
        return conf;
    }

    /** Whether the connector is configured with credentials of its own, not ambient ones. */
    private static boolean hasConfiguredCredentials(
            org.apache.hadoop.conf.Configuration hadoopConfig) {
        return isNotEmpty(
                        SERVICE_ACCOUNT_JSON_KEYFILE_SUFFIX
                                .withPrefixes(CREDENTIAL_KEY_PREFIXES)
                                .get(hadoopConfig, hadoopConfig::get))
                || isNotEmpty(
                        SERVICE_ACCOUNT_KEYFILE_SUFFIX
                                .withPrefixes(CREDENTIAL_KEY_PREFIXES)
                                .get(hadoopConfig, hadoopConfig::get))
                || isNotEmpty(
                        SERVICE_ACCOUNT_PRIVATE_KEY_ID_SUFFIX
                                .withPrefixes(CREDENTIAL_KEY_PREFIXES)
                                .getPassword(hadoopConfig))
                || HadoopCredentialConfiguration.getAccessTokenProviderImplClass(
                                hadoopConfig, GCS_CONFIG_PREFIX)
                        != null
                || isNotEmpty(DELEGATION_TOKEN_BINDING_CLASS.get(hadoopConfig, hadoopConfig::get));
    }

    private static boolean isNotEmpty(String value) {
        return value != null && !value.isEmpty();
    }

    private URI getInitURI(URI fsUri, org.apache.hadoop.conf.Configuration hadoopConfig) {
        final String scheme = fsUri.getScheme();
        final String authority = fsUri.getAuthority();

        if (scheme == null && authority == null) {
            fsUri = org.apache.hadoop.fs.FileSystem.getDefaultUri(hadoopConfig);
        } else if (scheme != null && authority == null) {
            URI defaultUri = org.apache.hadoop.fs.FileSystem.getDefaultUri(hadoopConfig);
            if (scheme.equals(defaultUri.getScheme()) && defaultUri.getAuthority() != null) {
                fsUri = defaultUri;
            }
        }
        return fsUri;
    }
}
