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

package org.apache.fluss.fs.s3;

import org.apache.fluss.config.ConfigBuilder;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.fs.FileSystem;
import org.apache.fluss.fs.FileSystemPlugin;
import org.apache.fluss.fs.s3.token.S3ADelegationTokenReceiver;
import org.apache.fluss.fs.s3.token.S3DelegationTokenReceiver;

import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.Objects;

import static org.apache.fluss.fs.s3.token.S3DelegationTokenReceiver.PROVIDER_CONFIG_NAME;

/** Simple factory for the s3 file system. */
public class S3FileSystemPlugin implements FileSystemPlugin {

    private static final Logger LOG = LoggerFactory.getLogger(S3FileSystemPlugin.class);

    private static final String[] FLUSS_CONFIG_PREFIXES = {"s3.", "s3a.", "fs.s3a."};

    private static final String HADOOP_CONFIG_PREFIX = "fs.s3a.";

    private static final String[][] MIRRORED_CONFIG_KEYS = {
        {"fs.s3a.access-key", "fs.s3a.access.key"},
        {"fs.s3a.secret-key", "fs.s3a.secret.key"},
        {"fs.s3a.path-style-access", "fs.s3a.path.style.access"}
    };

    @Override
    public String getScheme() {
        return "s3";
    }

    @Override
    public FileSystem create(URI fsUri, Configuration flussConfig) throws IOException {
        org.apache.hadoop.conf.Configuration hadoopConfig =
                mirrorCertainHadoopConfig(getHadoopConfiguration(flussConfig));
        // This config option is not added to Hadoop Config on purpose (prefix 'fs.s3.'), because it
        // is internal to Fluss.
        final boolean useTokenDelegation =
                flussConfig.getBoolean(ConfigOptions.FILE_SYSTEM_S3_ENABLE_TOKEN_DELEGATION);

        setCredentialProvider(hadoopConfig);

        org.apache.hadoop.fs.FileSystem fs = new S3AFileSystem();
        fs.initialize(getInitURI(fsUri, hadoopConfig), hadoopConfig);

        // Currently, if-else conditional is sufficient. If we add additional STS methods for token
        // delegation
        // (e.g., AssumeRoleWithWebIdentity), we need to decide based on the configuration set by
        // the user,
        // which type to use.
        final S3DelegationTokenProvider.Type delegationTokenProviderType =
                useTokenDelegation
                        ? S3DelegationTokenProvider.Type.STS_SESSION_TOKEN
                        : S3DelegationTokenProvider.Type.NO_TOKEN;

        LOG.debug("S3DelegationTokenProvider type is {}", delegationTokenProviderType);

        return new S3FileSystem(
                fs,
                () ->
                        new S3DelegationTokenProvider(
                                getScheme(), hadoopConfig, delegationTokenProviderType));
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

    // mirror certain keys to make use more uniform across implementations
    // with different keys
    private org.apache.hadoop.conf.Configuration mirrorCertainHadoopConfig(
            org.apache.hadoop.conf.Configuration hadoopConfig) {
        for (String[] mirrored : MIRRORED_CONFIG_KEYS) {
            String value = hadoopConfig.get(mirrored[0], null);
            if (value != null) {
                hadoopConfig.set(mirrored[1], value);
            }
        }
        return hadoopConfig;
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

    private void setCredentialProvider(org.apache.hadoop.conf.Configuration hadoopConfig) {
        if (Objects.equals(getScheme(), "s3") || Objects.equals(getScheme(), "s3a")) {
            S3DelegationTokenReceiver.updateHadoopConfig(hadoopConfig);
        } else {
            throw new IllegalArgumentException("Unsupported scheme: " + getScheme());
        }
    }
}
