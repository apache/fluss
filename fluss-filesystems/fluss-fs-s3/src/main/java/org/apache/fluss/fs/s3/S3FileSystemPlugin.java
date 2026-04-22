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

import org.apache.fluss.annotation.VisibleForTesting;
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

    private static final String[] FLUSS_CONFIG_PREFIXES = {
        "s3.", "s3a.", "fs.s3a.", "fs.delegation.s3a."
    };

    private static final String HADOOP_CONFIG_PREFIX = "fs.s3a.";

    private static final String ACCESS_KEY_ID = "fs.s3a.access.key";
    private static final String ACCESS_KEY_SECRET = "fs.s3a.secret.key";

    private static final String ROLE_ARN_KEY = "fs.s3a.assumed.role.arn";

    private static final String DELEGATION_CONFIG_PREFIX = "fs.delegation.s3a.";

    private static final String[][] MIRRORED_CONFIG_KEYS = {
        {"fs.s3a.access-key", "fs.s3a.access.key"},
        {"fs.s3a.secret-key", "fs.s3a.secret.key"},
        {"fs.s3a.path-style-access", "fs.s3a.path.style.access"},
        {"fs.delegation.s3a.access-key", "fs.delegation.s3a.access.key"},
        {"fs.delegation.s3a.secret-key", "fs.delegation.s3a.secret.key"}
    };

    @Override
    public String getScheme() {
        return "s3";
    }

    @Override
    public FileSystem create(URI fsUri, Configuration flussConfig) throws IOException {
        org.apache.hadoop.conf.Configuration hadoopConfig = buildHadoopConfiguration(flussConfig);

        // create the Hadoop FileSystem
        org.apache.hadoop.fs.FileSystem fs = new S3AFileSystem();
        fs.initialize(getInitURI(fsUri, hadoopConfig), hadoopConfig);
        return new S3FileSystem(getScheme(), fs, hadoopConfig);
    }

    @VisibleForTesting
    org.apache.hadoop.conf.Configuration buildHadoopConfiguration(Configuration flussConfig) {
        org.apache.hadoop.conf.Configuration hadoopConfig =
                mirrorCertainHadoopConfig(getHadoopConfiguration(flussConfig));
        setCredentialProvider(hadoopConfig, flussConfig);
        return hadoopConfig;
    }

    org.apache.hadoop.conf.Configuration getHadoopConfiguration(Configuration flussConfig) {
        org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
        if (flussConfig == null) {
            return conf;
        }

        for (String key : flussConfig.keySet()) {
            for (String prefix : FLUSS_CONFIG_PREFIXES) {
                if (key.startsWith(prefix)) {
                    String newValue =
                            flussConfig.getString(
                                    ConfigBuilder.key(key).stringType().noDefaultValue(), null);
                    // Delegation keys (fs.delegation.s3a.*) pass through as-is;
                    // other prefixes are remapped to fs.s3a.*
                    String newKey;
                    if (key.startsWith(DELEGATION_CONFIG_PREFIX)) {
                        newKey = key;
                    } else {
                        newKey = HADOOP_CONFIG_PREFIX + key.substring(prefix.length());
                    }
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

    /**
     * Checks whether a Hadoop config key was explicitly set through Fluss configuration, by
     * checking all possible Fluss prefixes that map to the given Hadoop key.
     */
    private static boolean hasFlussConfigKey(Configuration flussConfig, String hadoopKey) {
        if (flussConfig == null) {
            return false;
        }
        // The hadoopKey (e.g., "fs.s3a.aws.credentials.provider") could come from any
        // Fluss prefix: "s3.", "s3a.", or "fs.s3a.". Check all possibilities.
        for (String key : flussConfig.keySet()) {
            for (String prefix : FLUSS_CONFIG_PREFIXES) {
                if (key.startsWith(prefix)) {
                    String mappedHadoopKey = HADOOP_CONFIG_PREFIX + key.substring(prefix.length());
                    if (mappedHadoopKey.equals(hadoopKey)) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    private void setCredentialProvider(
            org.apache.hadoop.conf.Configuration hadoopConfig, Configuration flussConfig) {
        boolean hasStaticKeys =
                hadoopConfig.get(ACCESS_KEY_ID) != null
                        && hadoopConfig.get(ACCESS_KEY_SECRET) != null;
        boolean hasRoleArn = hadoopConfig.get(ROLE_ARN_KEY) != null;
        // Check if the user explicitly set a credentials provider in Fluss config.
        // We cannot check hadoopConfig because Hadoop's core-default.xml provides defaults.
        boolean hasExplicitProvider = hasFlussConfigKey(flussConfig, PROVIDER_CONFIG_NAME);

        if (hasStaticKeys || hasRoleArn) {
            LOG.info(
                    hasStaticKeys
                            ? "Using provided static credentials."
                            : "Using default AWS credential chain with AssumeRole.");
        } else if (hasExplicitProvider) {
            LOG.info(
                    "Using explicitly configured credential provider: {}.",
                    hadoopConfig.get(PROVIDER_CONFIG_NAME));
        } else {
            if (Objects.equals(getScheme(), "s3")) {
                S3DelegationTokenReceiver.updateHadoopConfig(hadoopConfig);
            } else if (Objects.equals(getScheme(), "s3a")) {
                S3ADelegationTokenReceiver.updateHadoopConfig(hadoopConfig);
            } else {
                throw new IllegalArgumentException("Unsupported scheme: " + getScheme());
            }
            LOG.info(
                    "Using credential provider {} for delegated tokens.",
                    hadoopConfig.get(PROVIDER_CONFIG_NAME));
        }
    }
}
