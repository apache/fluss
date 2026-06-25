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
import org.apache.fluss.fs.S3FileSystemConfigUtils;
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

    private static final String[][] MIRRORED_CONFIG_KEYS = {
        {S3FileSystemConfigUtils.ACCESS_KEY_ALIAS, S3FileSystemConfigUtils.ACCESS_KEY},
        {S3FileSystemConfigUtils.SECRET_KEY_ALIAS, S3FileSystemConfigUtils.SECRET_KEY},
        {S3FileSystemConfigUtils.PATH_STYLE_ACCESS_ALIAS, S3FileSystemConfigUtils.PATH_STYLE_ACCESS}
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
        setCredentialProvider(hadoopConfig);
        return hadoopConfig;
    }

    org.apache.hadoop.conf.Configuration getHadoopConfiguration(Configuration flussConfig) {
        org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
        if (flussConfig == null) {
            return conf;
        }

        for (String key : flussConfig.keySet()) {
            String hadoopConfigKey = S3FileSystemConfigUtils.toHadoopConfigKey(key);
            if (hadoopConfigKey != null) {
                String newValue =
                        flussConfig.getString(
                                ConfigBuilder.key(key).stringType().noDefaultValue(), null);
                conf.set(hadoopConfigKey, newValue);

                LOG.debug(
                        "Adding Fluss config entry for {} as {} to Hadoop config",
                        key,
                        hadoopConfigKey);
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
        boolean hasStaticKeys =
                hadoopConfig.get(S3FileSystemConfigUtils.ACCESS_KEY) != null
                        && hadoopConfig.get(S3FileSystemConfigUtils.SECRET_KEY) != null;
        boolean hasRoleArn = hadoopConfig.get(S3FileSystemConfigUtils.ROLE_ARN) != null;

        if (hasStaticKeys || hasRoleArn) {
            LOG.info(
                    hasStaticKeys
                            ? "Using provided static credentials."
                            : "Using default AWS credential chain with AssumeRole.");
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
