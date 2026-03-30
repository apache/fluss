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

package org.apache.fluss.fs.azure;

import org.apache.fluss.config.ConfigBuilder;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.fs.FileSystem;
import org.apache.fluss.fs.FileSystemPlugin;
import org.apache.fluss.fs.azure.token.AbfsDelegationTokenReceiver;
import org.apache.fluss.fs.azure.token.AbfssDelegationTokenReceiver;
import org.apache.fluss.fs.azure.token.WasbDelegationTokenReceiver;
import org.apache.fluss.fs.azure.token.WasbsDelegationTokenReceiver;

import org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.Objects;

import static org.apache.fluss.fs.azure.AzureFileSystemOptions.ACCOUNT_KEY;
import static org.apache.fluss.fs.azure.AzureFileSystemOptions.CLIENT_ID;
import static org.apache.fluss.fs.azure.AzureFileSystemOptions.PROVIDER_CONFIG_NAME;

/**
 * Abstract factory for creating Azure Blob Storage file systems. Supports multiple URI schemes
 * (abfs, abfss, wasb, wasbs) based on Azure HDFS support in the hadoop-azure module.
 */
abstract class AzureFileSystemPlugin implements FileSystemPlugin {
    private static final String[] FLUSS_CONFIG_PREFIXES = {"fs.azure."};

    private static final String HADOOP_CONFIG_PREFIX = "fs.azure.";

    private static final Logger LOG = LoggerFactory.getLogger(AbfsFileSystemPlugin.class);

    @Override
    public FileSystem create(URI fsUri, Configuration flussConfig) throws IOException {
        org.apache.hadoop.conf.Configuration hadoopConfig = getHadoopConfiguration(flussConfig);

        setCredentialProvider(hadoopConfig);

        // create the Azure Hadoop FileSystem
        org.apache.hadoop.fs.FileSystem fs = new AzureBlobFileSystem();
        fs.initialize(getInitURI(fsUri, hadoopConfig), hadoopConfig);
        return new AzureFileSystem(getScheme(), fs, flussConfig);
    }

    private void setCredentialProvider(org.apache.hadoop.conf.Configuration hadoopConfig) {
        if (hadoopConfig.get(ACCOUNT_KEY.key()) == null) {
            if (Objects.equals(getScheme(), "abfs")) {
                AbfsDelegationTokenReceiver.updateHadoopConfig(hadoopConfig);
            } else if (Objects.equals(getScheme(), "abfss")) {
                AbfssDelegationTokenReceiver.updateHadoopConfig(hadoopConfig);
            } else if (Objects.equals(getScheme(), "wasb")) {
                WasbDelegationTokenReceiver.updateHadoopConfig(hadoopConfig);
            } else if (Objects.equals(getScheme(), "wasbs")) {
                WasbsDelegationTokenReceiver.updateHadoopConfig(hadoopConfig);
            } else {
                throw new IllegalArgumentException("Unsupported scheme: " + getScheme());
            }
            LOG.info(
                    "{} is not set, using credential provider {}.",
                    CLIENT_ID.key(),
                    hadoopConfig.get(PROVIDER_CONFIG_NAME.key()));
        } else {
            LOG.info("{} is set, using provided account key.", ACCOUNT_KEY.key());
        }
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
