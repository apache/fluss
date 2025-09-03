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

package org.apache.fluss.fs.obs;

import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.config.ConfigBuilder;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.fs.FileSystem;
import org.apache.fluss.fs.FileSystemPlugin;
import org.apache.fluss.fs.obs.token.OBSSecurityTokenReceiver;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;

/** Simple factory for the HuaweiCloud OBS file system. */
public class OBSFileSystemPlugin implements FileSystemPlugin {

    private static final Logger LOG = LoggerFactory.getLogger(OBSFileSystemPlugin.class);

    public static final String SCHEME = "obs";

    /**
     * In order to simplify, we make fluss obs configuration keys same with hadoop obs module. So,
     * we add all configuration key with prefix `fs.obs` in fluss conf to hadoop conf
     */
    private static final String[] FLUSS_CONFIG_PREFIXES = {"fs.obs."};

    private static final String ACCESS_KEY_ID = "fs.obs.access.key";
    public static final String CREDENTIALS_PROVIDER = "fs.obs.security.provider";

    public static final String REGION_KEY = "fs.obs.region";

    @Override
    public String getScheme() {
        return SCHEME;
    }

    @Override
    public FileSystem create(URI fsUri, Configuration flussConfig) throws IOException {
        org.apache.hadoop.conf.Configuration hadoopConfig = getHadoopConfiguration(flussConfig);

        // set credential provider
        if (hadoopConfig.get(ACCESS_KEY_ID) == null) {
            String credentialsProvider = hadoopConfig.get(CREDENTIALS_PROVIDER);
            if (credentialsProvider != null) {
                LOG.info(
                        "{} is not set, but {} is set, using credential provider {}.",
                        ACCESS_KEY_ID,
                        CREDENTIALS_PROVIDER,
                        credentialsProvider);
            } else {
                // no ak, no credentialsProvider,
                // set default credential provider which will get token from
                // OBSSecurityTokenReceiver
                setDefaultCredentialProvider(hadoopConfig);
            }
        } else {
            LOG.info("{} is set, using provided access key id and secret.", ACCESS_KEY_ID);
        }

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

        org.apache.hadoop.fs.FileSystem fileSystem = initFileSystem(fsUri, hadoopConfig);
        return new OBSFileSystem(fileSystem, getScheme(), hadoopConfig);
    }

    protected org.apache.hadoop.fs.FileSystem initFileSystem(
            URI fsUri, org.apache.hadoop.conf.Configuration hadoopConfig) throws IOException {
        org.apache.hadoop.fs.obs.OBSFileSystem fileSystem =
                new org.apache.hadoop.fs.obs.OBSFileSystem();
        fileSystem.initialize(fsUri, hadoopConfig);
        return fileSystem;
    }

    protected void setDefaultCredentialProvider(org.apache.hadoop.conf.Configuration hadoopConfig) {
        // use OBSSecurityTokenReceiver to update hadoop config to set credentialsProvider
        OBSSecurityTokenReceiver.updateHadoopConfig(hadoopConfig);
    }

    @VisibleForTesting
    org.apache.hadoop.conf.Configuration getHadoopConfiguration(Configuration flussConfig) {
        org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
        if (flussConfig == null) {
            return conf;
        }

        // read all configuration with prefix 'FLUSS_CONFIG_PREFIXES'
        for (String key : flussConfig.keySet()) {
            for (String prefix : FLUSS_CONFIG_PREFIXES) {
                if (key.startsWith(prefix)) {
                    String value =
                            flussConfig.getString(
                                    ConfigBuilder.key(key).stringType().noDefaultValue(), null);
                    conf.set(key, value);

                    LOG.debug(
                            "Adding Fluss config entry for {} as {} to Hadoop config",
                            key,
                            conf.get(key));
                }
            }
        }
        return conf;
    }
}
