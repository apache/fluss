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

package com.alibaba.fluss.fs.cosn;

import com.alibaba.fluss.annotation.VisibleForTesting;
import com.alibaba.fluss.config.ConfigBuilder;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.fs.FileSystem;
import com.alibaba.fluss.fs.FileSystemPlugin;
import com.alibaba.fluss.fs.cosn.token.COSNSecurityTokenReceiver;

import org.apache.hadoop.fs.cosn.CosNFileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;

import static org.apache.hadoop.fs.cosn.CosNConfigKeys.COSN_CREDENTIALS_PROVIDER;
import static org.apache.hadoop.fs.cosn.CosNConfigKeys.COSN_SECRET_ID_KEY;

/* This file is based on source code of Apache Flink Project (https://flink.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/** Simple factory for the OSS file system. */
public class COSNFileSystemPlugin implements FileSystemPlugin {

    private static final Logger LOG = LoggerFactory.getLogger(COSNFileSystemPlugin.class);

    public static final String SCHEME = "cosn";

    /**
     * In order to simplify, we make fluss oss configuration keys same with hadoop oss module. So,
     * we add all configuration key with prefix `fs.cosn` in fluss conf to hadoop conf
     */
    private static final String[] FLUSS_CONFIG_PREFIXES = {"fs.cosn."};

    @Override
    public String getScheme() {
        return SCHEME;
    }

    @Override
    public FileSystem create(URI fsUri, Configuration flussConfig) throws IOException {
        org.apache.hadoop.conf.Configuration hadoopConfig = getHadoopConfiguration(flussConfig);

        // set credential provider
        if (hadoopConfig.get(COSN_SECRET_ID_KEY) == null) {
            String credentialsProvider = hadoopConfig.get(COSN_CREDENTIALS_PROVIDER);
            if (credentialsProvider != null) {
                LOG.info(
                        "{} is not set, but {} is set, using credential provider {}.",
                        COSN_SECRET_ID_KEY,
                        COSN_CREDENTIALS_PROVIDER,
                        credentialsProvider);
            } else {
                // no ak, no credentialsProvider,
                // set default credential provider which will get token from
                // COSNSecurityTokenReceiver
                setDefaultCredentialProvider(flussConfig, hadoopConfig);
            }
        } else {
            LOG.info("{} is set, using provided access key id and secret.", COSN_SECRET_ID_KEY);
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
        return new COSNFileSystem(fileSystem, getScheme(), hadoopConfig);
    }

    protected org.apache.hadoop.fs.FileSystem initFileSystem(
            URI fsUri, org.apache.hadoop.conf.Configuration hadoopConfig) throws IOException {
        CosNFileSystem fileSystem = new CosNFileSystem();
        fileSystem.initialize(fsUri, hadoopConfig);
        return fileSystem;
    }

    protected void setDefaultCredentialProvider(
            Configuration flussConfig, org.apache.hadoop.conf.Configuration hadoopConfig) {
        // use OSSSecurityTokenReceiver to update hadoop config to set credentialsProvider
        COSNSecurityTokenReceiver.updateHadoopConfig(hadoopConfig);
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
