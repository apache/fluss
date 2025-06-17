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

package com.alibaba.fluss.fs.s3;

import com.alibaba.fluss.config.ConfigBuilder;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.fs.FileSystem;
import com.alibaba.fluss.fs.FileSystemPlugin;
import com.alibaba.fluss.fs.s3.token.S3ADelegationTokenReceiver;
import com.alibaba.fluss.fs.s3.token.S3DelegationTokenReceiver;

import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.Objects;

import static com.alibaba.fluss.fs.s3.token.S3DelegationTokenReceiver.PROVIDER_CONFIG_NAME;

/** Simple factory for the s3 file system. */
public class S3FileSystemPlugin implements FileSystemPlugin {

    private static final Logger LOG = LoggerFactory.getLogger(S3FileSystemPlugin.class);

    private static final String[] FLUSS_CONFIG_PREFIXES = {"s3.", "s3a.", "fs.s3a."};

    private static final String HADOOP_CONFIG_PREFIX = "fs.s3a.";

    private static final String ACCESS_KEY_ID = "fs.s3a.access.key";

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

        // set credential provider
        setCredentialProvider(hadoopConfig);

        // create the Hadoop FileSystem
        org.apache.hadoop.fs.FileSystem fs = new S3AFileSystem();
        fs.initialize(getInitURI(fsUri, hadoopConfig), hadoopConfig);
        return new S3FileSystem(getScheme(), fs, hadoopConfig);
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
        if (hadoopConfig.get(ACCESS_KEY_ID) == null) {
            if (Objects.equals(getScheme(), "s3")) {
                S3DelegationTokenReceiver.updateHadoopConfig(hadoopConfig);
            } else if (Objects.equals(getScheme(), "s3a")) {
                S3ADelegationTokenReceiver.updateHadoopConfig(hadoopConfig);
            } else {
                throw new IllegalArgumentException("Unsupported scheme: " + getScheme());
            }
            LOG.info(
                    "{} is not set, using credential provider {}.",
                    ACCESS_KEY_ID,
                    hadoopConfig.get(PROVIDER_CONFIG_NAME));
        } else {
            LOG.info("{} is set, using provided access key id and secret.", ACCESS_KEY_ID);
        }
    }
}
