/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.fs.oss;

import com.alibaba.fluss.annotation.VisibleForTesting;
import com.alibaba.fluss.config.ConfigBuilder;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.exception.FlussRuntimeException;
import com.alibaba.fluss.fs.FileSystem;
import com.alibaba.fluss.fs.FileSystemPlugin;
import com.alibaba.fluss.fs.oss.token.OSSSecurityTokenReceiver;

import com.aliyun.oss.OSSClient;
import com.aliyun.oss.common.comm.SignVersion;
import org.apache.hadoop.fs.aliyun.oss.AliyunOSSFileSystem;
import org.apache.hadoop.fs.aliyun.oss.AliyunOSSFileSystemStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Field;
import java.net.URI;

import static org.apache.hadoop.fs.aliyun.oss.Constants.ACCESS_KEY_ID;
import static org.apache.hadoop.fs.aliyun.oss.Constants.CREDENTIALS_PROVIDER_KEY;

/* This file is based on source code of Apache Flink Project (https://flink.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/** Simple factory for the OSS file system. */
public class OSSFileSystemPlugin implements FileSystemPlugin {

    private static final Logger LOG = LoggerFactory.getLogger(OSSFileSystemPlugin.class);

    public static final String SCHEME = "oss";

    /**
     * In order to simplify, we make fluss oss configuration keys same with hadoop oss module. So,
     * we add all configuration key with prefix `fs.oss` in fluss conf to hadoop conf
     */
    private static final String[] FLUSS_CONFIG_PREFIXES = {"fs.oss."};

    public static final String REGION_KEY = "fs.oss.region";

    @Override
    public String getScheme() {
        return SCHEME;
    }

    @Override
    public FileSystem create(URI fsUri, Configuration flussConfig) throws IOException {
        org.apache.hadoop.conf.Configuration hadoopConfig = getHadoopConfiguration(flussConfig);

        // set credential provider
        if (hadoopConfig.get(ACCESS_KEY_ID) == null) {
            String credentialsProvider = hadoopConfig.get(CREDENTIALS_PROVIDER_KEY);
            if (credentialsProvider != null) {
                LOG.info(
                        "{} is not set, but {} is set, using credential provider {}.",
                        ACCESS_KEY_ID,
                        CREDENTIALS_PROVIDER_KEY,
                        credentialsProvider);
            } else {
                // no ak, no credentialsProvider,
                // set default credential provider which will get token from
                // OSSSecurityTokenReceiver
                setDefaultCredentialProvider(flussConfig, hadoopConfig);
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
        return new OSSFileSystem(fileSystem, getScheme(), hadoopConfig);
    }

    protected org.apache.hadoop.fs.FileSystem initFileSystem(
            URI fsUri, org.apache.hadoop.conf.Configuration hadoopConfig) throws IOException {
        AliyunOSSFileSystem fileSystem = new AliyunOSSFileSystem();
        fileSystem.initialize(fsUri, hadoopConfig);
        setSignatureVersion4(fileSystem, hadoopConfig);
        return fileSystem;
    }

    protected void setDefaultCredentialProvider(
            Configuration flussConfig, org.apache.hadoop.conf.Configuration hadoopConfig) {
        // use OSSSecurityTokenReceiver to update hadoop config to set credentialsProvider
        OSSSecurityTokenReceiver.updateHadoopConfig(hadoopConfig);
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

    private void setSignatureVersion4(
            AliyunOSSFileSystem aliyunOSSFileSystem,
            org.apache.hadoop.conf.Configuration hadoopConfig) {
        // hack logic, we use reflection to set signature version 4
        // todo: remove the hack logic once hadoop-aliyun lib support it
        AliyunOSSFileSystemStore aliyunOSSFileSystemStore = aliyunOSSFileSystem.getStore();
        try {
            // get oss client by reflection
            Field ossClientField =
                    aliyunOSSFileSystemStore.getClass().getDeclaredField("ossClient");
            ossClientField.setAccessible(true);
            OSSClient ossClient = (OSSClient) ossClientField.get(aliyunOSSFileSystemStore);
            ossClient.switchSignatureVersion(SignVersion.V4);
            String region = hadoopConfig.get(REGION_KEY);
            if (region == null) {
                throw new IllegalArgumentException(
                        String.format(
                                "Region key %s must be set for oss file system.", REGION_KEY));
            }
            ossClient.setRegion(region);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            // fail directly, we want to make sure use v4 signature
            throw new FlussRuntimeException(
                    "Fail to set signature version 4 for Oss filesystem.", e);
        }
    }
}
