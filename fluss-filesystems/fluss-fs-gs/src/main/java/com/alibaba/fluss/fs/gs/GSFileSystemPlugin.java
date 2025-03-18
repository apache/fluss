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

package com.alibaba.fluss.fs.gs;

import com.alibaba.fluss.config.ConfigBuilder;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.fs.FileSystem;
import com.alibaba.fluss.fs.FileSystemPlugin;

import com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;

/** Simple factory for the Google Cloud Storage file system. */
public class GSFileSystemPlugin implements FileSystemPlugin {
    private static final Logger LOG = LoggerFactory.getLogger(GSFileSystemPlugin.class);

    private static final String[] FLUSS_CONFIG_PREFIXES = {"gs.", "fs.gs."};

    private static final String HADOOP_CONFIG_PREFIX = "fs.gs.";

    @Override
    public String getScheme() {
        return "gs";
    }

    @Override
    public FileSystem create(URI fsUri, Configuration flussConfig) throws IOException {
        org.apache.hadoop.conf.Configuration hadoopConfig = getHadoopConfiguration(flussConfig);

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
