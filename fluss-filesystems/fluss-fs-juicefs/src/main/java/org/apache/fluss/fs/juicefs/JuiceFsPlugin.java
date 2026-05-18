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

package org.apache.fluss.fs.juicefs;

import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.config.ConfigBuilder;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.fs.FileSystem;
import org.apache.fluss.fs.FileSystemPlugin;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;

/**
 * Simple factory for the JuiceFS file system.
 *
 * <p>This plugin registers the {@code jfs} scheme and bridges Fluss to the JuiceFS Hadoop SDK
 * ({@code io.juicefs.JuiceFileSystem}). The JuiceFS client itself manages authentication via its
 * meta server / access keys, so no Fluss-side delegation token is required.
 *
 * <p>Configuration is propagated from Fluss to the underlying Hadoop {@link
 * org.apache.hadoop.conf.Configuration} for any key starting with {@code fs.jfs.} or {@code
 * juicefs.}. Two defaults are injected when not provided by the user:
 *
 * <ul>
 *   <li>{@code fs.jfs.impl=io.juicefs.JuiceFileSystem}
 *   <li>{@code fs.jfs.impl.disable.cache=false}
 * </ul>
 */
public class JuiceFsPlugin implements FileSystemPlugin {

    private static final Logger LOG = LoggerFactory.getLogger(JuiceFsPlugin.class);

    public static final String SCHEME = "jfs";

    /** Fully qualified class name of the JuiceFS Hadoop SDK FileSystem implementation. */
    static final String JUICEFS_HADOOP_FS_IMPL = "io.juicefs.JuiceFileSystem";

    /** Hadoop configuration key that selects the {@code jfs} scheme implementation. */
    static final String FS_JFS_IMPL_KEY = "fs.jfs.impl";

    /** Hadoop configuration key controlling the FileSystem cache for the {@code jfs} scheme. */
    static final String FS_JFS_IMPL_DISABLE_CACHE_KEY = "fs.jfs.impl.disable.cache";

    /**
     * In order to simplify, fluss juicefs configuration keys mirror the upstream Hadoop / JuiceFS
     * keys. Any Fluss config entry whose key starts with one of these prefixes is forwarded to the
     * Hadoop configuration.
     *
     * <ul>
     *   <li>{@code fs.jfs.} — Hadoop FileSystem-framework level keys (impl class, cache, etc.)
     *   <li>{@code juicefs.} — native JuiceFS client knobs (meta URL, cache-dir, access-key, ...)
     * </ul>
     */
    private static final String[] FLUSS_CONFIG_PREFIXES = {"fs.jfs.", "juicefs."};

    @Override
    public String getScheme() {
        return SCHEME;
    }

    @Override
    public FileSystem create(URI fsUri, Configuration flussConfig) throws IOException {
        org.apache.hadoop.conf.Configuration hadoopConfig = getHadoopConfiguration(flussConfig);
        applyJuiceFsDefaults(hadoopConfig);

        // handle missing scheme/authority by falling back to the configured default URI
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

        // load the JuiceFS Hadoop SDK reflectively via Hadoop's FileSystem.newInstance, so that
        // the Fluss bytecode does not have a hard compile-time dependency on io.juicefs classes
        org.apache.hadoop.fs.FileSystem hadoopFs =
                org.apache.hadoop.fs.FileSystem.newInstance(fsUri, hadoopConfig);
        LOG.info(
                "Created JuiceFS Hadoop FileSystem: scheme={}, authority={}, impl={}",
                fsUri.getScheme(),
                fsUri.getAuthority(),
                hadoopFs.getClass().getName());

        return new JuiceFsFileSystem(hadoopFs);
    }

    /**
     * Inject sensible defaults for the JuiceFS Hadoop bridge if the user has not specified them.
     *
     * <p>This is package-private to allow direct testing without a live JuiceFS meta server.
     */
    @VisibleForTesting
    static void applyJuiceFsDefaults(org.apache.hadoop.conf.Configuration hadoopConfig) {
        if (hadoopConfig.get(FS_JFS_IMPL_KEY) == null) {
            hadoopConfig.set(FS_JFS_IMPL_KEY, JUICEFS_HADOOP_FS_IMPL);
        }
        if (hadoopConfig.get(FS_JFS_IMPL_DISABLE_CACHE_KEY) == null) {
            hadoopConfig.set(FS_JFS_IMPL_DISABLE_CACHE_KEY, "false");
        }
    }

    @VisibleForTesting
    org.apache.hadoop.conf.Configuration getHadoopConfiguration(Configuration flussConfig) {
        org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
        if (flussConfig == null) {
            return conf;
        }

        // read all configuration entries with a prefix in 'FLUSS_CONFIG_PREFIXES'
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
                    break;
                }
            }
        }
        return conf;
    }
}
