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

package org.apache.fluss.utils;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.List;

/**
 * Utility class for extracting path-specific configurations for remote data directories.
 *
 * <p>This class supports per-path authentication by allowing configuration keys to be prefixed with
 * {@code remote.data.dir.<index>} to override global settings for specific paths.
 *
 * <p>Example configuration:
 *
 * <pre>
 * remote.data.dirs: [oss://bucket1/path1, oss://bucket2/path2]
 * # Global OSS configuration
 * fs.oss.endpoint: oss-cn-hangzhou.aliyuncs.com
 * fs.oss.region: cn-hangzhou
 * # Path-specific configuration for the first path (index 0)
 * remote.data.dir.0.fs.oss.accessKeyId: ak1
 * remote.data.dir.0.fs.oss.accessKeySecret: sk1
 * # Path-specific configuration for the second path (index 1)
 * remote.data.dir.1.fs.oss.accessKeyId: ak2
 * remote.data.dir.1.fs.oss.accessKeySecret: sk2
 * </pre>
 */
public class RemoteDataDirConfigUtils {

    private static final Logger LOG = LoggerFactory.getLogger(RemoteDataDirConfigUtils.class);

    private static final String REMOTE_DATA_DIR_PREFIX = "remote.data.dir.";

    /**
     * Creates a merged configuration for a given URI by combining global configuration with
     * path-specific overrides.
     *
     * <p>If the URI matches one of the configured remote data directories, path-specific
     * configurations (prefixed with {@code remote.data.dir.<index>}) will override the global
     * configuration.
     *
     * @param uri the URI to get configuration for
     * @param globalConfig the global configuration
     * @return a new Configuration object with path-specific overrides applied
     */
    public static Configuration getMergedConfigForUri(URI uri, Configuration globalConfig) {
        Configuration mergedConfig = new Configuration(globalConfig);

        // Find the index of the URI in remote.data.dirs
        Integer pathIndex = findPathIndex(uri, globalConfig);
        if (pathIndex == null) {
            // URI doesn't match any configured remote data directory, return global config
            return mergedConfig;
        }

        // Apply path-specific configuration overrides
        String pathPrefix = REMOTE_DATA_DIR_PREFIX + pathIndex + ".";
        for (String key : globalConfig.keySet()) {
            if (key.startsWith(pathPrefix)) {
                String originalKey = key.substring(pathPrefix.length());
                String value =
                        globalConfig.getString(
                                org.apache.fluss.config.ConfigBuilder.key(key)
                                        .stringType()
                                        .noDefaultValue(),
                                null);
                if (value != null) {
                    mergedConfig.setString(originalKey, value);
                    LOG.debug(
                            "Applied path-specific config override: {} = {} (from {})",
                            originalKey,
                            maskSensitiveValue(originalKey, value),
                            key);
                }
            }
        }

        return mergedConfig;
    }

    /**
     * Finds the index of the given URI in the remote.data.dirs configuration.
     *
     * @param uri the URI to find
     * @param config the configuration
     * @return the index if found, null otherwise
     */
    public static Integer findPathIndex(URI uri, Configuration config) {
        List<String> remoteDataDirs = config.get(ConfigOptions.REMOTE_DATA_DIRS);
        if (remoteDataDirs == null || remoteDataDirs.isEmpty()) {
            return null;
        }

        String uriString = uri.toString();
        // Normalize URI by removing trailing slashes for comparison
        String normalizedUri = normalizeUri(uriString);

        for (int i = 0; i < remoteDataDirs.size(); i++) {
            String remoteDataDir = remoteDataDirs.get(i);
            String normalizedDir = normalizeUri(remoteDataDir);
            // Check if the URI starts with the remote data dir path
            if (normalizedUri.startsWith(normalizedDir)) {
                LOG.debug(
                        "Found URI {} matches remote.data.dirs[{}] = {}",
                        uriString,
                        i,
                        remoteDataDir);
                return i;
            }
        }

        return null;
    }

    /**
     * Normalizes a URI string by removing trailing slashes.
     *
     * @param uriString the URI string to normalize
     * @return the normalized URI string
     */
    private static String normalizeUri(String uriString) {
        String normalized = uriString;
        while (normalized.endsWith("/")) {
            normalized = normalized.substring(0, normalized.length() - 1);
        }
        return normalized;
    }

    /**
     * Masks sensitive configuration values in log messages.
     *
     * @param key the configuration key
     * @param value the configuration value
     * @return the masked value if the key is sensitive, otherwise the original value
     */
    private static String maskSensitiveValue(String key, String value) {
        if (key == null || value == null) {
            return value;
        }
        String lowerKey = key.toLowerCase();
        if (lowerKey.contains("secret")
                || lowerKey.contains("password")
                || lowerKey.contains("key")
                || lowerKey.contains("token")) {
            return "***";
        }
        return value;
    }
}
