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
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.InvalidConfigException;
import org.apache.fluss.fs.FileSystem;
import org.apache.fluss.fs.FileSystemPlugin;
import org.apache.fluss.fs.s3.token.DynamicTemporaryAWSCredentialsProvider;
import org.apache.fluss.fs.s3.token.S3ADelegationTokenReceiver;
import org.apache.fluss.fs.s3.token.S3DelegationTokenProvider;
import org.apache.fluss.fs.s3.token.S3DelegationTokenReceiver;

import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;

/** Simple factory for the s3 file system. */
public class S3FileSystemPlugin implements FileSystemPlugin {

    private static final Logger LOG = LoggerFactory.getLogger(S3FileSystemPlugin.class);

    private static final String[] FLUSS_CONFIG_PREFIXES = {"s3.", "s3a.", "fs.s3a."};

    private static final String HADOOP_CONFIG_PREFIX = "fs.s3a.";

    private static final String[][] MIRRORED_CONFIG_KEYS = {
        {"fs.s3a.access-key", "fs.s3a.access.key"},
        {"fs.s3a.secret-key", "fs.s3a.secret.key"},
        {"fs.s3a.path-style-access", "fs.s3a.path.style.access"}
    };

    /**
     * When the file system is initialized by a client, all filesystem options are passed in with an
     * additional prefix. We only allow certain whitelisted configuration options ({@link
     * S3FileSystemPlugin#CLIENT_WHITELISTED_OPTIONS}) to avoid that the client passes in options
     * that might break the file system.
     */
    private static final String CLIENT_PREFIX = "client.fs.";

    private static final Set<String> CLIENT_WHITELISTED_OPTIONS =
            new HashSet<>(
                    Arrays.asList(
                            "access-key",
                            "access.key",
                            "secret-key",
                            "secret.key",
                            "aws.credentials.provider",
                            "assumed.role.credentials.provider"));

    @Override
    public String getScheme() {
        return "s3";
    }

    @Override
    public FileSystem create(URI fsUri, Configuration flussConfig) throws IOException {
        org.apache.hadoop.conf.Configuration hadoopConfig =
                mirrorCertainHadoopConfig(getHadoopConfiguration(flussConfig));

        final boolean isClient = isClient(flussConfig);
        final boolean useTokenDelegation;

        if (isClient) {
            // Only relevant on the server side, default to false
            useTokenDelegation = false;
            // We do not know if token delegation on the server will be activated or deactivated.
            // Hence, we just add the Fluss credential provider for token delegation
            // to the provider chain and the file system will figure out a valid provider
            // at runtime.
            setCredentialProviders(
                    hadoopConfig,
                    Collections.singletonList(DynamicTemporaryAWSCredentialsProvider.NAME));
            S3ADelegationTokenReceiver.updateHadoopConfigAdditionalInfos(hadoopConfig);
        } else {
            useTokenDelegation =
                    flussConfig.getBoolean(ConfigOptions.FILE_SYSTEM_S3_ENABLE_TOKEN_DELEGATION);
        }

        LOG.info("Hadoop configuration: {}", hadoopConfig);

        org.apache.hadoop.fs.FileSystem fs = new S3AFileSystem();
        fs.initialize(getInitURI(fsUri, hadoopConfig), hadoopConfig);

        final Supplier<S3DelegationTokenProvider> delegationTokenProvider =
                isClient
                        ? () -> {
                            throw new IllegalStateException(
                                    "Unexpected usage of delegation token provider. Delegation token provider should only be used on the server side.");
                        }
                        : () -> {
                            final S3DelegationTokenProvider.Type delegationTokenProviderType =
                                    useTokenDelegation
                                            ? S3DelegationTokenProvider.Type.STS_SESSION_TOKEN
                                            : S3DelegationTokenProvider.Type.NO_TOKEN;
                            return new S3DelegationTokenProvider(
                                    getScheme(), hadoopConfig, delegationTokenProviderType);
                        };

        return new S3FileSystem(fs, delegationTokenProvider);
    }

    /**
     * Creates a Hadoop configuration and adds file system-related configurations contained in the
     * Fluss configuration to the Hadoop configuration with a uniform prefix ({@link
     * S3FileSystemPlugin#HADOOP_CONFIG_PREFIX}). For client configurations ({@link
     * S3FileSystemPlugin#CLIENT_PREFIX}), only whitelisted configuration options are added.
     *
     * @param flussConfig The Fluss configuration.
     * @return The Hadoop configuration.
     */
    @VisibleForTesting
    org.apache.hadoop.conf.Configuration getHadoopConfiguration(Configuration flussConfig) {
        org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();

        if (flussConfig == null) {
            return conf;
        }

        for (String flussKey : flussConfig.keySet()) {
            for (String flussPrefix : FLUSS_CONFIG_PREFIXES) {
                if (flussKey.startsWith(flussPrefix)) {
                    String hadoopConfigKey =
                            HADOOP_CONFIG_PREFIX + flussKey.substring(flussPrefix.length());
                    String newValue =
                            flussConfig.getString(
                                    ConfigBuilder.key(flussKey).stringType().noDefaultValue(),
                                    null);
                    conf.set(hadoopConfigKey, newValue);

                    LOG.debug(
                            "Adding Fluss config entry for {} as {} to Hadoop config",
                            flussKey,
                            hadoopConfigKey);
                }

                String flussKeyClientPrefix = CLIENT_PREFIX + flussPrefix;
                if (flussKey.startsWith(flussKeyClientPrefix)) {
                    String flussClientKey = flussKey.substring(flussKeyClientPrefix.length());

                    if (CLIENT_WHITELISTED_OPTIONS.contains(flussClientKey)) {
                        String hadoopConfigKey = HADOOP_CONFIG_PREFIX + flussClientKey;
                        String newValue =
                                flussConfig.getString(
                                        ConfigBuilder.key(flussKey).stringType().noDefaultValue(),
                                        null);
                        conf.set(hadoopConfigKey, newValue);

                        LOG.debug(
                                "Adding Fluss config entry for whitelisted config {} as {} to Hadoop config",
                                flussKey,
                                hadoopConfigKey);
                    } else {
                        LOG.warn(
                                "Client passed non-whitelisted config option {}. Ignoring it",
                                flussClientKey);
                    }
                }
            }
        }
        return conf;
    }

    /**
     * Mirror certain keys to ensure uniformity across implementations with different keys.
     *
     * @param hadoopConfig The Hadoop configuration.
     * @return Hadoop configuration with added mirrored configurations.
     */
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
     * Determines whether the file system is initialized on the client or server side.
     *
     * <p>The file system is considered client-initialized if:
     *
     * <ol>
     *   <li>All configuration options start with the prefix {@link
     *       S3FileSystemPlugin#CLIENT_PREFIX}, or
     *   <li>no configuration options are provided.
     * </ol>
     *
     * <p>The file system is considered server-initialized if:
     *
     * <ol>
     *   <li>At least one configuration option is provided, and
     *   <li>None of the options start with the prefix {@link S3FileSystemPlugin#CLIENT_PREFIX}.
     * </ol>
     *
     * <p>Any other scenario is invalid.
     *
     * <p>Note that on the server side, there will always be at least one configuration option:
     *
     * <ul>
     *   <li>If token delegation is activated ({@link
     *       ConfigOptions#FILE_SYSTEM_S3_ENABLE_TOKEN_DELEGATION} is <code>true</code>, which is
     *       the default), then access key, secret key, endpoint, and region must be configured, or
     *   <li>If token delegation is deactivated, {@link
     *       ConfigOptions#FILE_SYSTEM_S3_ENABLE_TOKEN_DELEGATION} has to be explicitly set to
     *       <code>false</code>.
     * </ul>
     *
     * @param config The configuration.
     * @return {@code true} if initialized by a client, {@code false} if initialized by a server.
     * @throws org.apache.fluss.exception.InvalidConfigException if the configuration is invalid.
     */
    @VisibleForTesting
    static boolean isClient(Configuration config) {
        Map<String, String> configMap = config.toMap();

        boolean isClient = configMap.keySet().stream().allMatch(k -> k.startsWith(CLIENT_PREFIX));
        boolean isServer =
                !config.toMap().isEmpty()
                        && config.toMap().keySet().stream()
                                .noneMatch(k -> k.startsWith(CLIENT_PREFIX));

        if (isClient) {
            LOG.debug("File system is initialized by a client with configuration {}.", config);
            return true;
        } else if (isServer) {
            LOG.debug("File system is initialized by a server with configuration {}.", config);
            return false;
        } else {
            LOG.error("Detected invalid configuration: {}", config);
            throw new InvalidConfigException(
                    "Cannot initialize file system due to invalid configuration. If this exception occurs on the server side, make sure to provide access key, secret key, endpoint, and region when token delegation is activated.");
        }
    }

    private void setCredentialProviders(
            org.apache.hadoop.conf.Configuration hadoopConfig, List<String> credentialProviders) {
        if (Objects.equals(getScheme(), "s3") || Objects.equals(getScheme(), "s3a")) {
            S3DelegationTokenReceiver.updateHadoopConfigCredentialProviders(
                    hadoopConfig, credentialProviders);
        } else {
            throw new IllegalArgumentException("Unsupported scheme: " + getScheme());
        }
    }
}
