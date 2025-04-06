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

package com.alibaba.fluss.server.utils;

import com.alibaba.fluss.cluster.ServerType;
import com.alibaba.fluss.config.ConfigBuilder;
import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.exception.IllegalConfigurationException;
import com.alibaba.fluss.server.exception.FlussParseException;

import org.apache.commons.cli.MissingOptionException;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;

/** Test for {@link ConfigurationParserUtils}. */
public class ConfigurationParserUtilsTest {

    private static final String OLD_COMMON_CONFIG_FILE_NAME = "server.yaml";
    private static final String NEW_COMMON_CONFIG_FILE_NAME = "common.yaml";
    private static final String COORDINATOR_SERVER_CONF_FILE_NAME = "coordinator-server.yaml";
    private static final String TABLET_SERVER_CONF_FILE_NAME = "tablet-server.yaml";

    @ParameterizedTest
    @MethodSource("parameterizedTestConfiguration")
    void testLoadWithCommonConfigurationTile(
            ServerType serverType,
            String commonConfigFileName,
            String additionalConfigFileName,
            @TempDir Path tempFolder)
            throws Exception {
        Path yamlFile = tempFolder.resolve(commonConfigFileName);
        Files.write(yamlFile, Collections.singleton("bind.listeners: FLUSS://localhost:9124"));
        if (commonConfigFileName.equals(NEW_COMMON_CONFIG_FILE_NAME)) {
            Files.write(tempFolder.resolve(additionalConfigFileName), Collections.emptyList());
        }
        String confDir = tempFolder.toAbsolutePath().toString();

        final String key = "key";
        final String value = "value";
        final String arg1 = "arg1";
        final String arg2 = "arg2";

        final String[] args = {
            "--configDir", confDir, String.format("-D%s=%s", key, value), arg1, arg2
        };

        Configuration configuration =
                ConfigurationParserUtils.loadConfiguration(
                        args, ConfigurationParserUtilsTest.class.getSimpleName(), serverType);
        // should respect the configurations in args
        assertThat(configuration.getString(ConfigBuilder.key(key).stringType().noDefaultValue()))
                .isEqualTo(value);

        // should respect the configurations in the server.yaml
        assertThat(configuration.getString(ConfigOptions.BIND_LISTENERS))
                .isEqualTo("FLUSS://localhost:9124");
    }

    @ParameterizedTest
    @MethodSource("parameterizedTestConfiguration")
    void testLoadWithUserSpecifiedConfigFile(
            ServerType serverType,
            String commonConfigFileName,
            String additionalConfigFileName,
            @TempDir Path tempFolder)
            throws Exception {
        Path yamlFile = tempFolder.resolve(commonConfigFileName);
        Files.write(yamlFile, Collections.singleton("bind.listeners: FLUSS://localhost:9124"));
        if (commonConfigFileName.equals(NEW_COMMON_CONFIG_FILE_NAME)) {
            Files.write(tempFolder.resolve(additionalConfigFileName), Collections.emptyList());
        }
        String confDir = tempFolder.toAbsolutePath().toString();

        Path userDefinedConfigFile = tempFolder.resolve("user-defined-server.yaml");
        Files.write(yamlFile, Collections.singleton("bind.listeners: FLUSS://example.com:1000"));

        final String configKey = commonConfigFileName;
        final String configValue = userDefinedConfigFile.toString();

        final String[] args = {
            "--configDir", confDir, String.format("-D%s=%s", configKey, configValue)
        };
        Configuration configuration =
                ConfigurationParserUtils.loadConfiguration(
                        args, ConfigurationParserUtilsTest.class.getSimpleName(), serverType);
        // should use the configurations in the user-defined-server.yaml
        assertThat(
                        configuration.get(
                                ConfigBuilder.key("bind.listeners").stringType().noDefaultValue()))
                .isEqualTo("FLUSS://example.com:1000");
    }

    @ParameterizedTest
    @EnumSource(ServerType.class)
    void testMissingConfigDirOptionThrowsException(ServerType serverType) {
        // should throw exception when miss options 'c'('configDir')
        assertThatThrownBy(
                        () ->
                                ConfigurationParserUtils.loadConfiguration(
                                        new String[0],
                                        ConfigurationParserUtilsTest.class.getSimpleName(),
                                        serverType))
                .isInstanceOf(FlussParseException.class)
                .hasMessageContaining("Failed to parse the command line arguments")
                .cause()
                .isInstanceOf(MissingOptionException.class)
                .hasMessageContaining("Missing required option: c");
    }

    @ParameterizedTest
    @MethodSource("parameterizedTestConfiguration")
    void testOnlyLoadRelevantConfigFiles(
            ServerType serverType,
            String commonConfigFileName,
            String additionalConfigFileName,
            @TempDir Path tempFolder)
            throws Exception {
        Path commonYamlFile = tempFolder.resolve(commonConfigFileName);
        Files.write(commonYamlFile, Collections.singleton("bind.listeners: FLUSS://common:9124"));

        // make all possible config files available in config dir to check only the relevant ones
        // are loaded
        if (serverType.equals(ServerType.COORDINATOR)) {
            Files.write(
                    tempFolder.resolve(additionalConfigFileName),
                    Collections.singleton("bind.listeners: FLUSS://dedicated:9124"));
            Files.write(
                    tempFolder.resolve(TABLET_SERVER_CONF_FILE_NAME),
                    Collections.singleton(
                            "bind.listeners: FLUSS://expected-to-read-only-coordinator-server-yaml:9124"));
        } else {
            Files.write(
                    tempFolder.resolve(additionalConfigFileName),
                    Collections.singleton("bind.listeners: FLUSS://dedicated:9124"));
            Files.write(
                    tempFolder.resolve(COORDINATOR_SERVER_CONF_FILE_NAME),
                    Collections.singleton(
                            "bind.listeners: FLUSS://expected-to-read-only-tablet-server-yaml:9124"));
        }
        // in addition, put another YAML file on the config dir
        Files.write(
                tempFolder.resolve("conf.yaml"),
                Collections.singleton(
                        "bind.listeners: FLUSS://expected-conf-yaml-to-be-not-read:9124"));

        String confDir = tempFolder.toAbsolutePath().toString();

        String[] args = {"--configDir", confDir};

        Configuration configuration =
                ConfigurationParserUtils.loadConfiguration(
                        args, ConfigurationParserUtilsTest.class.getSimpleName(), serverType);

        if (commonConfigFileName.equals(OLD_COMMON_CONFIG_FILE_NAME)) {
            // backward compatability
            // should only load from old file, no additional files should be read
            assertThat(configuration.getString(ConfigOptions.BIND_LISTENERS))
                    .isEqualTo("FLUSS://common:9124");
        } else if (commonConfigFileName.equals(NEW_COMMON_CONFIG_FILE_NAME)) {
            // when using new common config file, dedicated config files should overwrite options
            // from common, but only the files for the respective server type should be read
            assertThat(configuration.getString(ConfigOptions.BIND_LISTENERS))
                    .isEqualTo("FLUSS://dedicated:9124");
        } else {
            fail("Unexpected common config file name: " + commonConfigFileName);
        }

        args =
                new String[] {
                    "--configDir",
                    confDir,
                    String.format(
                            "-D%s=%s",
                            "bind.listeners",
                            "FLUSS://dynamic-option-should-overwrite-config-file:9124"),
                    String.format("-D%s=%s", "key", 1234)
                };

        // dynamic configuration options should overwrite config file options
        configuration =
                ConfigurationParserUtils.loadConfiguration(
                        args, ConfigurationParserUtilsTest.class.getSimpleName(), serverType);
        assertThat(configuration.getString(ConfigOptions.BIND_LISTENERS))
                .isEqualTo("FLUSS://dynamic-option-should-overwrite-config-file:9124");
        assertThat(configuration.getInt(ConfigBuilder.key("key").intType().noDefaultValue()))
                .isEqualTo(1234);
    }

    @ParameterizedTest
    @EnumSource(ServerType.class)
    void testUsingOldAndNewCommonConfigThrowsException(
            ServerType serverType, @TempDir Path tempFolder) throws Exception {
        Path oldYaml = tempFolder.resolve(OLD_COMMON_CONFIG_FILE_NAME);
        Files.write(oldYaml, Collections.singleton("bind.listeners: FLUSS://localhost:9124"));
        Path newYaml = tempFolder.resolve(NEW_COMMON_CONFIG_FILE_NAME);
        Files.write(newYaml, Collections.singleton("bind.listeners: FLUSS://localhost:9124"));
        String confDir = tempFolder.toAbsolutePath().toString();

        final String key = "key";
        final String value = "value";
        final String arg1 = "arg1";
        final String arg2 = "arg2";

        final String[] args = {
            "--configDir", confDir, String.format("-D%s=%s", key, value), arg1, arg2
        };

        assertThatThrownBy(
                        () ->
                                ConfigurationParserUtils.loadConfiguration(
                                        args,
                                        ConfigurationParserUtilsTest.class.getSimpleName(),
                                        serverType))
                .isInstanceOf(IllegalConfigurationException.class)
                .hasMessageContaining("Only one of");
    }

    private static Stream<Arguments> parameterizedTestConfiguration() {
        return Stream.of(
                Arguments.of(
                        ServerType.COORDINATOR,
                        OLD_COMMON_CONFIG_FILE_NAME,
                        COORDINATOR_SERVER_CONF_FILE_NAME),
                Arguments.of(
                        ServerType.TABLET_SERVER,
                        OLD_COMMON_CONFIG_FILE_NAME,
                        TABLET_SERVER_CONF_FILE_NAME),
                Arguments.of(
                        ServerType.COORDINATOR,
                        NEW_COMMON_CONFIG_FILE_NAME,
                        COORDINATOR_SERVER_CONF_FILE_NAME),
                Arguments.of(
                        ServerType.TABLET_SERVER,
                        NEW_COMMON_CONFIG_FILE_NAME,
                        TABLET_SERVER_CONF_FILE_NAME));
    }
}
