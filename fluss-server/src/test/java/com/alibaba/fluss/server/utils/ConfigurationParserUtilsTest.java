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

package com.alibaba.fluss.server.utils;

import com.alibaba.fluss.config.ConfigBuilder;
import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.config.GlobalConfiguration;
import com.alibaba.fluss.server.exception.FlussParseException;

import org.apache.commons.cli.MissingOptionException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link ConfigurationParserUtils}. */
public class ConfigurationParserUtilsTest {

    @Test
    void testLoadCommonConfiguration(@TempDir Path tempFolder) throws Exception {
        Path yamlFile = tempFolder.resolve("server.yaml");
        Files.write(yamlFile, Collections.singleton("coordinator.port: 9124"));
        String confDir = tempFolder.toAbsolutePath().toString();
        final String key = "key";
        final String value = "value";
        final String arg1 = "arg1";
        final String arg2 = "arg2";

        final String[] args = {
            "--configDir", confDir, String.format("-D%s=%s", key, value), arg1, arg2
        };

        Configuration configuration =
                ConfigurationParserUtils.loadCommonConfiguration(
                        args, ConfigurationParserUtilsTest.class.getSimpleName());
        // should respect the configurations in args
        assertThat(configuration.getString(ConfigBuilder.key(key).stringType().noDefaultValue()))
                .isEqualTo(value);

        // should respect the configurations in the server.yaml
        assertThat(configuration.getString(ConfigOptions.COORDINATOR_PORT)).isEqualTo("9124");
    }

    @Test
    void testLoadWithUserSpecifiedConfigFile(@TempDir Path tempFolder) throws Exception {
        Path yamlFile = tempFolder.resolve("server.yaml");
        Files.write(yamlFile, Collections.singleton("coordinator.port: 9124"));
        String confDir = tempFolder.toAbsolutePath().toString();

        Path userDefinedConfigFile = tempFolder.resolve("user-defined-server.yaml");
        Files.write(yamlFile, Collections.singleton("coordinator.port: 1000"));

        final String configKey = GlobalConfiguration.FLUSS_CONF_FILENAME;
        final String configValue = userDefinedConfigFile.toString();

        final String[] args = {
            "--configDir", confDir, String.format("-D%s=%s", configKey, configValue)
        };
        Configuration configuration =
                ConfigurationParserUtils.loadCommonConfiguration(
                        args, ConfigurationParserUtilsTest.class.getSimpleName());
        // should use the configurations in the user-defined-server.yaml
        assertThat(
                        configuration.get(
                                ConfigBuilder.key("coordinator.port").intType().noDefaultValue()))
                .isEqualTo(1000);
    }

    @Test
    void testLoadCommonConfigurationThrowException() {
        // should throw exception when miss options 'c'('configDir')
        assertThatThrownBy(
                        () ->
                                ConfigurationParserUtils.loadCommonConfiguration(
                                        new String[0],
                                        ConfigurationParserUtilsTest.class.getSimpleName()))
                .isInstanceOf(FlussParseException.class)
                .hasMessageContaining("Failed to parse the command line arguments")
                .cause()
                .isInstanceOf(MissingOptionException.class)
                .hasMessageContaining("Missing required option: c");
    }
}
