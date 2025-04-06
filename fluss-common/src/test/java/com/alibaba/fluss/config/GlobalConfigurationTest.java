/*
 *  Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.alibaba.fluss.config;

import com.alibaba.fluss.exception.IllegalConfigurationException;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link com.alibaba.fluss.config.GlobalConfiguration}. */
public class GlobalConfigurationTest {

    private static final String OLD_COMMON_CONFIG_FILE_NAME = "server.yaml";
    private static final String NEW_COMMON_CONFIG_FILE_NAME = "common.yaml";

    @Test
    void testLoadConfigurationWithoutAdditionalFiles(@TempDir Path tempFolder) throws Exception {
        String confDir = tempFolder.toAbsolutePath().toString();

        // backward compatability for old common configuration file
        Path serverYaml = tempFolder.resolve(OLD_COMMON_CONFIG_FILE_NAME);
        Files.write(serverYaml, Collections.singleton("coordinator.host: localhost"));

        Configuration configuration = GlobalConfiguration.loadConfiguration(confDir, null);
        assertThat(configuration.get(ConfigOptions.COORDINATOR_HOST)).isEqualTo("localhost");

        // backward compatability for old common configuration file + precedence for dynamic
        // properties
        Configuration dynamicConfig = new Configuration();
        dynamicConfig.set(ConfigOptions.COORDINATOR_HOST, "example.com");
        configuration = GlobalConfiguration.loadConfiguration(confDir, dynamicConfig);
        assertThat(configuration.get(ConfigOptions.COORDINATOR_HOST)).isEqualTo("example.com");

        // old and new common configuration file should not be present at the same time
        Path commonYaml = tempFolder.resolve(NEW_COMMON_CONFIG_FILE_NAME);
        Files.write(commonYaml, Collections.singleton("bind.listeners: FLUSS://localhost:9124"));
        assertThatThrownBy(() -> GlobalConfiguration.loadConfiguration(confDir, null))
                .isInstanceOf(IllegalConfigurationException.class)
                .hasMessageContaining("Only one of");

        // new common configuration file
        Files.delete(serverYaml);
        configuration = GlobalConfiguration.loadConfiguration(confDir, null);
        assertThat(configuration.get(ConfigOptions.BIND_LISTENERS))
                .isEqualTo("FLUSS://localhost:9124");
        assertThat(configuration.get(ConfigOptions.COORDINATOR_HOST)).isEqualTo(null);

        // new common configuration file + precedence for dynamic properties
        dynamicConfig = new Configuration();
        dynamicConfig.set(ConfigOptions.BIND_LISTENERS, "FLUSS://example.com:9124");
        configuration = GlobalConfiguration.loadConfiguration(confDir, dynamicConfig);
        assertThat(configuration.get(ConfigOptions.BIND_LISTENERS))
                .isEqualTo("FLUSS://example.com:9124");
        assertThat(configuration.get(ConfigOptions.COORDINATOR_HOST)).isEqualTo(null);
    }

    @Test
    void testLoadConfigurationWithAdditionalFileSimple(@TempDir Path tempFolder) throws Exception {
        String confDir = tempFolder.toAbsolutePath().toString();

        // additional file should be read when old config is present
        Path serverYaml = tempFolder.resolve(OLD_COMMON_CONFIG_FILE_NAME);
        Files.write(
                serverYaml,
                Arrays.asList(
                        "remote.data.dir: /tmp/fluss-remote-data",
                        "bind.listeners: FLUSS://localhost:9124"));
        Path additionalYaml = tempFolder.resolve("additional-file.yaml");
        Files.write(
                additionalYaml,
                Arrays.asList(
                        "bind.listeners: FLUSS://example.com:9124",
                        "default.replication.factor: 5"));
        Configuration configuration =
                GlobalConfiguration.loadConfiguration(
                        confDir, Collections.singletonList("additional-file.yaml"), null);

        assertThat(configuration.get(ConfigOptions.REMOTE_DATA_DIR))
                .isEqualTo("/tmp/fluss-remote-data");
        assertThat(configuration.get(ConfigOptions.BIND_LISTENERS))
                .isEqualTo("FLUSS://localhost:9124");
        assertThat(configuration.get(ConfigOptions.DEFAULT_REPLICATION_FACTOR))
                .isEqualTo(ConfigOptions.DEFAULT_REPLICATION_FACTOR.defaultValue());

        Files.delete(serverYaml);

        // additional file should be read when new config is present
        Path commonYaml = tempFolder.resolve(NEW_COMMON_CONFIG_FILE_NAME);
        Files.write(
                commonYaml,
                Arrays.asList(
                        "remote.data.dir: /tmp/fluss-remote-data",
                        "bind.listeners: FLUSS://localhost:9124"));

        configuration =
                GlobalConfiguration.loadConfiguration(
                        confDir, Collections.singletonList("additional-file.yaml"), null);

        assertThat(configuration.get(ConfigOptions.REMOTE_DATA_DIR))
                .isEqualTo("/tmp/fluss-remote-data");
        assertThat(configuration.get(ConfigOptions.BIND_LISTENERS))
                .isEqualTo("FLUSS://example.com:9124");
        assertThat(configuration.get(ConfigOptions.DEFAULT_REPLICATION_FACTOR)).isEqualTo(5);
    }

    @Test
    void testLoadConfigurationWithAdditionalFilesRespectsPrecedence(@TempDir Path tempFolder)
            throws Exception {
        String confDir = tempFolder.toAbsolutePath().toString();

        ConfigOption<Integer> keyWillNotBeOverwritten =
                ConfigBuilder.key("willNotBeOverwritten").intType().noDefaultValue();
        ConfigOption<Integer> keyWillBeOverwrittenByFirstAdditionalFile =
                ConfigBuilder.key("willBeOverwrittenByFirstAdditionalFile")
                        .intType()
                        .noDefaultValue();
        ConfigOption<Integer> keyWillBeOverwrittenBySecondAdditionalFile =
                ConfigBuilder.key("willBeOverwrittenBySecondAdditionalFile")
                        .intType()
                        .noDefaultValue();
        ConfigOption<Integer> keyWillBeOverwrittenByDynamicProperty =
                ConfigBuilder.key("willBeOverwrittenByDynamicProperty").intType().noDefaultValue();

        Path commonYaml = tempFolder.resolve(NEW_COMMON_CONFIG_FILE_NAME);
        Files.write(
                commonYaml,
                Arrays.asList(
                        "willNotBeOverwritten: 0",
                        "willBeOverwrittenByFirstAdditionalFile: 1",
                        "willBeOverwrittenBySecondAdditionalFile: 2"));

        // only common, no additional file
        Configuration configuration =
                GlobalConfiguration.loadConfiguration(confDir, Collections.emptyList(), null);
        assertThat(configuration.get(keyWillNotBeOverwritten)).isEqualTo(0);
        assertThat(configuration.get(keyWillBeOverwrittenByFirstAdditionalFile)).isEqualTo(1);
        assertThat(configuration.get(keyWillBeOverwrittenBySecondAdditionalFile)).isEqualTo(2);

        // first additional file
        Path firstAdditionalYaml = tempFolder.resolve("first-additional.yaml");
        Files.write(
                firstAdditionalYaml,
                Arrays.asList(
                        "willBeOverwrittenByFirstAdditionalFile: 11",
                        "willBeOverwrittenByDynamicProperty: 12"));
        List<String> additionalFiles = new ArrayList<>(2);
        additionalFiles.add("first-additional.yaml");
        configuration = GlobalConfiguration.loadConfiguration(confDir, additionalFiles, null);
        assertThat(configuration.get(keyWillNotBeOverwritten)).isEqualTo(0);
        assertThat(configuration.get(keyWillBeOverwrittenByFirstAdditionalFile)).isEqualTo(11);
        assertThat(configuration.get(keyWillBeOverwrittenBySecondAdditionalFile)).isEqualTo(2);
        assertThat(configuration.get(keyWillBeOverwrittenByDynamicProperty)).isEqualTo(12);

        // second additional file
        Path secondAdditionalYaml = tempFolder.resolve("second-additional.yaml");
        Files.write(
                secondAdditionalYaml,
                Collections.singleton("willBeOverwrittenBySecondAdditionalFile: 20"));
        additionalFiles.add("second-additional.yaml");
        configuration = GlobalConfiguration.loadConfiguration(confDir, additionalFiles, null);
        assertThat(configuration.get(keyWillNotBeOverwritten)).isEqualTo(0);
        assertThat(configuration.get(keyWillBeOverwrittenByFirstAdditionalFile)).isEqualTo(11);
        assertThat(configuration.get(keyWillBeOverwrittenBySecondAdditionalFile)).isEqualTo(20);
        assertThat(configuration.get(keyWillBeOverwrittenByDynamicProperty)).isEqualTo(12);

        // dynamic property
        Configuration dynamicProperties = new Configuration();
        dynamicProperties.set(keyWillBeOverwrittenByDynamicProperty, 32);
        configuration =
                GlobalConfiguration.loadConfiguration(confDir, additionalFiles, dynamicProperties);
        assertThat(configuration.get(keyWillNotBeOverwritten)).isEqualTo(0);
        assertThat(configuration.get(keyWillBeOverwrittenByFirstAdditionalFile)).isEqualTo(11);
        assertThat(configuration.get(keyWillBeOverwrittenBySecondAdditionalFile)).isEqualTo(20);
        assertThat(configuration.get(keyWillBeOverwrittenByDynamicProperty)).isEqualTo(32);
    }
}
