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

package org.apache.fluss.config.provider;

import org.apache.fluss.config.ConfigBuilder;
import org.apache.fluss.config.ConfigOption;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.GlobalConfiguration;
import org.apache.fluss.exception.IllegalConfigurationException;
import org.apache.fluss.utils.InstantiationUtils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/** Tests for {@link ConfigProviders} and the built-in {@link ConfigProvider}s. */
class ConfigProvidersTest {

    @Test
    void testProviderDefaultMethods() {
        ConfigProvider provider =
                new ConfigProvider() {
                    @Override
                    public String identifier() {
                        return "noop";
                    }

                    @Override
                    public String resolve(String path, String key) {
                        return path + key;
                    }
                };
        provider.configure(Collections.emptyMap());
        assertThat(provider.resolve("a", "b")).isEqualTo("ab");
        provider.close();
    }

    @Test
    void testNoProvidersDeclaredLeavesMarkersUntouched() {
        Configuration config = new Configuration();
        config.setString("datalake.paimon.oauth.token", "${env:WHATEVER}");
        ConfigProviders.resolve(config);
        assertThat(config.toMap().get("datalake.paimon.oauth.token")).isEqualTo("${env:WHATEVER}");
    }

    @Test
    void testDirectoryProviderReadsFileVerbatim(@TempDir Path secretsDir) throws Exception {
        // K8s Secret volumes expose exact bytes with no trailing newline.
        Files.write(secretsDir.resolve("oauth-token"), "s3cr3t".getBytes(StandardCharsets.UTF_8));

        Configuration config = new Configuration();
        config.setString(ConfigOptions.CONFIG_PROVIDERS.key(), "directory");
        config.setString("config.providers.directory.param.allowed.paths", secretsDir.toString());
        config.setString(
                "datalake.paimon.oauth.token", "${directory:" + secretsDir + ":oauth-token}");
        ConfigProviders.resolve(config);

        assertThat(config.toMap().get("datalake.paimon.oauth.token")).isEqualTo("s3cr3t");
    }

    @Test
    void testDirectoryProviderRequiresAllowedPaths() {
        Configuration config = new Configuration();
        config.setString(ConfigOptions.CONFIG_PROVIDERS.key(), "directory");
        assertThatThrownBy(() -> ConfigProviders.resolve(config))
                .isInstanceOf(IllegalConfigurationException.class)
                .hasMessageContaining("requires the 'allowed.paths' parameter");
    }

    @Test
    void testUnknownProviderParamFailsFast(@TempDir Path secretsDir) {
        Configuration config = new Configuration();
        config.setString(ConfigOptions.CONFIG_PROVIDERS.key(), "directory");
        config.setString("config.providers.directory.param.allowd.paths", secretsDir.toString());
        assertThatThrownBy(() -> ConfigProviders.resolve(config))
                .isInstanceOf(IllegalConfigurationException.class)
                .hasMessageContaining("Unknown parameter 'allowd.paths'");
    }

    @Test
    void testDirectoryProviderAllowedPathsRejectsOutsideRoot(@TempDir Path base) throws Exception {
        Path allowed = base.resolve("allowed");
        Path other = base.resolve("other");
        Files.createDirectories(allowed);
        Files.createDirectories(other);
        Files.write(other.resolve("token"), "leak".getBytes(StandardCharsets.UTF_8));

        Configuration config = new Configuration();
        config.setString(ConfigOptions.CONFIG_PROVIDERS.key(), "directory");
        config.setString("config.providers.directory.param.allowed.paths", allowed.toString());
        config.setString("secret", "${directory:" + other + ":token}");

        assertThatThrownBy(() -> ConfigProviders.resolve(config))
                .isInstanceOf(IllegalConfigurationException.class)
                .hasMessageContaining("allowed.paths");
    }

    @Test
    void testFileProviderReadsProperty(@TempDir Path dir) throws Exception {
        Path props = dir.resolve("creds.properties");
        Files.write(props, Arrays.asList("db.user=alice", "db.password=hunter2"));

        Configuration config = new Configuration();
        config.setString(ConfigOptions.CONFIG_PROVIDERS.key(), "file");
        config.setString("config.providers.file.param.allowed.paths", dir.toString());
        config.setString("kv.password", "${file:" + props + ":db.password}");
        ConfigProviders.resolve(config);

        assertThat(config.toMap().get("kv.password")).isEqualTo("hunter2");
    }

    @Test
    void testEnvProviderResolvesAndRejectsMissing() {
        assumeTrue(System.getenv("PATH") != null, "requires PATH to be set");

        Configuration config = new Configuration();
        config.setString(ConfigOptions.CONFIG_PROVIDERS.key(), "env");
        config.setString("some.path", "${env:PATH}");
        ConfigProviders.resolve(config);
        assertThat(config.toMap().get("some.path")).isEqualTo(System.getenv("PATH"));

        Configuration missing = new Configuration();
        missing.setString(ConfigOptions.CONFIG_PROVIDERS.key(), "env");
        missing.setString("x", "${env:FLUSS_DEFINITELY_UNSET_VAR_XYZ}");
        assertThatThrownBy(() -> ConfigProviders.resolve(missing))
                .isInstanceOf(IllegalConfigurationException.class)
                .hasMessageContaining("FLUSS_DEFINITELY_UNSET_VAR_XYZ");
    }

    @Test
    void testDoubleDollarEscapesLiteralMarker() {
        Configuration config = new Configuration();
        config.setString(ConfigOptions.CONFIG_PROVIDERS.key(), "env");
        config.setString("literal", "$${env:PATH}");
        config.setString("dollars", "$$100");
        ConfigProviders.resolve(config);
        assertThat(config.toMap().get("literal")).isEqualTo("${env:PATH}");
        assertThat(config.toMap().get("dollars")).isEqualTo("$$100");
    }

    @Test
    void testResolvedValuesAreHiddenInToString(@TempDir Path secretsDir) throws Exception {
        Files.write(secretsDir.resolve("cred"), "raw-secret".getBytes(StandardCharsets.UTF_8));

        Configuration config = new Configuration();
        config.setString(ConfigOptions.CONFIG_PROVIDERS.key(), "directory");
        config.setString("config.providers.directory.param.allowed.paths", secretsDir.toString());
        // key name intentionally does not match the sensitive-key heuristics
        config.setString("some.resolved.value", "${directory:" + secretsDir + ":cred}");
        ConfigProviders.resolve(config);

        assertThat(config.toMap().get("some.resolved.value")).isEqualTo("raw-secret");
        assertThat(config.toString()).doesNotContain("raw-secret").contains("******");
        assertThat(new Configuration(config).toString()).doesNotContain("raw-secret");
        Configuration merged = new Configuration();
        merged.addAll(config);
        assertThat(merged.toString()).doesNotContain("raw-secret");
        // hiding survives java serialization
        Configuration deserialized = InstantiationUtils.clone(config);
        assertThat(deserialized.toMap().get("some.resolved.value")).isEqualTo("raw-secret");
        assertThat(deserialized.toString()).doesNotContain("raw-secret");
    }

    @Test
    void testResolvedValuesAreHiddenInParseErrors(@TempDir Path secretsDir) throws Exception {
        Files.write(secretsDir.resolve("cred"), "raw-secret".getBytes(StandardCharsets.UTF_8));

        Configuration config = new Configuration();
        config.setString(ConfigOptions.CONFIG_PROVIDERS.key(), "directory");
        config.setString("config.providers.directory.param.allowed.paths", secretsDir.toString());
        config.setString("some.resolved.value", "${directory:" + secretsDir + ":cred}");
        ConfigProviders.resolve(config);

        ConfigOption<Integer> intOption =
                ConfigBuilder.key("some.resolved.value").intType().noDefaultValue();
        assertThatThrownBy(() -> config.get(intOption))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageNotContaining("raw-secret");
    }

    @Test
    void testUndeclaredProviderFailsFast() {
        Configuration config = new Configuration();
        config.setString(ConfigOptions.CONFIG_PROVIDERS.key(), "env");
        config.setString("x", "${directory:/etc/secrets:token}");
        assertThatThrownBy(() -> ConfigProviders.resolve(config))
                .isInstanceOf(IllegalConfigurationException.class)
                .hasMessageContaining("not declared");
    }

    @Test
    void testUnknownDeclaredProviderFailsFast() {
        Configuration config = new Configuration();
        config.setString(ConfigOptions.CONFIG_PROVIDERS.key(), "vault");
        assertThatThrownBy(() -> ConfigProviders.resolve(config))
                .isInstanceOf(IllegalConfigurationException.class)
                .hasMessageContaining("vault");
    }

    @Test
    void testMalformedMarkerFailsFast() {
        Configuration config = new Configuration();
        config.setString(ConfigOptions.CONFIG_PROVIDERS.key(), "env");
        config.setString("x", "${nocolon}");
        assertThatThrownBy(() -> ConfigProviders.resolve(config))
                .isInstanceOf(IllegalConfigurationException.class)
                .hasMessageContaining("Malformed");
    }

    @Test
    void testInlineMarkerIsNotResolved() {
        Configuration config = new Configuration();
        config.setString(ConfigOptions.CONFIG_PROVIDERS.key(), "env");
        config.setString("url", "https://host/${env:PATH}/x");
        ConfigProviders.resolve(config);
        assertThat(config.toMap().get("url")).isEqualTo("https://host/${env:PATH}/x");
    }

    @Test
    void testLoadConfigurationResolvesMarkers(@TempDir Path confDir, @TempDir Path secretsDir)
            throws Exception {
        Files.write(secretsDir.resolve("oauth-token"), "tok-42".getBytes(StandardCharsets.UTF_8));
        Files.write(
                confDir.resolve(GlobalConfiguration.FLUSS_CONF_FILENAME),
                Arrays.asList(
                        "config.providers: directory",
                        "config.providers.directory.param.allowed.paths: " + secretsDir,
                        "datalake.paimon.oauth.token: ${directory:" + secretsDir + ":oauth-token}",
                        "bind.listeners: FLUSS://localhost:9123"),
                StandardCharsets.UTF_8);

        Configuration config = GlobalConfiguration.loadConfiguration(confDir.toString(), null);

        assertThat(config.toMap().get("datalake.paimon.oauth.token")).isEqualTo("tok-42");
        assertThat(config.toMap().get("bind.listeners")).isEqualTo("FLUSS://localhost:9123");
    }
}
