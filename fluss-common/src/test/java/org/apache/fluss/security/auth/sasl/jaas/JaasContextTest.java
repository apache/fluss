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

package org.apache.fluss.security.auth.sasl.jaas;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link JaasContext}. */
public class JaasContextTest {

    private File jaasConfigFile;

    @BeforeEach
    void before(@TempDir File tempDir) throws IOException {
        jaasConfigFile = new File(tempDir, "jaas.conf");
        if (!jaasConfigFile.exists()) {
            Files.createFile(jaasConfigFile.toPath());
        }
        System.setProperty(JaasContext.JAVA_LOGIN_CONFIG_PARAM, jaasConfigFile.toString());
        Configuration.setConfiguration(null);
    }

    @Test
    void testLoadClientContextFromSystemProperties() throws IOException {
        writeConfiguration(
                Collections.singletonList(
                        "FlussClient { org.apache.fluss.security.auth.sasl.jaas.DigestLoginModule required option1=value1; };"));
        JaasContext context = JaasContext.loadClientContext(null);
        checkEntry(
                context.configurationEntries().get(0),
                "org.apache.fluss.security.auth.sasl.jaas.DigestLoginModule",
                AppConfigurationEntry.LoginModuleControlFlag.REQUIRED,
                Collections.singletonMap("option1", "value1"));
    }

    @Test
    void testLoadClientContextFromDynamicJaasConfig() {
        String dynamicJaasConfig =
                "org.apache.fluss.security.auth.sasl.jaas.DigestLoginModule optional option2=value2; ";
        JaasContext context = JaasContext.loadClientContext(dynamicJaasConfig);
        checkEntry(
                context.configurationEntries().get(0),
                "org.apache.fluss.security.auth.sasl.jaas.DigestLoginModule",
                AppConfigurationEntry.LoginModuleControlFlag.OPTIONAL,
                Collections.singletonMap("option2", "value2"));
    }

    // -------- test server context ----------
    @Test
    void testControlFlag() throws Exception {
        AppConfigurationEntry.LoginModuleControlFlag[] controlFlags =
                new AppConfigurationEntry.LoginModuleControlFlag[] {
                    AppConfigurationEntry.LoginModuleControlFlag.REQUIRED,
                    AppConfigurationEntry.LoginModuleControlFlag.REQUISITE,
                    AppConfigurationEntry.LoginModuleControlFlag.SUFFICIENT,
                    AppConfigurationEntry.LoginModuleControlFlag.OPTIONAL
                };
        Map<String, Object> options = new HashMap<>();
        options.put("propName", "propValue");
        for (AppConfigurationEntry.LoginModuleControlFlag controlFlag : controlFlags) {
            checkConfiguration("test.testControlFlag", controlFlag, options);
        }
    }

    @Test
    void testSingleOption() throws Exception {
        Map<String, Object> options = new HashMap<>();
        options.put("propName", "propValue");
        checkConfiguration(
                "test.testSingleOption",
                AppConfigurationEntry.LoginModuleControlFlag.REQUISITE,
                options);
    }

    @Test
    void testMultipleOptions() throws Exception {
        Map<String, Object> options = new HashMap<>();
        for (int i = 0; i < 10; i++) {
            options.put("propName" + i, "propValue" + i);
        }
        checkConfiguration(
                "test.testMultipleOptions",
                AppConfigurationEntry.LoginModuleControlFlag.SUFFICIENT,
                options);
    }

    @Test
    void testQuotedOptionValue() throws Exception {
        Map<String, Object> options = new HashMap<>();
        options.put("propName", "prop value");
        options.put("propName2", "value1 = 1, value2 = 2");
        String config =
                String.format(
                        "test.testQuotedOptionValue required propName=\"%s\" propName2=\"%s\";",
                        options.get("propName"), options.get("propName2"));
        checkConfiguration(
                config,
                "test.testQuotedOptionValue",
                AppConfigurationEntry.LoginModuleControlFlag.REQUIRED,
                options);
    }

    @Test
    void testQuotedOptionName() throws Exception {
        Map<String, Object> options = new HashMap<>();
        options.put("prop name", "propValue");
        String config = "test.testQuotedOptionName required \"prop name\"=propValue;";
        checkConfiguration(
                config,
                "test.testQuotedOptionName",
                AppConfigurationEntry.LoginModuleControlFlag.REQUIRED,
                options);
    }

    @Test
    void testMultipleLoginModules() throws Exception {
        StringBuilder builder = new StringBuilder();
        int moduleCount = 3;
        Map<Integer, Map<String, Object>> moduleOptions = new HashMap<>();
        for (int i = 0; i < moduleCount; i++) {
            Map<String, Object> options = new HashMap<>();
            options.put("index", "Index" + i);
            options.put("module", "Module" + i);
            moduleOptions.put(i, options);
            String module =
                    jaasConfigProp(
                            "test.Module" + i,
                            AppConfigurationEntry.LoginModuleControlFlag.REQUIRED,
                            options);
            builder.append(' ');
            builder.append(module);
        }
        String jaasConfigProp = builder.toString();

        String clientContextName = "CLIENT";
        Configuration configuration = new JaasConfig(clientContextName, jaasConfigProp);
        AppConfigurationEntry[] dynamicEntries =
                configuration.getAppConfigurationEntry(clientContextName);
        assertThat(dynamicEntries.length).isEqualTo(moduleCount);

        for (int i = 0; i < moduleCount; i++) {
            AppConfigurationEntry entry = dynamicEntries[i];
            checkEntry(
                    entry,
                    "test.Module" + i,
                    AppConfigurationEntry.LoginModuleControlFlag.REQUIRED,
                    moduleOptions.get(i));
        }

        String serverContextName = "SERVER";
        writeConfiguration(serverContextName, jaasConfigProp);
        AppConfigurationEntry[] staticEntries =
                Configuration.getConfiguration().getAppConfigurationEntry(serverContextName);
        for (int i = 0; i < moduleCount; i++) {
            AppConfigurationEntry staticEntry = staticEntries[i];
            checkEntry(
                    staticEntry,
                    dynamicEntries[i].getLoginModuleName(),
                    AppConfigurationEntry.LoginModuleControlFlag.REQUIRED,
                    dynamicEntries[i].getOptions());
        }
    }

    @Test
    void testMissingLoginModule() throws Exception {
        checkInvalidConfiguration("  required option1=value1;");
    }

    @Test
    void testMissingControlFlag() throws Exception {
        checkInvalidConfiguration("test.loginModule option1=value1;");
    }

    @Test
    void testMissingOptionValue() throws Exception {
        checkInvalidConfiguration("loginModule required option1;");
    }

    @Test
    void testMissingSemicolon() throws Exception {
        checkInvalidConfiguration("test.testMissingSemicolon required option1=value1");
    }

    @Test
    void testNumericOptionWithoutQuotes() throws Exception {
        checkInvalidConfiguration("test.testNumericOptionWithoutQuotes required option1=3;");
    }

    @Test
    void testInvalidControlFlag() throws Exception {
        checkInvalidConfiguration("test.testInvalidControlFlag { option1=3;");
    }

    private void checkConfiguration(
            String loginModule,
            AppConfigurationEntry.LoginModuleControlFlag controlFlag,
            Map<String, Object> options)
            throws Exception {
        String jaasConfigProp = jaasConfigProp(loginModule, controlFlag, options);
        checkConfiguration(jaasConfigProp, loginModule, controlFlag, options);
    }

    private void checkConfiguration(
            String jaasConfigProp,
            String loginModule,
            AppConfigurationEntry.LoginModuleControlFlag controlFlag,
            Map<String, Object> options)
            throws Exception {
        AppConfigurationEntry dynamicEntry =
                configurationEntry(JaasContext.Type.CLIENT, jaasConfigProp);
        checkEntry(dynamicEntry, loginModule, controlFlag, options);
        assertThat(
                        Configuration.getConfiguration()
                                .getAppConfigurationEntry(JaasContext.Type.CLIENT.name()))
                .isNull();

        writeConfiguration(JaasContext.Type.SERVER.name(), jaasConfigProp);
        AppConfigurationEntry staticEntry = configurationEntry(JaasContext.Type.SERVER, null);
        checkEntry(staticEntry, loginModule, controlFlag, options);
    }

    private AppConfigurationEntry configurationEntry(
            JaasContext.Type contextType, String saslJaasConfig) {
        JaasContext context =
                JaasContext.load(contextType, null, contextType.name(), saslJaasConfig);
        List<AppConfigurationEntry> entries = context.configurationEntries();
        assertThat(entries).hasSize(1);
        return entries.get(0);
    }

    private String controlFlag(
            AppConfigurationEntry.LoginModuleControlFlag loginModuleControlFlag) {
        // LoginModuleControlFlag.toString() has format "LoginModuleControlFlag: flag"
        String[] tokens = loginModuleControlFlag.toString().split(" ");
        return tokens[tokens.length - 1];
    }

    private String jaasConfigProp(
            String loginModule,
            AppConfigurationEntry.LoginModuleControlFlag controlFlag,
            Map<String, Object> options) {
        StringBuilder builder = new StringBuilder();
        builder.append(loginModule);
        builder.append(' ');
        builder.append(controlFlag(controlFlag));
        for (Map.Entry<String, Object> entry : options.entrySet()) {
            builder.append(' ');
            builder.append(entry.getKey());
            builder.append('=');
            builder.append(entry.getValue());
        }
        builder.append(';');
        return builder.toString();
    }

    private void writeConfiguration(String contextName, String jaasConfigProp) throws IOException {
        List<String> lines = Arrays.asList(contextName + " { ", jaasConfigProp, "};");
        writeConfiguration(lines);
    }

    private void writeConfiguration(List<String> lines) throws IOException {
        Files.write(jaasConfigFile.toPath(), lines, StandardCharsets.UTF_8);
        Configuration.setConfiguration(null);
    }

    private void checkEntry(
            AppConfigurationEntry entry,
            String loginModule,
            AppConfigurationEntry.LoginModuleControlFlag controlFlag,
            Map<String, ?> options) {
        assertThat(entry.getLoginModuleName()).isEqualTo(loginModule);
        assertThat(entry.getControlFlag()).isEqualTo(controlFlag);
        assertThat(entry.getOptions()).isEqualTo(options);
    }

    private void checkInvalidConfiguration(String jaasConfigProp) throws IOException {
        writeConfiguration(JaasContext.Type.SERVER.name(), jaasConfigProp);
        assertThatThrownBy(() -> configurationEntry(JaasContext.Type.SERVER, null))
                .isExactlyInstanceOf(SecurityException.class);
        assertThatThrownBy(() -> configurationEntry(JaasContext.Type.CLIENT, jaasConfigProp))
                .isExactlyInstanceOf(IllegalArgumentException.class);
    }
}
