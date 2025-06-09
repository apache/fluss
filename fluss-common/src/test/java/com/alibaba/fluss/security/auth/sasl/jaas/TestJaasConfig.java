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

package com.alibaba.fluss.security.auth.sasl.jaas;

import com.alibaba.fluss.security.auth.sasl.plain.PlainLoginModule;

import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A test configuration class for SASL-related tests.
 *
 * <p>This class is used to programmatically set up a {@link Configuration} for testing purposes,
 * instead of relying on the default JVM-loaded JAAS configuration. It allows defining custom jaas
 * modules and their options for both client and server authentication contexts.
 *
 * <p>Supported SASL mechanisms include:
 *
 * <ul>
 *   <li>{@code PLAIN}
 *   <li>{@code DIGEST-MD5}
 * </ul>
 *
 * <p>Example usage:
 *
 * <pre>
 * TestJaasConfig.createConfiguration("PLAIN", Arrays.asList("DIGEST-MD5", "PLAIN"));
 * </pre>
 */
public class TestJaasConfig extends Configuration {
    static final String LOGIN_CONTEXT_CLIENT = "FlussClient";
    static final String LOGIN_CONTEXT_SERVER = "FlussServer";

    static final String USERNAME = "myuser";
    static final String PASSWORD = "mypassword";

    private final Map<String, AppConfigurationEntry[]> entryMap = new HashMap<>();

    @Override
    public AppConfigurationEntry[] getAppConfigurationEntry(String name) {
        return entryMap.get(name);
    }

    // Create a new TestJaasConfig instance and set it as the current JAAS configuration( which can
    // mock jaas operation from JVM option).

    public static void createConfiguration(String clientMechanism, List<String> serverMechanisms) {
        TestJaasConfig config = new TestJaasConfig();
        config.createOrUpdateEntry(
                LOGIN_CONTEXT_CLIENT, loginModule(clientMechanism), defaultClientOptions());
        for (String mechanism : serverMechanisms) {
            config.addEntry(
                    LOGIN_CONTEXT_SERVER, loginModule(mechanism), defaultServerOptions(mechanism));
        }
        Configuration.setConfiguration(config);
    }

    public void createOrUpdateEntry(String name, String loginModule, Map<String, Object> options) {
        AppConfigurationEntry entry =
                new AppConfigurationEntry(
                        loginModule,
                        AppConfigurationEntry.LoginModuleControlFlag.REQUIRED,
                        options);
        entryMap.put(name, new AppConfigurationEntry[] {entry});
    }

    public void addEntry(String name, String loginModule, Map<String, String> options) {
        AppConfigurationEntry entry =
                new AppConfigurationEntry(
                        loginModule,
                        AppConfigurationEntry.LoginModuleControlFlag.REQUIRED,
                        options);
        AppConfigurationEntry[] existing = entryMap.get(name);
        AppConfigurationEntry[] newEntries =
                existing == null
                        ? new AppConfigurationEntry[1]
                        : Arrays.copyOf(existing, existing.length + 1);
        newEntries[newEntries.length - 1] = entry;
        entryMap.put(name, newEntries);
    }

    private static String loginModule(String mechanism) {
        String loginModule;
        switch (mechanism) {
            case "PLAIN":
                loginModule = PlainLoginModule.class.getName();
                break;
            case "DIGEST-MD5":
                loginModule = DigestLoginModule.class.getName();
                break;
            default:
                throw new IllegalArgumentException("Unsupported mechanism " + mechanism);
        }
        return loginModule;
    }

    public static Map<String, Object> defaultClientOptions() {
        Map<String, Object> options = new HashMap<>();
        options.put("username", USERNAME);
        options.put("password", PASSWORD);
        return options;
    }

    public static Map<String, String> defaultServerOptions(String mechanism) {
        Map<String, String> options = new HashMap<>();
        switch (mechanism) {
            case "PLAIN":
            case "DIGEST-MD5":
                options.put("user_" + USERNAME, PASSWORD);
                break;
            default:
                throw new IllegalArgumentException("Unsupported mechanism " + mechanism);
        }
        return options;
    }
}
