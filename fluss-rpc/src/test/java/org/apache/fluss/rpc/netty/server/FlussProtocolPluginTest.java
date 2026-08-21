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

package org.apache.fluss.rpc.netty.server;

import org.apache.fluss.exception.ConfigException;
import org.apache.fluss.security.auth.sasl.jaas.JaasConfig;
import org.apache.fluss.security.auth.sasl.plain.PlainLoginModule;

import org.junit.jupiter.api.Test;

import javax.security.auth.login.AppConfigurationEntry;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Unit tests for the JAAS config regeneration logic of {@link FlussProtocolPlugin}. */
class FlussProtocolPluginTest {

    @Test
    void testCredentialsOverlayUsersWithoutDroppingOtherOptions() {
        String initialJaasConfig =
                PlainLoginModule.class.getName()
                        + " required"
                        + " user_admin=\"old-secret\""
                        + " impersonate_admin=\"alice\""
                        + " debug=true"
                        + " \"custom option\"=\"spaces = comma, semicolon; quote\\\""
                        + " backslash\\\\ controls\\b\\t\\n\\f\\r\\001\\177\";";

        Map<String, String> newCredentials = new LinkedHashMap<>();
        newCredentials.put("admin", "new-secret");
        newCredentials.put("bob", "bob-secret");

        AppConfigurationEntry mergedEntry =
                parseJaasConfig(
                        FlussProtocolPlugin.materializePlainJaasConfig(
                                initialJaasConfig, newCredentials));

        assertThat(mergedEntry.getLoginModuleName()).isEqualTo(PlainLoginModule.class.getName());
        assertThat(mergedEntry.getControlFlag())
                .isEqualTo(AppConfigurationEntry.LoginModuleControlFlag.REQUIRED);
        assertThat(new LinkedHashMap<String, Object>(mergedEntry.getOptions()))
                .containsEntry("user_admin", "new-secret")
                .containsEntry("user_bob", "bob-secret")
                .containsEntry("impersonate_admin", "alice")
                .containsEntry("debug", "true")
                .containsEntry(
                        "custom option",
                        "spaces = comma, semicolon; quote\" backslash\\ controls\b\t\n\f\r\001\177")
                .doesNotContainValue("old-secret");
    }

    @Test
    void testNoCredentialsRestoresStartupBaseline() {
        String startupJaasConfig = PlainLoginModule.class.getName() + " optional debug=true;";

        assertThat(FlussProtocolPlugin.materializePlainJaasConfig(startupJaasConfig, null))
                .isEqualTo(startupJaasConfig);
        assertThat(FlussProtocolPlugin.materializePlainJaasConfig(null, null))
                .isEqualTo(PlainLoginModule.class.getName() + " required;");
        assertThat(FlussProtocolPlugin.materializePlainJaasConfig("", null))
                .isEqualTo(PlainLoginModule.class.getName() + " required;");
    }

    @Test
    void testRejectsMultipleJaasEntries() {
        String entry = PlainLoginModule.class.getName() + " required user_admin=\"secret\";";

        assertThatThrownBy(
                        () ->
                                FlussProtocolPlugin.materializePlainJaasConfig(
                                        entry + entry, Collections.emptyMap()))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("contains 2 JAAS modules, should be 1 module");
    }

    private static AppConfigurationEntry parseJaasConfig(String jaasConfig) {
        String contextName = "TestContext";
        AppConfigurationEntry[] entries =
                new JaasConfig(contextName, jaasConfig).getAppConfigurationEntry(contextName);
        assertThat(entries).hasSize(1);
        return entries[0];
    }
}
