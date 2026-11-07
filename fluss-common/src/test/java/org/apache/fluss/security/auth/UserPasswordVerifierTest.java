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

package org.apache.fluss.security.auth;

import org.apache.fluss.config.Configuration;
import org.apache.fluss.security.auth.sasl.jaas.TestJaasConfig;
import org.apache.fluss.security.auth.sasl.plain.PlainLoginModule;

import org.junit.jupiter.api.Test;

import javax.security.auth.login.AppConfigurationEntry;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests credential verification shared by SASL and impersonation authentication. */
class UserPasswordVerifierTest {

    @Test
    void testUnknownUserAndWrongPasswordAreRejected() {
        UserPasswordVerifier verifier =
                new UserPasswordVerifier(
                        Collections.singletonList(entry("module", "alice", "alice-secret")),
                        "module");
        assertThat(verifier.authenticate("alice", "alice-secret".toCharArray())).isTrue();
        assertThat(verifier.authenticate("alice", "wrong-secret".toCharArray())).isFalse();
        assertThat(verifier.authenticate("bob", "alice-secret".toCharArray())).isFalse();
        assertThat(verifier.authenticate(null, "alice-secret".toCharArray())).isFalse();
    }

    @Test
    void testServerCredentialsFallBackToJvmJaas() {
        javax.security.auth.login.Configuration previous =
                javax.security.auth.login.Configuration.getConfiguration();
        TestJaasConfig jaasConfig = new TestJaasConfig();
        jaasConfig.addEntry(
                "FlussServer",
                PlainLoginModule.class.getName(),
                Collections.singletonMap("user_alice", "alice-secret"));
        javax.security.auth.login.Configuration.setConfiguration(jaasConfig);
        try {
            UserPasswordVerifier verifier =
                    UserPasswordVerifier.forServer(new Configuration(), "CLIENT");
            assertThat(verifier.authenticate("alice", "alice-secret".toCharArray())).isTrue();
        } finally {
            javax.security.auth.login.Configuration.setConfiguration(previous);
        }
    }

    @Test
    void testOnlySelectedModuleProvidesCredentials() {
        UserPasswordVerifier verifier =
                new UserPasswordVerifier(
                        Arrays.asList(
                                entry("other", "alice", "other-secret"),
                                entry("module", "bob", "bob-secret"),
                                entry("module", "alice", "alice-secret")),
                        "module");
        assertThat(verifier.authenticate("alice", "other-secret".toCharArray())).isFalse();
        assertThat(verifier.authenticate("alice", "alice-secret".toCharArray())).isTrue();
        assertThat(verifier.authenticate("bob", "bob-secret".toCharArray())).isTrue();
    }

    private static AppConfigurationEntry entry(String module, String user, String password) {
        Map<String, Object> options = new HashMap<>();
        options.put("user_" + user, password);
        return new AppConfigurationEntry(
                module, AppConfigurationEntry.LoginModuleControlFlag.REQUIRED, options);
    }
}
