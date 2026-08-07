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

import org.apache.fluss.security.auth.sasl.plain.PlainServerCallbackHandler;

import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for the JAAS config regeneration logic of {@link FlussProtocolPlugin}. */
class FlussProtocolPluginTest {

    @Test
    void testParseCoversEveryConsumedPrefixAndDropsOthers() {
        // Drive off KNOWN_OPTION_PREFIXES so a newly-registered prefix that the extraction regex
        // does not yet cover makes this test fail.
        StringBuilder jaas =
                new StringBuilder(
                        "org.apache.fluss.security.auth.sasl.plain.PlainLoginModule required");
        for (String prefix : PlainServerCallbackHandler.KNOWN_OPTION_PREFIXES) {
            jaas.append(String.format(" %ssample=\"value-%s\"", prefix, prefix));
        }
        jaas.append(" debug=\"true\";");

        Map<String, String> parsed =
                FlussProtocolPlugin.parseKnownOptionsFromJaasConfig(jaas.toString());

        for (String prefix : PlainServerCallbackHandler.KNOWN_OPTION_PREFIXES) {
            assertThat(parsed).containsEntry(prefix + "sample", "value-" + prefix);
        }
        assertThat(parsed).doesNotContainKey("debug");
    }

    @Test
    void testCredentialsMapMergesUsersWhilePreservingImpersonation() {
        Map<String, String> initialKnownOptions = new LinkedHashMap<>();
        initialKnownOptions.put("user_admin", "old-secret");
        initialKnownOptions.put("impersonate_admin", "alice");

        // credentials map overrides admin's password and adds bob
        Map<String, String> newCredentials = new LinkedHashMap<>();
        newCredentials.put("admin", "new-secret");
        newCredentials.put("bob", "bob-secret");

        String merged =
                FlussProtocolPlugin.generateMergedJaasConfig(initialKnownOptions, newCredentials);

        assertThat(merged)
                .contains("user_admin=\"new-secret\"")
                .contains("user_bob=\"bob-secret\"")
                .contains("impersonate_admin=\"alice\"")
                .doesNotContain("old-secret");
    }
}
