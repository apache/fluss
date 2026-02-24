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

package org.apache.fluss.security.auth.sasl.authenticator;

import org.apache.fluss.security.auth.sasl.jaas.JaasContext;
import org.apache.fluss.security.auth.sasl.jaas.JaasUtils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import javax.security.auth.login.AppConfigurationEntry;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for {@link SaslClientAuthenticator} JAAS escaping behavior. */
class SaslClientAuthenticatorTest {
    private static final String JAAS_CONF_FORMAT =
            "org.apache.fluss.security.auth.sasl.plain.PlainLoginModule required username=\"%s\" password=\"%s\";";

    @ParameterizedTest
    @ValueSource(strings = {" ", "\"", "#", "+", ",", ";", "<", "=", ">", "\\"})
    void testJaasValueSpecialCharacters(String specialChar) {
        assertJaasRoundTrip("user" + specialChar + "name", "pass" + specialChar + "word");
    }

    @Test
    void testJaasValueSpecialCharactersMultiple() {
        String specialChars = "\"#+,;<=>\\";
        assertJaasRoundTrip("user" + specialChars + "name", "pass" + specialChars + "word");
    }

    private void assertJaasRoundTrip(String username, String password) {
        String jaasConfig =
                String.format(
                        JAAS_CONF_FORMAT,
                        JaasUtils.escapeJaasValue(username),
                        JaasUtils.escapeJaasValue(password));

        AppConfigurationEntry entry =
                JaasContext.loadClientContext(jaasConfig).configurationEntries().get(0);
        Map<String, Object> options = new HashMap<>(entry.getOptions());

        assertThat(options).containsEntry("username", username);
        assertThat(options).containsEntry("password", password);
    }
}
