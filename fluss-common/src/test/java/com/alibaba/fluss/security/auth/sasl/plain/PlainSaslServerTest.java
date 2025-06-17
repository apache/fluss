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

package com.alibaba.fluss.security.auth.sasl.plain;

import com.alibaba.fluss.exception.AuthenticationException;
import com.alibaba.fluss.security.auth.sasl.jaas.JaasContext;
import com.alibaba.fluss.security.auth.sasl.jaas.LoginManager;
import com.alibaba.fluss.security.auth.sasl.jaas.SaslServerFactory;
import com.alibaba.fluss.security.auth.sasl.jaas.TestJaasConfig;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.security.sasl.SaslException;
import javax.security.sasl.SaslServer;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link PlainSaslServer}. */
public class PlainSaslServerTest {

    private static final String USER_A = "userA";
    private static final String PASSWORD_A = "passwordA";
    private static final String USER_B = "userB";
    private static final String PASSWORD_B = "passwordB";

    private SaslServer saslServer;
    private LoginManager loginManager;

    @BeforeEach
    public void setUp() throws Exception {
        TestJaasConfig jaasConfig = new TestJaasConfig();
        Map<String, String> options = new HashMap<>();
        options.put("user_" + USER_A, PASSWORD_A);
        options.put("user_" + USER_B, PASSWORD_B);
        jaasConfig.addEntry("jaasContext", PlainLoginModule.class.getName(), options);
        JaasContext jaasContext =
                new JaasContext("jaasContext", JaasContext.Type.SERVER, jaasConfig, null);
        loginManager = LoginManager.acquireLoginManager(jaasContext);
        saslServer =
                SaslServerFactory.createSaslServer(
                        "PLAIN",
                        "127.0.0.1",
                        options,
                        loginManager,
                        jaasContext.configurationEntries());
    }

    @AfterEach
    public void tearDown() {
        loginManager.release();
    }

    @Test
    public void testNoAuthorizationIdSpecified() throws SaslException {
        assertThat(saslServer.evaluateResponse(saslMessage("", USER_A, PASSWORD_A))).isEmpty();
        assertThat(saslServer.evaluateResponse(saslMessage("", USER_B, PASSWORD_B))).isEmpty();
    }

    @Test
    public void testAuthorizationIdEqualsAuthenticationId() throws SaslException {
        assertThat(saslServer.evaluateResponse(saslMessage(USER_A, USER_A, PASSWORD_A))).isEmpty();
        assertThat(saslServer.evaluateResponse(saslMessage(USER_B, USER_B, PASSWORD_B))).isEmpty();
    }

    @Test
    public void testAuthorizationIdNotEqualsAuthenticationId() {
        assertThatThrownBy(
                        () -> saslServer.evaluateResponse(saslMessage(USER_A, USER_B, PASSWORD_B)))
                .isExactlyInstanceOf(AuthenticationException.class)
                .hasMessage(
                        "Authentication failed: Client requested an authorization id that is different from username");
    }

    @Test
    public void testInvalidPassword() {
        assertThatThrownBy(
                        () -> saslServer.evaluateResponse(saslMessage(USER_A, USER_A, PASSWORD_B)))
                .isExactlyInstanceOf(AuthenticationException.class)
                .hasMessage("Authentication failed: Invalid username or password");
    }

    @Test
    public void testEmptyTokens() {
        assertThatThrownBy(() -> saslServer.evaluateResponse(saslMessage("", "", "")))
                .hasMessage("Authentication failed: username not specified");
        assertThatThrownBy(() -> saslServer.evaluateResponse(saslMessage("", "", "p")))
                .hasMessage("Authentication failed: username not specified");
        assertThatThrownBy(() -> saslServer.evaluateResponse(saslMessage("", "u", "")))
                .hasMessage("Authentication failed: password not specified");
        assertThatThrownBy(() -> saslServer.evaluateResponse(saslMessage("a", "", "")))
                .hasMessage("Authentication failed: username not specified");
        assertThatThrownBy(() -> saslServer.evaluateResponse(saslMessage("a", "", "p")))
                .hasMessage("Authentication failed: username not specified");
        assertThatThrownBy(() -> saslServer.evaluateResponse(saslMessage("a", "u", "")))
                .hasMessage("Authentication failed: password not specified");
        String nul = "\u0000";
        assertThatThrownBy(
                        () ->
                                saslServer.evaluateResponse(
                                        String.format("%s%s%s%s%s%s", "a", nul, "u", nul, "p", nul)
                                                .getBytes(StandardCharsets.UTF_8)))
                .hasMessage("Invalid SASL/PLAIN response: expected 3 tokens, got 4");
        assertThatThrownBy(
                        () ->
                                saslServer.evaluateResponse(
                                        String.format("%s%s%s", "", nul, "u")
                                                .getBytes(StandardCharsets.UTF_8)))
                .hasMessage("Invalid SASL/PLAIN response: expected 3 tokens, got 2");
    }

    private byte[] saslMessage(String authorizationId, String userName, String password) {
        String nul = "\u0000";
        String message = String.format("%s%s%s%s%s", authorizationId, nul, userName, nul, password);
        return message.getBytes(StandardCharsets.UTF_8);
    }
}
