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

package org.apache.fluss.security.auth.sasl.plain;

import org.apache.fluss.exception.AuthenticationException;

import javax.annotation.Nullable;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.sasl.Sasl;
import javax.security.sasl.SaslException;
import javax.security.sasl.SaslServer;
import javax.security.sasl.SaslServerFactory;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/* This file is based on source code of Apache Kafka Project (https://kafka.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/** A {@link SaslServer} implementation for the PLAIN mechanism. */
public class PlainSaslServer implements SaslServer {

    public static final String PLAIN_MECHANISM = "PLAIN";

    /**
     * SASL property enabling impersonation passthrough. Disabled by default; enabled by
     * authenticators that separately verify whether the authenticated user may act as the requested
     * authorization id.
     */
    public static final String IMPERSONATION_ENABLED_PROP =
            "fluss.sasl.plain.impersonation.enabled";

    /** Negotiated property exposing the client-requested authorization id, or {@code null}. */
    public static final String REQUESTED_AUTHORIZATION_ID_PROP =
            "fluss.sasl.plain.requested.authorization.id";

    private final CallbackHandler callbackHandler;
    private final boolean impersonationEnabled;
    private boolean complete;
    private String authorizationId;
    @Nullable private String requestedAuthorizationId;

    public PlainSaslServer(CallbackHandler callbackHandler) {
        this(callbackHandler, false);
    }

    public PlainSaslServer(CallbackHandler callbackHandler, boolean impersonationEnabled) {
        this.callbackHandler = callbackHandler;
        this.impersonationEnabled = impersonationEnabled;
    }

    @Override
    public byte[] evaluateResponse(byte[] responseBytes) throws AuthenticationException {
        /*
         * Message format (from https://tools.ietf.org/html/rfc4616):
         *
         * message   = [authzid] UTF8NUL authcid UTF8NUL passwd
         * authcid   = 1*SAFE ; MUST accept up to 255 octets
         * authzid   = 1*SAFE ; MUST accept up to 255 octets
         * passwd    = 1*SAFE ; MUST accept up to 255 octets
         * UTF8NUL   = %x00 ; UTF-8 encoded NUL character
         *
         * SAFE      = UTF1 / UTF2 / UTF3 / UTF4
         *                ;; any UTF-8 encoded Unicode character except NUL
         */

        String response = new String(responseBytes, StandardCharsets.UTF_8);
        List<String> tokens = extractTokens(response);
        String authorizationIdFromClient = tokens.get(0);
        String username = tokens.get(1);
        String password = tokens.get(2);

        if (username.isEmpty()) {
            throw new AuthenticationException("Authentication failed: username not specified");
        }
        if (password.isEmpty()) {
            throw new AuthenticationException("Authentication failed: password not specified");
        }

        NameCallback nameCallback = new NameCallback("username", username);
        PlainAuthenticateCallback authenticateCallback =
                new PlainAuthenticateCallback(password.toCharArray());
        try {
            callbackHandler.handle(new Callback[] {nameCallback, authenticateCallback});
        } catch (Throwable e) {
            throw new AuthenticationException(
                    "Authentication failed: credentials for user could not be verified", e);
        }
        if (!authenticateCallback.authenticated()) {
            throw new AuthenticationException(
                    "Authentication failed: Invalid username or password");
        }
        if (!authorizationIdFromClient.isEmpty() && !authorizationIdFromClient.equals(username)) {
            if (!impersonationEnabled) {
                throw new AuthenticationException(
                        "Authentication failed: Client requested an authorization id that is different from username");
            }
            // Record only; authorization is left to the caller that enabled the flag.
            this.requestedAuthorizationId = authorizationIdFromClient;
        }

        // Always the authenticated user, never the requested id.
        this.authorizationId = username;

        complete = true;
        return new byte[0];
    }

    private List<String> extractTokens(String string) {
        List<String> tokens = new ArrayList<>();
        int startIndex = 0;
        for (int i = 0; i < 4; ++i) {
            int endIndex = string.indexOf("\u0000", startIndex);
            if (endIndex == -1) {
                tokens.add(string.substring(startIndex));
                break;
            }
            tokens.add(string.substring(startIndex, endIndex));
            startIndex = endIndex + 1;
        }

        if (tokens.size() != 3) {
            throw new AuthenticationException(
                    "Invalid SASL/PLAIN response: expected 3 tokens, got " + tokens.size());
        }

        return tokens;
    }

    @Override
    public String getAuthorizationID() {
        if (!complete) {
            throw new IllegalStateException("Authentication exchange has not completed");
        }
        return authorizationId;
    }

    @Override
    public String getMechanismName() {
        return PLAIN_MECHANISM;
    }

    @Override
    public Object getNegotiatedProperty(String propName) {
        if (!complete) {
            throw new IllegalStateException("Authentication exchange has not completed");
        }
        if (REQUESTED_AUTHORIZATION_ID_PROP.equals(propName)) {
            return requestedAuthorizationId;
        }
        return null;
    }

    @Override
    public boolean isComplete() {
        return complete;
    }

    @Override
    public byte[] unwrap(byte[] incoming, int offset, int len) {
        if (!complete) {
            throw new IllegalStateException("Authentication exchange has not completed");
        }
        return Arrays.copyOfRange(incoming, offset, offset + len);
    }

    @Override
    public byte[] wrap(byte[] outgoing, int offset, int len) {
        if (!complete) {
            throw new IllegalStateException("Authentication exchange has not completed");
        }
        return Arrays.copyOfRange(outgoing, offset, offset + len);
    }

    @Override
    public void dispose() {}

    /** A {@link SaslServerFactory} implementation for the PLAIN mechanism. */
    public static class PlainSaslServerFactory implements SaslServerFactory {

        @Override
        public SaslServer createSaslServer(
                String mechanism,
                String protocol,
                String serverName,
                Map<String, ?> props,
                CallbackHandler cbh)
                throws SaslException {

            if (!PLAIN_MECHANISM.equals(mechanism)) {
                throw new SaslException(
                        String.format(
                                "Mechanism '%s' is not supported. Only PLAIN is supported.",
                                mechanism));
            }

            return new PlainSaslServer(cbh, isImpersonationEnabled(props));
        }

        @Override
        public String[] getMechanismNames(Map<String, ?> props) {
            if (props == null) {
                return new String[] {PLAIN_MECHANISM};
            }
            String noPlainText = (String) props.get(Sasl.POLICY_NOPLAINTEXT);
            if ("true".equals(noPlainText)) {
                return new String[] {};
            } else {
                return new String[] {PLAIN_MECHANISM};
            }
        }

        private static boolean isImpersonationEnabled(Map<String, ?> props) {
            if (props == null) {
                return false;
            }
            Object enabled = props.get(IMPERSONATION_ENABLED_PROP);
            return enabled != null && Boolean.parseBoolean(String.valueOf(enabled));
        }
    }
}
