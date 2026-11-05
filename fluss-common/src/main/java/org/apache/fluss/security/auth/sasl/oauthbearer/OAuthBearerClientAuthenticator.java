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

package org.apache.fluss.security.auth.sasl.oauthbearer;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.Password;
import org.apache.fluss.exception.AuthenticationException;
import org.apache.fluss.security.auth.ClientAuthenticator;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.fluss.utils.StringUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;

import static org.apache.fluss.config.ConfigOptions.CLIENT_SASL_OAUTHBEARER_CLIENT_ID;
import static org.apache.fluss.config.ConfigOptions.CLIENT_SASL_OAUTHBEARER_CLIENT_SECRET;
import static org.apache.fluss.config.ConfigOptions.CLIENT_SASL_OAUTHBEARER_REQUEST_TIMEOUT;
import static org.apache.fluss.config.ConfigOptions.CLIENT_SASL_OAUTHBEARER_SCOPE;
import static org.apache.fluss.config.ConfigOptions.CLIENT_SASL_OAUTHBEARER_TOKEN_ENDPOINT;

/** A connection-local SASL OAUTHBEARER client authenticator. */
@Internal
public final class OAuthBearerClientAuthenticator implements ClientAuthenticator {
    private static final Logger LOG = LoggerFactory.getLogger(OAuthBearerClientAuthenticator.class);

    /** The SASL mechanism name. */
    public static final String OAUTHBEARER_MECHANISM = "OAUTHBEARER";

    private static final String HTTP_POST_METHOD = "POST";
    private static final String BASIC_AUTHORIZATION_PREFIX = "Basic ";
    /** Required by RFC 6749, Section 4.4.2 for a client credentials token request. */
    private static final String CLIENT_CREDENTIALS_GRANT_PARAMETER =
            "grant_type=client_credentials";

    private static final String SCOPE_PARAMETER_PREFIX = "&scope=";
    private static final String ACCESS_TOKEN_FIELD = "access_token";
    private static final String TOKEN_TYPE_FIELD = "token_type";
    private static final String BEARER_TOKEN_TYPE = "Bearer";

    private final URI endpoint;
    private final String authorization;
    private final byte[] requestBody;
    private final int timeoutMs;

    private String token;
    private boolean completed;

    /** Creates a connection-local authenticator from the client configuration. */
    public OAuthBearerClientAuthenticator(Configuration configuration) {
        endpoint =
                OAuthBearerUtils.validateHttpEndpoint(
                        configuration.get(CLIENT_SASL_OAUTHBEARER_TOKEN_ENDPOINT),
                        CLIENT_SASL_OAUTHBEARER_TOKEN_ENDPOINT.key());
        if ("http".equalsIgnoreCase(endpoint.getScheme())) {
            LOG.warn("OAuth token endpoint uses insecure HTTP: {}", endpoint);
        }

        String clientId = configuration.get(CLIENT_SASL_OAUTHBEARER_CLIENT_ID);
        if (StringUtils.isNullOrWhitespaceOnly(clientId)) {
            throw new AuthenticationException(
                    "Configuration '" + CLIENT_SASL_OAUTHBEARER_CLIENT_ID.key() + "' must be set");
        }
        Password secret = configuration.get(CLIENT_SASL_OAUTHBEARER_CLIENT_SECRET);
        if (secret == null || secret.value().isEmpty()) {
            throw new AuthenticationException(
                    "Configuration '"
                            + CLIENT_SASL_OAUTHBEARER_CLIENT_SECRET.key()
                            + "' must be set");
        }
        authorization =
                BASIC_AUTHORIZATION_PREFIX
                        + Base64.getEncoder()
                                .encodeToString(
                                        (formEncode(clientId) + ":" + formEncode(secret.value()))
                                                .getBytes(StandardCharsets.UTF_8));

        StringBuilder body = new StringBuilder(CLIENT_CREDENTIALS_GRANT_PARAMETER);
        String scope = configuration.get(CLIENT_SASL_OAUTHBEARER_SCOPE);
        if (scope != null && !scope.trim().isEmpty()) {
            body.append(SCOPE_PARAMETER_PREFIX).append(formEncode(scope));
        }
        requestBody = body.toString().getBytes(StandardCharsets.UTF_8);

        Duration timeout = configuration.get(CLIENT_SASL_OAUTHBEARER_REQUEST_TIMEOUT);
        long timeoutMillis = timeout == null ? 0 : timeout.toMillis();
        if (timeoutMillis <= 0 || timeoutMillis > Integer.MAX_VALUE) {
            throw new AuthenticationException(
                    "Configuration '"
                            + CLIENT_SASL_OAUTHBEARER_REQUEST_TIMEOUT.key()
                            + "' must fit in positive milliseconds");
        }
        timeoutMs = (int) timeoutMillis;
    }

    @Override
    public String protocol() {
        return OAUTHBEARER_MECHANISM;
    }

    @Override
    public byte[] authenticate(byte[] data) throws AuthenticationException {
        if (token == null) {
            token = fetchToken();
        }
        completed = true;
        return OAuthBearerSaslMessage.createInitialResponse(token);
    }

    @Override
    public boolean isCompleted() {
        return completed;
    }

    @Override
    public boolean hasInitialTokenResponse() {
        return true;
    }

    @Override
    public void initialize(AuthenticateContext context) {
        completed = false;
    }

    private String fetchToken() {
        JsonNode response =
                OAuthBearerUtils.execute(
                        endpoint, HTTP_POST_METHOD, timeoutMs, authorization, requestBody);
        String token = requiredText(response, ACCESS_TOKEN_FIELD);
        if (!BEARER_TOKEN_TYPE.equalsIgnoreCase(requiredText(response, TOKEN_TYPE_FIELD))) {
            throw new AuthenticationException("OAuth token endpoint returned a non-Bearer token");
        }
        return token;
    }

    private static String requiredText(JsonNode object, String field) {
        JsonNode node = object == null ? null : object.get(field);
        if (node == null || !node.isTextual() || node.textValue().trim().isEmpty()) {
            throw new AuthenticationException(
                    "OAuth token endpoint response must contain a non-empty " + field);
        }
        return node.textValue();
    }

    private static String formEncode(String value) {
        try {
            return URLEncoder.encode(value, StandardCharsets.UTF_8.name());
        } catch (Exception e) {
            throw new AuthenticationException("Failed to encode OAuth request", e);
        }
    }
}
