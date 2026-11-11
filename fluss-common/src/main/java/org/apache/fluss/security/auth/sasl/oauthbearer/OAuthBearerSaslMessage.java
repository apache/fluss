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

import org.apache.fluss.exception.AuthenticationException;

import java.nio.charset.StandardCharsets;

/**
 * Builds and parses the minimal SASL OAUTHBEARER message supported by Fluss.
 *
 * <p>The message format follows <a
 * href="https://datatracker.ietf.org/doc/html/rfc7628#section-3.1">RFC 7628, Section 3.1</a>. The
 * {@code n,,} GS2 header means that channel binding and the authorization identity are not used,
 * and {@code \u0001} is the {@code kvsep} separator.
 */
final class OAuthBearerSaslMessage {
    private static final String KV_SEPARATOR = "\u0001";
    private static final String PREFIX = "n,," + KV_SEPARATOR;
    private static final String BEARER_ATTRIBUTE = "auth=Bearer ";

    private OAuthBearerSaslMessage() {}

    static byte[] createInitialResponse(String token) {
        return (PREFIX + BEARER_ATTRIBUTE + token + KV_SEPARATOR + KV_SEPARATOR)
                .getBytes(StandardCharsets.UTF_8);
    }

    static String parseToken(byte[] responseBytes) {
        if (responseBytes == null || responseBytes.length == 0) {
            throw new AuthenticationException("SASL OAUTHBEARER response is empty");
        }
        String response = new String(responseBytes, StandardCharsets.UTF_8);
        if (!response.startsWith(PREFIX) || !response.endsWith(KV_SEPARATOR + KV_SEPARATOR)) {
            throw new AuthenticationException("Invalid SASL OAUTHBEARER response format");
        }
        String[] attributes = response.substring(PREFIX.length()).split(KV_SEPARATOR, -1);
        String bearerToken = null;
        for (String attribute : attributes) {
            if (attribute.startsWith(BEARER_ATTRIBUTE)) {
                if (bearerToken != null) {
                    throw new AuthenticationException(
                            "SASL OAUTHBEARER response contains multiple auth attributes");
                }
                bearerToken = attribute.substring(BEARER_ATTRIBUTE.length());
            }
        }
        if (bearerToken == null || bearerToken.isEmpty()) {
            throw new AuthenticationException(
                    "SASL OAUTHBEARER response does not contain a Bearer token");
        }
        return bearerToken;
    }
}
