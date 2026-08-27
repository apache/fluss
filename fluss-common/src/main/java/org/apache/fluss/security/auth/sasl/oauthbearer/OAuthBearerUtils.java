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
import org.apache.fluss.exception.RetriableAuthenticationException;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;

import static org.apache.fluss.utils.StringUtils.isNullOrWhitespaceOnly;

/** Utilities shared by OAuth bearer client and server authentication. */
final class OAuthBearerUtils {
    static final int MAX_HTTP_RESPONSE_SIZE = 1024 * 1024;
    static final int MAX_JWT_SIZE = 64 * 1024;
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private OAuthBearerUtils() {}

    static URI validateHttpEndpoint(String value, String key) {
        if (isNullOrWhitespaceOnly(value)) {
            throw new AuthenticationException("Configuration '" + key + "' must be set");
        }
        try {
            URI uri = new URI(value);
            if (!uri.isAbsolute()
                    || (!("http".equalsIgnoreCase(uri.getScheme()))
                            && !("https".equalsIgnoreCase(uri.getScheme())))) {
                throw new AuthenticationException(
                        "Configuration '" + key + "' must be an absolute HTTP(S) URI");
            }
            return uri;
        } catch (URISyntaxException e) {
            throw new AuthenticationException("Configuration '" + key + "' is not a valid URI", e);
        }
    }

    static JsonNode execute(
            URI endpoint, String method, int timeoutMs, String authorization, byte[] requestBody)
            throws AuthenticationException {
        HttpURLConnection connection = null;
        try {
            URL url = endpoint.toURL();
            connection = (HttpURLConnection) url.openConnection();
            connection.setInstanceFollowRedirects(false);
            connection.setConnectTimeout(timeoutMs);
            connection.setReadTimeout(timeoutMs);
            connection.setRequestMethod(method);
            connection.setRequestProperty("Accept", "application/json");
            if (authorization != null) {
                connection.setRequestProperty("Authorization", authorization);
            }
            if (requestBody != null) {
                connection.setDoOutput(true);
                connection.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");
                connection.setFixedLengthStreamingMode(requestBody.length);
                try (OutputStream output = connection.getOutputStream()) {
                    output.write(requestBody);
                }
            }

            int status = connection.getResponseCode();
            InputStream input =
                    status >= 200 && status < 300
                            ? connection.getInputStream()
                            : connection.getErrorStream();
            byte[] response = readLimited(input);
            if (status < 200 || status >= 300) {
                String message = "OAuth endpoint returned HTTP status " + status;
                if (status == 429 || status >= 500) {
                    throw new RetriableAuthenticationException(message);
                }
                throw new AuthenticationException(message);
            }
            try {
                return OBJECT_MAPPER.readTree(response);
            } catch (IOException e) {
                throw new AuthenticationException("OAuth endpoint returned invalid JSON", e);
            }
        } catch (AuthenticationException e) {
            throw e;
        } catch (IOException e) {
            RetriableAuthenticationException exception =
                    new RetriableAuthenticationException("Failed to access OAuth endpoint");
            exception.initCause(e);
            throw exception;
        } finally {
            if (connection != null) {
                connection.disconnect();
            }
        }
    }

    private static byte[] readLimited(InputStream input) throws IOException {
        if (input == null) {
            return new byte[0];
        }
        try (InputStream in = input;
                ByteArrayOutputStream output = new ByteArrayOutputStream()) {
            byte[] buffer = new byte[8192];
            int total = 0;
            int read;
            while ((read = in.read(buffer)) >= 0) {
                total += read;
                if (total > MAX_HTTP_RESPONSE_SIZE) {
                    throw new IOException("OAuth endpoint response exceeds size limit");
                }
                output.write(buffer, 0, read);
            }
            return output.toByteArray();
        }
    }
}
