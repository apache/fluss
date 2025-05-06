/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.rpc.netty.authenticate;

import com.alibaba.fluss.config.ConfigOption;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.exception.AuthenticationException;
import com.alibaba.fluss.security.acl.FlussPrincipal;
import com.alibaba.fluss.security.auth.ClientAuthenticationPlugin;
import com.alibaba.fluss.security.auth.ClientAuthenticator;
import com.alibaba.fluss.security.auth.ServerAuthenticationPlugin;
import com.alibaba.fluss.security.auth.ServerAuthenticator;

import java.util.concurrent.ThreadLocalRandom;

import static com.alibaba.fluss.config.ConfigBuilder.key;

/**
 * An {@link com.alibaba.fluss.security.auth.AuthenticationPlugin} to mock mutual authentication.
 */
public class MutualAuthenticationPlugin
        implements ServerAuthenticationPlugin, ClientAuthenticationPlugin {

    private static final String MUTUAL_AUTH_PROTOCOL = "mutual";
    private static final ConfigOption<ErrorType> ERROR_TYPE =
            key("client.security.mutual.error-type")
                    .enumType(ErrorType.class)
                    .defaultValue(ErrorType.NONE);

    @Override
    public ClientAuthenticator createClientAuthenticator(Configuration configuration) {
        return new ClientAuthenticatorImpl(configuration);
    }

    @Override
    public ServerAuthenticator createServerAuthenticator(Configuration configuration) {
        return new ServerAuthenticatorImpl();
    }

    @Override
    public String authProtocol() {
        return MUTUAL_AUTH_PROTOCOL;
    }

    private static class ClientAuthenticatorImpl implements ClientAuthenticator {
        private boolean isCompleted = false;
        private int initialSalt;
        private final int errorType;

        public ClientAuthenticatorImpl(Configuration configuration) {
            this.errorType = configuration.get(ERROR_TYPE).code;
        }

        @Override
        public String protocol() {
            return MUTUAL_AUTH_PROTOCOL;
        }

        @Override
        public byte[] authenticate(byte[] data) throws AuthenticationException {
            if (isCompleted) {
                return null;
            }

            // Initial token
            if (data.length == 0) {
                initialSalt = generateInitialSalt();
                return String.valueOf(
                                isError(
                                                errorType,
                                                ErrorType.SERVER_NO_CHALLENGE,
                                                ErrorType.SERVER_ERROR_CHALLENGE)
                                        ? errorType
                                        : initialSalt)
                        .getBytes();
            }

            int challenge = parseToken(data);
            if (challenge == initialSalt + 1) {
                isCompleted = true;
                return String.valueOf(
                                isError(errorType, ErrorType.CLIENT_ERROR_SECOND_TOKEN)
                                        ? errorType
                                        : challenge + 1)
                        .getBytes();
            }

            throw new AuthenticationException(
                    "Invalid challenge value: expected "
                            + (initialSalt + 1)
                            + ", but got "
                            + challenge);
        }

        @Override
        public boolean isCompleted() {
            return isCompleted;
        }
    }

    private static class ServerAuthenticatorImpl implements ServerAuthenticator {
        private boolean isCompleted = false;
        private boolean firstAuthRequest = true;
        private int initialSalt;

        @Override
        public String protocol() {
            return MUTUAL_AUTH_PROTOCOL;
        }

        @Override
        public byte[] evaluateResponse(byte[] token) throws AuthenticationException {
            int tokenValue = parseToken(token);

            // If mock error, return null.
            if (isError(tokenValue, ErrorType.SERVER_NO_CHALLENGE)) {
                return null;
            }
            if (isError(tokenValue, ErrorType.SERVER_ERROR_CHALLENGE)) {
                return "-1".getBytes();
            }

            if (firstAuthRequest) {
                initialSalt = tokenValue + 1;
                firstAuthRequest = false;
                return String.valueOf(initialSalt).getBytes();
            }

            if (tokenValue == initialSalt + 1) {
                isCompleted = true;
                return new byte[0];
            }

            throw new IllegalArgumentException(
                    "Invalid token value: expected "
                            + (initialSalt + 1)
                            + ", but got "
                            + tokenValue);
        }

        @Override
        public FlussPrincipal createPrincipal() {
            return FlussPrincipal.ANONYMOUS;
        }

        @Override
        public boolean isCompleted() {
            return isCompleted;
        }
    }

    private static int parseToken(byte[] token) {
        if (token == null || token.length == 0) {
            throw new IllegalArgumentException("Token cannot be null or empty.");
        }
        return Integer.parseInt(new String(token));
    }

    private static int generateInitialSalt() {
        return ThreadLocalRandom.current().nextInt(0, Integer.MAX_VALUE);
    }

    private static boolean isError(int errorType, ErrorType... errorTypes) {
        for (ErrorType type : errorTypes) {
            if (errorType == type.code) {
                return true;
            }
        }
        return false;
    }

    enum ErrorType {
        NONE(-1),
        SERVER_NO_CHALLENGE(-2),
        SERVER_ERROR_CHALLENGE(-3),
        CLIENT_ERROR_SECOND_TOKEN(-4);

        final int code;

        ErrorType(int code) {
            this.code = code;
        }
    }
}
