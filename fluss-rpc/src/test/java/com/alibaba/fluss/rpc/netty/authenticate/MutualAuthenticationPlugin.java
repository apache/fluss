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

import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.exception.AuthenticationException;
import com.alibaba.fluss.exception.RetriableAuthenticationException;
import com.alibaba.fluss.security.acl.FlussPrincipal;
import com.alibaba.fluss.security.auth.ClientAuthenticationPlugin;
import com.alibaba.fluss.security.auth.ClientAuthenticator;
import com.alibaba.fluss.security.auth.ServerAuthenticationPlugin;
import com.alibaba.fluss.security.auth.ServerAuthenticator;

import java.io.IOException;
import java.util.concurrent.ThreadLocalRandom;

/**
 * An {@link com.alibaba.fluss.security.auth.AuthenticationPlugin} to mock mutual authentication.
 */
public class MutualAuthenticationPlugin
        implements ServerAuthenticationPlugin, ClientAuthenticationPlugin {

    private static final String MUTUAL_AUTH_PROTOCOL = "mutual";

    public static ErrorType errorType = ErrorType.NONE;

    @Override
    public ClientAuthenticator createClientAuthenticator(Configuration configuration) {
        return new ClientAuthenticatorImpl();
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
        private enum Status {
            SEND_CLIENT_FIRST_MESSAGE,
            RECEIVE_SERVER_FIRST_MESSAGE,
            RECEIVE_SERVER_FINAL_MESSAGE,
            COMPLETED
        }

        private Status status;
        Integer initialSalt;

        @Override
        public String protocol() {
            return MUTUAL_AUTH_PROTOCOL;
        }

        @Override
        public void initialize(AuthenticateContext context) {
            this.status = Status.SEND_CLIENT_FIRST_MESSAGE;
            this.initialSalt = null;
        }

        @Override
        public byte[] authenticate(byte[] data) throws AuthenticationException {
            switch (status) {
                case SEND_CLIENT_FIRST_MESSAGE:
                    initialSalt = generateInitialSalt();
                    status = Status.RECEIVE_SERVER_FIRST_MESSAGE;
                    return String.valueOf(initialSalt).getBytes();
                case RECEIVE_SERVER_FIRST_MESSAGE:
                    int challenge = parseToken(data);
                    if (challenge == initialSalt + 1) {
                        status = Status.RECEIVE_SERVER_FINAL_MESSAGE;
                        return String.valueOf(
                                        errorType == ErrorType.CLIENT_ERROR_SECOND_TOKE
                                                ? -1
                                                : challenge + 1)
                                .getBytes();
                    } else {
                        throw new AuthenticationException(
                                "Invalid challenge value: expected "
                                        + (initialSalt + 1)
                                        + ", but got "
                                        + challenge);
                    }
                case RECEIVE_SERVER_FINAL_MESSAGE:
                    if (data.length == 0) {
                        status = Status.COMPLETED;
                        return null;
                    } else {
                        throw new AuthenticationException(
                                "Invalid token value: expected empty token, but got "
                                        + new String(data));
                    }
                default:
                    return null;
            }
        }

        @Override
        public boolean isCompleted() {
            return status == Status.COMPLETED;
        }
    }

    private static class ServerAuthenticatorImpl implements ServerAuthenticator {
        private enum Status {
            RECEIVE_CLIENT_FIRST_MESSAGE,
            RECEIVE_CLIENT_FINAL_MESSAGE,
            COMPLETED,
            CLOSED
        }

        private Integer initialSalt;
        private int retryNumber = 0;
        private Status status = Status.RECEIVE_CLIENT_FIRST_MESSAGE;
        private String ip;

        @Override
        public String protocol() {
            return MUTUAL_AUTH_PROTOCOL;
        }

        @Override
        public void initialize(AuthenticateContext context) {
            this.status = Status.RECEIVE_CLIENT_FIRST_MESSAGE;
            this.ip = context.ipAddress();
            this.initialSalt = null;
        }

        @Override
        public byte[] evaluateResponse(byte[] token) throws AuthenticationException {
            int tokenValue = parseToken(token);
            switch (status) {
                case RECEIVE_CLIENT_FIRST_MESSAGE:
                    if (errorType == ErrorType.SERVER_NO_CHALLENGE) {
                        return null;
                    }
                    if (errorType == ErrorType.SERVER_ERROR_CHALLENGE) {
                        return "-1".getBytes();
                    }
                    if (errorType == ErrorType.RETRIABLE_EXCEPTION && retryNumber++ < 3) {
                        throw new RetriableAuthenticationException("Retriable exception " + ip);
                    }

                    initialSalt = tokenValue + 1;
                    status = Status.RECEIVE_CLIENT_FINAL_MESSAGE;
                    return String.valueOf(initialSalt).getBytes();
                case RECEIVE_CLIENT_FINAL_MESSAGE:
                    if (tokenValue == initialSalt + 1) {
                        status = Status.COMPLETED;
                        return new byte[0];
                    } else {
                        throw new IllegalArgumentException(
                                "Invalid token value: expected "
                                        + (initialSalt + 1)
                                        + ", but got "
                                        + tokenValue);
                    }
                default:
                    return null;
            }
        }

        @Override
        public FlussPrincipal createPrincipal() {
            return FlussPrincipal.ANONYMOUS;
        }

        @Override
        public boolean isCompleted() {
            return status == Status.COMPLETED;
        }

        @Override
        public void keepAlive(short apiKey) throws AuthenticationException {
            if (errorType == ErrorType.KEEP_ALIVE_ERROR) {
                throw new AuthenticationException("Keep alive error");
            }
        }

        @Override
        public void close() throws IOException {
            status = Status.CLOSED;
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

    /** Error types for testing. */
    public enum ErrorType {
        NONE,
        SERVER_NO_CHALLENGE,
        SERVER_ERROR_CHALLENGE,
        CLIENT_ERROR_SECOND_TOKE,
        RETRIABLE_EXCEPTION,
        KEEP_ALIVE_ERROR
    }
}
