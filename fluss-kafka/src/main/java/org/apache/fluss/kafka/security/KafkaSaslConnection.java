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

package org.apache.fluss.kafka.security;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.exception.AuthenticationException;
import org.apache.fluss.security.acl.FlussPrincipal;
import org.apache.fluss.security.auth.ServerAuthenticator;

import org.apache.kafka.common.protocol.ApiKeys;

import javax.annotation.Nullable;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.function.Supplier;

import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** Per-connection authentication state for a Kafka protocol channel. */
@Internal
public final class KafkaSaslConnection implements AutoCloseable {

    /** The only Kafka SASL mechanism supported by the initial implementation. */
    public static final String PLAIN_MECHANISM = "PLAIN";

    private enum State {
        AUTHENTICATION_REQUIRED,
        AUTHENTICATING,
        READY,
        FAILED,
        CLOSED
    }

    @Nullable private final Supplier<ServerAuthenticator> authenticatorSupplier;

    private volatile State state;
    private volatile FlussPrincipal principal;
    @Nullable private ServerAuthenticator authenticator;

    private KafkaSaslConnection(
            @Nullable Supplier<ServerAuthenticator> authenticatorSupplier, State initialState) {
        this.authenticatorSupplier = authenticatorSupplier;
        this.state = initialState;
        this.principal = FlussPrincipal.ANONYMOUS;
    }

    /** Creates an unauthenticated PLAINTEXT connection that is immediately ready. */
    public static KafkaSaslConnection plaintext() {
        return new KafkaSaslConnection(null, State.READY);
    }

    /** Creates a SASL connection that must authenticate before normal requests are accepted. */
    public static KafkaSaslConnection sasl(Supplier<ServerAuthenticator> authenticatorSupplier) {
        return new KafkaSaslConnection(
                checkNotNull(authenticatorSupplier), State.AUTHENTICATION_REQUIRED);
    }

    /** Returns whether SASL authentication is enabled for this connection. */
    public boolean authenticationEnabled() {
        return authenticatorSupplier != null;
    }

    /** Returns whether this connection is waiting for a SASL handshake. */
    public boolean isAwaitingHandshake() {
        return state == State.AUTHENTICATION_REQUIRED;
    }

    /** Returns whether this connection is exchanging SASL authentication tokens. */
    public boolean isAuthenticating() {
        return state == State.AUTHENTICATING;
    }

    /** Returns whether this connection is ready to serve normal Kafka requests. */
    public boolean isReady() {
        return state == State.READY;
    }

    /**
     * Returns whether the request is allowed in the current connection state.
     *
     * <p>Both SASL APIs are always routed while the connection is active so that their handlers can
     * return Kafka's precise illegal-state error. ApiVersions is additionally routed before and
     * during authentication so its handler can return Kafka's state-specific response. Normal
     * business APIs are accepted only after authentication succeeds.
     */
    public boolean isRequestAllowed(ApiKeys apiKey) {
        checkNotNull(apiKey);
        State currentState = state;
        if (currentState == State.FAILED || currentState == State.CLOSED) {
            return false;
        }
        if (apiKey == ApiKeys.SASL_HANDSHAKE || apiKey == ApiKeys.SASL_AUTHENTICATE) {
            return true;
        }
        if (currentState == State.READY) {
            return true;
        }
        if (currentState == State.AUTHENTICATION_REQUIRED || currentState == State.AUTHENTICATING) {
            return apiKey == ApiKeys.API_VERSIONS;
        }
        return false;
    }

    /** Returns the authenticated principal, or the anonymous principal before authentication. */
    public FlussPrincipal principal() {
        return principal;
    }

    /** Returns whether the network channel must close after its pending response is flushed. */
    public boolean shouldClose() {
        State currentState = state;
        return currentState == State.FAILED || currentState == State.CLOSED;
    }

    /**
     * Starts a PLAIN authentication exchange for this connection.
     *
     * @param mechanism mechanism selected by the Kafka client
     * @param listenerName listener on which the client connected
     * @param remoteAddress remote client address
     */
    public synchronized void beginAuthentication(
            String mechanism, String listenerName, @Nullable SocketAddress remoteAddress) {
        if (state != State.AUTHENTICATION_REQUIRED) {
            throw new IllegalStateException("SASL handshake is not allowed in the current state.");
        }
        if (!PLAIN_MECHANISM.equals(mechanism)) {
            throw new AuthenticationException("Unsupported SASL mechanism.");
        }

        ServerAuthenticator newAuthenticator = null;
        try {
            newAuthenticator = checkNotNull(authenticatorSupplier).get();
            newAuthenticator.initialize(
                    new DefaultAuthenticateContext(
                            listenerName, clientIpAddress(remoteAddress), mechanism));
            authenticator = newAuthenticator;
            state = State.AUTHENTICATING;
        } catch (AuthenticationException e) {
            closeAuthenticator(newAuthenticator);
            failAuthentication();
            throw e;
        } catch (RuntimeException e) {
            closeAuthenticator(newAuthenticator);
            failAuthentication();
            throw new AuthenticationException("Failed to initialize SASL authentication.", e);
        }
    }

    /**
     * Evaluates one client SASL token and advances the connection to ready when authentication
     * completes.
     */
    public synchronized byte[] authenticate(byte[] token) {
        if (state != State.AUTHENTICATING || authenticator == null) {
            throw new IllegalStateException(
                    "SASL authentication is not allowed in the current state.");
        }

        try {
            byte[] challenge = authenticator.evaluateResponse(checkNotNull(token));
            if (authenticator.isCompleted()) {
                principal = checkNotNull(authenticator.createPrincipal());
                state = State.READY;
                closeAuthenticator(authenticator);
                authenticator = null;
            }
            return challenge == null ? new byte[0] : challenge;
        } catch (AuthenticationException e) {
            failAuthentication();
            throw e;
        } catch (RuntimeException e) {
            failAuthentication();
            throw new AuthenticationException("SASL authentication failed.", e);
        }
    }

    /** Marks authentication as failed and releases its authenticator. */
    public synchronized void failAuthentication() {
        if (state == State.CLOSED) {
            return;
        }
        closeAuthenticator(authenticator);
        authenticator = null;
        principal = FlussPrincipal.ANONYMOUS;
        state = State.FAILED;
    }

    /** Releases the per-connection authenticator. */
    @Override
    public synchronized void close() {
        closeAuthenticator(authenticator);
        authenticator = null;
        state = State.CLOSED;
    }

    private static String clientIpAddress(@Nullable SocketAddress remoteAddress) {
        if (remoteAddress instanceof InetSocketAddress) {
            InetSocketAddress inetAddress = (InetSocketAddress) remoteAddress;
            if (inetAddress.getAddress() != null) {
                return inetAddress.getAddress().getHostAddress();
            }
            return inetAddress.getHostString();
        }
        return remoteAddress == null ? "UNKNOWN" : remoteAddress.toString();
    }

    private static void closeAuthenticator(@Nullable ServerAuthenticator authenticator) {
        if (authenticator == null) {
            return;
        }
        try {
            authenticator.close();
        } catch (Exception ignored) {
            // The connection is already closing or failed; there is no recovery action here.
        }
    }

    private static final class DefaultAuthenticateContext
            implements ServerAuthenticator.AuthenticateContext {

        private final String listenerName;
        private final String ipAddress;
        private final String protocol;

        private DefaultAuthenticateContext(String listenerName, String ipAddress, String protocol) {
            this.listenerName = checkNotNull(listenerName);
            this.ipAddress = checkNotNull(ipAddress);
            this.protocol = checkNotNull(protocol);
        }

        @Override
        public String ipAddress() {
            return ipAddress;
        }

        @Override
        public String listenerName() {
            return listenerName;
        }

        @Override
        public String protocol() {
            return protocol;
        }
    }
}
