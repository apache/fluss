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

package org.apache.fluss.security.auth.ssl;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.AuthenticationException;
import org.apache.fluss.security.acl.FlussPrincipal;
import org.apache.fluss.security.auth.ClientAuthenticationPlugin;
import org.apache.fluss.security.auth.ClientAuthenticator;
import org.apache.fluss.security.auth.ServerAuthenticationPlugin;
import org.apache.fluss.security.auth.ServerAuthenticator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLSession;
import javax.security.auth.x500.X500Principal;

import java.security.Principal;
import java.util.Collections;
import java.util.Set;

/**
 * Authentication plugin for mutual TLS (mTLS). The server derives the principal from the X.509
 * certificate that the client presented during the TLS handshake.
 *
 * <p>The TLS layer validates the certificate. The authentication exchange only maps the
 * distinguished name of the certificate to a principal name with {@code
 * security.ssl.principal.mapping.rules}.
 */
public class MutualTlsAuthenticationPlugin
        implements ServerAuthenticationPlugin, ClientAuthenticationPlugin {

    /** The name of the protocol in {@code security.protocol.map} and the client configuration. */
    public static final String AUTH_PROTOCOL = "mTLS";

    /** Parsed from the rules on the first connection, then reused for all later connections. */
    @Nullable private volatile SslPrincipalMapper principalMapper;

    @Override
    public String authProtocol() {
        return AUTH_PROTOCOL;
    }

    @Override
    public ClientAuthenticator createClientAuthenticator(Configuration configuration) {
        return new MutualTlsClientAuthenticator();
    }

    @Override
    public ServerAuthenticator createServerAuthenticator(Configuration configuration) {
        SslPrincipalMapper mapper = principalMapper;
        if (mapper == null) {
            // Concurrent first connections may each parse the rules. The results are equal.
            mapper =
                    SslPrincipalMapper.fromRules(
                            configuration.get(ConfigOptions.SERVER_SSL_PRINCIPAL_MAPPING_RULES));
            principalMapper = mapper;
        }
        return new MutualTlsServerAuthenticator(mapper, configuration);
    }

    /**
     * Client authenticator for mTLS. The client certificate travels in the TLS handshake, so the
     * client sends one empty token.
     */
    public static class MutualTlsClientAuthenticator implements ClientAuthenticator {

        private boolean completed;

        @Override
        public String protocol() {
            return AUTH_PROTOCOL;
        }

        /**
         * Returns an empty token, not null: the client sends no authenticate request for a null
         * token, and the server would then reject every request of the connection.
         */
        @Override
        public byte[] authenticate(byte[] data) {
            completed = true;
            return new byte[0];
        }

        @Override
        public boolean isCompleted() {
            return completed;
        }
    }

    /** Server authenticator for mTLS. Maps the client certificate to a {@link FlussPrincipal}. */
    public static class MutualTlsServerAuthenticator implements ServerAuthenticator {

        private static final Logger LOG =
                LoggerFactory.getLogger(MutualTlsServerAuthenticator.class);

        private final SslPrincipalMapper principalMapper;
        private final Configuration configuration;

        @Nullable private SSLSession sslSession;
        @Nullable private String listenerName;

        /**
         * Read once from the session. A later TLS 1.2 renegotiation with another certificate cannot
         * change the principal of the connection.
         */
        @Nullable private FlussPrincipal principal;

        public MutualTlsServerAuthenticator(
                SslPrincipalMapper principalMapper, Configuration configuration) {
            this.principalMapper = principalMapper;
            this.configuration = configuration;
        }

        @Override
        public String protocol() {
            return AUTH_PROTOCOL;
        }

        @Override
        public void initialize(AuthenticateContext context) {
            this.sslSession = context.sslSession();
            this.listenerName = context.listenerName();
        }

        @Override
        public byte[] evaluateResponse(byte[] token) throws AuthenticationException {
            if (sslSession == null) {
                throw new AuthenticationException(
                        String.format(
                                "Listener '%s' uses the %s authentication protocol, which "
                                        + "requires TLS transport, but the connection is not "
                                        + "encrypted.",
                                listenerName, AUTH_PROTOCOL));
            }

            Principal peerPrincipal;
            try {
                peerPrincipal = sslSession.getPeerPrincipal();
            } catch (SSLPeerUnverifiedException e) {
                throw new AuthenticationException(
                        "The client presented no certificate in the TLS handshake.");
            }
            if (!(peerPrincipal instanceof X500Principal)) {
                throw new AuthenticationException(
                        String.format(
                                "The client certificate identifies the client with a %s, not "
                                        + "with an X.500 distinguished name.",
                                peerPrincipal.getClass().getName()));
            }

            String distinguishedName = ((X500Principal) peerPrincipal).getName();
            String principalName;
            try {
                principalName = principalMapper.getName(distinguishedName);
            } catch (SslPrincipalMapper.NoMatchingRuleException e) {
                // Don't use e.getMessage() here, because it lists the configured rules and this
                // message is sent to the client.
                throw new AuthenticationException(
                        String.format(
                                "No principal mapping rule applies to the distinguished name "
                                        + "'%s' of the client certificate.",
                                distinguishedName));
            }

            FlussPrincipal mapped = new FlussPrincipal(principalName, "User");
            checkAllowedOnInternalListener(mapped);
            principal = mapped;
            LOG.debug(
                    "Authenticated the client certificate '{}' as principal {}.",
                    distinguishedName,
                    principal);
            return new byte[0];
        }

        /**
         * The server skips ACL checks for requests on the internal listener, so only a super user
         * may authenticate there. Servers authenticate to each other with a certificate that maps
         * to a super user.
         */
        private void checkAllowedOnInternalListener(FlussPrincipal mapped)
                throws AuthenticationException {
            if (!configuration.get(ConfigOptions.INTERNAL_LISTENER_NAME).equals(listenerName)) {
                return;
            }
            // Parsed per connection: one split of a short string, after a full TLS handshake.
            boolean ignoreCase =
                    configuration.get(ConfigOptions.SECURITY_ACL_PRINCIPAL_IGNORE_CASE);
            Set<FlussPrincipal> superUsers =
                    configuration
                            .getOptional(ConfigOptions.SUPER_USERS)
                            .map(FlussPrincipal::parsePrincipals)
                            .orElse(Collections.emptySet());
            for (FlussPrincipal superUser : superUsers) {
                if (superUser.matches(mapped, ignoreCase)) {
                    return;
                }
            }
            throw new AuthenticationException(
                    String.format(
                            "The client certificate maps to the principal '%s:%s'. The internal "
                                    + "listener '%s' accepts only super users.",
                            mapped.getType(), mapped.getName(), listenerName));
        }

        @Override
        public boolean isCompleted() {
            return principal != null;
        }

        @Override
        public FlussPrincipal createPrincipal() {
            return principal;
        }
    }
}
