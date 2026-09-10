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

package org.apache.fluss.security.auth.sasl;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.AuthenticationException;
import org.apache.fluss.security.auth.ClientAuthenticator;
import org.apache.fluss.security.auth.ServerAuthenticator;
import org.apache.fluss.security.auth.sasl.authenticator.PlainSaslServerAuthenticator;
import org.apache.fluss.security.auth.sasl.authenticator.SaslClientAuthenticator;
import org.apache.fluss.security.auth.sasl.plain.PlainSaslServer;

import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;

/** Factory for creating connection-local SASL authenticators. */
@Internal
public final class SaslAuthenticatorFactory {
    private static final Map<String, AuthenticatorCreators> AUTHENTICATOR_CREATORS;

    static {
        Map<String, AuthenticatorCreators> creators = new HashMap<>();
        creators.put(
                PlainSaslServer.PLAIN_MECHANISM,
                new AuthenticatorCreators(
                        configuration -> new SaslClientAuthenticator(configuration),
                        configuration -> new PlainSaslServerAuthenticator(configuration)));
        AUTHENTICATOR_CREATORS = Collections.unmodifiableMap(creators);
    }

    private SaslAuthenticatorFactory() {}

    /** Creates a connection-local client SASL authenticator. */
    public static ClientAuthenticator createClientAuthenticator(Configuration configuration) {
        String mechanism = configuration.get(ConfigOptions.CLIENT_SASL_MECHANISM);
        String normalizedMechanism = mechanism.toUpperCase(Locale.ROOT);
        AuthenticatorCreators creators = AUTHENTICATOR_CREATORS.get(normalizedMechanism);
        if (creators == null) {
            throw new AuthenticationException(
                    "Unable to find a matching SASL mechanism for " + normalizedMechanism);
        }
        return creators.clientCreator.apply(configuration);
    }

    /** Creates a server authenticator for the requested mechanism. */
    public static ServerAuthenticator createServerAuthenticator(
            String mechanism, Configuration configuration) {
        String normalizedMechanism = mechanism.toUpperCase(Locale.ROOT);
        AuthenticatorCreators creators = AUTHENTICATOR_CREATORS.get(normalizedMechanism);
        if (creators == null) {
            throw new AuthenticationException(
                    "Unable to find a matching SASL mechanism for " + normalizedMechanism);
        }
        return creators.serverCreator.apply(configuration);
    }

    /** Returns whether the server supports the requested mechanism. */
    public static boolean supportsServerMechanism(String mechanism) {
        return AUTHENTICATOR_CREATORS.containsKey(mechanism.toUpperCase(Locale.ROOT));
    }

    private static final class AuthenticatorCreators {
        private final Function<Configuration, ClientAuthenticator> clientCreator;
        private final Function<Configuration, ServerAuthenticator> serverCreator;

        private AuthenticatorCreators(
                Function<Configuration, ClientAuthenticator> clientCreator,
                Function<Configuration, ServerAuthenticator> serverCreator) {
            this.clientCreator = clientCreator;
            this.serverCreator = serverCreator;
        }
    }
}
