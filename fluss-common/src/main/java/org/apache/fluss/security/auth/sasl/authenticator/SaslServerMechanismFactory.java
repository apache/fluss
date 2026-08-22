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

package org.apache.fluss.security.auth.sasl.authenticator;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.AuthenticationException;
import org.apache.fluss.security.auth.ServerAuthenticator;
import org.apache.fluss.security.auth.sasl.oauthbearer.OAuthBearerClientAuthenticator;
import org.apache.fluss.security.auth.sasl.oauthbearer.OAuthBearerJwksResolver;
import org.apache.fluss.security.auth.sasl.oauthbearer.OAuthBearerServerAuthenticator;
import org.apache.fluss.security.auth.sasl.plain.PlainSaslServer;

import java.util.List;
import java.util.Locale;

import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** Creates connection-local SASL authenticators and owns shared mechanism state. */
final class SaslServerMechanismFactory {
    private final Configuration configuration;
    private final OAuthBearerJwksResolver jwksResolver;

    SaslServerMechanismFactory(Configuration configuration) {
        this.configuration = configuration;
        List<String> mechanisms =
                configuration.get(ConfigOptions.SERVER_SASL_ENABLED_MECHANISMS_CONFIG);
        this.jwksResolver =
                containsOAuthBearer(mechanisms) ? new OAuthBearerJwksResolver(configuration) : null;
    }

    ServerAuthenticator createAuthenticator(String mechanism) {
        switch (normalize(mechanism)) {
            case PlainSaslServer.PLAIN_MECHANISM:
                return new PlainSaslServerAuthenticator(configuration);
            case OAuthBearerClientAuthenticator.OAUTHBEARER_MECHANISM:
                return new OAuthBearerServerAuthenticator(
                        configuration, checkNotNull(jwksResolver));
            default:
                throw new AuthenticationException(
                        "Unable to find a matching SASL mechanism for " + normalize(mechanism));
        }
    }

    boolean supportsMechanism(String mechanism) {
        switch (normalize(mechanism)) {
            case PlainSaslServer.PLAIN_MECHANISM:
            case OAuthBearerClientAuthenticator.OAUTHBEARER_MECHANISM:
                return true;
            default:
                return false;
        }
    }

    private static boolean containsOAuthBearer(List<String> mechanisms) {
        return mechanisms != null
                && mechanisms.stream()
                        .anyMatch(
                                OAuthBearerClientAuthenticator.OAUTHBEARER_MECHANISM
                                        ::equalsIgnoreCase);
    }

    private static String normalize(String mechanism) {
        return mechanism.toUpperCase(Locale.ROOT);
    }
}
