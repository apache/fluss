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

import java.util.Locale;

/** Factory for creating connection-local SASL authenticators. */
@Internal
public final class SaslAuthenticatorFactory {
    private SaslAuthenticatorFactory() {}

    /** Creates a connection-local client SASL authenticator. */
    public static ClientAuthenticator createClientAuthenticator(Configuration configuration) {
        String mechanism = configuration.get(ConfigOptions.CLIENT_SASL_MECHANISM);
        switch (normalize(mechanism)) {
            case PlainSaslServer.PLAIN_MECHANISM:
                return new SaslClientAuthenticator(configuration);
            default:
                // TODO: Add OAUTHBEARER client authenticator in a follow-up change.
                throw unsupportedMechanism(mechanism);
        }
    }

    /** Creates a server authenticator for the requested mechanism. */
    public static ServerAuthenticator createServerAuthenticator(
            String mechanism, Configuration configuration) {
        switch (normalize(mechanism)) {
            case PlainSaslServer.PLAIN_MECHANISM:
                return new PlainSaslServerAuthenticator(configuration);
            default:
                // TODO: Add OAUTHBEARER server authenticator in a follow-up change.
                throw unsupportedMechanism(mechanism);
        }
    }

    /** Returns whether the server supports the requested mechanism. */
    public static boolean supportsServerMechanism(String mechanism) {
        // TODO: Add OAUTHBEARER server authenticator in a follow-up change.
        switch (normalize(mechanism)) {
            case PlainSaslServer.PLAIN_MECHANISM:
                return true;
            default:
                return false;
        }
    }

    private static String normalize(String mechanism) {
        return mechanism.toUpperCase(Locale.ROOT);
    }

    private static AuthenticationException unsupportedMechanism(String mechanism) {
        return new AuthenticationException(
                "Unable to find a matching SASL mechanism for " + normalize(mechanism));
    }
}
