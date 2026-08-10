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
import org.apache.fluss.exception.AuthenticationException;
import org.apache.fluss.security.acl.FlussPrincipal;
import org.apache.fluss.security.auth.ServerAuthenticator;

/** A connection-local SASL OAUTHBEARER server authenticator. */
@Internal
public final class OAuthBearerServerAuthenticator implements ServerAuthenticator {
    private final OAuthBearerJwtValidator validator;

    private FlussPrincipal principal;
    private long expiresAtMs;

    /** Creates a connection-local authenticator using the shared JWKS cache. */
    public OAuthBearerServerAuthenticator(
            Configuration configuration, OAuthBearerJwksResolver jwksResolver) {
        validator = new OAuthBearerJwtValidator(configuration, jwksResolver);
    }

    @Override
    public String protocol() {
        return OAuthBearerClientAuthenticator.OAUTHBEARER_MECHANISM;
    }

    @Override
    public byte[] evaluateResponse(byte[] tokenBytes) throws AuthenticationException {
        String token = OAuthBearerSaslMessage.parseToken(tokenBytes);
        OAuthBearerJwtValidator.ValidatedToken validatedToken = validator.validate(token);
        principal = new FlussPrincipal(validatedToken.subject(), FlussPrincipal.USER_TYPE);
        expiresAtMs = validatedToken.expiresAtMs();
        return null;
    }

    @Override
    public boolean isCompleted() {
        return principal != null;
    }

    @Override
    public FlussPrincipal createPrincipal() {
        if (principal == null) {
            throw new AuthenticationException("SASL OAUTHBEARER authentication is not completed");
        }
        return principal;
    }

    @Override
    public void validateSession() {
        if (principal != null && System.currentTimeMillis() >= expiresAtMs) {
            throw new AuthenticationException("JWT access token is expired");
        }
    }
}
