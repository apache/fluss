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

import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.AuthenticationException;
import org.apache.fluss.utils.StringUtils;

import org.jose4j.jwa.AlgorithmConstraints.ConstraintType;
import org.jose4j.jws.AlgorithmIdentifiers;
import org.jose4j.jwt.JwtClaims;
import org.jose4j.jwt.MalformedClaimException;
import org.jose4j.jwt.consumer.ErrorCodes;
import org.jose4j.jwt.consumer.InvalidJwtException;
import org.jose4j.jwt.consumer.JwtConsumer;
import org.jose4j.jwt.consumer.JwtConsumerBuilder;

import java.util.List;
import java.util.Objects;

import static org.apache.fluss.config.ConfigOptions.SERVER_SASL_OAUTHBEARER_EXPECTED_AUDIENCES;
import static org.apache.fluss.config.ConfigOptions.SERVER_SASL_OAUTHBEARER_EXPECTED_ISSUER;

/** Validates RS256 JWT access tokens using a shared JWKS cache. */
final class OAuthBearerJwtValidator {
    private final JwtConsumer jwtConsumer;

    OAuthBearerJwtValidator(Configuration configuration, OAuthBearerJwksResolver jwksResolver) {
        String expectedIssuer = configuration.get(SERVER_SASL_OAUTHBEARER_EXPECTED_ISSUER);
        if (StringUtils.isNullOrWhitespaceOnly(expectedIssuer)) {
            throw new AuthenticationException(
                    "Configuration '"
                            + SERVER_SASL_OAUTHBEARER_EXPECTED_ISSUER.key()
                            + "' must be set");
        }
        List<String> audiences = configuration.get(SERVER_SASL_OAUTHBEARER_EXPECTED_AUDIENCES);
        String[] expectedAudiences =
                audiences == null
                        ? new String[0]
                        : audiences.stream()
                                .filter(Objects::nonNull)
                                .map(String::trim)
                                .filter(audience -> !audience.isEmpty())
                                .toArray(String[]::new);
        if (expectedAudiences.length == 0) {
            throw new AuthenticationException(
                    "Configuration '"
                            + SERVER_SASL_OAUTHBEARER_EXPECTED_AUDIENCES.key()
                            + "' must be set");
        }
        jwtConsumer =
                new JwtConsumerBuilder()
                        .setExpectedIssuer(expectedIssuer)
                        .setExpectedAudience(expectedAudiences)
                        .setRequireExpirationTime()
                        .setRequireSubject()
                        .setVerificationKeyResolver(
                                (jsonWebSignature, nestingContext) -> {
                                    String keyId = jsonWebSignature.getKeyIdHeaderValue();
                                    if (StringUtils.isNullOrWhitespaceOnly(keyId)) {
                                        throw new AuthenticationException(
                                                "JWT access token must contain a non-empty kid claim");
                                    }
                                    return jwksResolver.resolveKey(
                                            jsonWebSignature, nestingContext);
                                })
                        .setJwsAlgorithmConstraints(
                                ConstraintType.PERMIT, AlgorithmIdentifiers.RSA_USING_SHA256)
                        .build();
    }

    ValidatedToken validate(String token) {
        if (token == null || token.isEmpty() || token.length() > OAuthBearerUtils.MAX_JWT_SIZE) {
            throw new AuthenticationException("Invalid JWT access token");
        }

        try {
            JwtClaims claims = jwtConsumer.processToClaims(token);
            String subject = claims.getSubject();
            if (StringUtils.isNullOrWhitespaceOnly(subject)) {
                throw new AuthenticationException(
                        "JWT access token must contain a non-empty sub claim");
            }
            return new ValidatedToken(subject, claims.getExpirationTime().getValueInMillis());
        } catch (InvalidJwtException e) {
            throw invalidToken(e);
        } catch (MalformedClaimException e) {
            throw new AuthenticationException("Invalid JWT access token claims", e);
        }
    }

    private static AuthenticationException invalidToken(InvalidJwtException exception) {
        if (exception.hasExpired()) {
            return new AuthenticationException("JWT access token is expired", exception);
        }
        if (exception.hasErrorCode(ErrorCodes.NOT_YET_VALID)) {
            return new AuthenticationException("JWT access token is not active yet", exception);
        }
        if (exception.hasErrorCode(ErrorCodes.AUDIENCE_MISSING)
                || exception.hasErrorCode(ErrorCodes.AUDIENCE_INVALID)) {
            return new AuthenticationException(
                    "JWT audience does not match configured audiences", exception);
        }
        if (exception.hasErrorCode(ErrorCodes.ISSUER_MISSING)
                || exception.hasErrorCode(ErrorCodes.ISSUER_INVALID)) {
            return new AuthenticationException(
                    "JWT issuer does not match configured issuer", exception);
        }
        if (exception.hasErrorCode(ErrorCodes.SIGNATURE_INVALID)
                || exception.hasErrorCode(ErrorCodes.SIGNATURE_MISSING)) {
            return new AuthenticationException("JWT signature validation failed", exception);
        }
        return new AuthenticationException("Invalid JWT access token", exception);
    }

    static final class ValidatedToken {
        private final String subject;
        private final long expiresAtMs;

        private ValidatedToken(String subject, long expiresAtMs) {
            this.subject = subject;
            this.expiresAtMs = expiresAtMs;
        }

        String subject() {
            return subject;
        }

        long expiresAtMs() {
            return expiresAtMs;
        }
    }
}
