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
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.AuthenticationException;

import org.jose4j.http.Get;
import org.jose4j.jwk.HttpsJwks;
import org.jose4j.jws.JsonWebSignature;
import org.jose4j.jwx.JsonWebStructure;
import org.jose4j.keys.resolvers.HttpsJwksVerificationKeyResolver;
import org.jose4j.keys.resolvers.VerificationKeyResolver;
import org.jose4j.lang.UnresolvableKeyException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.security.Key;
import java.time.Duration;
import java.util.List;

/** Resolves JWT verification keys using jose4j's shared JWKS cache. */
@Internal
public final class OAuthBearerJwksResolver implements VerificationKeyResolver {
    private static final Logger LOG = LoggerFactory.getLogger(OAuthBearerJwksResolver.class);

    private final HttpsJwksVerificationKeyResolver delegate;

    /** Creates a process-local JWKS resolver from the server configuration. */
    public OAuthBearerJwksResolver(Configuration configuration) {
        URI endpoint =
                OAuthBearerUtils.validateHttpEndpoint(
                        configuration.get(ConfigOptions.SERVER_SASL_OAUTHBEARER_JWKS_ENDPOINT),
                        ConfigOptions.SERVER_SASL_OAUTHBEARER_JWKS_ENDPOINT.key());
        if ("http".equalsIgnoreCase(endpoint.getScheme())) {
            LOG.warn("OAuth JWKS endpoint uses insecure HTTP: {}", endpoint);
        }

        Duration timeout =
                configuration.get(ConfigOptions.SERVER_SASL_OAUTHBEARER_JWKS_REQUEST_TIMEOUT);
        long timeoutMillis = timeout == null ? 0 : timeout.toMillis();
        if (timeoutMillis <= 0 || timeoutMillis > Integer.MAX_VALUE) {
            throw new AuthenticationException(
                    "Configuration '"
                            + ConfigOptions.SERVER_SASL_OAUTHBEARER_JWKS_REQUEST_TIMEOUT.key()
                            + "' must fit in positive milliseconds");
        }
        int timeoutMs = (int) timeoutMillis;
        Get httpGet = new Get();
        httpGet.setConnectTimeout(timeoutMs);
        httpGet.setReadTimeout(timeoutMs);
        httpGet.setRetries(0);
        httpGet.setResponseBodySizeLimit(OAuthBearerUtils.MAX_HTTP_RESPONSE_SIZE);

        Duration refreshInterval =
                configuration.get(ConfigOptions.SERVER_SASL_OAUTHBEARER_JWKS_REFRESH_MIN_INTERVAL);
        if (refreshInterval == null || refreshInterval.isNegative()) {
            throw new AuthenticationException(
                    "Configuration '"
                            + ConfigOptions.SERVER_SASL_OAUTHBEARER_JWKS_REFRESH_MIN_INTERVAL.key()
                            + "' must not be negative");
        }

        HttpsJwks httpsJwks = new HttpsJwks(endpoint.toString());
        httpsJwks.setSimpleHttpGet(httpGet);
        httpsJwks.setRefreshReprieveThreshold(refreshInterval.toMillis());
        delegate = new HttpsJwksVerificationKeyResolver(httpsJwks);
    }

    @Override
    public Key resolveKey(JsonWebSignature jws, List<JsonWebStructure> nestingContext)
            throws UnresolvableKeyException {
        return delegate.resolveKey(jws, nestingContext);
    }
}
