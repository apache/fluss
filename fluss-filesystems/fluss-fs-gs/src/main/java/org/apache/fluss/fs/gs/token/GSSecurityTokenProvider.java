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

package org.apache.fluss.fs.gs.token;

import org.apache.fluss.exception.FlussRuntimeException;
import org.apache.fluss.fs.token.CredentialsJsonSerde;
import org.apache.fluss.fs.token.ObtainedSecurityToken;

import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.compute.ComputeCredential;
import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.ComputeEngineCredentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.hadoop.util.HadoopCredentialConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import static com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystemConfiguration.DELEGATION_TOKEN_BINDING_CLASS;
import static com.google.cloud.hadoop.util.HadoopCredentialConfiguration.GROUP_IMPERSONATION_SERVICE_ACCOUNT_SUFFIX;
import static com.google.cloud.hadoop.util.HadoopCredentialConfiguration.IMPERSONATION_SERVICE_ACCOUNT_SUFFIX;
import static com.google.cloud.hadoop.util.HadoopCredentialConfiguration.USER_IMPERSONATION_SERVICE_ACCOUNT_SUFFIX;

/**
 * Mints a short-lived, read-only Cloud Storage access token for the identity the bundled GCS
 * connector writes with. Clients receive that bearer token and do not need the server credentials.
 *
 * <p>The identity is resolved by the connector's own {@code CredentialFactory}, so the precedence
 * (service account key settings, then {@code GOOGLE_APPLICATION_CREDENTIALS}, then the metadata
 * server) and the {@code fs.gs.token.server.url} and {@code fs.gs.proxy.*} settings match the
 * connector. The bundled connector (gcs-connector 2.x) does not read {@code fs.gs.auth.type}, so
 * this provider does not either.
 *
 * <p>Only service accounts can mint the token: user credentials keep the scopes granted at login,
 * so the read-only scope would not apply to the token sent to clients. A custom access token
 * provider, delegation tokens and impersonation are rejected because the connector would then write
 * with an identity this provider does not reproduce.
 */
public class GSSecurityTokenProvider {

    private static final Logger LOG = LoggerFactory.getLogger(GSSecurityTokenProvider.class);

    private static final String GCS_CONFIG_PREFIX = "fs.gs";

    private static final List<String> CONFIG_KEY_PREFIXES =
            HadoopCredentialConfiguration.getConfigKeyPrefixes(GCS_CONFIG_PREFIX);

    /**
     * Cloud Storage read-only scope. This stays on the storage API instead of cloud-platform, which
     * would authorize every Google API the account can call.
     */
    static final String STORAGE_SCOPE = "https://www.googleapis.com/auth/devstorage.read_only";

    private static final List<String> SCOPES = Collections.singletonList(STORAGE_SCOPE);

    private final String scheme;

    private final Configuration conf;

    /**
     * Creates a provider for the given filesystem scheme.
     *
     * @param scheme filesystem scheme, expected to be {@code gs}
     * @param conf Hadoop configuration the GCS connector was initialized with
     */
    public GSSecurityTokenProvider(String scheme, Configuration conf) {
        this.scheme = scheme;
        this.conf = conf;
    }

    /**
     * Refreshes a read-only OAuth access token for the connector's identity. Credentials are
     * resolved on every call, so a rotated key file is picked up without a restart.
     */
    public ObtainedSecurityToken obtainSecurityToken() {
        LOG.info("Obtaining session credentials token");
        rejectIdentitiesThatCannotBeReproduced();

        final String tokenValue;
        final Long expirationTimeMs;
        try {
            Credential credential =
                    HadoopCredentialConfiguration.getCredentialFactory(conf, GCS_CONFIG_PREFIX)
                            .getCredential(SCOPES);
            if (credential instanceof ComputeCredential) {
                // The connector ignores scopes for the metadata server, so request the read-only
                // scope from it directly.
                GoogleCredentials scoped = ComputeEngineCredentials.create().createScoped(SCOPES);
                scoped.refresh();
                AccessToken accessToken = scoped.getAccessToken();
                tokenValue = accessToken == null ? null : accessToken.getTokenValue();
                expirationTimeMs =
                        accessToken == null || accessToken.getExpirationTime() == null
                                ? null
                                : accessToken.getExpirationTime().getTime();
            } else if (isServiceAccount(credential)) {
                credential.refreshToken();
                tokenValue = credential.getAccessToken();
                expirationTimeMs = credential.getExpirationTimeMilliseconds();
            } else {
                throw new IllegalArgumentException(
                        "Only a service account can mint a Cloud Storage security token, because "
                                + "user credentials cannot be limited to "
                                + STORAGE_SCOPE
                                + ". Configure fs.gs.auth.service.account.json.keyfile with a "
                                + "service account key, or run where the metadata server provides "
                                + "a service account (GCE or GKE Workload Identity).");
            }
        } catch (IOException | GeneralSecurityException e) {
            throw new FlussRuntimeException("Fail to obtain session credentials token", e);
        }

        if (tokenValue == null || expirationTimeMs == null) {
            throw new FlussRuntimeException("Session credentials token is empty");
        }
        LOG.info("Session credentials obtained successfully with expiration: {}", expirationTimeMs);

        // GCS session credentials are a bearer token. Access key fields stay empty.
        org.apache.fluss.fs.token.Credentials flussCredentials =
                new org.apache.fluss.fs.token.Credentials("", "", tokenValue);
        return new ObtainedSecurityToken(
                scheme,
                CredentialsJsonSerde.toJson(flussCredentials),
                expirationTimeMs,
                new HashMap<>());
    }

    /** Service accounts sign a JWT with their private key; user credentials have no key. */
    private static boolean isServiceAccount(Credential credential) {
        return credential instanceof GoogleCredential
                && ((GoogleCredential) credential).getServiceAccountPrivateKey() != null;
    }

    private void rejectIdentitiesThatCannotBeReproduced() {
        if (isConfigured(DELEGATION_TOKEN_BINDING_CLASS.get(conf, conf::get))) {
            throw new IllegalArgumentException(
                    "Cannot obtain a Cloud Storage security token when "
                            + DELEGATION_TOKEN_BINDING_CLASS.getKey()
                            + " is set.");
        }
        Class<?> accessTokenProvider =
                HadoopCredentialConfiguration.getAccessTokenProviderImplClass(
                        conf, GCS_CONFIG_PREFIX);
        if (accessTokenProvider != null) {
            throw new IllegalArgumentException(
                    "Cannot obtain a Cloud Storage security token when a custom access token "
                            + "provider is configured: "
                            + accessTokenProvider.getName());
        }
        boolean impersonation =
                isConfigured(
                                IMPERSONATION_SERVICE_ACCOUNT_SUFFIX
                                        .withPrefixes(CONFIG_KEY_PREFIXES)
                                        .get(conf, conf::get))
                        || !USER_IMPERSONATION_SERVICE_ACCOUNT_SUFFIX
                                .withPrefixes(CONFIG_KEY_PREFIXES)
                                .getPropsWithPrefix(conf)
                                .isEmpty()
                        || !GROUP_IMPERSONATION_SERVICE_ACCOUNT_SUFFIX
                                .withPrefixes(CONFIG_KEY_PREFIXES)
                                .getPropsWithPrefix(conf)
                                .isEmpty();
        if (impersonation) {
            throw new IllegalArgumentException(
                    "Cannot obtain a Cloud Storage security token when service account "
                            + "impersonation is configured.");
        }
    }

    private static boolean isConfigured(String value) {
        return value != null && !value.isEmpty();
    }
}
