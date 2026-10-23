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

import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.ComputeEngineCredentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import static com.google.cloud.hadoop.util.HadoopCredentialConfiguration.SERVICE_ACCOUNT_JSON_KEYFILE_SUFFIX;

/**
 * Mints a short-lived Cloud Storage access token from the server identity. Clients receive that
 * bearer token and do not need the key file. The token uses the Cloud Storage read-only scope, and
 * only a service account can mint it.
 *
 * <p>{@code SERVICE_ACCOUNT_JSON_KEYFILE} exchanges a JWT at the key's {@code token_uri}. {@code
 * COMPUTE_ENGINE} reads the instance metadata server. {@code APPLICATION_DEFAULT} calls {@link
 * GoogleCredentials#getApplicationDefault()} and accepts only {@link ServiceAccountCredentials} or
 * {@link ComputeEngineCredentials}. User credentials, such as those from {@code gcloud auth
 * application-default login}, are rejected because {@code createScoped} leaves their original
 * scopes in place.
 *
 * <p>When {@code fs.gs.auth.type} is unset, a configured {@code
 * fs.gs.auth.service.account.json.keyfile} is used. Application default credentials are used only
 * when that key file is absent. The bundled GCS connector does not read {@code fs.gs.auth.type} and
 * authenticates with the key file whenever one is set.
 */
public class GSSecurityTokenProvider {

    private static final Logger LOG = LoggerFactory.getLogger(GSSecurityTokenProvider.class);

    private static final String AUTH_TYPE_KEY = "fs.gs.auth.type";

    private static final String JSON_KEYFILE_KEY = "fs.gs.auth.service.account.json.keyfile";

    private static final String AUTH_TYPE_APPLICATION_DEFAULT = "APPLICATION_DEFAULT";

    private static final String AUTH_TYPE_COMPUTE_ENGINE = "COMPUTE_ENGINE";

    private static final String AUTH_TYPE_SERVICE_ACCOUNT_JSON_KEYFILE =
            "SERVICE_ACCOUNT_JSON_KEYFILE";

    private static final String AUTH_TYPE_UNAUTHENTICATED = "UNAUTHENTICATED";

    /**
     * Cloud Storage read-only scope. This stays on the storage API instead of cloud-platform, which
     * would authorize every Google API the account can call.
     */
    static final String STORAGE_SCOPE = "https://www.googleapis.com/auth/devstorage.read_only";

    private final String scheme;

    private final GoogleCredentials googleCredentials;

    /**
     * Creates a provider for the given filesystem scheme.
     *
     * @param scheme filesystem scheme, expected to be {@code gs}
     * @param conf Hadoop configuration that carries {@code fs.gs.auth.type} and the credential
     *     source
     */
    public GSSecurityTokenProvider(String scheme, Configuration conf) {
        this.scheme = scheme;
        this.googleCredentials = extractProvider(conf);
    }

    /**
     * Refreshes an OAuth access token for the configured identity. A service account key exchanges
     * a JWT at its {@code token_uri}. Compute Engine credentials read the instance metadata server.
     */
    public ObtainedSecurityToken obtainSecurityToken() {
        LOG.info("Obtaining session credentials token");

        GoogleCredentials scoped =
                googleCredentials.createScoped(Collections.singletonList(STORAGE_SCOPE));
        try {
            scoped.refresh();
        } catch (IOException e) {
            throw new FlussRuntimeException("Fail to obtain session credentials token", e);
        }
        AccessToken accessToken = scoped.getAccessToken();
        if (accessToken == null
                || accessToken.getTokenValue() == null
                || accessToken.getExpirationTime() == null) {
            throw new FlussRuntimeException("Session credentials token is empty");
        }
        LOG.info(
                "Session credentials obtained successfully with expiration: {}",
                accessToken.getExpirationTime());

        return new ObtainedSecurityToken(
                scheme,
                toJson(accessToken),
                accessToken.getExpirationTime().getTime(),
                new HashMap<>());
    }

    private byte[] toJson(AccessToken accessToken) {
        // GCS session credentials are a bearer token. Access key fields stay empty.
        org.apache.fluss.fs.token.Credentials flussCredentials =
                new org.apache.fluss.fs.token.Credentials("", "", accessToken.getTokenValue());
        return CredentialsJsonSerde.toJson(flussCredentials);
    }

    private static GoogleCredentials extractProvider(Configuration conf) {
        final String authType = conf.get(AUTH_TYPE_KEY);
        if (authType == null || authType.isEmpty()) {
            // The bundled connector ignores fs.gs.auth.type and uses the key file when one is set.
            // Minting from ADC here would hand clients a different identity than the server writes
            // with.
            if (isConfigured(serviceAccountKeyFile(conf))) {
                return getServiceAccountCredentials(conf);
            }
            return applicationDefaultCredentials();
        } else if (AUTH_TYPE_APPLICATION_DEFAULT.equals(authType)) {
            return applicationDefaultCredentials();
        } else if (AUTH_TYPE_COMPUTE_ENGINE.equals(authType)) {
            return ComputeEngineCredentials.newBuilder().build();
        } else if (AUTH_TYPE_SERVICE_ACCOUNT_JSON_KEYFILE.equals(authType)) {
            return getServiceAccountCredentials(conf);
        } else if (AUTH_TYPE_UNAUTHENTICATED.equals(authType)) {
            throw new IllegalArgumentException(
                    "UNAUTHENTICATED cannot obtain a Google Cloud Storage security token.");
        } else {
            throw new IllegalArgumentException("Unsupported authentication type: " + authType);
        }
    }

    private static GoogleCredentials applicationDefaultCredentials() {
        final GoogleCredentials credentials;
        try {
            credentials = GoogleCredentials.getApplicationDefault();
        } catch (IOException e) {
            throw new IllegalArgumentException(
                    "Application default credentials are not available. "
                            + "Run where Application Default Credentials can be resolved, "
                            + "set fs.gs.auth.type to COMPUTE_ENGINE on a GCE instance, "
                            + "or set fs.gs.auth.type to SERVICE_ACCOUNT_JSON_KEYFILE.",
                    e);
        }
        return requireServiceAccount(credentials);
    }

    /**
     * User credentials ignore {@code createScoped} and keep the scopes granted at login, usually
     * {@code cloud-platform}. Distributing that bearer would give every client the server user's
     * access. Service account and Compute Engine credentials honor the read-only scope.
     */
    private static GoogleCredentials requireServiceAccount(GoogleCredentials credentials) {
        if (credentials instanceof ServiceAccountCredentials
                || credentials instanceof ComputeEngineCredentials) {
            return credentials;
        }
        throw new IllegalArgumentException(
                "Only a service account can mint a Cloud Storage security token. "
                        + "Application default credentials resolved to "
                        + credentials.getClass().getSimpleName()
                        + ", which cannot be limited to "
                        + STORAGE_SCOPE
                        + ". Use a service account JSON key file, or run where Application "
                        + "Default Credentials are a service account (GCE or GKE Workload Identity).");
    }

    private static ServiceAccountCredentials getServiceAccountCredentials(Configuration conf) {
        String keyFile = serviceAccountKeyFile(conf);
        if (!isConfigured(keyFile)) {
            throw new IllegalArgumentException(
                    JSON_KEYFILE_KEY + " must be set to obtain a security token.");
        }
        try (FileInputStream fis = new FileInputStream(keyFile)) {
            return ServiceAccountCredentials.fromStream(fis);
        } catch (IOException e) {
            throw new FlussRuntimeException("Fail to read service account json file", e);
        }
    }

    private static String serviceAccountKeyFile(Configuration conf) {
        List<String> prefixes = new ArrayList<>();
        // Suffixes already start with '.', so the prefix must not.
        prefixes.add("fs.gs");
        return SERVICE_ACCOUNT_JSON_KEYFILE_SUFFIX.withPrefixes(prefixes).get(conf, conf::get);
    }

    private static boolean isConfigured(String value) {
        return value != null && !value.isEmpty();
    }
}
