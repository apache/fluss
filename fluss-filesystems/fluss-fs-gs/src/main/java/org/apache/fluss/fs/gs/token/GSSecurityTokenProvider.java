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
 * Mints a short-lived Cloud Storage access token from the server's service account key or Compute
 * Engine identity. Clients receive that bearer token and do not need the key file.
 */
public class GSSecurityTokenProvider {

    private static final Logger LOG = LoggerFactory.getLogger(GSSecurityTokenProvider.class);

    private static final String AUTH_TYPE_KEY = "fs.gs.auth.type";

    private static final String JSON_KEYFILE_KEY = "fs.gs.auth.service.account.json.keyfile";

    /**
     * Cloud Storage read/write scope. This stays on the storage API instead of cloud-platform,
     * which would authorize every Google API the account can call.
     */
    static final String STORAGE_SCOPE = "https://www.googleapis.com/auth/devstorage.read_write";

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
        if (authType == null) {
            throw new IllegalArgumentException(
                    "fs.gs.auth.type must be set to obtain a security token.");
        } else if (authType.equals("COMPUTE_ENGINE")) {
            return ComputeEngineCredentials.newBuilder().build();
        } else if (authType.equals("SERVICE_ACCOUNT_JSON_KEYFILE")) {
            return getServiceAccountCredentials(conf);
        } else if (authType.equals("UNAUTHENTICATED")) {
            throw new IllegalArgumentException(
                    "UNAUTHENTICATED cannot obtain a Google Cloud Storage security token.");
        } else {
            throw new IllegalArgumentException("Unsupported authentication type: " + authType);
        }
    }

    private static ServiceAccountCredentials getServiceAccountCredentials(Configuration conf) {
        List<String> prefixes = new ArrayList<>();
        // Suffixes already start with '.', so the prefix must not.
        prefixes.add("fs.gs");

        String keyFile =
                SERVICE_ACCOUNT_JSON_KEYFILE_SUFFIX.withPrefixes(prefixes).get(conf, conf::get);
        if (keyFile == null || keyFile.isEmpty()) {
            throw new IllegalArgumentException(
                    JSON_KEYFILE_KEY + " must be set to obtain a security token.");
        }
        try (FileInputStream fis = new FileInputStream(keyFile)) {
            return ServiceAccountCredentials.fromStream(fis);
        } catch (IOException e) {
            throw new FlussRuntimeException("Fail to read service account json file", e);
        }
    }
}
