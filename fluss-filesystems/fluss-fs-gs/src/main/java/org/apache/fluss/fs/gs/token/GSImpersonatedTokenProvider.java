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
import com.google.auth.oauth2.ImpersonatedCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static com.google.cloud.hadoop.util.HadoopCredentialConfiguration.SERVICE_ACCOUNT_JSON_KEYFILE_SUFFIX;
import static org.apache.fluss.fs.gs.GSFileSystemPlugin.HADOOP_CONFIG_PREFIX;

/** Impersonation token provider for GCS Hadoop filesystems. */
public class GSImpersonatedTokenProvider {

    private static final Logger LOG = LoggerFactory.getLogger(GSImpersonatedTokenProvider.class);

    private final String scheme;

    private GoogleCredentials googleCredentials;
    private String targetPrincipal;

    private static final String AUTH_TYPE_SUFFIX = ".auth.type";

    public GSImpersonatedTokenProvider(String scheme, Configuration conf) {
        this.scheme = scheme;
        Pair<String, GoogleCredentials> pair = extractProvider(conf);
        googleCredentials = pair.getValue();
        targetPrincipal = pair.getKey();
    }

    public ObtainedSecurityToken obtainSecurityToken() {
        LOG.info("Obtaining session credentials token");

        String scope = "https://www.googleapis.com/auth/cloud-platform";

        List<String> scopes = new ArrayList<>();
        scopes.add(scope);

        ImpersonatedCredentials impersonatedCredentials =
                ImpersonatedCredentials.newBuilder()
                        .setSourceCredentials(googleCredentials)
                        .setTargetPrincipal(targetPrincipal)
                        .setScopes(scopes)
                        .setLifetime(3600)
                        .setDelegates(null)
                        .build();

        AccessToken accessToken = impersonatedCredentials.getAccessToken();
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
        org.apache.fluss.fs.token.Credentials flussCredentials =
                new org.apache.fluss.fs.token.Credentials(null, null, accessToken.getTokenValue());
        return CredentialsJsonSerde.toJson(flussCredentials);
    }

    private static Pair<String, GoogleCredentials> extractProvider(Configuration conf) {
        final String authType = conf.get(HADOOP_CONFIG_PREFIX + AUTH_TYPE_SUFFIX);
        if (authType.equals("COMPUTE_ENGINE")) {
            ComputeEngineCredentials credentials = getComputeEngineCredentials();
            return Pair.of(credentials.getAccount(), credentials);
        } else if (authType.equals("SERVICE_ACCOUNT_JSON_KEYFILE")) {
            ServiceAccountCredentials credentials = getServiceAccountCredentials(conf);
            return Pair.of(credentials.getAccount(), credentials);
        } else if (authType.equals("UNAUTHENTICATED")) {
            return null;
        } else {
            throw new IllegalArgumentException("Unsupported authentication type: " + authType);
        }
    }

    private static ComputeEngineCredentials getComputeEngineCredentials() {
        ComputeEngineCredentials credentials = ComputeEngineCredentials.newBuilder().build();
        credentials.getAccount();
        return credentials;
    }

    private static ServiceAccountCredentials getServiceAccountCredentials(Configuration conf) {
        List<String> prefixes = new ArrayList<>();
        prefixes.add(HADOOP_CONFIG_PREFIX);

        String keyFile =
                SERVICE_ACCOUNT_JSON_KEYFILE_SUFFIX.withPrefixes(prefixes).get(conf, conf::get);
        try (FileInputStream fis = new FileInputStream(keyFile)) {
            ServiceAccountCredentials accountCredentials =
                    ServiceAccountCredentials.fromStream(fis);
            accountCredentials.getAccount();
            return accountCredentials;
        } catch (IOException e) {
            throw new FlussRuntimeException("Fail to read service account json file" + e);
        }
    }
}
