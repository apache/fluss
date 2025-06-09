/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.security.auth.sasl.authenticator;

import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.exception.AuthenticationException;
import com.alibaba.fluss.security.auth.ClientAuthenticator;
import com.alibaba.fluss.security.auth.sasl.jaas.JaasContext;
import com.alibaba.fluss.security.auth.sasl.jaas.LoginManager;

import javax.annotation.Nullable;
import javax.security.auth.login.LoginException;
import javax.security.sasl.SaslClient;

import java.util.Map;

import static com.alibaba.fluss.config.ConfigOptions.CLIENT_MECHANISM;
import static com.alibaba.fluss.config.ConfigOptions.CLIENT_SASL_JAAS_CONFIG;
import static com.alibaba.fluss.security.auth.sasl.jaas.SaslServerFactory.createSaslClient;

/** An authenticator that uses SASL to authenticate with a server. */
public class SaslClientAuthenticator implements ClientAuthenticator {
    private final String mechanism;
    private final Map<String, String> pros;
    private final String jaasConfig;

    private SaslClient saslClient;
    private LoginManager loginManager;

    public SaslClientAuthenticator(Configuration configuration) {
        this.mechanism = configuration.get(CLIENT_MECHANISM).toUpperCase();
        this.jaasConfig = configuration.getString(CLIENT_SASL_JAAS_CONFIG);
        this.pros = configuration.toMap();
    }

    @Override
    public String protocol() {
        return mechanism;
    }

    @Nullable
    @Override
    public byte[] authenticate(byte[] data) throws AuthenticationException {
        try {
            return saslClient.evaluateChallenge(data);
        } catch (Exception e) {
            throw new AuthenticationException("Failed to evaluate SASL challenge", e);
        }
    }

    @Override
    public boolean isCompleted() {
        return saslClient.isComplete();
    }

    @Override
    public boolean hasInitialTokenResponse() {
        return saslClient.hasInitialResponse();
    }

    @Override
    public void initialize(AuthenticateContext context) throws AuthenticationException {
        String hostAddress = context.ipAddress();
        JaasContext jaasContext = JaasContext.loadClientContext(jaasConfig);

        try {
            loginManager = LoginManager.acquireLoginManager(jaasContext);
        } catch (LoginException exception) {
            throw new AuthenticationException("Failed to load login manager", exception);
        }

        try {
            saslClient = createSaslClient(mechanism, hostAddress, pros, loginManager);
        } catch (Exception e) {
            throw new AuthenticationException("Failed to create SASL client", e);
        }

        if (saslClient == null) {
            throw new AuthenticationException(
                    "Unable to find a matching SASL mechanism for " + mechanism);
        }
    }

    @Override
    public void close() {
        if (loginManager != null) {
            loginManager.release();
        }
    }
}
