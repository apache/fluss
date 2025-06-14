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
import com.alibaba.fluss.security.acl.FlussPrincipal;
import com.alibaba.fluss.security.auth.ServerAuthenticator;
import com.alibaba.fluss.security.auth.sasl.jaas.JaasContext;
import com.alibaba.fluss.security.auth.sasl.jaas.LoginManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.sasl.SaslException;
import javax.security.sasl.SaslServer;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

import static com.alibaba.fluss.config.ConfigOptions.SERVER_SASL_ENABLED_MECHANISMS_CONFIG;
import static com.alibaba.fluss.security.auth.sasl.authenticator.SaslAuthenticationPlugin.SASL_AUTH_PROTOCOL;
import static com.alibaba.fluss.security.auth.sasl.jaas.JaasContext.SASL_JAAS_CONFIG;
import static com.alibaba.fluss.security.auth.sasl.jaas.SaslServerFactory.createSaslServer;

/** An authenticator that uses SASL to authenticate clients. */
public class SaslServerAuthenticator implements ServerAuthenticator {
    private static final Logger LOG = LoggerFactory.getLogger(SaslServerAuthenticator.class);
    private static final String SERVER_AUTHENTICATOR_PREFIX = "security.sasl.";
    private final List<String> enabledMechanisms;
    private SaslServer saslServer;
    private final Map<String, String> configs;

    public SaslServerAuthenticator(Configuration configuration) {
        this.configs = configuration.toMap();
        List<String> enabledMechanisms = configuration.get(SERVER_SASL_ENABLED_MECHANISMS_CONFIG);
        if (enabledMechanisms == null || enabledMechanisms.isEmpty()) {
            throw new IllegalArgumentException("No SASL mechanisms are enabled");
        }
        this.enabledMechanisms =
                enabledMechanisms.stream().map(String::toUpperCase).collect(Collectors.toList());
    }

    @Override
    public void initialize(AuthenticateContext context) {
        String mechanism = context.protocol();
        String listenerName = context.listenerName();
        String address = context.ipAddress();
        matchProtocol(mechanism);
        // Try to load JAAS config in the following order:
        // 1. security.sasl.listener.name.{listenerName}.{mechanism}.jaas.config (fine-grained per
        // listener and mechanism)
        // 2. security.sasl.{mechanism}.jaas.config (fallback global config for mechanism)
        // 3. JVM option -Djava.security.auth.login.config (system-level fallback)

        String dynamicJaasConfig;

        // 1. Check listener-specific and mechanism-specific config
        String listenerMechanismKey =
                String.format(
                        SERVER_AUTHENTICATOR_PREFIX + "listener.name.%s.%s." + SASL_JAAS_CONFIG,
                        listenerName.toLowerCase(Locale.ROOT),
                        mechanism.toLowerCase(Locale.ROOT));
        dynamicJaasConfig = configs.get(listenerMechanismKey);

        if (dynamicJaasConfig == null || dynamicJaasConfig.isEmpty()) {
            String globalMechanismKey =
                    SERVER_AUTHENTICATOR_PREFIX
                            + mechanism.toLowerCase(Locale.ROOT)
                            + "."
                            + SASL_JAAS_CONFIG;
            LOG.debug(
                    "No listener-mechanism JAAS config found for key: '{}'. Falling back to mechanism-level config: '{}'",
                    listenerMechanismKey,
                    globalMechanismKey);
            // 2. Fallback to global mechanism-level config
            dynamicJaasConfig = configs.get(globalMechanismKey);
            if (dynamicJaasConfig == null || dynamicJaasConfig.isEmpty()) {
                LOG.warn(
                        "No mechanism-level JAAS config found for key: '{}'. Falling back to JVM option: -D{}",
                        globalMechanismKey,
                        JaasContext.JAVA_LOGIN_CONFIG_PARAM);
            }
        }

        JaasContext jaasContext = JaasContext.loadServerContext(listenerName, dynamicJaasConfig);

        try {
            LoginManager loginManager = LoginManager.acquireLoginManager(jaasContext);
            saslServer =
                    createSaslServer(
                            mechanism,
                            address,
                            configs,
                            loginManager,
                            jaasContext.configurationEntries());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String protocol() {
        return SASL_AUTH_PROTOCOL;
    }

    @Override
    public void matchProtocol(String protocol) {
        if (!enabledMechanisms.contains(protocol.toUpperCase())) {
            throw new AuthenticationException(
                    String.format(
                            "SASL server enables %s while protocol of client is '%s'",
                            enabledMechanisms, protocol));
        }
    }

    @Override
    public byte[] evaluateResponse(byte[] token) throws AuthenticationException {
        try {
            return saslServer.evaluateResponse(token);
        } catch (SaslException e) {
            throw new AuthenticationException(
                    String.format("Failed to evaluate SASL response，reason is %s", e.getMessage()));
        }
    }

    @Override
    public boolean isCompleted() {
        return saslServer != null && saslServer.isComplete();
    }

    @Override
    public FlussPrincipal createPrincipal() {
        return new FlussPrincipal(saslServer.getAuthorizationID(), "User");
    }
}
