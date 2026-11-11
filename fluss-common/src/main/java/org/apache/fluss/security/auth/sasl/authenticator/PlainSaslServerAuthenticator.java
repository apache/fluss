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

package org.apache.fluss.security.auth.sasl.authenticator;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.AuthenticationException;
import org.apache.fluss.security.acl.FlussPrincipal;
import org.apache.fluss.security.auth.ServerAuthenticator;
import org.apache.fluss.security.auth.sasl.jaas.JaasContext;
import org.apache.fluss.security.auth.sasl.jaas.LoginManager;
import org.apache.fluss.security.auth.sasl.jaas.SaslServerFactory;
import org.apache.fluss.security.auth.sasl.plain.PlainSaslProxyAuthorization;
import org.apache.fluss.security.auth.sasl.plain.PlainSaslServer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.sasl.SaslException;
import javax.security.sasl.SaslServer;

import java.util.Locale;

import static org.apache.fluss.config.ConfigBuilder.key;

/** A connection-local SASL/PLAIN server authenticator. */
@Internal
public final class PlainSaslServerAuthenticator implements ServerAuthenticator {
    private static final Logger LOG = LoggerFactory.getLogger(PlainSaslServerAuthenticator.class);
    private static final String SERVER_AUTHENTICATOR_PREFIX = "security.sasl.";

    private final Configuration configuration;
    private final PlainSaslProxyAuthorization proxyAuthorization;
    private SaslServer saslServer;
    private LoginManager loginManager;

    /** Creates an authenticator that reads the effective credentials when initialized. */
    public PlainSaslServerAuthenticator(
            Configuration configuration, PlainSaslProxyAuthorization proxyAuthorization) {
        this.configuration = configuration;
        this.proxyAuthorization = proxyAuthorization;
    }

    @Override
    public String protocol() {
        return PlainSaslServer.PLAIN_MECHANISM;
    }

    @Override
    public void initialize(AuthenticateContext context) {
        close();
        String dynamicJaasConfig = findJaasConfig(context.listenerName());
        JaasContext contextConfig =
                JaasContext.loadServerContext(context.listenerName(), dynamicJaasConfig);
        LoginManager acquiredLoginManager = null;
        try {
            acquiredLoginManager = LoginManager.acquireLoginManager(contextConfig);
            SaslServer newSaslServer =
                    SaslServerFactory.createSaslServer(
                            PlainSaslServer.PLAIN_MECHANISM,
                            contextConfig.configurationEntries(),
                            proxyAuthorization);
            loginManager = acquiredLoginManager;
            saslServer = newSaslServer;
        } catch (Exception e) {
            if (acquiredLoginManager != null) {
                acquiredLoginManager.release();
            }
            throw new RuntimeException(e);
        }
    }

    @Override
    public byte[] evaluateResponse(byte[] token) throws AuthenticationException {
        try {
            return saslServer.evaluateResponse(token);
        } catch (SaslException e) {
            throw new AuthenticationException(
                    String.format(
                            "Failed to evaluate SASL response, reason is %s", e.getMessage()));
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

    @Override
    public void close() {
        if (saslServer != null) {
            try {
                saslServer.dispose();
            } catch (SaslException e) {
                LOG.debug("Failed to dispose SASL server.", e);
            } finally {
                saslServer = null;
            }
        }
        if (loginManager != null) {
            loginManager.release();
            loginManager = null;
        }
    }

    private String findJaasConfig(String listenerName) {
        String listenerMechanismKey =
                String.format(
                        SERVER_AUTHENTICATOR_PREFIX
                                + "listener.name.%s.%s."
                                + JaasContext.SASL_JAAS_CONFIG,
                        listenerName.toLowerCase(Locale.ROOT),
                        PlainSaslServer.PLAIN_MECHANISM.toLowerCase(Locale.ROOT));
        String dynamicJaasConfig =
                configuration.get(key(listenerMechanismKey).stringType().noDefaultValue());
        if (dynamicJaasConfig != null && !dynamicJaasConfig.isEmpty()) {
            return dynamicJaasConfig;
        }

        String globalMechanismKey = ConfigOptions.SERVER_SASL_PLAIN_JAAS_CONFIG.key();
        LOG.debug(
                "No listener-mechanism JAAS config found for key: '{}'. Falling back to mechanism-level config: '{}'",
                listenerMechanismKey,
                globalMechanismKey);
        dynamicJaasConfig = configuration.get(ConfigOptions.SERVER_SASL_PLAIN_JAAS_CONFIG);
        if (dynamicJaasConfig == null || dynamicJaasConfig.isEmpty()) {
            LOG.warn(
                    "No mechanism-level JAAS config found for key: '{}'. Falling back to JVM option: -D{}",
                    globalMechanismKey,
                    JaasContext.JAVA_LOGIN_CONFIG_PARAM);
        }
        return dynamicJaasConfig;
    }
}
