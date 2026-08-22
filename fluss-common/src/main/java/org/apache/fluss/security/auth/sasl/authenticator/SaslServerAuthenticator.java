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

import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.AuthenticationException;
import org.apache.fluss.security.acl.FlussPrincipal;
import org.apache.fluss.security.auth.ServerAuthenticator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

import static org.apache.fluss.config.ConfigOptions.SERVER_SASL_ENABLED_MECHANISMS_CONFIG;
import static org.apache.fluss.security.auth.sasl.authenticator.SaslAuthenticationPlugin.SASL_AUTH_PROTOCOL;

/** A connection-local authenticator that selects one SASL mechanism. */
public class SaslServerAuthenticator implements ServerAuthenticator {
    private static final Logger LOG = LoggerFactory.getLogger(SaslServerAuthenticator.class);

    private final List<String> enabledMechanisms;
    private final SaslServerMechanismFactory mechanismFactory;

    private ServerAuthenticator delegate;

    public SaslServerAuthenticator(
            Configuration configuration, SaslServerMechanismFactory mechanismFactory) {
        this.mechanismFactory = mechanismFactory;
        List<String> enabledMechanisms = configuration.get(SERVER_SASL_ENABLED_MECHANISMS_CONFIG);
        if (enabledMechanisms == null || enabledMechanisms.isEmpty()) {
            throw new IllegalArgumentException("No SASL mechanisms are enabled");
        }
        this.enabledMechanisms =
                enabledMechanisms.stream()
                        .map(mechanism -> mechanism.toUpperCase(Locale.ROOT))
                        .collect(Collectors.toList());
    }

    @Override
    public void initialize(AuthenticateContext context) {
        String mechanism = context.protocol().toUpperCase(Locale.ROOT);
        matchProtocol(mechanism);
        delegate = mechanismFactory.createAuthenticator(mechanism);
        delegate.initialize(context);
    }

    @Override
    public String protocol() {
        return SASL_AUTH_PROTOCOL;
    }

    @Override
    public void matchProtocol(String protocol) {
        if (!enabledMechanisms.contains(protocol.toUpperCase(Locale.ROOT))) {
            throw new AuthenticationException(
                    String.format(
                            "SASL server enables %s while protocol of client is '%s'",
                            enabledMechanisms, protocol));
        }
        if (!mechanismFactory.supportsMechanism(protocol)) {
            throw new AuthenticationException(
                    "Unable to find a matching SASL mechanism for "
                            + protocol.toUpperCase(Locale.ROOT));
        }
    }

    @Override
    public byte[] evaluateResponse(byte[] token) throws AuthenticationException {
        return delegate.evaluateResponse(token);
    }

    @Override
    public boolean isCompleted() {
        return delegate != null && delegate.isCompleted();
    }

    @Override
    public FlussPrincipal createPrincipal() {
        return delegate.createPrincipal();
    }

    @Override
    public void validateSession() throws AuthenticationException {
        delegate.validateSession();
    }

    @Override
    public void close() {
        if (delegate != null) {
            try {
                delegate.close();
            } catch (Exception e) {
                LOG.warn("Failed to close SASL server authenticator.", e);
            }
            delegate = null;
        }
    }
}
