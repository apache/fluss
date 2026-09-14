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

package org.apache.fluss.security.auth.impersonation;

import org.apache.fluss.config.Configuration;
import org.apache.fluss.security.auth.ClientAuthenticationPlugin;
import org.apache.fluss.security.auth.ClientAuthenticator;
import org.apache.fluss.security.auth.ServerAuthenticationPlugin;
import org.apache.fluss.security.auth.ServerAuthenticator;

/**
 * Authentication plugin for SASL that additionally lets a user act as another user.
 *
 * <p>This is a separate protocol from {@code sasl} on purpose, so that enabling impersonation is an
 * explicit per-listener decision: a listener using {@code sasl} always rejects a client that asks
 * to act as another user. The client names the user it wants to act as through {@code
 * client.security.sasl.authorization-id}, which the server grants only per {@code
 * security.impersonation.proxy-users}.
 */
public class ImpersonationAuthenticationPlugin
        implements ClientAuthenticationPlugin, ServerAuthenticationPlugin {

    static final String IMPERSONATION_AUTH_PROTOCOL = "sasl-impersonation";

    @Override
    public String authProtocol() {
        return IMPERSONATION_AUTH_PROTOCOL;
    }

    @Override
    public ClientAuthenticator createClientAuthenticator(Configuration configuration) {
        return new ImpersonationClientAuthenticator(configuration);
    }

    @Override
    public ServerAuthenticator createServerAuthenticator(Configuration configuration) {
        return new ImpersonationServerAuthenticator(configuration);
    }
}
