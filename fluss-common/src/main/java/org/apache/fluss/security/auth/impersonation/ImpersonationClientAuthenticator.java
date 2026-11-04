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
import org.apache.fluss.security.auth.ClientAuthenticator;

import static org.apache.fluss.config.ConfigOptions.CLIENT_IMPERSONATION_ACT_AS_USER;
import static org.apache.fluss.config.ConfigOptions.CLIENT_SASL_JAAS_PASSWORD;
import static org.apache.fluss.config.ConfigOptions.CLIENT_SASL_JAAS_USERNAME;
import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkState;

/** Sends credentials and an optional target user using the impersonation protocol. */
public class ImpersonationClientAuthenticator implements ClientAuthenticator {

    private final String username;
    private final String password;
    private final String actAsUser;
    private boolean completed;

    /** Creates a client using explicit account credentials and an optional impersonation target. */
    public ImpersonationClientAuthenticator(Configuration configuration) {
        this.username = configuration.get(CLIENT_SASL_JAAS_USERNAME);
        this.password = configuration.get(CLIENT_SASL_JAAS_PASSWORD);
        this.actAsUser = configuration.getOptional(CLIENT_IMPERSONATION_ACT_AS_USER).orElse("");
        checkArgument(
                username != null && !username.isEmpty(),
                "%s must be configured for impersonation",
                CLIENT_SASL_JAAS_USERNAME.key());
        checkArgument(
                password != null && !password.isEmpty(),
                "%s must be configured for impersonation",
                CLIENT_SASL_JAAS_PASSWORD.key());
    }

    @Override
    public String protocol() {
        return ImpersonationAuthenticationPlugin.IMPERSONATION_AUTH_PROTOCOL;
    }

    @Override
    public void initialize(AuthenticateContext context) {
        completed = false;
    }

    @Override
    public byte[] authenticate(byte[] data) {
        checkState(!completed, "Authentication exchange has already completed");
        checkArgument(data.length == 0, "Impersonation authentication does not accept challenges");
        byte[] token = ImpersonationToken.encode(username, password, actAsUser);
        completed = true;
        return token;
    }

    @Override
    public boolean isCompleted() {
        return completed;
    }

    @Override
    public void close() {
        completed = false;
    }
}
