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
import org.apache.fluss.security.auth.sasl.authenticator.SaslClientAuthenticator;

import javax.annotation.Nullable;

import static org.apache.fluss.config.ConfigOptions.CLIENT_SASL_AUTHORIZATION_ID;

/**
 * A SASL client authenticator that additionally requests to act as another user.
 *
 * <p>The requested user is carried as the SASL authorization id (RFC 4616). When no user is
 * requested, this behaves identically to plain SASL authentication.
 */
public class ImpersonationClientAuthenticator extends SaslClientAuthenticator {

    @Nullable private final String authorizationId;

    public ImpersonationClientAuthenticator(Configuration configuration) {
        super(configuration);
        this.authorizationId = configuration.get(CLIENT_SASL_AUTHORIZATION_ID);
    }

    @Nullable
    @Override
    protected String authorizationId() {
        return authorizationId;
    }
}
