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
import com.alibaba.fluss.security.auth.ClientAuthenticationPlugin;
import com.alibaba.fluss.security.auth.ClientAuthenticator;
import com.alibaba.fluss.security.auth.ServerAuthenticationPlugin;
import com.alibaba.fluss.security.auth.ServerAuthenticator;

/** Authentication plugin for SASL. */
public class SaslAuthenticationPlugin
        implements ClientAuthenticationPlugin, ServerAuthenticationPlugin {
    static final String SASL_AUTH_PROTOCOL = "sasl";

    @Override
    public ClientAuthenticator createClientAuthenticator(Configuration configuration) {
        return new SaslClientAuthenticator(configuration);
    }

    @Override
    public ServerAuthenticator createServerAuthenticator(Configuration configuration) {
        return new SaslServerAuthenticator(configuration);
    }

    @Override
    public String authProtocol() {
        return SASL_AUTH_PROTOCOL;
    }
}
