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

package com.alibaba.fluss.security.auth;

import com.alibaba.fluss.config.Configuration;

/**
 * A {@link AuthenticationPlugin} that is conflicting with the {@link
 * TestConflictingAuthenticationPlugin2}.
 */
public class TestConflictingAuthenticationPlugin1
        implements ClientAuthenticationPlugin, ServerAuthenticationPlugin {

    public String authProtocol() {
        return "conflicting";
    }

    @Override
    public ServerAuthenticator createServerAuthenticator(Configuration configuration) {
        return null;
    }

    public ClientAuthenticator createClientAuthenticator(Configuration configuration) {
        return null;
    }
}
