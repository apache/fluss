/*
 *  Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.alibaba.fluss.rpc.netty.authenticate;

import com.alibaba.fluss.config.ConfigOption;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.exception.AuthenticationException;
import com.alibaba.fluss.security.acl.FlussPrincipal;
import com.alibaba.fluss.security.auth.ClientAuthenticationPlugin;
import com.alibaba.fluss.security.auth.ClientAuthenticator;
import com.alibaba.fluss.security.auth.ServerAuthenticationPlugin;
import com.alibaba.fluss.security.auth.ServerAuthenticator;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Map;

import static com.alibaba.fluss.config.ConfigBuilder.key;

/** A test authentication plugin which need username and password. */
public class UsernamePasswordAuthenticationPlugin
        implements ServerAuthenticationPlugin, ClientAuthenticationPlugin {

    private static final ConfigOption<String> CLIENT_USERNAME =
            key("client.security.username_password.username").stringType().noDefaultValue();

    private static final ConfigOption<String> CLIENT_PASSWORD =
            key("client.security.username_password.password").stringType().noDefaultValue();

    private static final ConfigOption<Map<String, String>> CREDENTIALS =
            key("security.username_password.credentials")
                    .mapType()
                    .defaultValue(Collections.emptyMap());

    private static final String AUTH_PROTOCOL = "username_password";

    @Override
    public String authProtocol() {
        return AUTH_PROTOCOL;
    }

    @Override
    public ClientAuthenticator createClientAuthenticator(Configuration configuration) {
        String username = configuration.getString(CLIENT_USERNAME);
        String password = configuration.getString(CLIENT_PASSWORD);
        if (username == null || password == null) {
            throw new AuthenticationException("username and password shouldn't be null.");
        }
        return new ClientAuthenticator() {
            volatile boolean isComplete = false;

            @Override
            public String protocol() {
                return AUTH_PROTOCOL;
            }

            @Override
            public byte[] authenticate(byte[] data) throws AuthenticationException {
                isComplete = true;
                return serializeToken(username, password);
            }

            @Override
            public boolean isCompleted() {
                return isComplete;
            }
        };
    }

    @Override
    public ServerAuthenticator createServerAuthenticator(Configuration configuration) {
        Map<String, String> credentials = configuration.getMap(CREDENTIALS);
        return new ServerAuthenticator() {
            volatile boolean isComplete = false;
            volatile FlussPrincipal flussPrincipal;

            @Override
            public String protocol() {
                return AUTH_PROTOCOL;
            }

            @Override
            public byte[] evaluateResponse(byte[] token) throws AuthenticationException {
                String[] credential = deserializeToken(token);
                if (!credentials.containsKey(credential[0])
                        || !credentials.get(credential[0]).equals(credential[1])) {
                    throw new AuthenticationException("username or password is incorrect.");
                }

                isComplete = true;
                flussPrincipal = new FlussPrincipal(credential[0], "USER");

                return new byte[0];
            }

            @Override
            public FlussPrincipal createPrincipal() {
                return flussPrincipal;
            }

            @Override
            public boolean isCompleted() {
                return isComplete;
            }
        };
    }

    private byte[] serializeToken(String username, String password) {
        return (username + "," + password).getBytes(StandardCharsets.UTF_8);
    }

    private String[] deserializeToken(byte[] token) {
        String[] parts = new String(token, StandardCharsets.UTF_8).split(",");
        if (parts.length != 2) {
            throw new AuthenticationException("Invalid token format.");
        }
        return parts;
    }
}
