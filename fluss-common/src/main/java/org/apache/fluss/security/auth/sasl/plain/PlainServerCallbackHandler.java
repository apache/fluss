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

package org.apache.fluss.security.auth.sasl.plain;

import org.apache.fluss.security.auth.sasl.jaas.AuthenticateCallbackHandler;
import org.apache.fluss.security.auth.sasl.jaas.JaasContext;
import org.apache.fluss.utils.ArrayUtils;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.AppConfigurationEntry;

import java.util.List;

/* This file is based on source code of Apache Kafka Project (https://kafka.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/**
 * A {@link javax.security.auth.callback.CallbackHandler} implementation used by the SASL PLAIN
 * mechanism on the server side.
 *
 * <p>This handler is responsible for validating the username and password provided by the client
 * against the credentials configured in the JAAS configuration entries. It works with {@link
 * PlainLoginModule} and expects user credentials to be defined using the format:
 *
 * <pre>
 * user_${username} = ${password}
 * </pre>
 *
 * <p>During the SASL authentication process, this handler processes callbacks such as:
 *
 * <ul>
 *   <li>{@link NameCallback}: Retrieves the username from the client.
 *   <li>{@link PlainAuthenticateCallback}: Verifies the provided password against the expected
 *       value.
 * </ul>
 *
 * <p>If the username is not found or the password does not match, authentication will fail.
 */
public class PlainServerCallbackHandler implements AuthenticateCallbackHandler {
    private static final String JAAS_USER_PREFIX = "user_";
    private List<AppConfigurationEntry> jaasConfigEntries;

    @Override
    public void configure(String saslMechanism, List<AppConfigurationEntry> jaasConfigEntries) {
        this.jaasConfigEntries = jaasConfigEntries;
    }

    @Override
    public void handle(Callback[] callbacks) throws UnsupportedCallbackException {
        String username = null;
        for (Callback callback : callbacks) {
            if (callback instanceof NameCallback) {
                username = ((NameCallback) callback).getDefaultName();
            } else if (callback instanceof PlainAuthenticateCallback) {
                PlainAuthenticateCallback plainCallback = (PlainAuthenticateCallback) callback;
                boolean authenticated = authenticate(username, plainCallback.password());
                plainCallback.authenticated(authenticated);
            } else {
                throw new UnsupportedCallbackException(callback);
            }
        }
    }

    protected boolean authenticate(String username, char[] password) {
        if (username == null) {
            return false;
        } else {
            String expectedPassword =
                    JaasContext.configEntryOption(
                            jaasConfigEntries,
                            JAAS_USER_PREFIX + username,
                            PlainLoginModule.class.getName());
            return expectedPassword != null
                    && ArrayUtils.isEqualConstantTime(password, expectedPassword.toCharArray());
        }
    }
}
