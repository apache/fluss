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

package org.apache.fluss.security.auth;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.security.auth.sasl.jaas.JaasContext;
import org.apache.fluss.security.auth.sasl.plain.PlainLoginModule;
import org.apache.fluss.utils.ArrayUtils;

import javax.annotation.Nullable;
import javax.security.auth.login.AppConfigurationEntry;

import java.util.List;

import static org.apache.fluss.security.auth.sasl.plain.PlainSaslServer.PLAIN_MECHANISM;

/** Verifies user passwords against the user options of a configured JAAS login module. */
@Internal
public final class UserPasswordVerifier {

    private final List<AppConfigurationEntry> entries;
    private final String loginModuleName;

    /** Creates a verifier for the credentials of the given login module. */
    public UserPasswordVerifier(List<AppConfigurationEntry> entries, String loginModuleName) {
        this.entries = entries;
        this.loginModuleName = loginModuleName;
    }

    /**
     * Loads the shared PLAIN credential store, using listener-specific JAAS options before the
     * global JAAS options and the JVM JAAS configuration. The caller supplies the configuration
     * materialized by the server's credential manager, including dynamic credential updates.
     */
    public static UserPasswordVerifier forServer(Configuration configuration, String listenerName) {
        return new UserPasswordVerifier(
                JaasContext.loadServerContext(listenerName, PLAIN_MECHANISM, configuration.toMap())
                        .configurationEntries(),
                PlainLoginModule.class.getName());
    }

    /** Returns whether the username and password match a configured credential. */
    public boolean authenticate(@Nullable String username, char[] password) {
        if (username == null) {
            return false;
        }
        String expected =
                JaasContext.configEntryOption(entries, "user_" + username, loginModuleName);
        return expected != null && ArrayUtils.isEqualConstantTime(password, expected.toCharArray());
    }
}
