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

package com.alibaba.fluss.security.auth.sasl.jaas;

import javax.security.auth.Subject;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.spi.LoginModule;

import java.util.Map;

/** A login module that authenticates users based on a username and password which is digested. */
public class DigestLoginModule implements LoginModule {
    private static final String USERNAME_CONFIG = "username";
    private static final String PASSWORD_CONFIG = "password";

    @Override
    public void initialize(
            Subject subject,
            CallbackHandler callbackHandler,
            Map<String, ?> sharedState,
            Map<String, ?> options) {
        String username = (String) options.get(USERNAME_CONFIG);
        if (username != null) {
            subject.getPublicCredentials().add(username);
        }
        String password = (String) options.get(PASSWORD_CONFIG);
        if (password != null) {
            subject.getPrivateCredentials().add(password);
        }
    }

    @Override
    public boolean login() {
        // Unlike with Krb5LoginModule, we don't do any actual jaas or credential passing here:
        // authentication to Zookeeper
        // is done later, through the SASLClient object.
        return true;
    }

    @Override
    public boolean logout() {
        return true;
    }

    @Override
    public boolean commit() {
        return true;
    }

    @Override
    public boolean abort() {
        return false;
    }
}
