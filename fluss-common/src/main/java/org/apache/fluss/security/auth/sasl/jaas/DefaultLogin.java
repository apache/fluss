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

package org.apache.fluss.security.auth.sasl.jaas;

import org.apache.fluss.utils.TemporaryClassLoaderContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.security.auth.Subject;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;

/* This file is based on source code of Apache Kafka Project (https://kafka.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/** DefaultLogin is a default implementation of {@link Login}. */
public class DefaultLogin implements Login {
    private static final Logger LOG = LoggerFactory.getLogger(DefaultLogin.class);
    private String contextName;
    private @Nullable javax.security.auth.login.Configuration jaasConfig;
    private LoginContext loginContext;

    @Override
    public void configure(String contextName, javax.security.auth.login.Configuration jaasConfig) {
        this.contextName = contextName;
        this.jaasConfig = jaasConfig;
    }

    @Override
    public LoginContext login() throws LoginException {
        try (TemporaryClassLoaderContext ignored =
                TemporaryClassLoaderContext.of(DefaultLogin.class.getClassLoader())) {
            loginContext =
                    new LoginContext(
                            contextName,
                            null,
                            callbacks -> {
                                // Nothing here until we support some mechanisms such as sasl/GSSAPI
                                // later.
                                throw new UnsupportedCallbackException(
                                        callbacks[0], "Unrecognized SASL mechanism.");
                            },
                            jaasConfig);
            loginContext.login();
        }
        LOG.info("Successfully logged in.");
        return loginContext;
    }

    @Override
    public Subject subject() {
        return loginContext.getSubject();
    }

    @Override
    public String serviceName() {
        return contextName;
    }

    @Override
    public void close() {}
}
