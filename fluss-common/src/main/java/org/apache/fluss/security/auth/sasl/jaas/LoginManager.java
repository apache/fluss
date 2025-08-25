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

import org.apache.fluss.annotation.VisibleForTesting;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.Subject;
import javax.security.auth.login.LoginException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/* This file is based on source code of Apache Kafka Project (https://kafka.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/**
 * LoginManager is a wrapper around a Login object. It is used to track the number of references to
 * a Login object.
 */
public class LoginManager {
    private static final Logger LOG = LoggerFactory.getLogger(DefaultLogin.class);

    private final Login login;

    private int refCount;
    private final String loginKey;

    /**
     * A global cache of LoginManager instances keyed by static JAAS configuration names (e.g.,
     * "FlussClient", "sasl_ssl.FlussServer"). These are typically loaded from system-wide JAAS
     * configurations such as the file specified via `-Djava.security.auth.login.config`.
     */
    private static final Map<String, LoginManager> STATIC_INSTANCES = new HashMap<>();

    /**
     * A global cache of LoginManager instances keyed by dynamic JAAS configuration strings. Each
     * key is the actual JAAS configuration string used to create the LoginModule, e.g.,
     * "com.example.SampleLoginModule required username=\"user\" password=\"pass\";".
     */
    private static final Map<String, LoginManager> DYNAMIC_INSTANCES = new HashMap<>();

    /**
     * Constructs a new LoginManager using the provided JAAS context.
     *
     * @param jaasContext the JAAS context containing the configuration name and underlying JAAS
     *     configuration
     * @param loginKey a string key used to uniquely identify and cache this LoginManager instance
     * @throws LoginException if the login operation fails due to invalid credentials, missing
     *     modules, or misconfigured JAAS settings
     */
    private LoginManager(JaasContext jaasContext, String loginKey) throws LoginException {
        this.login = new DefaultLogin();
        login.configure(jaasContext.name(), jaasContext.getConfiguration());
        login.login();
        this.loginKey = loginKey;
    }

    public Subject subject() {
        return login.subject();
    }

    public String serviceName() {
        return login.serviceName();
    }

    public static LoginManager acquireLoginManager(JaasContext jaasContext) throws LoginException {
        synchronized (LoginManager.class) {
            LoginManager loginManager;
            String jaasConfigValue = jaasContext.dynamicJaasConfig();
            if (jaasConfigValue != null) {
                loginManager = DYNAMIC_INSTANCES.get(jaasConfigValue);
                if (loginManager == null) {
                    loginManager = new LoginManager(jaasContext, jaasConfigValue);
                    DYNAMIC_INSTANCES.put(jaasConfigValue, loginManager);
                }
            } else {
                String jaasContextName = jaasContext.name();
                loginManager = STATIC_INSTANCES.get(jaasContextName);
                if (loginManager == null) {
                    loginManager = new LoginManager(jaasContext, jaasContextName);
                    STATIC_INSTANCES.put(jaasContextName, loginManager);
                }
            }
            return loginManager.acquire();
        }
    }

    private LoginManager acquire() {
        ++refCount;
        LOG.trace("{} acquired", this);
        return this;
    }

    /** Decrease the reference count for this instance and release resources if it reaches 0. */
    public void release() {
        synchronized (LoginManager.class) {
            if (refCount == 0) {
                throw new IllegalStateException("release() called on disposed " + this);
            } else if (refCount == 1) {
                login.close();
            }
            --refCount;
            LOG.trace("{} released", this);
        }
    }

    public static void closeAll() {
        synchronized (LoginManager.class) {
            for (String key : new ArrayList<>(STATIC_INSTANCES.keySet())) {
                STATIC_INSTANCES.remove(key).login.close();
            }
            for (String key : new ArrayList<>(DYNAMIC_INSTANCES.keySet())) {
                DYNAMIC_INSTANCES.remove(key).login.close();
            }
        }
    }

    @VisibleForTesting
    public String cacheKey() {
        return loginKey;
    }
}
