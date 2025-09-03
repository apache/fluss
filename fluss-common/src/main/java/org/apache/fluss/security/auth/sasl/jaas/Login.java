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

import javax.security.auth.Subject;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;

/* This file is based on source code of Apache Kafka Project (https://kafka.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/**
 * An interface representing a JAAS login module responsible for authentication and managing the
 * {@link LoginContext} lifecycle.
 *
 * <p>Implementations of this interface are expected to handle user authentication using configured
 * JAAS login modules, and provide access to the authenticated {@link Subject}. This interface is
 * typically used in SASL-based authentication flows on both client and server sides.
 *
 * <p>The implementing class should manage the full lifecycle of the {@link LoginContext},
 * including:
 *
 * <ul>
 *   <li>Initialization via {@link #configure(String, javax.security.auth.login.Configuration)}
 *   <li>Authentication via {@link #login()}
 *   <li>Cleanup via {@link #close()}
 * </ul>
 */
public interface Login {

    /**
     * Configures this jaas instance.
     *
     * @param contextName JAAS context name for this jaas which may be used to obtain the jaas
     *     context from `jaasConfiguration`.
     * @param jaasConfiguration JAAS configuration containing the jaas context named `contextName`.
     *     If static JAAS configuration is used, this `Configuration` may also contain other jaas
     *     contexts.
     */
    void configure(String contextName, javax.security.auth.login.Configuration jaasConfiguration);

    /** Performs jaas for each jaas module specified for the jaas context of this instance. */
    LoginContext login() throws LoginException;

    /** Returns the authenticated subject of this jaas context. */
    Subject subject();

    /** Returns the service name to be used for SASL. */
    String serviceName();

    /** Closes this instance. */
    void close();
}
