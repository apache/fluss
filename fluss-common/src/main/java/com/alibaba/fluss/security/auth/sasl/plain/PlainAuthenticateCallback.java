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

package com.alibaba.fluss.security.auth.sasl.plain;

import javax.security.auth.callback.Callback;

/* This file is based on source code of Apache Kafka Project (https://kafka.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/**
 * A {@link Callback} implementation used during SASL/PLAIN authentication to pass the
 * client-provided username and password to the server-side {@link PlainServerCallbackHandler} for
 * validation.
 *
 * <p>This class acts as a bridge between:
 *
 * <ul>
 *   <li>{@link PlainSaslServer}: which extracts the username and password from the client's SASL
 *       response
 *   <li>{@link PlainServerCallbackHandler}: which performs the actual authentication check using
 *       JAAS configuration
 * </ul>
 *
 * <p>The SASL framework uses this callback to securely pass sensitive credentials through the
 * configured {@link Callback}, allowing modular and extensible authentication logic.
 */
public class PlainAuthenticateCallback implements Callback {

    private final char[] password;
    private boolean authenticated;

    /**
     * Creates a callback with the password provided by the client.
     *
     * @param password The password provided by the client during SASL/PLAIN authentication.
     */
    public PlainAuthenticateCallback(char[] password) {
        this.password = password;
    }

    /** Returns the password provided by the client during SASL/PLAIN authentication. */
    public char[] password() {
        return password;
    }

    /**
     * Returns true if client password matches expected password, false otherwise. This state is set
     * the server-side callback handler.
     */
    public boolean authenticated() {
        return this.authenticated;
    }

    /**
     * Sets the authenticated state. This is set by the server-side callback handler by matching the
     * client provided password with the expected password.
     *
     * @param authenticated true indicates successful authentication.
     */
    public void authenticated(boolean authenticated) {
        this.authenticated = authenticated;
    }
}
