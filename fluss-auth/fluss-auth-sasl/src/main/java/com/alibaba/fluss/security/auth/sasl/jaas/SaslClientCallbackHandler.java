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
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.AppConfigurationEntry;

import java.security.AccessController;
import java.util.List;

/* This file is based on source code of Apache Kafka Project (https://kafka.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/**
 * A {@link AuthenticateCallbackHandler} implementation used by {@link
 * javax.security.sasl.SaslClient} to handle authentication callbacks.
 *
 * <p>This handler is responsible for responding to various SASL callback types during the
 * client-side authentication process, including:
 *
 * <ul>
 *   <li>{@link NameCallback}: Provides the username from the current subject or falls back to a
 *       default name.
 *   <li>{@link PasswordCallback}: Retrieves the password from the subject's private credentials. If
 *       unavailable, throws an exception since user interaction is not supported.
 * </ul>
 *
 * <p>It assumes that the authenticated {@link Subject} has already been established (e.g., via JAAS
 * login), and retrieves credentials from the subject’s public/private credential sets.
 *
 * <p>If no password is available in the subject and one is requested, this handler will throw an
 * {@link UnsupportedCallbackException}, as interactive password prompting is not supported.
 */
public class SaslClientCallbackHandler implements AuthenticateCallbackHandler {

    @Override
    public void configure(String saslMechanism, List<AppConfigurationEntry> jaasConfigEntries) {}

    @Override
    public void handle(Callback[] callbacks) throws UnsupportedCallbackException {
        Subject subject = Subject.getSubject(AccessController.getContext());
        for (Callback callback : callbacks) {
            if (callback instanceof NameCallback) {
                NameCallback nc = (NameCallback) callback;
                if (subject != null && !subject.getPublicCredentials(String.class).isEmpty()) {
                    nc.setName(subject.getPublicCredentials(String.class).iterator().next());
                } else {
                    nc.setName(nc.getDefaultName());
                }
            } else if (callback instanceof PasswordCallback) {
                if (subject != null && !subject.getPrivateCredentials(String.class).isEmpty()) {
                    char[] password =
                            subject.getPrivateCredentials(String.class)
                                    .iterator()
                                    .next()
                                    .toCharArray();
                    ((PasswordCallback) callback).setPassword(password);
                } else {
                    String errorMessage =
                            "Could not login: the client is being asked for a password, but the Fluss"
                                    + " client code does not currently support obtaining a password from the user.";
                    throw new UnsupportedCallbackException(callback, errorMessage);
                }
            } else {
                throw new UnsupportedCallbackException(
                        callback, "Unrecognized SASL ClientCallback");
            }
        }
    }
}
