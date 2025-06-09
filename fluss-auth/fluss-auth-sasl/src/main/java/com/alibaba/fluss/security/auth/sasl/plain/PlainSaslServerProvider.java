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

package com.alibaba.fluss.security.auth.sasl.plain;

import java.security.Provider;
import java.security.Security;

/* This file is based on source code of Apache Kafka Project (https://kafka.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/**
 * Provider for SASL/PLAIN authentication. Only register this provider, SaslServerFactory can be
 * found.
 */
public class PlainSaslServerProvider extends Provider {

    private static final long serialVersionUID = 1L;

    @SuppressWarnings("this-escape")
    protected PlainSaslServerProvider() {
        super(
                "Simple SASL/PLAIN Server Provider",
                1.0,
                "Simple SASL/PLAIN Server Provider for Fluss");
        put(
                "SaslServerFactory." + PlainSaslServer.PLAIN_MECHANISM,
                PlainSaslServer.PlainSaslServerFactory.class.getName());
    }

    public static void initialize() {
        Security.addProvider(new PlainSaslServerProvider());
    }
}
