/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.fluss.trino;

import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.session.PropertyMetadata;

import java.util.Collections;
import java.util.List;

import static io.trino.spi.StandardErrorCode.INVALID_SESSION_PROPERTY;
import static io.trino.spi.session.PropertyMetadata.integerProperty;

/** Session settings for physical scan pruning. */
final class FlussSessionProperties {
    static final int DEFAULT_DOMAIN_COMPACTION_THRESHOLD = 256;
    private static final String DOMAIN_COMPACTION_THRESHOLD = "domain_compaction_threshold";

    private static final List<PropertyMetadata<?>> SESSION_PROPERTIES =
            Collections.singletonList(
                    integerProperty(
                            DOMAIN_COMPACTION_THRESHOLD,
                            "Domain complexity threshold for Arrow Log batch pruning",
                            DEFAULT_DOMAIN_COMPACTION_THRESHOLD,
                            value -> {
                                if (value < 1) {
                                    throw new TrinoException(
                                            INVALID_SESSION_PROPERTY,
                                            DOMAIN_COMPACTION_THRESHOLD + " must be positive");
                                }
                            },
                            false));

    private FlussSessionProperties() {}

    static List<PropertyMetadata<?>> getSessionProperties() {
        return SESSION_PROPERTIES;
    }

    static int getDomainCompactionThreshold(ConnectorSession session) {
        return session.getProperty(DOMAIN_COMPACTION_THRESHOLD, Integer.class);
    }
}
