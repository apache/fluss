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

package com.alibaba.fluss.client.token;

import com.alibaba.fluss.annotation.Internal;
import com.alibaba.fluss.fs.token.ObtainedSecurityToken;
import com.alibaba.fluss.fs.token.SecurityTokenReceiver;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.function.Consumer;

import static com.alibaba.fluss.utils.Preconditions.checkState;

/** Repository for security token receivers. */
@Internal
class SecurityTokenReceiverRepository {

    private static final Logger LOG =
            LoggerFactory.getLogger(SecurityTokenReceiverRepository.class);

    private final Map<String, SecurityTokenReceiver> securityTokenReceivers;

    SecurityTokenReceiverRepository() {
        this.securityTokenReceivers = loadReceivers();
    }

    private Map<String, SecurityTokenReceiver> loadReceivers() {
        LOG.info("Loading security token receivers");

        Map<String, SecurityTokenReceiver> receivers = new HashMap<>();

        Consumer<SecurityTokenReceiver> loadReceiver =
                (receiver) -> {
                    checkState(
                            !receivers.containsKey(receiver.scheme()),
                            "Security token receiver with scheme name {} has multiple implementations",
                            receiver.scheme());
                    receivers.put(receiver.scheme(), receiver);
                    LOG.info(
                            "Security token receiver '{}' loaded and initialized",
                            receiver.scheme());
                };

        ServiceLoader.load(SecurityTokenReceiver.class).iterator().forEachRemaining(loadReceiver);

        LOG.info("Security token receivers loaded successfully");
        return receivers;
    }

    /**
     * Callback function when new security token obtained.
     *
     * @param token security token obtained. The token will be forwarded to the appropriate {@link
     *     SecurityTokenReceiver} based on scheme name.
     */
    void onNewTokensObtained(ObtainedSecurityToken token) {
        String schemeName = token.getScheme();
        LOG.info("New security tokens arrived, sending them to receiver");
        if (!securityTokenReceivers.containsKey(schemeName)) {
            throw new IllegalStateException(
                    "Token arrived for service but no receiver found for it: " + schemeName);
        }
        try {
            securityTokenReceivers.get(schemeName).onNewTokensObtained(token);
        } catch (Exception e) {
            LOG.warn("Failed to send token to security token receiver {}", schemeName, e);
        }
        LOG.info("Security token sent to receiver");
    }
}
