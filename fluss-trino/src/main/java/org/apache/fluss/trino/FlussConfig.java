/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.trino;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.ConfigSecuritySensitive;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;

import java.util.Optional;

/** Configuration for the Fluss connector. */
public class FlussConfig {

    private String bootstrapServers;
    private String securityProtocol;
    private String saslMechanism;
    private String saslUsername;
    private String saslPassword;

    @NotEmpty
    public String getBootstrapServers() {
        return bootstrapServers;
    }

    @Config("bootstrap.servers")
    @ConfigDescription("Bootstrap servers of the Fluss cluster")
    public FlussConfig setBootstrapServers(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
        return this;
    }

    @NotNull
    public Optional<String> getSecurityProtocol() {
        return Optional.ofNullable(securityProtocol);
    }

    @Config("client.security.protocol")
    @ConfigDescription("Security protocol used by the Fluss client")
    public FlussConfig setSecurityProtocol(String securityProtocol) {
        this.securityProtocol = securityProtocol;
        return this;
    }

    @NotNull
    public Optional<String> getSaslMechanism() {
        return Optional.ofNullable(saslMechanism);
    }

    @Config("client.security.sasl.mechanism")
    @ConfigDescription("SASL mechanism used by the Fluss client")
    public FlussConfig setSaslMechanism(String saslMechanism) {
        this.saslMechanism = saslMechanism;
        return this;
    }

    @NotNull
    public Optional<String> getSaslUsername() {
        return Optional.ofNullable(saslUsername);
    }

    @Config("client.security.sasl.username")
    @ConfigDescription("SASL username used by the Fluss client")
    public FlussConfig setSaslUsername(String saslUsername) {
        this.saslUsername = saslUsername;
        return this;
    }

    @NotNull
    public Optional<String> getSaslPassword() {
        return Optional.ofNullable(saslPassword);
    }

    @Config("client.security.sasl.password")
    @ConfigDescription("SASL password used by the Fluss client")
    @ConfigSecuritySensitive
    public FlussConfig setSaslPassword(String saslPassword) {
        this.saslPassword = saslPassword;
        return this;
    }
}
