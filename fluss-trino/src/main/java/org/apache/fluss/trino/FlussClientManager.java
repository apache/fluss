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

import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.utils.IOUtils;

import com.google.inject.Inject;
import jakarta.annotation.PreDestroy;

import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** Owns the shared Fluss connection and Admin client. */
public final class FlussClientManager {
    private static final String BOOTSTRAP_SERVERS = "bootstrap.servers";
    private static final String SECURITY_PROTOCOL = "client.security.protocol";
    private static final String SASL_MECHANISM = "client.security.sasl.mechanism";
    private static final String SASL_USERNAME = "client.security.sasl.username";
    private static final String SASL_PASSWORD = "client.security.sasl.password";

    private final Connection connection;
    private final Admin admin;

    @Inject
    public FlussClientManager(FlussConfig config) {
        checkNotNull(config, "config is null");

        Configuration configuration = new Configuration();
        configuration.setString(BOOTSTRAP_SERVERS, config.getBootstrapServers());

        config.getSecurityProtocol()
                .ifPresent(value -> configuration.setString(SECURITY_PROTOCOL, value));
        config.getSaslMechanism()
                .ifPresent(value -> configuration.setString(SASL_MECHANISM, value));
        config.getSaslUsername().ifPresent(value -> configuration.setString(SASL_USERNAME, value));
        config.getSaslPassword().ifPresent(value -> configuration.setString(SASL_PASSWORD, value));

        connection = ConnectionFactory.createConnection(configuration);
        try {
            admin = connection.getAdmin();
        } catch (RuntimeException | Error failure) {
            try {
                connection.close();
            } catch (Exception closeFailure) {
                failure.addSuppressed(closeFailure);
            }
            throw failure;
        }
    }

    /** Returns the shared Admin client. Callers must not close it. */
    Admin getAdmin() {
        return admin;
    }

    /** Opens an independently owned table; the caller must close it. */
    Table openTable(TablePath tablePath) {
        return connection.getTable(tablePath);
    }

    @PreDestroy
    public void close() throws Exception {
        IOUtils.closeAll(admin, connection);
    }
}
