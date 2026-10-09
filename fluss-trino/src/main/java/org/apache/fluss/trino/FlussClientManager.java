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

import com.google.inject.Inject;
import io.trino.spi.TrinoException;
import jakarta.annotation.PreDestroy;

import static org.apache.fluss.trino.FlussErrorCode.AUTHENTICATION_NOT_SUPPORTED;
import static org.apache.fluss.utils.ExceptionUtils.firstOrSuppressed;
import static org.apache.fluss.utils.ExceptionUtils.rethrowException;
import static org.apache.fluss.utils.Preconditions.checkNotNull;
import static org.apache.fluss.utils.Preconditions.checkState;

/** Lazily initializes and owns the shared Fluss connection and Admin client. */
public final class FlussClientManager {

    private static final String BOOTSTRAP_SERVERS = "bootstrap.servers";
    private static final String SECURITY_PROTOCOL = "client.security.protocol";
    private static final String SASL_MECHANISM = "client.security.sasl.mechanism";
    private static final String SASL_USERNAME = "client.security.sasl.username";
    private static final String SASL_PASSWORD = "client.security.sasl.password";

    private static final String PLAINTEXT = "PLAINTEXT";

    private final Object lock = new Object();
    private final Configuration configuration;
    private final boolean authenticationConfigured;

    // All mutable state is guarded by lock.
    private Connection connection;
    private Admin admin;
    private boolean closed;

    /** Captures connector configuration without connecting to Fluss. */
    @Inject
    public FlussClientManager(FlussConfig config) {
        checkNotNull(config, "config is null");

        this.configuration = new Configuration();
        configuration.setString(BOOTSTRAP_SERVERS, config.getBootstrapServers());

        config.getSecurityProtocol()
                .ifPresent(value -> configuration.setString(SECURITY_PROTOCOL, value));
        config.getSaslMechanism()
                .ifPresent(value -> configuration.setString(SASL_MECHANISM, value));
        config.getSaslUsername().ifPresent(value -> configuration.setString(SASL_USERNAME, value));
        config.getSaslPassword().ifPresent(value -> configuration.setString(SASL_PASSWORD, value));
        this.authenticationConfigured = isAuthenticationConfigured(config);
    }

    /** Closes initialized resources without triggering initialization. */
    @PreDestroy
    public void close() throws Exception {
        Admin adminToClose;
        Connection connectionToClose;

        synchronized (lock) {
            if (closed) {
                return;
            }

            closed = true;
            adminToClose = admin;
            connectionToClose = connection;

            admin = null;
            connection = null;
        }

        Throwable failure = null;

        if (adminToClose != null) {
            try {
                adminToClose.close();
            } catch (Exception | Error e) {
                failure = firstOrSuppressed(e, failure);
            }
        }

        if (connectionToClose != null) {
            try {
                connectionToClose.close();
            } catch (Exception | Error e) {
                failure = firstOrSuppressed(e, failure);
            }
        }

        if (failure != null) {
            rethrowException(failure, "Failed closing Fluss client resources");
        }
    }

    /** Returns the shared Admin client. Callers must not close it. */
    Admin getAdmin() {
        synchronized (lock) {
            initializeIfNeeded();
            return admin;
        }
    }

    /** Opens an independently owned table that the caller must close. */
    Table openTable(TablePath tablePath) {
        checkNotNull(tablePath, "tablePath is null");

        synchronized (lock) {
            initializeIfNeeded();
            return connection.getTable(tablePath);
        }
    }

    /** Initializes both resources while holding lock. */
    private void initializeIfNeeded() {
        checkState(!closed, "Fluss client manager is closed");

        if (connection != null) {
            return;
        }

        if (authenticationConfigured) {
            throw new TrinoException(
                    AUTHENTICATION_NOT_SUPPORTED,
                    "Authentication is not supported by the Fluss Trino connector; "
                            + "only PLAINTEXT connections are currently supported");
        }

        Connection newConnection = null;

        try {
            newConnection = ConnectionFactory.createConnection(configuration);
            Admin newAdmin = newConnection.getAdmin();

            connection = newConnection;
            admin = newAdmin;
        } catch (RuntimeException | Error failure) {
            if (newConnection != null) {
                try {
                    newConnection.close();
                } catch (Exception | Error closeFailure) {
                    firstOrSuppressed(closeFailure, failure);
                }
            }

            throw failure;
        }
    }

    private static boolean isAuthenticationConfigured(FlussConfig config) {
        return (config.getSecurityProtocol().isPresent()
                        && !PLAINTEXT.equalsIgnoreCase(config.getSecurityProtocol().get()))
                || config.getSaslMechanism().isPresent()
                || config.getSaslUsername().isPresent()
                || config.getSaslPassword().isPresent();
    }
}
