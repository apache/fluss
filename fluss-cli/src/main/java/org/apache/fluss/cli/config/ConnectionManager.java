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

package org.apache.fluss.cli.config;

import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;

import java.io.Closeable;
import java.io.IOException;

/** Manages Fluss connection lifecycle. */
public class ConnectionManager implements Closeable {
    private Connection connection;
    private final ConnectionConfig config;

    public ConnectionManager(ConnectionConfig config) {
        this.config = config;
    }

    public Connection getConnection() {
        if (connection == null) {
            connection = ConnectionFactory.createConnection(config.getConfiguration());
        }
        return connection;
    }

    @Override
    public void close() throws IOException {
        if (connection != null) {
            try {
                connection.close();
            } catch (Exception e) {
                throw new IOException("Failed to close connection", e);
            }
            connection = null;
        }
    }
}
