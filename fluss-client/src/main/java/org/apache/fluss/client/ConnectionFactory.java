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

package org.apache.fluss.client;

import org.apache.fluss.annotation.PublicEvolving;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metrics.registry.MetricRegistry;

/**
 * A non-instantiable class that manages creation of {@link Connection}s. Managing the lifecycle of
 * the {@link Connection}s to the cluster is the responsibility of the caller. From a {@link
 * Connection}, {@link Admin} implementations are retrieved with {@link Connection#getAdmin()}.
 *
 * @since 0.1
 */
@PublicEvolving
public class ConnectionFactory {

    private ConnectionFactory() {}

    /**
     * Creates a new {@link Connection} to the Fluss cluster. The given configuration at least needs
     * to contain "bootstrap.servers" to discover the Fluss cluster. Here is a simple example:
     *
     * <pre>{@code
     * Configuration conf = new Configuration();
     * conf.setString("bootstrap.servers", "localhost:9092");
     * Connection connection = ConnectionFactory.createConnection(conf);
     * Admin admin = connection.getAdmin();
     * try {
     *    // Use the admin as needed, for a single operation and a single thread
     *  } finally {
     *    admin.close();
     *    connection.close();
     *  }
     * }</pre>
     */
    public static Connection createConnection(Configuration conf) {
        return new FlussConnection(conf, false);
    }

    /**
     * Create a new {@link Connection} to the Fluss cluster with registering metrics to the given
     * {@code metricRegistry}. It's mainly used for client to register metrics to external metrics
     * system.
     *
     * <p>See more comments in method {@link #createConnection(Configuration)}
     */
    public static Connection createConnection(Configuration conf, MetricRegistry metricRegistry) {
        return new FlussConnection(conf, metricRegistry, false);
    }

    /**
     * Creates a new {@link Connection} to the Fluss cluster in recovery mode.
     *
     * <p>In recovery mode, all write operations bypass the merge engine and write directly to the
     * KV store (overwrite mode). This is typically used for undo recovery scenarios where you need
     * to restore historical state and then switch back to normal mode using {@link
     * Connection#switchToNormalMode()}.
     *
     * <p>Example usage:
     *
     * <pre>{@code
     * Configuration conf = new Configuration();
     * conf.setString("bootstrap.servers", "localhost:9092");
     * try (Connection recoveryConn = ConnectionFactory.createRecoveryConnection(conf)) {
     *     UpsertWriter writer = recoveryConn.getTable(tablePath).newUpsert().createWriter();
     *     // Apply undo operations in recovery mode (overwrite mode)
     *     writer.upsert(undoRow1);
     *     writer.upsert(undoRow2);
     *     writer.flush();
     *     // Switch to normal mode after undo recovery is complete
     *     recoveryConn.switchToNormalMode();
     *     // Continue with normal writes
     *     writer.upsert(normalRow);
     * }
     * }</pre>
     *
     * @param conf the configuration to use
     * @return a new Connection in recovery mode
     */
    public static Connection createRecoveryConnection(Configuration conf) {
        return new FlussConnection(conf, true);
    }

    /**
     * Create a new {@link Connection} to the Fluss cluster in recovery mode with registering
     * metrics to the given {@code metricRegistry}.
     *
     * <p>See more comments in method {@link #createRecoveryConnection(Configuration)}
     */
    public static Connection createRecoveryConnection(
            Configuration conf, MetricRegistry metricRegistry) {
        return new FlussConnection(conf, metricRegistry, true);
    }
}
