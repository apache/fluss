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

package com.alibaba.fluss.client;

import com.alibaba.fluss.annotation.PublicEvolving;
import com.alibaba.fluss.client.admin.Admin;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.metrics.registry.MetricRegistry;

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
        return new FlussConnection(conf);
    }

    /**
     * Create a new {@link Connection} to the Fluss cluster with registering metrics to the given
     * {@code metricRegistry}. It's mainly used for client to register metrics to external metrics
     * system.
     *
     * <p>See more comments in method {@link #createConnection(Configuration)}
     */
    public static Connection createConnection(Configuration conf, MetricRegistry metricRegistry) {
        return new FlussConnection(conf, metricRegistry);
    }
}
