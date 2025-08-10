/*
 * Copyright (c) 2024 Alibaba Group Holding Ltd.
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

package com.alibaba.fluss.cli;

import com.alibaba.fluss.client.Connection;
import com.alibaba.fluss.client.ConnectionFactory;
import com.alibaba.fluss.client.admin.Admin;
import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.utils.MapUtils;

import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Map;

/** Connection Utils to get connection from coordinator. */
public class ConnectionUtils implements AutoCloseable {
    private static final Map<String, Connection> connections = MapUtils.newConcurrentHashMap();

    public static Connection getConnection(String bootstrapServers) {
        return connections.computeIfAbsent(
                bootstrapServers,
                e -> {
                    Configuration flussConf = new Configuration();
                    flussConf.setString(ConfigOptions.BOOTSTRAP_SERVERS.key(), bootstrapServers);
                    return ConnectionFactory.createConnection(flussConf);
                });
    }

    public static Admin getAdmin(List<String> bootstrapServers) {
        String bootstrapServerStr = StringUtils.join(bootstrapServers, ",");
        return getConnection(bootstrapServerStr).getAdmin();
    }

    @Override
    public void close() throws Exception {
        for (Connection conn : connections.values()) {
            conn.close();
        }
        connections.clear();
    }
}
