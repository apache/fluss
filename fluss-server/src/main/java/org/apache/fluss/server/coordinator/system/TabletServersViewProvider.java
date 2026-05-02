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

package org.apache.fluss.server.coordinator.system;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.cluster.Endpoint;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.SystemTableConstants;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.predicate.Predicate;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.data.TabletServerRegistration;
import org.apache.fluss.types.DataTypes;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Provider for the {@code sys.servers} system view.
 *
 * <p>This view exposes information about all registered tablet servers by querying ZooKeeper. The
 * schema is:
 *
 * <ul>
 *   <li>{@code server_id} (INT) — tablet server id
 *   <li>{@code endpoints} (STRING) — comma-separated list of endpoints in {@code
 *       <listener>://<host>:<port>} format
 *   <li>{@code rack} (STRING, nullable) — rack identifier
 *   <li>{@code register_timestamp} (BIGINT) — when the server registered
 * </ul>
 */
@Internal
public class TabletServersViewProvider implements SystemViewProvider {

    private static final TablePath VIEW_PATH =
            TablePath.of(
                    SystemTableConstants.SYSTEM_DATABASE, SystemTableConstants.TABLET_SERVERS_VIEW);

    private static final Schema SCHEMA =
            Schema.newBuilder()
                    .column("server_id", DataTypes.INT())
                    .column("endpoints", DataTypes.STRING())
                    .column("rack", DataTypes.STRING())
                    .column("register_timestamp", DataTypes.BIGINT())
                    .build();

    private final ZooKeeperClient zkClient;

    public TabletServersViewProvider(ZooKeeperClient zkClient) {
        this.zkClient = zkClient;
    }

    @Override
    public TablePath viewPath() {
        return VIEW_PATH;
    }

    @Override
    public Schema schema() {
        return SCHEMA;
    }

    @Override
    public int schemaId() {
        return 1;
    }

    @Override
    public byte[] scanRows(@Nullable int[] projectedFields, @Nullable Predicate filterPredicate)
            throws Exception {
        int[] serverIds = zkClient.getSortedTabletServerList();
        if (serverIds.length == 0) {
            return SystemViewProvider.serializeRows(
                    new ArrayList<>(), projectSchema(projectedFields), schemaId());
        }
        Map<Integer, TabletServerRegistration> registrations = zkClient.getTabletServers(serverIds);

        List<InternalRow> rows = new ArrayList<>(registrations.size());
        for (Map.Entry<Integer, TabletServerRegistration> entry : registrations.entrySet()) {
            int serverId = entry.getKey();
            TabletServerRegistration reg = entry.getValue();

            // Build comma-separated endpoints string: <listener>://<host>:<port>,...
            List<Endpoint> endpoints = reg.getEndpoints();
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < endpoints.size(); i++) {
                if (i > 0) {
                    sb.append(',');
                }
                sb.append(endpoints.get(i).listenerString());
            }

            GenericRow row = new GenericRow(4);
            row.setField(0, serverId);
            row.setField(1, BinaryString.fromString(sb.toString()));
            row.setField(2, reg.getRack() != null ? BinaryString.fromString(reg.getRack()) : null);
            row.setField(3, reg.getRegisterTimestamp());

            if (filterPredicate == null || filterPredicate.test(row)) {
                rows.add(projectRow(row, projectedFields));
            }
        }
        return SystemViewProvider.serializeRows(rows, projectSchema(projectedFields), schemaId());
    }

    private Schema projectSchema(@Nullable int[] projectedFields) {
        if (projectedFields == null) {
            return SCHEMA;
        }
        Schema.Builder builder = Schema.newBuilder();
        List<Schema.Column> columns = SCHEMA.getColumns();
        for (int idx : projectedFields) {
            Schema.Column col = columns.get(idx);
            builder.column(col.getName(), col.getDataType());
        }
        return builder.build();
    }

    private static InternalRow projectRow(GenericRow fullRow, @Nullable int[] projectedFields) {
        if (projectedFields == null) {
            return fullRow;
        }
        GenericRow projected = new GenericRow(projectedFields.length);
        for (int i = 0; i < projectedFields.length; i++) {
            projected.setField(i, fullRow.getField(projectedFields[i]));
        }
        return projected;
    }
}
