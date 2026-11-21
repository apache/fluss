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

package org.apache.fluss.trino;

import org.apache.fluss.client.Connection;
import org.apache.fluss.client.FlussConnection;
import org.apache.fluss.client.admin.FlussAdmin;
import org.apache.fluss.client.metadata.MetadataUpdater;
import org.apache.fluss.rpc.RpcClient;

/** Real Admin with owned or borrowed RPC transport for targeted tests. */
class TestingAdmin extends FlussAdmin {
    private final RpcClient ownedRpcClient;

    TestingAdmin(Connection connection) {
        this(
                RpcClient.create(
                        connection.getConfiguration(),
                        ((FlussConnection) connection).getClientMetricGroup()),
                ((FlussConnection) connection).getMetadataUpdater(),
                true);
    }

    TestingAdmin(MetadataUpdater metadataUpdater, RpcClient rpcClient) {
        this(rpcClient, metadataUpdater, false);
    }

    private TestingAdmin(
            RpcClient rpcClient, MetadataUpdater metadataUpdater, boolean ownsRpcClient) {
        super(rpcClient, metadataUpdater);
        ownedRpcClient = ownsRpcClient ? rpcClient : null;
    }

    @Override
    public void close() {
        try {
            super.close();
        } finally {
            if (ownedRpcClient != null) {
                try {
                    ownedRpcClient.close();
                } catch (Exception e) {
                    throw new RuntimeException("Failed to close Admin transport", e);
                }
            }
        }
    }
}
