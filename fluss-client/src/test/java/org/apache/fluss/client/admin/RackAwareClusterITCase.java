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

package org.apache.fluss.client.admin;

import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.DatabaseDescriptor;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.server.zk.data.BucketAssignment;
import org.apache.fluss.server.zk.data.TableAssignment;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.Optional;

import static org.apache.fluss.client.admin.FlussAdminITCase.DEFAULT_SCHEMA;
import static org.apache.fluss.client.admin.FlussAdminITCase.DEFAULT_TABLE_PATH;
import static org.assertj.core.api.Assertions.assertThat;

/** ITCase for rack aware cluster. */
public class RackAwareClusterITCase {

    @RegisterExtension
    public static final FlussClusterExtension FLUSS_CLUSTER_EXTENSION =
            FlussClusterExtension.builder()
                    .setNumOfTabletServers(4)
                    .setRacks(new String[] {"rack-0", "rack-1", "rack-2", "rack-0"})
                    .build();

    protected Connection conn;
    protected Admin admin;
    protected Configuration clientConf;

    @BeforeEach
    protected void setup() throws Exception {
        clientConf = FLUSS_CLUSTER_EXTENSION.getClientConfig();
        conn = ConnectionFactory.createConnection(clientConf);
        admin = conn.getAdmin();
    }

    @AfterEach
    protected void teardown() throws Exception {
        if (admin != null) {
            admin.close();
            admin = null;
        }

        if (conn != null) {
            conn.close();
            conn = null;
        }
    }

    @Test
    void testCreateTableWithInsufficientRack() throws Exception {
        TablePath tablePath =
                TablePath.of(DEFAULT_TABLE_PATH.getDatabaseName(), "t1-with-replica-factor-4");
        // set replica factor to a number larger than available racks (ts-0: rack-0, ts-1: rack-1,
        // ts-2: rack-2)
        TableDescriptor tableDescriptor =
                TableDescriptor.builder()
                        .schema(DEFAULT_SCHEMA)
                        .comment("test table")
                        .customProperty("connector", "fluss")
                        .distributedBy(1, "id")
                        .property(ConfigOptions.TABLE_REPLICATION_FACTOR.key(), "4")
                        .build();
        admin.createDatabase(DEFAULT_TABLE_PATH.getDatabaseName(), DatabaseDescriptor.EMPTY, false)
                .get();

        // In this case, create table will success with two replicas on one rack.
        admin.createTable(tablePath, tableDescriptor, false).get();

        TableInfo tableInfo = admin.getTableInfo(tablePath).get();
        Optional<TableAssignment> assignmentOpt =
                FLUSS_CLUSTER_EXTENSION
                        .getZooKeeperClient()
                        .getTableAssignment(tableInfo.getTableId());
        assertThat(assignmentOpt.isPresent()).isTrue();
        TableAssignment assignment = assignmentOpt.get();
        BucketAssignment bucketAssignment = assignment.getBucketAssignment(0);
        assertThat(bucketAssignment.getReplicas()).containsExactlyInAnyOrder(0, 1, 2, 3);

        admin.dropTable(tablePath, false).get();
        admin.dropDatabase(DEFAULT_TABLE_PATH.getDatabaseName(), false, false).get();
    }
}
