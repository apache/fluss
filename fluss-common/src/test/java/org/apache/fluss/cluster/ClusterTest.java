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

package org.apache.fluss.cluster;

import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.TablePath;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.fluss.record.TestData.DATA1_PHYSICAL_TABLE_PATH;
import static org.apache.fluss.record.TestData.DATA1_TABLE_ID;
import static org.apache.fluss.record.TestData.DATA1_TABLE_PATH;
import static org.apache.fluss.record.TestData.DATA2_TABLE_ID;
import static org.apache.fluss.record.TestData.DATA2_TABLE_PATH;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link Cluster}. */
class ClusterTest {
    private static final ServerNode COORDINATOR_SERVER =
            new ServerNode(0, "localhost", 98, ServerType.COORDINATOR);
    private static final ServerNode[] NODES =
            new ServerNode[] {
                new ServerNode(0, "localhost", 99, ServerType.TABLET_SERVER, "rack0"),
                new ServerNode(1, "localhost", 100, ServerType.TABLET_SERVER, "rack1"),
                new ServerNode(2, "localhost", 101, ServerType.TABLET_SERVER, "rack2"),
                new ServerNode(11, "localhost", 102, ServerType.TABLET_SERVER, "rack11")
            };

    private static final int[] NODES_IDS = new int[] {0, 1, 2, 11};
    private Map<Integer, ServerNode> aliveTabletServersById;

    @BeforeEach
    void setup() {
        aliveTabletServersById = new HashMap<>();
        for (ServerNode node : NODES) {
            aliveTabletServersById.put(node.id(), node);
        }
    }

    @Test
    void testReturnModifiableCollections() {
        Cluster cluster = createCluster(aliveTabletServersById);
        assertThatThrownBy(() -> cluster.getAliveTabletServers().put(1, NODES[3]))
                .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(
                        () ->
                                cluster.getAvailableBucketsForPhysicalTablePath(
                                                DATA1_PHYSICAL_TABLE_PATH)
                                        .add(
                                                new BucketLocation(
                                                        DATA1_PHYSICAL_TABLE_PATH,
                                                        DATA1_TABLE_ID,
                                                        3,
                                                        NODES_IDS[3],
                                                        NODES_IDS)))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void testInvalidMetaAndUpdate() {
        Cluster cluster = createCluster(aliveTabletServersById);
        for (int i = 0; i < 10000; i++) {
            // mock invalid meta
            cluster =
                    cluster.invalidPhysicalTableBucketMeta(
                            Collections.singleton(DATA1_PHYSICAL_TABLE_PATH));
            // mock update meta
            cluster =
                    new Cluster(
                            aliveTabletServersById,
                            COORDINATOR_SERVER,
                            new HashMap<>(cluster.getBucketLocationsByPath()),
                            new HashMap<>(cluster.getTableIdByPath()),
                            Collections.emptyMap());
        }

        // verify available buckets
        List<BucketLocation> availableBuckets =
                cluster.getAvailableBucketsForPhysicalTablePath(
                        PhysicalTablePath.of(DATA2_TABLE_PATH));
        assertThat(availableBuckets)
                .isEqualTo(
                        Collections.singletonList(
                                new BucketLocation(
                                        PhysicalTablePath.of(DATA2_TABLE_PATH),
                                        DATA2_TABLE_ID,
                                        1,
                                        NODES_IDS[0],
                                        NODES_IDS)));
    }

    @Test
    void testGetRandomTabletServer() {
        Map<Integer, ServerNode> aliveTabletServersById = new HashMap<>();
        for (int i = 0; i < 10; i++) {
            aliveTabletServersById.put(
                    i, new ServerNode(i, "localhost", 99 + i, ServerType.TABLET_SERVER));
        }
        Cluster cluster = createCluster(aliveTabletServersById);

        Set<ServerNode> selectedNodes = new HashSet<>();
        for (int i = 0; i < 10; i++) {
            ServerNode serverNode = cluster.getRandomTabletServer();
            assertThat(serverNode).isNotNull();
            selectedNodes.add(serverNode);
        }

        assertThat(selectedNodes).hasSizeGreaterThan(1);
    }

    private Cluster createCluster(Map<Integer, ServerNode> aliveTabletServersById) {
        Map<PhysicalTablePath, List<BucketLocation>> tablePathToBucketLocations = new HashMap<>();
        tablePathToBucketLocations.put(
                DATA1_PHYSICAL_TABLE_PATH,
                Arrays.asList(
                        new BucketLocation(
                                DATA1_PHYSICAL_TABLE_PATH,
                                DATA1_TABLE_ID,
                                0,
                                NODES_IDS[0],
                                NODES_IDS),
                        new BucketLocation(
                                DATA1_PHYSICAL_TABLE_PATH, DATA1_TABLE_ID, 1, null, NODES_IDS),
                        new BucketLocation(
                                DATA1_PHYSICAL_TABLE_PATH,
                                DATA1_TABLE_ID,
                                2,
                                NODES_IDS[2],
                                NODES_IDS)));
        tablePathToBucketLocations.put(
                PhysicalTablePath.of(DATA2_TABLE_PATH),
                Arrays.asList(
                        new BucketLocation(
                                PhysicalTablePath.of(DATA2_TABLE_PATH),
                                DATA2_TABLE_ID,
                                0,
                                null,
                                NODES_IDS),
                        new BucketLocation(
                                PhysicalTablePath.of(DATA2_TABLE_PATH),
                                DATA2_TABLE_ID,
                                1,
                                NODES_IDS[0],
                                NODES_IDS)));

        Map<TablePath, Long> tablePathToTableId = new HashMap<>();
        tablePathToTableId.put(DATA1_TABLE_PATH, DATA1_TABLE_ID);
        tablePathToTableId.put(DATA2_TABLE_PATH, DATA2_TABLE_ID);

        return new Cluster(
                aliveTabletServersById,
                COORDINATOR_SERVER,
                tablePathToBucketLocations,
                tablePathToTableId,
                Collections.emptyMap());
    }
}
