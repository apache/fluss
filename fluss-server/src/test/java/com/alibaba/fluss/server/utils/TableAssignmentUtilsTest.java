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

package com.alibaba.fluss.server.utils;

import com.alibaba.fluss.cluster.TabletServerInfo;
import com.alibaba.fluss.exception.InvalidBucketsException;
import com.alibaba.fluss.exception.InvalidReplicationFactorException;
import com.alibaba.fluss.exception.InvalidServerRackInfoException;
import com.alibaba.fluss.server.zk.data.BucketAssignment;
import com.alibaba.fluss.server.zk.data.TableAssignment;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.alibaba.fluss.server.utils.TableAssignmentUtils.generateAssignment;
import static com.alibaba.fluss.server.utils.TableAssignmentUtils.getRackAlternatedTabletServerList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link TableAssignmentUtils} for rack unaware mode and rack aware mode. */
class TableAssignmentUtilsTest {

    @Test
    void testTableAssignmentWithRackUnAware() {
        // should throw exception since servers is empty
        assertThatThrownBy(
                        () ->
                                generateAssignment(
                                        5,
                                        -1,
                                        toTabletServerInfo(
                                                Collections.emptyMap(), Collections.emptyList())))
                .isInstanceOf(InvalidReplicationFactorException.class);

        // should throw exception since the buckets is less than 0
        assertThatThrownBy(
                        () ->
                                generateAssignment(
                                        -1,
                                        3,
                                        toTabletServerInfo(
                                                Collections.emptyMap(), Arrays.asList(0, 1))))
                .isInstanceOf(InvalidBucketsException.class);

        // should throw exception since the server is less than replication factor
        assertThatThrownBy(
                        () ->
                                generateAssignment(
                                        5,
                                        3,
                                        toTabletServerInfo(
                                                Collections.emptyMap(), Arrays.asList(0, 1))))
                .isInstanceOf(InvalidReplicationFactorException.class);

        // should throw exception since replication factor is less than 0
        assertThatThrownBy(
                        () ->
                                generateAssignment(
                                        5,
                                        -1,
                                        toTabletServerInfo(
                                                Collections.emptyMap(), Arrays.asList(0, 1))))
                .isInstanceOf(InvalidReplicationFactorException.class);

        // test replica factor 1
        TableAssignment tableAssignment =
                generateAssignment(
                        3,
                        1,
                        toTabletServerInfo(Collections.emptyMap(), Arrays.asList(0, 1, 2, 3)),
                        0,
                        0);
        TableAssignment expectedAssignment =
                TableAssignment.builder()
                        .add(0, BucketAssignment.of(0))
                        .add(1, BucketAssignment.of(1))
                        .add(2, BucketAssignment.of(2))
                        .build();
        assertThat(tableAssignment).isEqualTo(expectedAssignment);

        // test replica factor 3
        tableAssignment =
                generateAssignment(
                        3,
                        3,
                        toTabletServerInfo(Collections.emptyMap(), Arrays.asList(0, 1, 2, 3)),
                        1,
                        0);
        expectedAssignment =
                TableAssignment.builder()
                        .add(0, BucketAssignment.of(1, 2, 3))
                        .add(1, BucketAssignment.of(2, 3, 0))
                        .add(2, BucketAssignment.of(3, 0, 1))
                        .build();
        assertThat(tableAssignment).isEqualTo(expectedAssignment);

        // test with 10 buckets and 3 replies
        tableAssignment =
                generateAssignment(
                        10,
                        3,
                        toTabletServerInfo(Collections.emptyMap(), Arrays.asList(0, 1, 2, 3, 4)),
                        0,
                        0);
        expectedAssignment =
                TableAssignment.builder()
                        .add(0, BucketAssignment.of(0, 1, 2))
                        .add(1, BucketAssignment.of(1, 2, 3))
                        .add(2, BucketAssignment.of(2, 3, 4))
                        .add(3, BucketAssignment.of(3, 4, 0))
                        .add(4, BucketAssignment.of(4, 0, 1))
                        .add(5, BucketAssignment.of(0, 2, 3))
                        .add(6, BucketAssignment.of(1, 3, 4))
                        .add(7, BucketAssignment.of(2, 4, 0))
                        .add(8, BucketAssignment.of(3, 0, 1))
                        .add(9, BucketAssignment.of(4, 1, 2))
                        .build();
        assertThat(tableAssignment).isEqualTo(expectedAssignment);
    }

    @Test
    void testGetRackAlternatedTabletServerListAndAssignReplicasToServers() {
        Map<Integer, String> rackMap = new HashMap<>();
        rackMap.put(0, "rack1");
        rackMap.put(1, "rack3");
        rackMap.put(2, "rack3");
        rackMap.put(3, "rack2");
        rackMap.put(4, "rack2");
        rackMap.put(5, "rack1");

        List<Integer> newList = getRackAlternatedTabletServerList(rackMap);
        assertThat(newList).containsExactly(0, 3, 1, 5, 4, 2);

        // Test with tabletServer 5 removed.
        HashMap<Integer, String> copyMap = new HashMap<>(rackMap);
        copyMap.remove(5);
        newList = getRackAlternatedTabletServerList(copyMap);
        assertThat(newList).containsExactly(0, 3, 1, 4, 2);

        TableAssignment tableAssignment =
                generateAssignment(
                        7, 3, toTabletServerInfo(rackMap, Collections.emptyList()), 0, 0);
        TableAssignment expectedAssignment =
                TableAssignment.builder()
                        .add(0, BucketAssignment.of(0, 3, 1))
                        .add(1, BucketAssignment.of(3, 1, 5))
                        .add(2, BucketAssignment.of(1, 5, 4))
                        .add(3, BucketAssignment.of(5, 4, 2))
                        .add(4, BucketAssignment.of(4, 2, 0))
                        .add(5, BucketAssignment.of(2, 0, 3))
                        .add(6, BucketAssignment.of(0, 4, 2))
                        .build();
        assertThat(tableAssignment).isEqualTo(expectedAssignment);
    }

    @Test
    void testTableAssignmentWithRackAware() {
        Map<Integer, String> rackMap = new HashMap<>();
        rackMap.put(0, "rack1");
        rackMap.put(1, "rack2");
        rackMap.put(2, "rack2");
        rackMap.put(3, "rack3");
        rackMap.put(4, "rack3");
        rackMap.put(5, "rack1");

        int nBuckets = 6;
        int replicationFactor = 3;
        TableAssignment tableAssignment =
                generateAssignment(
                        nBuckets,
                        replicationFactor,
                        toTabletServerInfo(rackMap, Collections.emptyList()),
                        2,
                        0);
        checkTableAssignment(tableAssignment, rackMap, 6, nBuckets, replicationFactor);
    }

    @Test
    void testAssignmentWithRackAwareWithRandomStartIndex() {
        Map<Integer, String> rackMap = new HashMap<>();
        rackMap.put(0, "rack1");
        rackMap.put(1, "rack2");
        rackMap.put(2, "rack2");
        rackMap.put(3, "rack3");
        rackMap.put(4, "rack3");
        rackMap.put(5, "rack1");

        int nBuckets = 6;
        int replicationFactor = 3;
        TableAssignment tableAssignment =
                generateAssignment(
                        nBuckets,
                        replicationFactor,
                        toTabletServerInfo(rackMap, Collections.emptyList()));
        checkTableAssignment(tableAssignment, rackMap, 6, nBuckets, replicationFactor);
    }

    @Test
    void testAssignmentWithRackAwareWithUnevenReplicas() {
        Map<Integer, String> rackMap = new HashMap<>();
        rackMap.put(0, "rack1");
        rackMap.put(1, "rack2");
        rackMap.put(2, "rack2");
        rackMap.put(3, "rack3");
        rackMap.put(4, "rack3");
        rackMap.put(5, "rack1");

        int nBuckets = 13;
        int replicationFactor = 3;
        TableAssignment tableAssignment =
                generateAssignment(
                        nBuckets,
                        replicationFactor,
                        toTabletServerInfo(rackMap, Collections.emptyList()),
                        0,
                        0);
        checkTableAssignment(
                tableAssignment, rackMap, 6, nBuckets, replicationFactor, true, false, false);
    }

    @Test
    void testAssignmentWithRackAwareWithUnevenRacks() {
        Map<Integer, String> rackMap = new HashMap<>();
        rackMap.put(0, "rack1");
        rackMap.put(1, "rack1");
        rackMap.put(2, "rack2");
        rackMap.put(3, "rack3");
        rackMap.put(4, "rack3");
        rackMap.put(5, "rack1");

        int nBuckets = 12;
        int replicationFactor = 3;
        TableAssignment tableAssignment =
                generateAssignment(
                        nBuckets,
                        replicationFactor,
                        toTabletServerInfo(rackMap, Collections.emptyList()));
        checkTableAssignment(
                tableAssignment, rackMap, 6, nBuckets, replicationFactor, true, true, false);
    }

    @Test
    void testAssignmentWith2ReplicasRackAware() {
        Map<Integer, String> rackMap = new HashMap<>();
        rackMap.put(0, "rack1");
        rackMap.put(1, "rack2");
        rackMap.put(2, "rack2");
        rackMap.put(3, "rack3");
        rackMap.put(4, "rack3");
        rackMap.put(5, "rack1");

        int nBuckets = 12;
        int replicationFactor = 2;
        TableAssignment tableAssignment =
                generateAssignment(
                        nBuckets,
                        replicationFactor,
                        toTabletServerInfo(rackMap, Collections.emptyList()));
        checkTableAssignment(tableAssignment, rackMap, 6, nBuckets, replicationFactor);
    }

    @Test
    void testRackAwareExpansion() {
        Map<Integer, String> rackMap = new HashMap<>();
        rackMap.put(6, "rack1");
        rackMap.put(7, "rack2");
        rackMap.put(8, "rack2");
        rackMap.put(9, "rack3");
        rackMap.put(10, "rack3");
        rackMap.put(11, "rack1");

        int nBuckets = 12;
        int replicationFactor = 2;
        TableAssignment tableAssignment =
                generateAssignment(
                        nBuckets,
                        replicationFactor,
                        toTabletServerInfo(rackMap, Collections.emptyList()),
                        0,
                        12);
        checkTableAssignment(tableAssignment, rackMap, 6, nBuckets, replicationFactor);
    }

    @Test
    void testAssignmentWith2ReplicasRackAwareWith6Buckets() {
        Map<Integer, String> rackMap = new HashMap<>();
        rackMap.put(0, "rack1");
        rackMap.put(1, "rack2");
        rackMap.put(2, "rack2");
        rackMap.put(3, "rack3");
        rackMap.put(4, "rack3");
        rackMap.put(5, "rack1");

        int nBuckets = 6;
        int replicationFactor = 2;
        TableAssignment tableAssignment =
                generateAssignment(
                        nBuckets,
                        replicationFactor,
                        toTabletServerInfo(rackMap, Collections.emptyList()));
        checkTableAssignment(tableAssignment, rackMap, 6, nBuckets, replicationFactor);
    }

    @Test
    void testAssignmentWith2ReplicasRackAwareWith6BucketsAnd3Servers() {
        Map<Integer, String> rackMap = new HashMap<>();
        rackMap.put(0, "rack1");
        rackMap.put(1, "rack2");
        rackMap.put(4, "rack3");

        int nBuckets = 6;
        int replicationFactor = 2;
        TableAssignment tableAssignment =
                generateAssignment(
                        nBuckets,
                        replicationFactor,
                        toTabletServerInfo(rackMap, Collections.emptyList()));
        checkTableAssignment(tableAssignment, rackMap, 3, nBuckets, replicationFactor);
    }

    @Test
    void testLargeNumberBucketsAssignment() {
        Map<Integer, String> rackMap = new HashMap<>();
        rackMap.put(0, "rack1");
        rackMap.put(1, "rack2");
        rackMap.put(2, "rack2");
        rackMap.put(3, "rack3");
        rackMap.put(4, "rack3");
        rackMap.put(5, "rack1");
        rackMap.put(6, "rack1");
        rackMap.put(7, "rack2");
        rackMap.put(8, "rack2");
        rackMap.put(9, "rack3");
        rackMap.put(10, "rack1");
        rackMap.put(11, "rack3");

        int nBuckets = 96;
        int replicationFactor = 3;
        TableAssignment tableAssignment =
                generateAssignment(
                        nBuckets,
                        replicationFactor,
                        toTabletServerInfo(rackMap, Collections.emptyList()));
        checkTableAssignment(tableAssignment, rackMap, 12, nBuckets, replicationFactor);
    }

    @Test
    void testMoreReplicasThanRacks() {
        Map<Integer, String> rackMap = new HashMap<>();
        rackMap.put(0, "rack1");
        rackMap.put(1, "rack2");
        rackMap.put(2, "rack2");
        rackMap.put(3, "rack3");
        rackMap.put(4, "rack3");
        rackMap.put(5, "rack1");

        int nBuckets = 6;
        int replicationFactor = 5;
        TableAssignment tableAssignment =
                generateAssignment(
                        nBuckets,
                        replicationFactor,
                        toTabletServerInfo(rackMap, Collections.emptyList()));
        ReplicaDistributions distribution = getReplicaDistribution(tableAssignment, rackMap);
        for (int bucket = 0; bucket < nBuckets; bucket++) {
            Set<String> racksForBucket = new HashSet<>(distribution.getBucketRacks().get(bucket));
            assertThat(racksForBucket.size()).isEqualTo(3);
        }
    }

    @Test
    void testLessReplicasThanRacks() {
        Map<Integer, String> rackMap = new HashMap<>();
        rackMap.put(0, "rack1");
        rackMap.put(1, "rack2");
        rackMap.put(2, "rack2");
        rackMap.put(3, "rack3");
        rackMap.put(4, "rack3");
        rackMap.put(5, "rack2");

        int nBuckets = 6;
        int replicationFactor = 2;
        TableAssignment tableAssignment =
                generateAssignment(
                        nBuckets,
                        replicationFactor,
                        toTabletServerInfo(rackMap, Collections.emptyList()));
        ReplicaDistributions distribution = getReplicaDistribution(tableAssignment, rackMap);
        for (int bucket = 0; bucket <= 5; bucket++) {
            Set<String> racksForBucket = new HashSet<>(distribution.getBucketRacks().get(bucket));
            assertThat(racksForBucket.size()).isEqualTo(2);
        }
    }

    @Test
    void testSingleRack() {
        Map<Integer, String> rackMap = new HashMap<>();
        rackMap.put(0, "rack1");
        rackMap.put(1, "rack1");
        rackMap.put(2, "rack1");
        rackMap.put(3, "rack1");
        rackMap.put(4, "rack1");
        rackMap.put(5, "rack1");

        int nBuckets = 6;
        int replicationFactor = 3;
        TableAssignment tableAssignment =
                generateAssignment(
                        nBuckets,
                        replicationFactor,
                        toTabletServerInfo(rackMap, Collections.emptyList()));
        ReplicaDistributions distribution = getReplicaDistribution(tableAssignment, rackMap);
        for (int bucket = 0; bucket < nBuckets; bucket++) {
            Set<String> racksForBucket = new HashSet<>(distribution.getBucketRacks().get(bucket));
            assertThat(racksForBucket.size()).isEqualTo(1);
        }

        for (Integer serverId : rackMap.keySet()) {
            assertThat(distribution.getServerLeaderCount().get(serverId)).isEqualTo(1);
        }
    }

    @Test
    void testSkipTabletServerWithReplicaAlreadyAssigned() {
        Map<Integer, String> rackMap = new HashMap<>();
        rackMap.put(0, "a");
        rackMap.put(1, "b");
        rackMap.put(2, "c");
        rackMap.put(3, "a");
        rackMap.put(4, "a");

        int nBuckets = 6;
        int replicationFactor = 4;
        TableAssignment tableAssignment =
                generateAssignment(
                        nBuckets,
                        replicationFactor,
                        toTabletServerInfo(rackMap, Collections.emptyList()),
                        2,
                        0);
        checkTableAssignment(
                tableAssignment, rackMap, 12, nBuckets, replicationFactor, false, false, false);
    }

    @Test
    void testPartialTabletServersHaveRackInfo() {
        Map<Integer, String> rackMap = new HashMap<>();
        rackMap.put(0, "rack1");
        rackMap.put(1, null);
        rackMap.put(2, "rack2");
        rackMap.put(3, null);
        rackMap.put(4, "rack3");

        assertThatThrownBy(
                        () ->
                                generateAssignment(
                                        6, 3, toTabletServerInfo(rackMap, Collections.emptyList())))
                .isInstanceOf(InvalidServerRackInfoException.class)
                .hasMessageContaining(
                        "Not all tabletServers have rack information for replica rack aware assignment.");
    }

    private static void checkTableAssignment(
            TableAssignment assignment,
            Map<Integer, String> serverRackMapping,
            int numTabletServers,
            int nBuckets,
            int replicationFactor) {
        checkTableAssignment(
                assignment,
                serverRackMapping,
                numTabletServers,
                nBuckets,
                replicationFactor,
                true,
                true,
                true);
    }

    private static void checkTableAssignment(
            TableAssignment assignment,
            Map<Integer, String> serverRackMapping,
            int numTabletServers,
            int nBuckets,
            int replicationFactor,
            boolean verifyRackAware,
            boolean verifyLeaderDistribution,
            boolean verifyReplicaDistribution) {
        for (Map.Entry<Integer, BucketAssignment> entry :
                assignment.getBucketAssignments().entrySet()) {
            List<Integer> serverList = entry.getValue().getReplicas();
            Set<Integer> uniqueServers = new HashSet<>(serverList);
            assertThat(uniqueServers.size()).isEqualTo(serverList.size());
        }

        ReplicaDistributions distribution = getReplicaDistribution(assignment, serverRackMapping);

        // verify RackAware.
        if (verifyRackAware) {
            Map<Integer, List<String>> bucketRackMap = distribution.getBucketRacks();
            List<Integer> expectedRackCounts = Collections.nCopies(nBuckets, replicationFactor);
            List<Integer> actualRackCounts =
                    bucketRackMap.values().stream()
                            .map(racks -> new HashSet<>(racks).size())
                            .collect(Collectors.toList());
            assertThat(expectedRackCounts).isEqualTo(actualRackCounts);
        }

        // verify leader distribution
        if (verifyLeaderDistribution) {
            Map<Integer, Integer> leaderCount = distribution.getServerLeaderCount();
            int leaderCountPerServer = nBuckets / numTabletServers;
            List<Integer> expectedLeaderCounts =
                    Collections.nCopies(numTabletServers, leaderCountPerServer);
            List<Integer> actualLeaderCounts =
                    leaderCount.values().stream().sorted().collect(Collectors.toList());
            assertThat(expectedLeaderCounts).isEqualTo(actualLeaderCounts);
        }

        // verify replicas distribution
        if (verifyReplicaDistribution) {
            Map<Integer, Integer> replicasCount = distribution.getServerReplicasCount();
            int numReplicasPerServer = (nBuckets * replicationFactor) / numTabletServers;
            List<Integer> expectedReplicaCounts =
                    Collections.nCopies(numTabletServers, numReplicasPerServer);
            List<Integer> actualReplicaCounts =
                    replicasCount.values().stream().sorted().collect(Collectors.toList());
            assertThat(expectedReplicaCounts).isEqualTo(actualReplicaCounts);
        }
    }

    private static ReplicaDistributions getReplicaDistribution(
            TableAssignment assignment, Map<Integer, String> serverRackMapping) {
        Map<Integer, Integer> leaderCount = new HashMap<>();
        Map<Integer, Integer> bucketCount = new HashMap<>();
        Map<Integer, List<String>> bucketRackMap = new HashMap<>();

        for (Map.Entry<Integer, BucketAssignment> entry :
                assignment.getBucketAssignments().entrySet()) {
            int bucketId = entry.getKey();
            List<Integer> replicaList = entry.getValue().getReplicas();
            int leader = replicaList.get(0);
            leaderCount.put(leader, leaderCount.getOrDefault(leader, 0) + 1);
            for (int tabletServerId : replicaList) {
                bucketCount.put(tabletServerId, bucketCount.getOrDefault(tabletServerId, 0) + 1);

                String rack = serverRackMapping.get(tabletServerId);
                if (rack == null) {
                    throw new IllegalArgumentException(
                            "No mapping found for " + tabletServerId + " in `serverRackMapping`");
                }

                bucketRackMap.computeIfAbsent(bucketId, k -> new ArrayList<>()).add(rack);
            }
        }

        return new ReplicaDistributions(bucketRackMap, leaderCount, bucketCount);
    }

    private static TabletServerInfo[] toTabletServerInfo(
            Map<Integer, String> rackMap, List<Integer> serversWithoutRack) {

        List<TabletServerInfo> res = new ArrayList<>();
        for (Map.Entry<Integer, String> entry : rackMap.entrySet()) {
            int tabletServerId = entry.getKey();
            String rack = entry.getValue();
            res.add(new TabletServerInfo(tabletServerId, rack));
        }

        for (int tabletServerId : serversWithoutRack) {
            res.add(new TabletServerInfo(tabletServerId, null));
        }

        res.sort(Comparator.comparingInt(TabletServerInfo::getId));
        return res.toArray(new TabletServerInfo[0]);
    }

    private static class ReplicaDistributions {
        private final Map<Integer, List<String>> bucketRacks;
        private final Map<Integer, Integer> serverLeaderCount;
        private final Map<Integer, Integer> serverReplicasCount;

        public ReplicaDistributions(
                Map<Integer, List<String>> bucketRacks,
                Map<Integer, Integer> serverLeaderCount,
                Map<Integer, Integer> serverReplicasCount) {
            this.bucketRacks = bucketRacks;
            this.serverLeaderCount = serverLeaderCount;
            this.serverReplicasCount = serverReplicasCount;
        }

        public Map<Integer, List<String>> getBucketRacks() {
            return bucketRacks;
        }

        public Map<Integer, Integer> getServerLeaderCount() {
            return serverLeaderCount;
        }

        public Map<Integer, Integer> getServerReplicasCount() {
            return serverReplicasCount;
        }
    }
}
