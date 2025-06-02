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

import com.alibaba.fluss.annotation.VisibleForTesting;
import com.alibaba.fluss.cluster.TabletServerInfo;
import com.alibaba.fluss.exception.InvalidBucketsException;
import com.alibaba.fluss.exception.InvalidReplicationFactorException;
import com.alibaba.fluss.exception.InvalidServerRackInfoException;
import com.alibaba.fluss.server.zk.data.BucketAssignment;
import com.alibaba.fluss.server.zk.data.TableAssignment;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

/** Utils for the assignment of tables. */
public class TableAssignmentUtils {

    private static final Random rand = new Random();

    @VisibleForTesting
    protected static TableAssignment generateAssignment(
            int nBuckets,
            int replicationFactor,
            TabletServerInfo[] servers,
            int startIndex,
            int nextReplicaShift) {
        if (nBuckets < 0) {
            throw new InvalidBucketsException("Number of buckets must be larger than 0.");
        }

        if (replicationFactor <= 0) {
            throw new InvalidReplicationFactorException(
                    "Replication factor must be larger than 0.");
        }
        if (replicationFactor > servers.length) {
            throw new InvalidReplicationFactorException(
                    String.format(
                            "Replication factor: " + "%s larger than available tablet servers: %s.",
                            replicationFactor, servers.length));
        }

        if (Arrays.stream(servers).noneMatch(tsInfo -> tsInfo.getRack() != null)) {
            return generateRackUnawareAssigment(
                    nBuckets,
                    replicationFactor,
                    Arrays.stream(servers).mapToInt(TabletServerInfo::getId).toArray(),
                    startIndex,
                    nextReplicaShift);
        } else {
            if (Arrays.stream(servers).anyMatch(tsInfo -> tsInfo.getRack() == null)) {
                throw new InvalidServerRackInfoException(
                        "Not all tabletServers have rack information for replica rack aware assignment.");
            } else {
                return generateRackAwareAssigment(
                        nBuckets, replicationFactor, servers, startIndex, nextReplicaShift);
            }
        }
    }

    /**
     * There are two goals of the table assignment:
     *
     * <ol>
     *   <li>Spread replicas evenly among the tabletServers
     *   <li>For buckets assigned to a particular tabletServer, their other replicas are spread over
     *       the other tabletServers.
     *   <li>If all tabletServers have rack information, assign the replicas for each bucket to
     *       different racks if possible
     * </ol>
     *
     * <p>To achieve this goal for replica assignment, we:
     *
     * <ol>
     *   <li>Assign the first replica of each bucket by round-robin, starting from a random position
     *       in the tablet server list.
     *   <li>Assign the remaining replicas of each bucket with an increasing shift.
     * </ol>
     *
     * <p>Here is an example of assigning:
     *
     * <table cellpadding="2" cellspacing="2">
     * <tr><th>server-0</th><th>server-1</th><th>server-2</th><th>server-3</th><th>server-4</th><th>&nbsp;</th></tr>
     * <tr><td>bucket0      </td><td>bucket1      </td><td>bucket2      </td><td>bucket3      </td><td>bucket4      </td><td>(1st replica)</td></tr>
     * <tr><td>bucket5      </td><td>bucket6      </td><td>bucket7      </td><td>bucket8      </td><td>bucket9      </td><td>(1st replica)</td></tr>
     * <tr><td>bucket4      </td><td>bucket0      </td><td>bucket1      </td><td>bucket2      </td><td>bucket3      </td><td>(2nd replica)</td></tr>
     * <tr><td>bucket8      </td><td>bucket9      </td><td>bucket5      </td><td>bucket6      </td><td>bucket7      </td><td>(2nd replica)</td></tr>
     * <tr><td>bucket3      </td><td>bucket4      </td><td>bucket0      </td><td>bucket1      </td><td>bucket2      </td><td>(3nd replica)</td></tr>
     * <tr><td>bucket7      </td><td>bucket8      </td><td>bucket9      </td><td>bucket5      </td><td>bucket6      </td><td>(3nd replica)</td></tr>
     * </table>
     *
     * <p>To create rack aware assigment, this API will first create a rack alternated tabletServers
     * list. For example, from this tabletServerId -> rack mapping:
     *
     * <pre>
     *     0 -> "rack1"
     *     1 -> "rack3"
     *     2 -> "rack3"
     *     3 -> "rack2"
     *     4 -> "rack2"
     *     5 -> "rack1"
     * </pre>
     *
     * <p>The rack alternated list will be:
     *
     * <pre>
     *     0, 3, 1, 5, 4, 2
     * </pre>
     *
     * <p>Then an easy round-robin assignment can be applied. Assume 6 buckets with replica factor
     * of 3, the assignment will be:
     *
     * <pre>
     *     bucket0 -> 0,3,1
     *     bucket1 -> 3,1,5
     *     bucket2 -> 1,5,4
     *     bucket3 -> 5,4,2
     *     bucket4 -> 4,2,0
     *     bucket5 -> 2,0,3
     * </pre>
     *
     * <p>Once it has completed the first round-robin, if there are more buckets to assign, the
     * algorithm will start shifting the followers. This is to ensure we will not always get the
     * same set of sequences. In this case, if there is another bucket to assign (bucket6), the
     * assignment will be:
     *
     * <pre>
     *     bucket6 -> 0,4,2 (instead of repeating 0,3,1 as bucket0)
     * </pre>
     *
     * <p>The rack aware assignment always chooses the 1st replica of the bucket using round-robin
     * on the rack alternated tabletServer list. For rest of the replicas, it will be biased towards
     * tabletServers on racks that do not have any replica assignment, until every rack has a
     * replica. Then the assignment will go back to round-robin on the tabletServer list.
     *
     * <p>As the result, if the number of replicas is equal to or greater than the number of racks,
     * it will ensure that each rack will get at least one replica. Otherwise, each rack will get at
     * most one replica. In a perfect situation where the number of replicas is the same as the
     * number of racks and each rack has the same number of tabletServers, it guarantees that the
     * replica distribution is even across tabletServers and racks.
     */
    public static TableAssignment generateAssignment(
            int nBuckets, int replicationFactor, TabletServerInfo[] servers)
            throws InvalidReplicationFactorException {
        return generateAssignment(
                nBuckets,
                replicationFactor,
                servers,
                randomInt(servers.length),
                randomInt(servers.length));
    }

    private static TableAssignment generateRackUnawareAssigment(
            int nBuckets,
            int replicationFactor,
            int[] serverIds,
            int startIndex,
            int nextReplicaShift) {
        Map<Integer, BucketAssignment> assignments = new HashMap<>();
        int currentBucketId = 0;
        for (int i = 0; i < nBuckets; i++) {
            if (currentBucketId > 0 && (currentBucketId % serverIds.length == 0)) {
                nextReplicaShift += 1;
            }
            int firstReplicaIndex = (currentBucketId + startIndex) % serverIds.length;
            List<Integer> replicas = new ArrayList<>();
            replicas.add(serverIds[firstReplicaIndex]);
            for (int j = 0; j < replicationFactor - 1; j++) {
                int replicaIndex =
                        replicaIndex(firstReplicaIndex, nextReplicaShift, j, serverIds.length);
                replicas.add(serverIds[replicaIndex]);
            }
            assignments.put(currentBucketId, new BucketAssignment(replicas));
            currentBucketId++;
        }
        return new TableAssignment(assignments);
    }

    private static TableAssignment generateRackAwareAssigment(
            int nBuckets,
            int replicationFactor,
            TabletServerInfo[] servers,
            int startIndex,
            int nextReplicaShift) {
        Map<Integer, String> serverRackMap = new HashMap<>();
        for (TabletServerInfo server : servers) {
            serverRackMap.put(server.getId(), server.getRack());
        }
        int numRacks = new HashSet<>(serverRackMap.values()).size();
        List<Integer> arrangedServerList = getRackAlternatedTabletServerList(serverRackMap);
        int numServers = arrangedServerList.size();
        Map<Integer, BucketAssignment> assignments = new HashMap<>();
        int currentBucketId = 0;
        for (int i = 0; i < nBuckets; i++) {
            if (currentBucketId > 0 && (currentBucketId % arrangedServerList.size() == 0)) {
                nextReplicaShift += 1;
            }

            int firstReplicaIndex = (currentBucketId + startIndex) % arrangedServerList.size();
            int leader = arrangedServerList.get(firstReplicaIndex);
            List<Integer> replicas = new ArrayList<>();
            replicas.add(leader);
            Set<String> racksWithReplicas = new HashSet<>();
            racksWithReplicas.add(serverRackMap.get(leader));
            Set<Integer> brokersWithReplicas = new HashSet<>();
            brokersWithReplicas.add(leader);
            int k = 0;
            for (int j = 0; j < replicationFactor - 1; j++) {
                boolean done = false;
                while (!done) {
                    Integer broker =
                            arrangedServerList.get(
                                    replicaIndex(
                                            firstReplicaIndex,
                                            nextReplicaShift * numRacks,
                                            k,
                                            arrangedServerList.size()));
                    String rack = serverRackMap.get(broker);
                    // Skip this tabletServer if
                    // 1. there is already a tabletServer in the same rack that has assigned a
                    // replica AND there is one or more racks that do not have any replica, or
                    // 2. the tabletServer has already assigned a replica AND there is one or more
                    // tabletServers that do not have replica assigned
                    if ((!racksWithReplicas.contains(rack) || racksWithReplicas.size() == numRacks)
                            && (!brokersWithReplicas.contains(broker)
                                    || brokersWithReplicas.size() == numServers)) {
                        replicas.add(broker);
                        racksWithReplicas.add(rack);
                        brokersWithReplicas.add(broker);
                        done = true;
                    }
                    k += 1;
                }
            }
            assignments.put(currentBucketId, new BucketAssignment(replicas));
            currentBucketId++;
        }
        return new TableAssignment(assignments);
    }

    /**
     * Given tabletServer and rack information, returns a list of tabletServers alternated by the
     * rack. Assume this is the rack and its tabletServer ids:
     *
     * <pre>
     *     rack1: 0, 1, 2
     *     rack2: 3, 4, 5
     *     rack3: 6, 7, 8
     * </pre>
     *
     * <p>This API would return the list of 0, 3, 6, 1, 4, 7, 2, 5, 8
     *
     * <p>This is essential to make sure that the generateAssignment API can use such list and
     * assign replicas to tabletServers in a simple round-robin fashion, while ensuring an even
     * distribution of leader and replica counts on each tabletServer and that replicas are
     * distributed to all racks.
     */
    @VisibleForTesting
    static List<Integer> getRackAlternatedTabletServerList(Map<Integer, String> serverRackMap) {
        Map<String, Iterator<Integer>> serversIteratorByRack = new HashMap<>();
        getInverseMap(serverRackMap)
                .forEach((rack, servers) -> serversIteratorByRack.put(rack, servers.iterator()));
        String[] racks = serversIteratorByRack.keySet().toArray(new String[0]);
        Arrays.sort(racks);
        List<Integer> result = new ArrayList<>();
        int rackIndex = 0;
        while (result.size() < serverRackMap.size()) {
            Iterator<Integer> rackIterator = serversIteratorByRack.get(racks[rackIndex]);
            if (rackIterator.hasNext()) {
                result.add(rackIterator.next());
            }
            rackIndex = (rackIndex + 1) % racks.length;
        }
        return result;
    }

    private static int randomInt(int nServers) {
        return nServers == 0 ? 0 : rand.nextInt(nServers);
    }

    private static int replicaIndex(
            int firstReplicaIndex, int secondReplicaShift, int replicaIndex, int nServers) {
        int shift = 1 + (secondReplicaShift + replicaIndex) % (nServers - 1);
        return (firstReplicaIndex + shift) % nServers;
    }

    private static Map<String, List<Integer>> getInverseMap(Map<Integer, String> serverRackMap) {
        Map<String, List<Integer>> results = new HashMap<>();
        serverRackMap.forEach(
                (id, rack) -> results.computeIfAbsent(rack, key -> new ArrayList<>()).add(id));
        results.forEach((rack, rackAndIdList) -> rackAndIdList.sort(Integer::compareTo));
        return results;
    }
}
