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

package com.alibaba.fluss.server.utils;

import com.alibaba.fluss.annotation.VisibleForTesting;
import com.alibaba.fluss.exception.InvalidBucketsException;
import com.alibaba.fluss.exception.InvalidReplicationFactorException;
import com.alibaba.fluss.server.zk.data.BucketAssignment;
import com.alibaba.fluss.server.zk.data.TableAssignment;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

/** Utils for the assignment of tables. */
public class TableAssignmentUtils {

    private static final Random rand = new Random();

    @VisibleForTesting
    protected static TableAssignment generateAssignment(
            int nBuckets,
            int replicationFactor,
            int[] servers,
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

        Map<Integer, BucketAssignment> assignments = new HashMap<>();
        int currentBucketId = 0;
        for (int i = 0; i < nBuckets; i++) {
            if (currentBucketId > 0 && (currentBucketId % servers.length == 0)) {
                nextReplicaShift += 1;
            }
            int firstReplicaIndex = (currentBucketId + startIndex) % servers.length;
            List<Integer> replicas = new ArrayList<>();
            replicas.add(servers[firstReplicaIndex]);
            for (int j = 0; j < replicationFactor - 1; j++) {
                int replicaIndex =
                        replicaIndex(firstReplicaIndex, nextReplicaShift, j, servers.length);
                replicas.add(servers[replicaIndex]);
            }
            assignments.put(currentBucketId, new BucketAssignment(replicas));
            currentBucketId++;
        }
        return new TableAssignment(assignments);
    }

    /**
     * There are two goals of the table assignment:
     *
     * <ol>
     *   <li>Spread replicas evenly among the tablet servers
     *   <li>For buckets assigned to a particular broker, their other replicas are spread over the
     *       other tablet servers.
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
     * <p>Note: the assignment won't consider rack information currently,
     */
    public static TableAssignment generateAssignment(
            int nBuckets, int replicationFactor, int[] servers)
            throws InvalidReplicationFactorException {
        return generateAssignment(
                nBuckets,
                replicationFactor,
                servers,
                randomInt(servers.length),
                randomInt(servers.length));
    }

    private static int randomInt(int nServers) {
        return nServers == 0 ? 0 : rand.nextInt(nServers);
    }

    private static int replicaIndex(
            int firstReplicaIndex, int secondReplicaShift, int replicaIndex, int nServers) {
        int shift = 1 + (secondReplicaShift + replicaIndex) % (nServers - 1);
        return (firstReplicaIndex + shift) % nServers;
    }
}
