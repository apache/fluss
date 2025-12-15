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

package org.apache.fluss.server.zk.data;

import org.apache.fluss.utils.json.JsonSerdeTestBase;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/** Test for {@link KvSnapshotConsumerJsonSerde}. */
public class KvSnapshotConsumerJsonSerdeTest extends JsonSerdeTestBase<KvSnapshotConsumer> {

    KvSnapshotConsumerJsonSerdeTest() {
        super(KvSnapshotConsumerJsonSerde.INSTANCE);
    }

    @Override
    protected KvSnapshotConsumer[] createObjects() {
        KvSnapshotConsumer[] kvSnapshotConsumerData = new KvSnapshotConsumer[1];

        Map<Long, Long[]> tableIdToSnapshots = new HashMap<>();
        tableIdToSnapshots.put(1L, new Long[] {1L, -1L, 1L, 2L});
        tableIdToSnapshots.put(3L, new Long[] {5L, -1L, 3L, 7L});

        Map<Long, Set<Long>> tableIdToPartitions = new HashMap<>();
        tableIdToPartitions.put(2L, new HashSet<>(Arrays.asList(1001L, 1002L, 1003L)));

        Map<Long, Long[]> partitionIdToSnapshots = new HashMap<>();
        partitionIdToSnapshots.put(1001L, new Long[] {1L, -1L, 1L, 2L});
        partitionIdToSnapshots.put(1002L, new Long[] {3L, -1L, 4L, 5L});
        partitionIdToSnapshots.put(1003L, new Long[] {2L, -1L, 1L, 1L});

        tableIdToPartitions.put(4L, new HashSet<>(Arrays.asList(2001L, 2002L)));
        partitionIdToSnapshots.put(2001L, new Long[] {10L, -1L, 20L, 30L});
        partitionIdToSnapshots.put(2002L, new Long[] {15L, -1L, 25L, 35L});

        kvSnapshotConsumerData[0] =
                new KvSnapshotConsumer(
                        1735538268L,
                        tableIdToSnapshots,
                        tableIdToPartitions,
                        partitionIdToSnapshots);

        return kvSnapshotConsumerData;
    }

    @Override
    protected String[] expectedJsons() {
        return new String[] {
            "{\"version\":1,\"expiration_time\":1735538268,\"tables\":"
                    + "{\"1\":[1,-1,1,2],"
                    + "\"3\":[5,-1,3,7],"
                    + "\"2\":{\"1001\":[1,-1,1,2],\"1002\":[3,-1,4,5],\"1003\":[2,-1,1,1]},"
                    + "\"4\":{\"2001\":[10,-1,20,30],\"2002\":[15,-1,25,35]}}}"
        };
    }
}
