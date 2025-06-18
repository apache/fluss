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

package com.alibaba.fluss.server.zk.data;

import com.alibaba.fluss.utils.json.JsonSerdeTestBase;

import java.util.Map;

/** Test for {@link com.alibaba.fluss.server.zk.data.PartitionAssignmentJsonSerde} . */
class PartitionAssignmentJsonSerdeTest extends JsonSerdeTestBase<PartitionAssignment> {

    PartitionAssignmentJsonSerdeTest() {
        super(PartitionAssignmentJsonSerde.INSTANCE);
    }

    @Override
    protected PartitionAssignment[] createObjects() {
        Map<Integer, BucketAssignment> bucketAssignments =
                TableAssignment.builder()
                        .add(0, BucketAssignment.of(1, 4, 5))
                        .add(1, BucketAssignment.of(2, 3))
                        .build()
                        .getBucketAssignments();
        return new PartitionAssignment[] {new PartitionAssignment(1, bucketAssignments)};
    }

    @Override
    protected String[] expectedJsons() {
        return new String[] {
            "{\"version\":1,\"table_id\":1,\"buckets\":{\"0\":[1,4,5],\"1\":[2,3]}}"
        };
    }
}
