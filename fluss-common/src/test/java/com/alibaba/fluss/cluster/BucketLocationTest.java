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

package com.alibaba.fluss.cluster;

import com.alibaba.fluss.metadata.PhysicalTablePath;
import com.alibaba.fluss.metadata.TablePath;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link com.alibaba.fluss.cluster.BucketLocation}. */
public class BucketLocationTest {

    @Test
    void testToString() {
        TablePath tablePath = new TablePath("test_db", "test_table");
        int bucketId = 0;
        long tableId = 150001L;
        ServerNode leader = new ServerNode(0, "localhost", 9092, ServerType.TABLET_SERVER, "rack0");
        ServerNode replica1 =
                new ServerNode(1, "localhost", 9093, ServerType.TABLET_SERVER, "rack1");
        ServerNode replica2 =
                new ServerNode(2, "localhost", 9094, ServerType.TABLET_SERVER, "rack2");
        int[] replicas = new int[] {leader.id(), replica1.id(), replica2.id()};
        BucketLocation bucketLocation =
                new BucketLocation(
                        PhysicalTablePath.of(tablePath), tableId, bucketId, leader.id(), replicas);

        assertThat(bucketLocation.getReplicas()).isEqualTo(replicas);

        assertThat(bucketLocation.toString())
                .isEqualTo(
                        "Bucket(physicalTablePath = test_db.test_table, TableBucket{tableId=150001, bucket=0}, "
                                + "leader = 0, replicas = [0,1,2])");

        bucketLocation =
                new BucketLocation(
                        PhysicalTablePath.of(tablePath), tableId, bucketId, null, replicas);
        assertThat(bucketLocation.toString())
                .isEqualTo(
                        "Bucket(physicalTablePath = test_db.test_table, TableBucket{tableId=150001, bucket=0}, "
                                + "leader = none, replicas = [0,1,2])");
    }
}
