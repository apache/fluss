/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.server.tablet.consumer.group;

import org.apache.fluss.exception.InvalidConsumerGroupIdException;
import org.apache.fluss.server.coordinator.LakeCatalogDynamicLoader;
import org.apache.fluss.server.coordinator.MetadataManager;
import org.apache.fluss.server.replica.ReplicaTestBase;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class GroupManagerTest extends ReplicaTestBase {

    private LakeCatalogDynamicLoader lakeCatalogLoader;
    private GroupManager groupManager;

    @BeforeEach
    void createGroupManager() {
        lakeCatalogLoader = new LakeCatalogDynamicLoader(conf, null, true);
        groupManager =
                GroupManager.from(
                        conf,
                        TABLET_SERVER_ID,
                        replicaManager,
                        new MetadataManager(zkClient, conf, lakeCatalogLoader),
                        zkClient);
    }

    @AfterEach
    void closeLakeCatalogLoader() throws Exception {
        if (lakeCatalogLoader != null) {
            lakeCatalogLoader.close();
        }
    }

    @Test
    void testStableBucketRouting() {
        int bucketCount = 50;
        int bucket = groupManager.bucketFor("group-a", bucketCount);

        assertThat(groupManager.bucketFor("group-a", bucketCount)).isEqualTo(bucket);
        assertThat(bucket).isBetween(0, bucketCount - 1);
    }

    @Test
    void testInvalidGroupId() {
        assertThatThrownBy(() -> groupManager.bucketFor(null, 50))
                .isInstanceOf(InvalidConsumerGroupIdException.class);
        assertThatThrownBy(() -> groupManager.bucketFor("", 50))
                .isInstanceOf(InvalidConsumerGroupIdException.class);
    }
}
