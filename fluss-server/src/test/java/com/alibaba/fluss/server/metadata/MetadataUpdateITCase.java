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

package com.alibaba.fluss.server.metadata;

import com.alibaba.fluss.server.testutils.FlussClusterExtension;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

/** IT Case for metadata update. */
class MetadataUpdateITCase {

    @RegisterExtension
    public static final FlussClusterExtension FLUSS_CLUSTER_EXTENSION =
            FlussClusterExtension.builder().setNumOfTabletServers(3).build();

    @Test
    void testMetadataUpdate() throws Exception {
        // todo: need to test tables once coordinator server also send tables metadata
        // to tablet servers;

        // get metadata and check it
        FLUSS_CLUSTER_EXTENSION.waitUtilAllGatewayHasSameMetadata();

        // now, start one tablet server and check it
        FLUSS_CLUSTER_EXTENSION.startTabletServer(3);
        FLUSS_CLUSTER_EXTENSION.waitUtilAllGatewayHasSameMetadata();

        // now, kill one tablet server and check it
        FLUSS_CLUSTER_EXTENSION.stopTabletServer(1);
        FLUSS_CLUSTER_EXTENSION.waitUtilAllGatewayHasSameMetadata();

        // check when coordinator start, it should send update metadata request
        // to all tablet servers

        // let's stop the coordinator server
        FLUSS_CLUSTER_EXTENSION.stopCoordinatorServer();
        // then kill one tablet server again
        FLUSS_CLUSTER_EXTENSION.stopTabletServer(2);
        // let's start the coordinator server again;
        FLUSS_CLUSTER_EXTENSION.startCoordinatorServer();
        // check the metadata again
        FLUSS_CLUSTER_EXTENSION.waitUtilAllGatewayHasSameMetadata();
    }
}
