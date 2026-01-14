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

package org.apache.fluss.server.coordinator.rebalance.model;

import org.apache.fluss.metadata.TableBucket;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link RackModel}. */
public class RackModelTest {

    @Test
    void testRackModel() {
        RackModel rackModel = new RackModel("rack0");
        assertThat(rackModel.rack()).isEqualTo("rack0");
        assertThat(rackModel.server(0)).isNull();

        ServerModel serverModel = new ServerModel(0, "rack0", false);
        rackModel.addServer(serverModel);
        assertThat(rackModel.server(0)).isEqualTo(serverModel);

        assertThat(rackModel.removeReplica(0, new TableBucket(1L, 0))).isNull();

        ReplicaModel replicaModel = new ReplicaModel(new TableBucket(1L, 0), serverModel, false);
        rackModel.addReplica(replicaModel);
        assertThat(serverModel.replica(new TableBucket(1L, 0))).isEqualTo(replicaModel);
    }

    @Test
    void testToString() {
        RackModel rackModel = new RackModel("rack0");
        assertThat(rackModel.toString()).isEqualTo("RackModel[rack=rack0,servers=0]");
    }
}
