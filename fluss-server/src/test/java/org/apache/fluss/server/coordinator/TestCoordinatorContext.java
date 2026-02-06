/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.server.coordinator;

import org.apache.fluss.server.zk.data.ZkVersion;

/** A coordinator context for test purpose which can set epoch manually. */
public class TestCoordinatorContext extends CoordinatorContext {
    public TestCoordinatorContext() {
        // When create or modify ZooKeeper node, it should check ZooKeeper epoch node version
        // to ensure the coordinator is still holding the leadership. However, in the test
        // cases, we don't register epoch node, so we skip the check process by setting
        // "coordinatorEpochZkVersion" to ZkVersion.MATCH_ANY_VERSION
        super();
        this.setCoordinatorEpochAndZkVersion(
                INITIAL_COORDINATOR_EPOCH, ZkVersion.MATCH_ANY_VERSION.getVersion());
    }

    public TestCoordinatorContext(int coordinatorEpoch, int coordinatorEpochZkVersion) {
        super();
        this.setCoordinatorEpochAndZkVersion(coordinatorEpoch, coordinatorEpochZkVersion);
    }
}
