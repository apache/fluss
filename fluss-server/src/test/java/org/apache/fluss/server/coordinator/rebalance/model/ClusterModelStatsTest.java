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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.SortedSet;
import java.util.TreeSet;

/** Tests for the {@link ClusterModelStats}. */
public class ClusterModelStatsTest {
    private SortedSet<ServerModel> servers;

    @BeforeEach
    public void setup() {
        servers = new TreeSet<>();
        ServerModel server0 = new ServerModel(0, "rack0", true);
        ServerModel server1 = new ServerModel(1, "rack1", true);
        servers.add(server0);
        servers.add(server1);
    }

    @Test
    void testPopulate() throws Exception {
        // TODO add test for this method, trace by https://github.com/apache/fluss/issues/2315
    }
}
