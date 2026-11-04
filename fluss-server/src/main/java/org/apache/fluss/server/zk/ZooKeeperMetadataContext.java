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

package org.apache.fluss.server.zk;

import org.apache.fluss.shaded.curator5.org.apache.curator.framework.CuratorFramework;
import org.apache.fluss.shaded.zookeeper3.org.apache.zookeeper.KeeperException;
import org.apache.fluss.shaded.zookeeper3.org.apache.zookeeper.ZooKeeper;

import java.util.Collections;

/** The ZooKeeper session and epoch shared by one Coordinator leader's metadata operations. */
public final class ZooKeeperMetadataContext {

    final CuratorFramework client;
    final ZooKeeper session;
    final long instanceIndex;
    final int epochVersion;
    final long maxMultiBytes;

    ZooKeeperMetadataContext(
            CuratorFramework client,
            ZooKeeper session,
            long instanceIndex,
            int epochVersion,
            long maxMultiBytes) {
        this.client = client;
        this.session = session;
        this.instanceIndex = instanceIndex;
        this.epochVersion = epochVersion;
        this.maxMultiBytes = maxMultiBytes;
    }

    /** Starts an operation after ordering all earlier writes in this leader's session. */
    public ZooKeeperMetadataOperation beginOperation() throws Exception {
        ZooKeeperMetadataOperation operation = new ZooKeeperMetadataOperation(this);
        operation.multi(Collections.emptyList());
        return operation;
    }

    void checkSession() throws Exception {
        if (client.getZookeeperClient().getInstanceIndex() != instanceIndex
                || client.getZookeeperClient().getZooKeeper() != session
                || !session.getState().isAlive()
                || client.getZookeeperClient().getInstanceIndex() != instanceIndex) {
            throw new KeeperException.SessionExpiredException();
        }
    }
}
