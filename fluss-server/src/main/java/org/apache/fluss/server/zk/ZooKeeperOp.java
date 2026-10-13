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
import org.apache.fluss.shaded.curator5.org.apache.curator.framework.api.transaction.CuratorOp;
import org.apache.fluss.shaded.zookeeper3.org.apache.zookeeper.CreateMode;

import java.util.ArrayList;
import java.util.List;

/** This class contains some utility methods for wrap/unwrap operations for Zookeeper. */
public class ZooKeeperOp {
    private final CuratorFramework zkClient;

    public ZooKeeperOp(CuratorFramework zkClient) {
        this.zkClient = zkClient;
    }

    public CuratorOp checkOp(String path, int expectZkVersion) throws Exception {
        return zkClient.transactionOp().check().withVersion(expectZkVersion).forPath(path);
    }

    public CuratorOp createOp(String path, byte[] data, CreateMode createMode) throws Exception {
        return zkClient.transactionOp().create().withMode(createMode).forPath(path, data);
    }

    public CuratorOp updateOp(String path, byte[] data) throws Exception {
        return zkClient.transactionOp().setData().forPath(path, data);
    }

    public CuratorOp deleteOp(String path) throws Exception {
        return zkClient.transactionOp().delete().forPath(path);
    }

    public static List<CuratorOp> multiRequest(CuratorOp op1, CuratorOp op2) {
        List<CuratorOp> ops = new ArrayList<>();
        ops.add(op1);
        ops.add(op2);
        return ops;
    }

    public static List<CuratorOp> multiRequest(CuratorOp op, List<CuratorOp> ops) {
        List<CuratorOp> result = new ArrayList<>();
        result.add(op);
        result.addAll(ops);
        return result;
    }
}
