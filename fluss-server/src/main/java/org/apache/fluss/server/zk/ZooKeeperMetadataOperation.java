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

import org.apache.fluss.server.zk.data.ZkData;
import org.apache.fluss.shaded.curator5.org.apache.curator.RetryLoop;
import org.apache.fluss.shaded.curator5.org.apache.curator.framework.api.transaction.CuratorOp;
import org.apache.fluss.shaded.guava32.com.google.common.io.ByteStreams;
import org.apache.fluss.shaded.guava32.com.google.common.io.CountingOutputStream;
import org.apache.fluss.shaded.zookeeper3.org.apache.jute.BinaryOutputArchive;
import org.apache.fluss.shaded.zookeeper3.org.apache.zookeeper.CreateMode;
import org.apache.fluss.shaded.zookeeper3.org.apache.zookeeper.KeeperException;
import org.apache.fluss.shaded.zookeeper3.org.apache.zookeeper.MultiOperationRecord;
import org.apache.fluss.shaded.zookeeper3.org.apache.zookeeper.Op;
import org.apache.fluss.shaded.zookeeper3.org.apache.zookeeper.OpResult;
import org.apache.fluss.shaded.zookeeper3.org.apache.zookeeper.data.Stat;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;

import static org.apache.fluss.utils.Preconditions.checkArgument;

/** Epoch-checked metadata writes bound to the session that elected their Coordinator. */
public final class ZooKeeperMetadataOperation {

    private final ZooKeeperMetadataContext context;

    ZooKeeperMetadataOperation(ZooKeeperMetadataContext context) {
        this.context = context;
    }

    /** Rejects a read batch if the leader's original ZooKeeper session was replaced. */
    public void checkSession() throws Exception {
        context.checkSession();
    }

    /** Reads metadata through existing APIs, rejecting results from a replaced session. */
    public <T> T read(Callable<T> action) throws Exception {
        checkSession();
        T result = action.call();
        checkSession();
        return result;
    }

    /** Returns the epoch version checked by every mutation in this operation. */
    public int getEpochVersion() {
        return context.epochVersion;
    }

    /** Submits an immutable operation batch with an atomic check of the original epoch. */
    public List<OpResult> multi(List<CuratorOp> operations) throws Exception {
        checkSession();
        List<Op> requests = new ArrayList<>(operations.size() + 1);
        requests.add(
                context.client
                        .transactionOp()
                        .check()
                        .withVersion(context.epochVersion)
                        .forPath(ZkData.CoordinatorEpochZNode.path())
                        .get());
        for (CuratorOp operation : operations) {
            requests.add(operation.get());
        }
        CountingOutputStream output = new CountingOutputStream(ByteStreams.nullOutputStream());
        new MultiOperationRecord(requests)
                .serialize(BinaryOutputArchive.getArchive(output), "request");
        checkArgument(
                8L + output.getCount() <= context.maxMultiBytes,
                "ZooKeeper metadata transaction exceeds the configured buffer limit.");
        return RetryLoop.callWithRetry(
                context.client.getZookeeperClient(), () -> context.session.multi(requests));
    }

    /** Creates a persistent node whose parent already exists. */
    public void create(String path, byte[] data) throws Exception {
        multi(
                Collections.singletonList(
                        context.client
                                .transactionOp()
                                .create()
                                .withMode(CreateMode.PERSISTENT)
                                .forPath(path, data)));
    }

    /** Creates an empty persistent node if absent, requiring its parent to exist. */
    public void ensurePath(String path) throws Exception {
        try {
            create(path, new byte[0]);
        } catch (KeeperException.NodeExistsException ignored) {
            // The existing node is preserved.
        }
    }

    /** Updates a persistent node using the version read by the caller. */
    public void setData(String path, byte[] data, int version) throws Exception {
        multi(
                Collections.singletonList(
                        context.client
                                .transactionOp()
                                .setData()
                                .withVersion(version)
                                .forPath(path, data)));
    }

    /** Deletes one node using the version read by the caller. */
    public void delete(String path, int version) throws Exception {
        multi(
                Collections.singletonList(
                        context.client
                                .transactionOp()
                                .delete()
                                .withVersion(version)
                                .forPath(path)));
    }

    /** Deletes a tree, checking the original epoch and each node version on every deletion. */
    public void deleteRecursively(String path) throws Exception {
        List<String> children = read(() -> context.client.getChildren().forPath(path));
        for (String child : children) {
            try {
                deleteRecursively(path + "/" + child);
            } catch (KeeperException.NoNodeException ignored) {
                // A child that is already absent needs no deletion.
            }
        }
        Stat stat = read(() -> context.client.checkExists().forPath(path));
        if (stat == null) {
            throw new KeeperException.NoNodeException(path);
        }
        delete(path, stat.getVersion());
    }
}
