/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
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

package org.apache.fluss.server.zk;

import org.apache.fluss.shaded.curator5.org.apache.curator.framework.api.CuratorEvent;
import org.apache.fluss.shaded.zookeeper3.org.apache.zookeeper.KeeperException;
import org.apache.fluss.shaded.zookeeper3.org.apache.zookeeper.KeeperException.Code;

import java.util.List;
import java.util.Optional;

/** Base class for ZooKeeper async operation responses. */
public abstract class ZkAsyncResponse {
    private final String path;
    private final Code resultCode;

    protected ZkAsyncResponse(String path, Code resultCode) {
        this.path = path;
        this.resultCode = resultCode;
    }

    public String getPath() {
        return path;
    }

    public Code getResultCode() {
        return resultCode;
    }

    /** Return None if the result code is OK and KeeperException otherwise. */
    public Optional<KeeperException> resultException() {
        if (resultCode == Code.OK) {
            return Optional.empty();
        } else {
            return Optional.of(KeeperException.create(resultCode, path));
        }
    }

    /** Throw KeeperException if the result code is not OK. */
    public void maybeThrow() throws KeeperException {
        if (resultCode != Code.OK) {
            throw KeeperException.create(resultCode, path);
        }
    }

    // -------------------------------------------------------------------------------------------

    /** The response for ZooKeeper getData async operation. */
    public static class ZkGetDataResponse extends ZkAsyncResponse {

        private final byte[] data;

        public ZkGetDataResponse(String path, Code resultCode, byte[] data) {
            super(path, resultCode);
            this.data = data;
        }

        public byte[] getData() {
            return data;
        }

        public static ZkGetDataResponse create(CuratorEvent event) {
            return new ZkGetDataResponse(
                    event.getPath(), Code.get(event.getResultCode()), event.getData());
        }
    }

    /** The response for ZooKeeper getChildren async operation. */
    public static class ZkGetChildrenResponse extends ZkAsyncResponse {

        private final List<String> children;

        public ZkGetChildrenResponse(
                String path, KeeperException.Code resultCode, List<String> children) {
            super(path, resultCode);
            this.children = children;
        }

        public List<String> getChildren() {
            return children;
        }

        public static ZkGetChildrenResponse create(CuratorEvent event) {
            return new ZkGetChildrenResponse(
                    event.getPath(), Code.get(event.getResultCode()), event.getChildren());
        }
    }
}
