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

package org.apache.fluss.server.zk.data;

import org.apache.fluss.fs.FsPath;

import java.util.Objects;

/** A ZooKeeper handle for a rebalance task stored in remote storage. */
public final class RebalanceTaskHandle {

    private final String rebalanceId;
    private final FsPath taskPath;

    /** Creates a handle for the immutable task file referenced by ZooKeeper. */
    public RebalanceTaskHandle(String rebalanceId, FsPath taskPath) {
        this.rebalanceId = rebalanceId;
        this.taskPath = taskPath;
    }

    /** Returns the rebalance identifier stored in both the handle and task file. */
    public String getRebalanceId() {
        return rebalanceId;
    }

    /** Returns the remote path of the serialized task. */
    public FsPath getTaskPath() {
        return taskPath;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RebalanceTaskHandle that = (RebalanceTaskHandle) o;
        return Objects.equals(rebalanceId, that.rebalanceId)
                && Objects.equals(taskPath, that.taskPath);
    }

    @Override
    public int hashCode() {
        return Objects.hash(rebalanceId, taskPath);
    }

    @Override
    public String toString() {
        return "RebalanceTaskHandle{"
                + "rebalanceId='"
                + rebalanceId
                + '\''
                + ", taskPath="
                + taskPath
                + '}';
    }
}
