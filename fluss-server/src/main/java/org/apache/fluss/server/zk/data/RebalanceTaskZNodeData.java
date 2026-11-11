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

import javax.annotation.Nullable;

import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** Data stored in the rebalance znode, either an inline task or a remote task handle. */
public final class RebalanceTaskZNodeData {

    private final @Nullable RebalanceTask inlineTask;
    private final @Nullable RebalanceTaskHandle remoteTaskHandle;

    private RebalanceTaskZNodeData(
            @Nullable RebalanceTask inlineTask, @Nullable RebalanceTaskHandle remoteTaskHandle) {
        checkArgument(
                (inlineTask == null) != (remoteTaskHandle == null),
                "Exactly one rebalance task representation must be set.");
        this.inlineTask = inlineTask;
        this.remoteTaskHandle = remoteTaskHandle;
    }

    /** Creates znode data containing a legacy inline task. */
    public static RebalanceTaskZNodeData inline(RebalanceTask task) {
        return new RebalanceTaskZNodeData(checkNotNull(task), null);
    }

    /** Creates znode data pointing to a task in remote storage. */
    public static RebalanceTaskZNodeData remote(RebalanceTaskHandle handle) {
        return new RebalanceTaskZNodeData(null, checkNotNull(handle));
    }

    /** Returns whether this znode embeds the task rather than referencing remote storage. */
    public boolean isInline() {
        return inlineTask != null;
    }

    /** Returns the inline task. */
    public RebalanceTask getInlineTask() {
        return checkNotNull(inlineTask, "The rebalance task is stored remotely.");
    }

    /** Returns the remote task handle. */
    public RebalanceTaskHandle getRemoteTaskHandle() {
        return checkNotNull(remoteTaskHandle, "The rebalance task is stored inline.");
    }
}
