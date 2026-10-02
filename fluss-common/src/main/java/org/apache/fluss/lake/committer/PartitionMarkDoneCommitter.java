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

package org.apache.fluss.lake.committer;

import org.apache.fluss.annotation.Internal;

import java.io.IOException;

/**
 * Optional committer capability for marking idle lake partitions done.
 *
 * @param <WriteResult> the write result type
 * @param <CommittableT> the committable type
 */
@Internal
public interface PartitionMarkDoneCommitter<WriteResult, CommittableT>
        extends LakeCommitter<WriteResult, CommittableT> {

    /**
     * Runs idempotent mark-done actions and attaches the full state, even when unchanged.
     *
     * <p>Disabled tables return false without modifying the committable. The caller invokes this
     * once after snapshot recovery and commits data or offset progress regardless of the return
     * value; an idle round commits only when the state changed. Actions may be retried after a
     * failure.
     *
     * @param committable the data or empty maintenance committable
     * @return whether the state was successfully prepared and changed
     */
    boolean preparePartitionMarkDone(CommittableT committable) throws IOException;
}
