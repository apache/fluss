/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.lakehouse.paimon.sink.committer;

import java.io.IOException;
import java.io.Serializable;

/** A committer to commit snapshot of tables in lake. */
public interface LakeTableSnapshotCommitter extends Serializable, AutoCloseable {

    /**
     * Commit the data has been tiered.
     *
     * @param lakeTableSnapshotInfo the info for the data has been tiered to lakehouse
     */
    void commit(LakeTableSnapshotInfo lakeTableSnapshotInfo) throws IOException;
}
