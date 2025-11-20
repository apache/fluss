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

package org.apache.fluss.lake.values.tiering;

import org.apache.fluss.lake.committer.CommittedLakeSnapshot;
import org.apache.fluss.lake.committer.LakeCommitter;
import org.apache.fluss.lake.values.ValuesLake;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * {@link LakeCommitter} for {@link ValuesLake}.
 */
public class ValuesLakeCommitter implements LakeCommitter<ValuesWriteResult, ValuesCommittable> {
    private final String tableId;

    public ValuesLakeCommitter(String tableId) {
        this.tableId = tableId;
    }

    @Override
    public ValuesCommittable toCommittable(List<ValuesWriteResult> valuesWriteResults)
            throws IOException {
        return new ValuesCommittable(
                valuesWriteResults.stream()
                        .map(ValuesWriteResult::getStageId)
                        .collect(Collectors.toList()));
    }

    @Override
    public long commit(ValuesCommittable committable, Map<String, String> snapshotProperties)
            throws IOException {
        return ValuesLake.commit(tableId, committable.getStageIds(), snapshotProperties);
    }

    @Override
    public void abort(ValuesCommittable committable) throws IOException {
        ValuesLake.abort(tableId, committable.getStageIds());
    }

    @Override
    public CommittedLakeSnapshot getMissingLakeSnapshot(
            @Nullable Long latestLakeSnapshotIdOfFluss) throws IOException {
        return null;
    }

    @Override
    public void close() throws Exception {
    }
}
