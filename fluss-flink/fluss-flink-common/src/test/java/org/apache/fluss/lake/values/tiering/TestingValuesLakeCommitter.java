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
import org.apache.fluss.lake.serializer.SimpleVersionedSerializer;
import org.apache.fluss.lake.values.TestingValuesLake;
import org.apache.fluss.utils.InstantiationUtils;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/** Implementation of {@link LakeCommitter} for values lake. */
public class TestingValuesLakeCommitter
        implements LakeCommitter<
                TestingValuesLakeWriter.TestingValuesWriteResult,
                TestingValuesLakeCommitter.TestingValuesCommittable> {
    private final String tableId;

    public TestingValuesLakeCommitter(String tableId) {
        this.tableId = tableId;
    }

    @Override
    public TestingValuesCommittable toCommittable(
            List<TestingValuesLakeWriter.TestingValuesWriteResult> valuesWriteResults)
            throws IOException {
        return new TestingValuesCommittable(
                valuesWriteResults.stream()
                        .map(TestingValuesLakeWriter.TestingValuesWriteResult::getStageId)
                        .collect(Collectors.toList()));
    }

    @Override
    public long commit(TestingValuesCommittable committable, Map<String, String> snapshotProperties)
            throws IOException {
        return TestingValuesLake.commit(tableId, committable.getStageIds(), snapshotProperties);
    }

    @Override
    public void abort(TestingValuesCommittable committable) throws IOException {
        TestingValuesLake.abort(tableId, committable.getStageIds());
    }

    @Override
    public CommittedLakeSnapshot getMissingLakeSnapshot(@Nullable Long latestLakeSnapshotIdOfFluss)
            throws IOException {
        return null;
    }

    @Override
    public void close() throws Exception {}

    /** Committable of {@link TestingValuesLake}. */
    public static class TestingValuesCommittable implements Serializable {
        private static final long serialVersionUID = 1L;

        private final List<String> stageIdList = new ArrayList<>();

        public TestingValuesCommittable(List<String> stageIds) {
            this.stageIdList.addAll(stageIds);
        }

        public List<String> getStageIds() {
            return stageIdList;
        }
    }

    /** A serializer for {@link TestingValuesCommittable}. */
    public static class ValuesCommittableSerializer
            implements SimpleVersionedSerializer<TestingValuesCommittable> {
        private static final int CURRENT_VERSION = 1;

        @Override
        public int getVersion() {
            return CURRENT_VERSION;
        }

        @Override
        public byte[] serialize(TestingValuesCommittable committable) throws IOException {
            return InstantiationUtils.serializeObject(committable);
        }

        @Override
        public TestingValuesCommittable deserialize(int version, byte[] serialized)
                throws IOException {
            TestingValuesCommittable valuesCommittable;
            try {
                valuesCommittable =
                        InstantiationUtils.deserializeObject(
                                serialized, getClass().getClassLoader());
            } catch (ClassNotFoundException e) {
                throw new IOException(e);
            }
            return valuesCommittable;
        }
    }
}
