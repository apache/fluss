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

import org.apache.fluss.lake.writer.LakeWriteResult;

import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for default methods of {@link LakeCommitter}. */
class LakeCommitterTest {

    @Test
    void testToCommittableWithoutWatermark() throws IOException {
        TestingLakeCommitter committer = new TestingLakeCommitter();
        List<TestingWriteResult> writeResults = Collections.singletonList(new TestingWriteResult());

        Object committable = committer.toCommittable(writeResults);

        assertThat(committable).isSameAs(committer.committable);
        assertThat(committer.writeResults).isSameAs(writeResults);
        assertThat(committer.watermark).isNull();
    }

    private static final class TestingWriteResult implements LakeWriteResult {}

    private static final class TestingLakeCommitter
            implements LakeCommitter<TestingWriteResult, Object> {

        private final Object committable = new Object();
        private List<TestingWriteResult> writeResults;
        private Long watermark = Long.MAX_VALUE;

        @Override
        public Object toCommittable(
                List<TestingWriteResult> writeResults, @Nullable Long watermark) {
            this.writeResults = writeResults;
            this.watermark = watermark;
            return committable;
        }

        @Override
        public LakeCommitResult commit(Object committable, Map<String, String> snapshotProperties) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void abort(Object committable) {
            throw new UnsupportedOperationException();
        }

        @Nullable
        @Override
        public CommittedLakeSnapshot getMissingLakeSnapshot(
                @Nullable Long latestLakeSnapshotIdOfFluss) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close() {
            // No-op.
        }
    }
}
