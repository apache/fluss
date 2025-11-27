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

package org.apache.fluss.flink.tiering;

import org.apache.fluss.flink.tiering.committer.TestingCommittable;
import org.apache.fluss.flink.tiering.source.TestingWriteResultSerializer;
import org.apache.fluss.lake.committer.CommittedLakeSnapshot;
import org.apache.fluss.lake.committer.CommitterInitContext;
import org.apache.fluss.lake.committer.LakeCommitter;
import org.apache.fluss.lake.serializer.SimpleVersionedSerializer;
import org.apache.fluss.lake.writer.LakeTieringFactory;
import org.apache.fluss.lake.writer.LakeWriter;
import org.apache.fluss.lake.writer.WriterInitContext;
import org.apache.fluss.record.LogRecord;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/** An implementation of {@link LakeTieringFactory} for testing purpose. */
public class TestingLakeTieringFactory
        implements LakeTieringFactory<TestingWriteResult, TestingCommittable> {

    @Nullable private TestingLakeCommitter testingLakeCommitter;

    public TestingLakeTieringFactory(@Nullable TestingLakeCommitter testingLakeCommitter) {
        this.testingLakeCommitter = testingLakeCommitter;
    }

    public TestingLakeTieringFactory() {
        this(null);
    }

    @Override
    public LakeWriter<TestingWriteResult> createLakeWriter(WriterInitContext writerInitContext)
            throws IOException {
        return new TestingLakeWriter();
    }

    @Override
    public SimpleVersionedSerializer<TestingWriteResult> getWriteResultSerializer() {
        return new TestingWriteResultSerializer();
    }

    @Override
    public LakeCommitter<TestingWriteResult, TestingCommittable> createLakeCommitter(
            CommitterInitContext committerInitContext) throws IOException {
        if (testingLakeCommitter == null) {
            this.testingLakeCommitter = new TestingLakeCommitter();
        }
        return testingLakeCommitter;
    }

    @Override
    public SimpleVersionedSerializer<TestingCommittable> getCommittableSerializer() {
        throw new UnsupportedOperationException(
                "method getCommittableSerializer is not supported.");
    }

    private static final class TestingLakeWriter implements LakeWriter<TestingWriteResult> {

        private int writtenRecords;

        @Override
        public void write(LogRecord record) throws IOException {
            writtenRecords += 1;
        }

        @Override
        public TestingWriteResult complete() throws IOException {
            return new TestingWriteResult(writtenRecords);
        }

        @Override
        public void close() throws IOException {}
    }

    /** A lake committer for testing purpose. */
    public static final class TestingLakeCommitter
            implements LakeCommitter<TestingWriteResult, TestingCommittable> {

        private long currentSnapshot;

        @Nullable private final CommittedLakeSnapshot mockCommittedSnapshot;

        public TestingLakeCommitter() {
            this(null);
        }

        public TestingLakeCommitter(@Nullable CommittedLakeSnapshot mockCommittedSnapshot) {
            this.mockCommittedSnapshot = mockCommittedSnapshot;
            this.currentSnapshot =
                    mockCommittedSnapshot == null ? 0 : mockCommittedSnapshot.getLakeSnapshotId();
        }

        @Override
        public TestingCommittable toCommittable(List<TestingWriteResult> testingWriteResults)
                throws IOException {
            List<Integer> writeResults = new ArrayList<>();
            for (TestingWriteResult testingWriteResult : testingWriteResults) {
                writeResults.add(testingWriteResult.getWriteResult());
            }
            return new TestingCommittable(writeResults);
        }

        @Override
        public long commit(TestingCommittable committable, Map<String, String> snapshotProperties)
                throws IOException {
            return ++currentSnapshot;
        }

        @Override
        public void abort(TestingCommittable committable) throws IOException {
            // do nothing
        }

        @Override
        public @Nullable CommittedLakeSnapshot getMissingLakeSnapshot(
                @Nullable Long knownSnapshotId) throws IOException {
            if (knownSnapshotId == null) {
                return mockCommittedSnapshot;
            } else {
                return null;
            }
        }

        @Override
        public void close() throws Exception {}
    }
}
