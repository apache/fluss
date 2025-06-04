/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.flink.tiering.source;

import com.alibaba.fluss.lakehouse.committer.LakeCommitter;
import com.alibaba.fluss.lakehouse.serializer.SimpleVersionedSerializer;
import com.alibaba.fluss.lakehouse.writer.LakeTieringFactory;
import com.alibaba.fluss.lakehouse.writer.LakeWriter;
import com.alibaba.fluss.lakehouse.writer.WriterInitContext;
import com.alibaba.fluss.record.LogRecord;

import java.io.IOException;

/** An implementation of {@link LakeTieringFactory} for testing purpose. */
class TestingLakeTieringFactory implements LakeTieringFactory<TestingWriteResult, Integer> {

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
    public LakeCommitter<TestingWriteResult, Integer> createLakeCommitter() throws IOException {
        throw new UnsupportedOperationException("method createLakeCommitter is not supported.");
    }

    @Override
    public SimpleVersionedSerializer<Integer> getCommitableSerializer() {
        throw new UnsupportedOperationException("method getCommitableSerializer is not supported.");
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
}
