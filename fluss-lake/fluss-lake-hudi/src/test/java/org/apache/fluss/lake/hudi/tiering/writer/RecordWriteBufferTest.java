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

package org.apache.fluss.lake.hudi.tiering.writer;

import org.apache.fluss.lake.writer.WriterInitContext;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.util.MutableObjectIterator;
import org.apache.hudi.client.HoodieFlinkWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.model.HoodieFlinkInternalRow;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.table.action.commit.BucketInfo;
import org.apache.hudi.table.action.commit.BucketType;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

/** Test for {@link RecordWriteBuffer}. */
class RecordWriteBufferTest {

    @Test
    void testFlushesCurrentBucketWhenBucketInfoChanges() throws Exception {
        TestingRecordWriteBuffer buffer = new TestingRecordWriteBuffer(false);
        BucketInfo firstBucketInfo = bucketInfo("partition_a", "file_a", BucketType.INSERT);
        BucketInfo secondBucketInfo = bucketInfo("partition_b", "file_b", BucketType.INSERT);

        buffer.bufferRecord(row("partition_a", "file_a", "I"));
        buffer.bufferRecord(row("partition_a", "file_a", "I"));
        assertThat(buffer.flushedBucketInfos).isEmpty();

        buffer.bufferRecord(row("partition_b", "file_b", "I"));
        assertThat(buffer.flushedBucketInfos).containsExactly(firstBucketInfo);

        buffer.flushRemaining();
        assertThat(buffer.flushedBucketInfos).containsExactly(firstBucketInfo, secondBucketInfo);
    }

    @Test
    void testDisposesEmptyBucketBeforeRetryingRecord() throws Exception {
        TestingRecordWriteBuffer buffer = new TestingRecordWriteBuffer(true);
        BucketInfo bucketInfo = bucketInfo("partition", "file", BucketType.INSERT);

        buffer.bufferRecord(row("partition", "file", "I"));

        assertThat(buffer.createdBuckets).hasSize(2);
        assertThat(buffer.createdBuckets.get(0).disposed).isTrue();
        assertThat(buffer.createdBuckets.get(1).isEmpty()).isFalse();
        assertThat(buffer.flushedBucketInfos).isEmpty();

        buffer.flushRemaining();
        assertThat(buffer.flushedBucketInfos).containsExactly(bucketInfo);
    }

    private static HoodieFlinkInternalRow row(String partition, String fileId, String instantTime) {
        HoodieFlinkInternalRow internalRow =
                new HoodieFlinkInternalRow(
                        "record_key", partition, WriteOperationType.INSERT.value(), rowData());
        internalRow.setFileId(fileId);
        internalRow.setInstantTime(instantTime);
        return internalRow;
    }

    private static RowData rowData() {
        return mock(RowData.class);
    }

    private static BucketInfo bucketInfo(String partition, String fileId, BucketType bucketType) {
        return new BucketInfo(bucketType, fileId, partition);
    }

    @SuppressWarnings("rawtypes")
    private static class TestingRecordWriteBuffer extends RecordWriteBuffer {

        private final List<TestingDataBucket> createdBuckets = new ArrayList<>();
        private final List<BucketInfo> flushedBucketInfos = new ArrayList<>();
        private boolean failNextBucketWrite;

        private TestingRecordWriteBuffer(boolean failFirstBucketWrite) {
            super(
                    null,
                    createConfig(),
                    mock(HoodieFlinkWriteClient.class),
                    null,
                    null,
                    WriterInitContext.UNKNOWN_TIERING_ROUND_TIMESTAMP);
            this.failNextBucketWrite = failFirstBucketWrite;
        }

        @Override
        protected DataBucket createDataBucket(BucketInfo bucketInfo) {
            TestingDataBucket dataBucket = new TestingDataBucket(bucketInfo, failNextBucketWrite);
            failNextBucketWrite = false;
            createdBuckets.add(dataBucket);
            return dataBucket;
        }

        @Override
        protected List<WriteStatus> writeRecords(
                String instant, String writeOperation, DataBucket dataBucket) {
            flushedBucketInfos.add(dataBucket.getBucketInfo());
            return Collections.emptyList();
        }

        @Override
        public String getLastPendingInstant() {
            return "001";
        }
    }

    private static Configuration createConfig() {
        Configuration config = new Configuration();
        config.set(FlinkOptions.OPERATION, WriteOperationType.INSERT.value());
        config.set(FlinkOptions.WRITE_TASK_MAX_SIZE, 128.0);
        config.set(FlinkOptions.WRITE_BATCH_SIZE, 128.0);
        config.set(FlinkOptions.WRITE_MERGE_MAX_MEMORY, 1);
        return config;
    }

    private static class TestingDataBucket extends RecordWriteBuffer.DataBucket {

        private final boolean failFirstWrite;

        private boolean failed;
        private boolean empty = true;
        private boolean disposed;

        private TestingDataBucket(BucketInfo bucketInfo, boolean failFirstWrite) {
            super(null, bucketInfo, 128.0);
            this.failFirstWrite = failFirstWrite;
        }

        @Override
        public MutableObjectIterator<BinaryRowData> getDataIterator() {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean writeRow(RowData rowData) throws IOException {
            if (failFirstWrite && !failed) {
                failed = true;
                return false;
            }
            empty = false;
            return true;
        }

        @Override
        public long getBufferSize() {
            return empty ? 0L : 1L;
        }

        @Override
        public boolean isEmpty() {
            return empty;
        }

        @Override
        public boolean isFull() {
            return false;
        }

        @Override
        public long getLastRecordSize() {
            return 1L;
        }

        @Override
        public void dispose() {
            disposed = true;
            empty = true;
        }
    }
}
