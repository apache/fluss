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

import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.lake.hudi.tiering.HudiWriteTableInfo;
import org.apache.fluss.lake.hudi.utils.meta.CkpMetadata;
import org.apache.fluss.lake.writer.WriterInitContext;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.runtime.operators.sort.BinaryInMemorySortBuffer;
import org.apache.flink.util.MutableObjectIterator;
import org.apache.hudi.client.HoodieFlinkWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.model.HoodieFlinkInternalRow;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.timeline.HoodieInstantTimeGenerator;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.MappingIterator;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.sink.buffer.MemorySegmentPoolFactory;
import org.apache.hudi.sink.bulk.RowDataKeyGen;
import org.apache.hudi.sink.exception.MemoryPagesExhaustedException;
import org.apache.hudi.sink.utils.BufferUtils;
import org.apache.hudi.sink.utils.TimeWait;
import org.apache.hudi.table.action.commit.BucketInfo;
import org.apache.hudi.table.action.commit.BucketType;
import org.apache.hudi.util.MutableIteratorWrapperIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;

/** Buffers records and writes them to Hudi in batches. */
public class RecordWriteBuffer {

    private static final Logger LOG = LoggerFactory.getLogger(RecordWriteBuffer.class);
    private static final long WAIT_INSTANT_TIMEOUT_MS = 5 * 60 * 1000L;

    private DataBucket bucket;

    private final TotalSizeTracer tracer;
    private final HoodieFlinkWriteClient writeClient;
    private final HudiWriteTableInfo hudiTableInfo;
    private final Configuration config;
    private final CkpMetadata ckpMetadata;
    private final Map<String, List<WriteStatus>> writeStatuses;
    private final HudiRecordConverter recordConverter;
    private final long tieringRoundTimestamp;

    public RecordWriteBuffer(
            HudiWriteTableInfo hudiTableInfo, CkpMetadata ckpMetadata, long tieringRoundTimestamp) {
        this(
                hudiTableInfo,
                hudiTableInfo.getFlinkConfig(),
                hudiTableInfo.getWriteClient(),
                ckpMetadata,
                HudiRecordConverter.getInstance(
                        RowDataKeyGen.instance(
                                hudiTableInfo.getFlinkConfig(), hudiTableInfo.getRowType())),
                tieringRoundTimestamp);
    }

    @VisibleForTesting
    RecordWriteBuffer(
            HudiWriteTableInfo hudiTableInfo,
            Configuration config,
            HoodieFlinkWriteClient writeClient,
            CkpMetadata ckpMetadata,
            HudiRecordConverter recordConverter,
            long tieringRoundTimestamp) {
        this.hudiTableInfo = hudiTableInfo;
        this.config = config;
        this.tracer = new TotalSizeTracer(config);
        this.writeClient = writeClient;
        this.ckpMetadata = ckpMetadata;
        this.writeStatuses = new HashMap<>();
        this.tieringRoundTimestamp = tieringRoundTimestamp;
        this.recordConverter = recordConverter;
    }

    public void bufferRecord(HoodieFlinkInternalRow internalRow) throws IOException {
        BucketInfo bucketInfo = getBucketInfo(internalRow);
        if (bucket != null && !isSameBucketInfo(bucket.getBucketInfo(), bucketInfo)) {
            flushAndDisposeBucket();
        }

        boolean success = doBufferRecord(internalRow, bucketInfo);
        if (!success) {
            flushAndDisposeBucket();
            success = doBufferRecord(internalRow, bucketInfo);
            if (!success) {
                throw new IOException("Hudi write buffer is too small to hold a single record.");
            }
        }

        if (bucket != null) {
            boolean totalSizeExceedsThreshold = tracer.trace(bucket.getLastRecordSize());
            if (bucket.isFull() || totalSizeExceedsThreshold) {
                flushAndDisposeBucket();
            }
        }
    }

    public void flushRemaining() {
        flushAndDisposeBucket();
        tracer.reset();
        writeClient.cleanHandles();
    }

    public Map<String, List<WriteStatus>> getWriteStatuses() {
        return writeStatuses;
    }

    private boolean doBufferRecord(HoodieFlinkInternalRow internalRow, BucketInfo bucketInfo)
            throws IOException {
        try {
            if (bucket == null) {
                bucket = createDataBucket(bucketInfo);
                LOG.debug("Initialized a new Hudi data bucket.");
            }
            return bucket.writeRow(internalRow.getRowData());
        } catch (MemoryPagesExhaustedException e) {
            LOG.warn("Hudi write buffer memory pages are exhausted, flushing current bucket.", e);
            return false;
        }
    }

    private void flushAndDisposeBucket() {
        if (bucket == null) {
            return;
        }
        if (bucket.isEmpty()) {
            bucket.dispose();
            bucket = null;
            return;
        }
        if (flushBucket(bucket)) {
            tracer.countDown(bucket.getBufferSize());
            bucket.dispose();
            bucket = null;
        }
    }

    private boolean flushBucket(DataBucket bucket) {
        String instantTime = getLastPendingInstant();
        LOG.info(
                "Flushing Hudi records for instant {}, size {}.",
                instantTime,
                bucket.getBufferSize());

        ValidationUtils.checkState(!bucket.isEmpty(), "Data bucket to flush has no records.");
        List<WriteStatus> writeStatus = writeStatuses.getOrDefault(instantTime, new ArrayList<>());
        writeStatus.addAll(
                writeRecords(instantTime, config.getString(FlinkOptions.OPERATION), bucket));
        writeStatuses.put(instantTime, writeStatus);
        return true;
    }

    protected DataBucket createDataBucket(BucketInfo bucketInfo) {
        return new DataBucket(
                BufferUtils.createBuffer(
                        hudiTableInfo.getRowType(),
                        MemorySegmentPoolFactory.createMemorySegmentPool(config)),
                bucketInfo,
                config.getDouble(FlinkOptions.WRITE_BATCH_SIZE));
    }

    protected List<WriteStatus> writeRecords(
            String instant, String writeOperation, DataBucket dataBucket) {
        Iterator<BinaryRowData> rowItr =
                new MutableIteratorWrapperIterator<>(
                        dataBucket.getDataIterator(),
                        () -> new BinaryRowData(hudiTableInfo.getRowType().getFieldCount()));
        Iterator<HoodieRecord> recordItr =
                new MappingIterator<>(
                        rowItr,
                        rowData -> recordConverter.convert(rowData, dataBucket.getBucketInfo()));

        if (WriteOperationType.UPSERT.value().equals(writeOperation)) {
            return writeClient.upsert(recordItr, dataBucket.bucketInfo, instant);
        } else if (WriteOperationType.INSERT.value().equals(writeOperation)) {
            return writeClient.insert(recordItr, dataBucket.bucketInfo, instant);
        }
        LOG.warn("Unsupported Hudi write operation {}, skip writing records.", writeOperation);
        return Collections.emptyList();
    }

    public String getLastPendingInstant() {
        String instant = ckpMetadata.lastPendingInstant();
        TimeWait timeWait =
                TimeWait.builder()
                        .timeout(WAIT_INSTANT_TIMEOUT_MS)
                        .action("instant initialize")
                        .build();
        String roundLowerBoundInstant = roundLowerBoundInstant();
        while (instant == null
                || (roundLowerBoundInstant != null
                        && roundLowerBoundInstant.compareTo(instant) >= 0)) {
            timeWait.waitFor();
            instant = ckpMetadata.lastPendingInstant();
            LOG.info("Waiting for Hudi instant to be initialized: {}", instant);
        }
        return instant;
    }

    private String roundLowerBoundInstant() {
        if (tieringRoundTimestamp == WriterInitContext.UNKNOWN_TIERING_ROUND_TIMESTAMP) {
            return null;
        }
        return HoodieInstantTimeGenerator.formatDate(new Date(tieringRoundTimestamp));
    }

    private static BucketInfo getBucketInfo(HoodieFlinkInternalRow internalRow) {
        BucketType bucketType;
        switch (internalRow.getInstantTime()) {
            case "I":
                bucketType = BucketType.INSERT;
                break;
            case "U":
                bucketType = BucketType.UPDATE;
                break;
            default:
                throw new HoodieException(
                        "Unexpected Hudi bucket type: " + internalRow.getInstantTime());
        }
        return new BucketInfo(bucketType, internalRow.getFileId(), internalRow.getPartitionPath());
    }

    private static boolean isSameBucketInfo(BucketInfo left, BucketInfo right) {
        return left.getBucketType() == right.getBucketType()
                && Objects.equals(left.getFileIdPrefix(), right.getFileIdPrefix())
                && Objects.equals(left.getPartitionPath(), right.getPartitionPath());
    }

    /** Buffered rows for one Hudi file bucket. */
    protected static class DataBucket {

        private final BinaryInMemorySortBuffer dataBuffer;
        private final BucketInfo bucketInfo;
        private final BufferSizeDetector detector;

        protected DataBucket(
                BinaryInMemorySortBuffer dataBuffer, BucketInfo bucketInfo, double batchSize) {
            this.dataBuffer = dataBuffer;
            this.bucketInfo = bucketInfo;
            this.detector = new BufferSizeDetector(batchSize);
        }

        public MutableObjectIterator<BinaryRowData> getDataIterator() {
            return dataBuffer.getIterator();
        }

        public boolean writeRow(RowData rowData) throws IOException {
            boolean success = dataBuffer.write(rowData);
            if (success) {
                detector.detect(rowData);
            }
            return success;
        }

        public BucketInfo getBucketInfo() {
            return bucketInfo;
        }

        public long getBufferSize() {
            return detector.totalSize;
        }

        public boolean isEmpty() {
            return dataBuffer.isEmpty();
        }

        public boolean isFull() {
            return detector.isFull();
        }

        public long getLastRecordSize() {
            return detector.getLastRecordSize();
        }

        public void dispose() {
            dataBuffer.dispose();
            detector.reset();
        }
    }

    private static class BufferSizeDetector {

        private static final int DENOMINATOR = 100;

        private final Random random = new Random(47);
        private final double batchSizeBytes;

        private long lastRecordSize = -1L;
        private long totalSize;

        BufferSizeDetector(double batchSizeMb) {
            this.batchSizeBytes = batchSizeMb * 1024 * 1024;
        }

        void detect(Object record) {
            if (record instanceof BinaryRowData) {
                lastRecordSize = ((BinaryRowData) record).getSizeInBytes();
            } else if (lastRecordSize == -1 || sampling()) {
                lastRecordSize = ((FlussRowAsHudiRow) record).sizeInBytes();
            }
            totalSize += lastRecordSize;
        }

        boolean isFull() {
            return totalSize > batchSizeBytes;
        }

        long getLastRecordSize() {
            return lastRecordSize;
        }

        void reset() {
            lastRecordSize = -1L;
            totalSize = 0L;
        }

        private boolean sampling() {
            return random.nextInt(DENOMINATOR) == 1;
        }
    }

    private static class TotalSizeTracer {

        private long bufferSize;
        private final double maxBufferSize;

        TotalSizeTracer(Configuration conf) {
            long mergeReaderMem = 100;
            long mergeMapMaxMem = conf.getInteger(FlinkOptions.WRITE_MERGE_MAX_MEMORY);
            this.maxBufferSize =
                    (conf.getDouble(FlinkOptions.WRITE_TASK_MAX_SIZE)
                                    - mergeReaderMem
                                    - mergeMapMaxMem)
                            * 1024
                            * 1024;
            ValidationUtils.checkState(
                    maxBufferSize > 0,
                    String.format(
                            "'%s' should be greater than '%s' plus merge reader memory.",
                            FlinkOptions.WRITE_TASK_MAX_SIZE.key(),
                            FlinkOptions.WRITE_MERGE_MAX_MEMORY.key()));
        }

        boolean trace(long recordSize) {
            bufferSize += recordSize;
            return bufferSize > maxBufferSize;
        }

        void countDown(long size) {
            bufferSize -= size;
        }

        void reset() {
            bufferSize = 0L;
        }
    }
}
