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

package org.apache.fluss.lake.hudi.tiering;

import org.apache.fluss.lake.hudi.tiering.writer.FlussRecordAsHudiRow;
import org.apache.fluss.lake.hudi.tiering.writer.RecordWriteBuffer;
import org.apache.fluss.lake.hudi.utils.meta.CkpMetadata;
import org.apache.fluss.lake.writer.WriterInitContext;
import org.apache.fluss.record.LogRecord;

import org.apache.flink.configuration.Configuration;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.model.HoodieFlinkInternalRow;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.WriteConcurrencyMode;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.index.bucket.BucketIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Base writer that writes Fluss log records to Hudi. */
public abstract class RecordWriter implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(RecordWriter.class);

    protected final int bucketNum;
    protected final HudiWriteTableInfo hudiTableInfo;
    protected final RecordWriteBuffer recordWriteBuffer;
    protected final FlussRecordAsHudiRow flussRecordAsHudiRecord;
    protected final CkpMetadata ckpMetadata;

    private final Map<String, Map<Integer, String>> bucketIndex = new HashMap<>();
    private final Set<String> incBucketIndex = new HashSet<>();

    public RecordWriter(
            WriterInitContext writerInitContext,
            HudiWriteTableInfo hudiTableInfo,
            CkpMetadata ckpMetadata) {
        this.bucketNum = writerInitContext.tableBucket().getBucket();
        this.flussRecordAsHudiRecord =
                new FlussRecordAsHudiRow(bucketNum, hudiTableInfo.getRowType());
        this.hudiTableInfo = hudiTableInfo;
        this.ckpMetadata = ckpMetadata;
        this.recordWriteBuffer =
                new RecordWriteBuffer(
                        hudiTableInfo, ckpMetadata, writerInitContext.tieringRoundTimestamp());
    }

    public abstract void write(LogRecord record) throws Exception;

    public Map<String, List<WriteStatus>> complete() throws Exception {
        try {
            recordWriteBuffer.flushRemaining();
            Map<String, List<WriteStatus>> writeResult = recordWriteBuffer.getWriteStatuses();
            LOG.info("Wrote Hudi records and got write statuses {}.", writeResult);
            return writeResult;
        } catch (Exception e) {
            throw new IOException("Failed to write Hudi records.", e);
        }
    }

    @Override
    public void close() {
        hudiTableInfo.close();
    }

    public void setRecordLocation(HoodieFlinkInternalRow internalRow, Configuration configuration) {
        String partition = internalRow.getPartitionPath();

        if (!configuration.get(FlinkOptions.OPERATION).equals(WriteOperationType.INSERT.value())) {
            bootstrapIndexIfNeed(partition);
        }

        Map<Integer, String> bucketToFileId =
                bucketIndex.computeIfAbsent(partition, ignored -> new HashMap<>());
        String bucketId = partition + "/" + bucketNum;

        if (incBucketIndex.contains(bucketId)) {
            internalRow.setInstantTime("I");
            internalRow.setFileId(bucketToFileId.get(bucketNum));
        } else if (bucketToFileId.containsKey(bucketNum)) {
            internalRow.setInstantTime("U");
            internalRow.setFileId(bucketToFileId.get(bucketNum));
        } else {
            String newFileId =
                    isNonBlockingConcurrencyControl(configuration)
                            ? BucketIdentifier.newBucketFileIdForNBCC(bucketNum)
                            : BucketIdentifier.newBucketFileIdPrefix(bucketNum);
            internalRow.setInstantTime("I");
            internalRow.setFileId(newFileId);
            bucketToFileId.put(bucketNum, newFileId);
            incBucketIndex.add(bucketId);
        }
    }

    public boolean isNonBlockingConcurrencyControl(Configuration config) {
        return WriteConcurrencyMode.isNonBlockingConcurrencyControl(
                config.getString(
                        HoodieWriteConfig.WRITE_CONCURRENCY_MODE.key(),
                        HoodieWriteConfig.WRITE_CONCURRENCY_MODE.defaultValue()));
    }

    private void bootstrapIndexIfNeed(String partition) {
        if (bucketIndex.containsKey(partition) && !bucketIndex.get(partition).isEmpty()) {
            return;
        }

        Map<Integer, String> bucketToFileIDMap = new HashMap<>();
        hudiTableInfo
                .getWriteClient()
                .getHoodieTable()
                .getHoodieView()
                .getLatestFileSlices(partition)
                .forEach(
                        fileSlice -> {
                            String fileId = fileSlice.getFileId();
                            int bucketNumber = BucketIdentifier.bucketIdFromFileId(fileId);
                            if (bucketNumber != bucketNum) {
                                return;
                            }
                            if (bucketToFileIDMap.containsKey(bucketNumber)
                                    && hudiTableInfo.getTableType()
                                            == HoodieTableType.MERGE_ON_READ) {
                                throw new IllegalStateException(
                                        String.format(
                                                "Duplicate fileId %s for bucket %s of partition %s.",
                                                fileId, bucketNumber, partition));
                            }
                            bucketToFileIDMap.put(bucketNumber, fileId);
                        });
        bucketIndex.put(partition, bucketToFileIDMap);
        LOG.debug("Bootstrapped Hudi bucket index {} for partition {}.", bucketIndex, partition);
    }
}
