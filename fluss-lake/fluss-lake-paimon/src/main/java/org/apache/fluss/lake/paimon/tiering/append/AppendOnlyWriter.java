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

package org.apache.fluss.lake.paimon.tiering.append;

import org.apache.fluss.lake.paimon.tiering.RecordWriter;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.record.ArrowBatchData;
import org.apache.fluss.record.LogRecord;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.paimon.arrow.ArrowBundleRecords;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.TableWriteImpl;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;

import static org.apache.fluss.lake.paimon.tiering.PaimonLakeTieringFactory.FLUSS_LAKE_TIERING_COMMIT_USER;

/** A {@link RecordWriter} to write to Paimon's append-only table. */
public class AppendOnlyWriter extends RecordWriter<InternalRow> {

    private final FileStoreTable fileStoreTable;

    // Dedicated allocator for system column vectors, independent of any batch's allocator
    @Nullable private BufferAllocator systemColumnAllocator;

    // Reusable resources for enriched VectorSchemaRoot with system columns
    @Nullable private VectorSchemaRoot enrichedRoot;
    @Nullable private Schema enrichedSchema;
    @Nullable private IntVector bucketVector;
    @Nullable private BigIntVector offsetVector;
    @Nullable private TimeStampMilliVector timestampVector;

    public AppendOnlyWriter(
            FileStoreTable fileStoreTable,
            TableBucket tableBucket,
            @Nullable String partition,
            List<String> partitionKeys) {
        //noinspection unchecked
        super(
                (TableWriteImpl<InternalRow>)
                        // todo: set ioManager to support write-buffer-spillable
                        fileStoreTable.newWrite(FLUSS_LAKE_TIERING_COMMIT_USER),
                fileStoreTable.rowType(),
                tableBucket,
                partition,
                partitionKeys); // Pass to parent
        this.fileStoreTable = fileStoreTable;
    }

    @Override
    public void write(LogRecord record) throws Exception {
        flussRecordAsPaimonRow.setFlussRecord(record);

        // get partition once
        if (partition == null) {
            partition = tableWrite.getPartition(flussRecordAsPaimonRow);
        }

        // hacky, call internal method tableWrite.getWrite() to support
        // to write to given partition, otherwise, it'll always extract a partition from Paimon row
        // which may be costly
        int writtenBucket = bucket;
        // if bucket-unaware mode, we have to use bucket = 0 to write to follow paimon best practice
        if (fileStoreTable.store().bucketMode() == BucketMode.BUCKET_UNAWARE) {
            writtenBucket = 0;
        }
        tableWrite.getWrite().write(partition, writtenBucket, flussRecordAsPaimonRow);
    }

    /**
     * Writes an Arrow batch directly to Paimon Parquet files. Enriches the VectorSchemaRoot with
     * system columns (__bucket, __offset, __timestamp) and uses Paimon's {@link ArrowBundleRecords}
     * for efficient batch writing.
     */
    public void writeArrowBatch(ArrowBatchData arrowBatchData) throws Exception {
        int writtenBucket = bucket;
        if (fileStoreTable.store().bucketMode() == BucketMode.BUCKET_UNAWARE) {
            writtenBucket = 0;
        }

        VectorSchemaRoot originalRoot = arrowBatchData.getVectorSchemaRoot();
        long baseOffset = arrowBatchData.getBaseLogOffset();
        long timestamp = arrowBatchData.getTimestamp();
        int rowCount = originalRoot.getRowCount();

        ensureEnrichedRootInitialized(originalRoot);
        updateEnrichedVectorSchemaRoot(writtenBucket, baseOffset, timestamp, rowCount);

        ArrowBundleRecords arrowBundleRecords =
                new ArrowBundleRecords(enrichedRoot, tableRowType, false);

        // derive partition from the first row if not yet determined
        if (partition == null) {
            // todo: optimize how to get paimon partition
            InternalRow firstRow = arrowBundleRecords.iterator().next();
            partition = tableWrite.getPartition(firstRow);
        }

        tableWrite.writeBundle(partition, writtenBucket, arrowBundleRecords);
    }

    /**
     * Ensures the enriched VectorSchemaRoot is initialized with system column vectors. Reuses
     * system column vectors if schema matches. The enrichedRoot references the current
     * originalRoot's data vectors plus the system column vectors.
     */
    private void ensureEnrichedRootInitialized(VectorSchemaRoot originalRoot) {
        Schema originalSchema = originalRoot.getSchema();
        List<Field> originalFields = originalSchema.getFields();
        int currentFieldCount = originalFields.size();

        // initialize system column vectors on first call
        if (bucketVector == null) {
            Field bucketField =
                    new Field(
                            TableDescriptor.BUCKET_COLUMN_NAME,
                            new FieldType(false, new ArrowType.Int(32, true), null),
                            null);
            Field offsetField =
                    new Field(
                            TableDescriptor.OFFSET_COLUMN_NAME,
                            new FieldType(false, new ArrowType.Int(64, true), null),
                            null);
            Field timestampField =
                    new Field(
                            TableDescriptor.TIMESTAMP_COLUMN_NAME,
                            new FieldType(
                                    false,
                                    new ArrowType.Timestamp(TimeUnit.MILLISECOND, null),
                                    null),
                            null);

            List<Field> enrichedFields = new ArrayList<>(originalFields);
            enrichedFields.add(bucketField);
            enrichedFields.add(offsetField);
            enrichedFields.add(timestampField);
            enrichedSchema = new Schema(enrichedFields);

            if (systemColumnAllocator == null) {
                systemColumnAllocator = new RootAllocator(Long.MAX_VALUE);
            }
            bucketVector = new IntVector(bucketField, systemColumnAllocator);
            offsetVector = new BigIntVector(offsetField, systemColumnAllocator);
            timestampVector = new TimeStampMilliVector(timestampField, systemColumnAllocator);
        }

        // recreate enrichedRoot to reference the current originalRoot's data vectors
        List<FieldVector> allVectors = new ArrayList<>();
        for (int i = 0; i < currentFieldCount; i++) {
            allVectors.add(originalRoot.getVector(i));
        }
        allVectors.add(bucketVector);
        allVectors.add(offsetVector);
        allVectors.add(timestampVector);

        enrichedRoot = new VectorSchemaRoot(enrichedSchema, allVectors, originalRoot.getRowCount());
    }

    /**
     * Updates system column values in the enriched VectorSchemaRoot. Data columns are already
     * referenced from the original root.
     */
    private void updateEnrichedVectorSchemaRoot(
            int bucket, long baseOffset, long timestamp, int rowCount) {
        enrichedRoot.setRowCount(rowCount);

        if (bucketVector.getValueCapacity() < rowCount) {
            bucketVector.allocateNew(rowCount);
        }
        if (offsetVector.getValueCapacity() < rowCount) {
            offsetVector.allocateNew(rowCount);
        }
        if (timestampVector.getValueCapacity() < rowCount) {
            timestampVector.allocateNew(rowCount);
        }

        for (int i = 0; i < rowCount; i++) {
            bucketVector.set(i, bucket);
        }
        bucketVector.setValueCount(rowCount);

        for (int i = 0; i < rowCount; i++) {
            offsetVector.set(i, baseOffset + i);
        }
        offsetVector.setValueCount(rowCount);

        for (int i = 0; i < rowCount; i++) {
            timestampVector.set(i, timestamp);
        }
        timestampVector.setValueCount(rowCount);
    }

    @Override
    public void close() throws Exception {
        if (bucketVector != null) {
            bucketVector.close();
            bucketVector = null;
        }
        if (offsetVector != null) {
            offsetVector.close();
            offsetVector = null;
        }
        if (timestampVector != null) {
            timestampVector.close();
            timestampVector = null;
        }
        if (systemColumnAllocator != null) {
            systemColumnAllocator.close();
            systemColumnAllocator = null;
        }
        enrichedRoot = null;
        enrichedSchema = null;

        super.close();
    }
}
