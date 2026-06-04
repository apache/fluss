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

import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.record.ArrowBatchData;

import org.apache.arrow.memory.BufferAllocator;
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
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.TableWriteImpl;
import org.apache.paimon.types.RowType;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;

/**
 * Helper class that encapsulates Arrow-dependent batch writing logic for append-only tables.
 *
 * <p>This class is separated from {@link AppendOnlyWriter} to avoid loading Arrow classes when
 * Arrow is not on the classpath. It is lazily loaded only when Arrow batch writing is actually
 * needed.
 */
class AppendOnlyArrowBatchHelper implements AutoCloseable {

    private final FileStoreTable fileStoreTable;
    private final TableWriteImpl<InternalRow> tableWrite;
    private final RowType tableRowType;
    private final int bucket;

    private static final Field BUCKET_FIELD =
            new Field(
                    TableDescriptor.BUCKET_COLUMN_NAME,
                    new FieldType(false, new ArrowType.Int(32, true), null),
                    null);
    private static final Field OFFSET_FIELD =
            new Field(
                    TableDescriptor.OFFSET_COLUMN_NAME,
                    new FieldType(false, new ArrowType.Int(64, true), null),
                    null);
    private static final Field TIMESTAMP_FIELD =
            new Field(
                    TableDescriptor.TIMESTAMP_COLUMN_NAME,
                    new FieldType(false, new ArrowType.Timestamp(TimeUnit.MILLISECOND, null), null),
                    null);

    // Child allocator for system column vectors, sharing the same root as the batch allocator
    @Nullable private BufferAllocator systemColumnAllocator;

    // Reusable resources for enriched VectorSchemaRoot with system columns
    @Nullable private VectorSchemaRoot enrichedRoot;
    @Nullable private Schema enrichedSchema;
    @Nullable private IntVector bucketVector;
    @Nullable private BigIntVector offsetVector;
    @Nullable private TimeStampMilliVector timestampVector;

    AppendOnlyArrowBatchHelper(
            FileStoreTable fileStoreTable,
            TableWriteImpl<InternalRow> tableWrite,
            RowType tableRowType,
            int bucket) {
        this.fileStoreTable = fileStoreTable;
        this.tableWrite = tableWrite;
        this.tableRowType = tableRowType;
        this.bucket = bucket;
    }

    /**
     * Writes an Arrow batch directly to Paimon Parquet files. Enriches the VectorSchemaRoot with
     * system columns (__bucket, __offset, __timestamp) and uses Paimon's {@link ArrowBundleRecords}
     * for efficient batch writing.
     */
    void writeArrowBatch(ArrowBatchData arrowBatchData, BinaryRow partition) throws Exception {
        int writtenBucket = bucket;
        if (fileStoreTable.store().bucketMode() == BucketMode.BUCKET_UNAWARE) {
            writtenBucket = 0;
        }

        VectorSchemaRoot originalRoot = arrowBatchData.getVectorSchemaRoot();
        long baseOffset = arrowBatchData.getBaseLogOffset();
        long timestamp = arrowBatchData.getTimestamp();
        int rowCount = originalRoot.getRowCount();

        ensureEnrichedRootInitialized(originalRoot, originalRoot.getVector(0).getAllocator());
        updateEnrichedVectorSchemaRoot(writtenBucket, baseOffset, timestamp, rowCount);

        ArrowBundleRecords arrowBundleRecords =
                new ArrowBundleRecords(enrichedRoot, tableRowType, false);

        tableWrite.writeBundle(partition, writtenBucket, arrowBundleRecords);
    }

    /**
     * Ensures the enriched VectorSchemaRoot is initialized with system column vectors. Reuses
     * system column vectors if schema matches. The enrichedRoot references the current
     * originalRoot's data vectors plus the system column vectors.
     */
    private void ensureEnrichedRootInitialized(
            VectorSchemaRoot originalRoot, BufferAllocator batchAllocator) {
        List<Field> originalFields = originalRoot.getSchema().getFields();
        int currentFieldCount = originalFields.size();

        // initialize system column vectors and enriched schema on first call, using a child
        // allocator that shares the same root as the batch allocator so all vectors are compatible
        if (bucketVector == null) {
            List<Field> enrichedFields = new ArrayList<>(originalFields);
            enrichedFields.add(BUCKET_FIELD);
            enrichedFields.add(OFFSET_FIELD);
            enrichedFields.add(TIMESTAMP_FIELD);
            enrichedSchema = new Schema(enrichedFields);

            if (systemColumnAllocator == null) {
                systemColumnAllocator =
                        batchAllocator
                                .getRoot()
                                .newChildAllocator("system-column-allocator", 0, Long.MAX_VALUE);
            }
            bucketVector = new IntVector(BUCKET_FIELD, systemColumnAllocator);
            offsetVector = new BigIntVector(OFFSET_FIELD, systemColumnAllocator);
            timestampVector = new TimeStampMilliVector(TIMESTAMP_FIELD, systemColumnAllocator);
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
    public void close() {
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
    }
}
