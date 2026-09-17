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

package org.apache.fluss.client.arrow;

import org.apache.fluss.annotation.PublicEvolving;
import org.apache.fluss.record.ArrowIpcBatch;
import org.apache.fluss.utils.ByteBufferReadableChannel;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ReadChannel;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.TransferPair;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** Decodes serialized Fluss Arrow batches using a caller-owned Arrow Java allocator. */
@PublicEvolving
public final class ArrowBatchReader {
    private ArrowBatchReader() {}

    /**
     * Decodes a batch, applying its logical row range, column projection and schema evolution.
     *
     * <p>The caller owns the returned vectors and must close them before closing {@code allocator}.
     * This method does not close the allocator, including on failure. The immutable input can be
     * decoded again or shared with other consumers.
     *
     * @param batch the serialized batch returned by the scanner
     * @param allocator allocator for the decoded vectors
     * @return a batch whose vectors and change types describe the same logical rows
     * @throws IOException if the IPC messages cannot be read
     */
    public static ArrowBatch read(ArrowIpcBatch batch, BufferAllocator allocator)
            throws IOException {
        checkNotNull(batch, "batch");
        checkNotNull(allocator, "allocator");
        VectorSchemaRoot root = VectorSchemaRoot.create(readSchema(batch.getSchema()), allocator);
        try {
            try (ReadChannel channel =
                            new ReadChannel(new ByteBufferReadableChannel(batch.getRecordBatch()));
                    ArrowRecordBatch recordBatch =
                            MessageSerializer.deserializeRecordBatch(channel, allocator)) {
                new UnshadedFlussVectorLoader(root, UnshadedArrowCompressionFactory.INSTANCE)
                        .load(recordBatch);
            }
            checkArgument(
                    batch.getRowOffset() <= root.getRowCount() - batch.getRecordCount(),
                    "Logical row range exceeds the Arrow payload row count.");
            if (batch.getRowOffset() != 0 || batch.getRecordCount() != root.getRowCount()) {
                VectorSchemaRoot sliced = root.slice(batch.getRowOffset(), batch.getRecordCount());
                root.close();
                root = sliced;
            }
            int[] mapping = batch.getColumnMapping();
            if (mapping != null) {
                VectorSchemaRoot projected =
                        project(root, readSchema(batch.getOutputSchema()), mapping, allocator);
                root.close();
                root = projected;
            }
            ArrowBatch result = new ArrowBatch(root, batch);
            root = null;
            return result;
        } finally {
            if (root != null) {
                root.close();
            }
        }
    }

    private static Schema readSchema(ByteBuffer bytes) throws IOException {
        try (ReadChannel channel = new ReadChannel(new ByteBufferReadableChannel(bytes))) {
            return MessageSerializer.deserializeSchema(channel);
        }
    }

    private static VectorSchemaRoot project(
            VectorSchemaRoot source, Schema schema, int[] mapping, BufferAllocator allocator) {
        List<Field> fields = schema.getFields();
        List<FieldVector> vectors = new ArrayList<>(mapping.length);
        try {
            for (int i = 0; i < mapping.length; i++) {
                if (mapping[i] < 0) {
                    FieldVector vector = fields.get(i).createVector(allocator);
                    vectors.add(vector);
                    vector.allocateNew();
                    for (int row = 0; row < source.getRowCount(); row++) {
                        vector.setNull(row);
                    }
                    vector.setValueCount(source.getRowCount());
                } else {
                    TransferPair transfer =
                            source.getVector(mapping[i]).getTransferPair(fields.get(i), allocator);
                    vectors.add((FieldVector) transfer.getTo());
                    transfer.splitAndTransfer(0, source.getRowCount());
                }
            }
            return new VectorSchemaRoot(fields, vectors, source.getRowCount());
        } catch (Throwable t) {
            vectors.forEach(FieldVector::close);
            throw t;
        }
    }
}
