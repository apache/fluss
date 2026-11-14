/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.kafka.transcode;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.exception.FlussRuntimeException;
import org.apache.fluss.kafka.backend.produce.KafkaProduceCommand.Record;
import org.apache.fluss.kafka.format.KafkaFieldDecoder;
import org.apache.fluss.kafka.format.KafkaFormatFactoryRegistry;
import org.apache.fluss.memory.LazyMemorySegmentPool;
import org.apache.fluss.metadata.KafkaTableSchema;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.row.arrow.ArrowWriter;
import org.apache.fluss.row.arrow.ArrowWriterPool;
import org.apache.fluss.server.kv.wal.ArrowWalBuilder;
import org.apache.fluss.shaded.arrow.org.apache.arrow.memory.BufferAllocator;

import javax.annotation.concurrent.ThreadSafe;

import java.util.List;

import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

/**
 * Shared, thread-safe converter of Kafka records into pooled Arrow log records.
 *
 * <p>Each conversion borrows a writer for the table ID, schema ID and compression settings.
 * Concurrent batches use separate writers, sharing the Arrow allocator and memory segment pool.
 * Callers must deallocate each result after the log append completes, including exceptional
 * completion.
 *
 * <p>The caller owns both shared memory resources and is responsible for closing them. The Arrow
 * allocator must remain open until all conversions finish, and the memory segment pool until all
 * results have been deallocated. Closing this transcoder only closes its writer pool and allows
 * in-flight conversions to finish.
 */
@Internal
@ThreadSafe
public final class ArrowKafkaRecordTranscoder implements KafkaRecordTranscoder {
    private final KafkaFormatFactoryRegistry formats = new KafkaFormatFactoryRegistry();
    private final KafkaCompiledWritePlanCache writePlanCache = new KafkaCompiledWritePlanCache();

    // Shared across server components; the caller closes it after all conversions finish.
    private final BufferAllocator arrowBufferAllocator;

    // Shared across server components; the caller closes it after all results are deallocated.
    private final LazyMemorySegmentPool memorySegmentPool;

    private final ArrowWriterPool writerPool;

    /** Creates a transcoder using shared memory resources owned and closed by the caller. */
    public ArrowKafkaRecordTranscoder(
            BufferAllocator arrowBufferAllocator, LazyMemorySegmentPool memorySegmentPool) {
        this.arrowBufferAllocator = checkNotNull(arrowBufferAllocator);
        this.memorySegmentPool = checkNotNull(memorySegmentPool);
        this.writerPool = new ArrowWriterPool(arrowBufferAllocator);
    }

    @Override
    public EncodedRecords transcode(int partitionId, List<Record> records, TableInfo tableInfo)
            throws Exception {
        checkArgument(partitionId >= 0, "Kafka partition ID must be non-negative.");
        checkArgument(!records.isEmpty(), "Cannot transcode an empty Kafka partition.");
        checkArgument(
                tableInfo.getSchemaId() <= Short.MAX_VALUE,
                "schemaId shouldn't be greater than the max value of short: %s",
                Short.MAX_VALUE);
        KafkaTopicWritePlan plan =
                writePlanCache.getOrCompile(
                        tableInfo,
                        () ->
                                new KafkaTopicWritePlan(
                                        KafkaTableSchema.resolve(tableInfo.toTableDescriptor()),
                                        formats));
        KafkaFieldDecoder valueDecoder = plan.valueDecoder();
        KafkaRowAssembler assembler = plan.rowAssembler();
        ArrowWriter writer = null;
        long epoch = 0;
        ArrowWalBuilder builder = null;
        boolean ownershipTransferred = false;
        try {
            writer =
                    writerPool.getOrCreateWriter(
                            tableInfo.getTableId(),
                            tableInfo.getSchemaId(),
                            // Keep the whole conversion in one batch.
                            Integer.MAX_VALUE,
                            tableInfo.getRowType(),
                            tableInfo.getTableConfig().getArrowCompressionInfo());
            epoch = writer.getEpoch();
            builder = new ArrowWalBuilder(tableInfo.getSchemaId(), writer, memorySegmentPool, true);
            for (Record record : records) {
                builder.append(
                        ChangeType.APPEND_ONLY,
                        assembler.assemble(
                                valueDecoder.decode(record.value()), record, partitionId));
            }
            EncodedRecords result = new EncodedRecords(builder.build(), builder);
            ownershipTransferred = true;
            return result;
        } catch (KafkaRecordEncodingException e) {
            throw e;
        } catch (Exception e) {
            throw new FlussRuntimeException(
                    "Kafka transcode Arrow memory limit exceeded (allocator limit "
                            + arrowBufferAllocator.getLimit()
                            + " bytes).",
                    e);
        } finally {
            if (!ownershipTransferred) {
                if (builder != null) {
                    builder.deallocate();
                } else if (writer != null) {
                    writer.recycle(epoch);
                }
            }
        }
    }

    /** Closes the writer pool; shared memory resources remain owned by the caller. */
    @Override
    public void close() {
        writerPool.close();
    }

    @VisibleForTesting
    ArrowWriterPool writerPool() {
        return writerPool;
    }
}
