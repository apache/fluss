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

package com.alibaba.fluss.lake.lance.tiering;

import com.alibaba.fluss.lake.lance.utils.LanceArrowUtils;
import com.alibaba.fluss.record.GenericRecord;
import com.alibaba.fluss.record.LogRecord;
import com.alibaba.fluss.row.GenericRow;
import com.alibaba.fluss.types.DataField;
import com.alibaba.fluss.types.DataTypes;
import com.alibaba.fluss.types.RowType;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static com.alibaba.fluss.record.ChangeType.APPEND_ONLY;
import static org.assertj.core.api.Assertions.assertThat;

/** The UT for Lance Arrow Writer. */
public class LanceArrowWriterTest {
    @Test
    public void test() throws Exception {
        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
            List<DataField> fields = Arrays.asList(new DataField("column1", DataTypes.INT()));

            RowType rowType = new RowType(fields);
            final int totalRows = 125;
            final int batchSize = 34;

            final LanceArrowWriter arrowWriter =
                    new LanceArrowWriter(
                            allocator, LanceArrowUtils.toArrowSchema(rowType), batchSize, rowType);
            AtomicInteger rowsWritten = new AtomicInteger(0);
            AtomicInteger rowsRead = new AtomicInteger(0);
            AtomicLong expectedBytesRead = new AtomicLong(0);

            Thread writerThread =
                    new Thread(
                            () -> {
                                try {
                                    for (int i = 0; i < totalRows; i++) {
                                        GenericRow genericRow = new GenericRow(1);
                                        genericRow.setField(0, rowsWritten.incrementAndGet());
                                        LogRecord logRecord =
                                                new GenericRecord(
                                                        i,
                                                        System.currentTimeMillis(),
                                                        APPEND_ONLY,
                                                        genericRow);

                                        arrowWriter.write(logRecord);
                                    }
                                    arrowWriter.setFinished();
                                } catch (Exception e) {
                                    e.printStackTrace();
                                    throw e;
                                }
                            });

            Thread readerThread =
                    new Thread(
                            () -> {
                                try {
                                    while (arrowWriter.loadNextBatch()) {
                                        VectorSchemaRoot root = arrowWriter.getVectorSchemaRoot();
                                        int rowCount = root.getRowCount();
                                        rowsRead.addAndGet(rowCount);
                                        try (ArrowRecordBatch recordBatch =
                                                new VectorUnloader(root).getRecordBatch()) {
                                            expectedBytesRead.addAndGet(
                                                    recordBatch.computeBodyLength());
                                        }
                                        for (int i = 0; i < rowCount; i++) {
                                            int value =
                                                    (int) root.getVector("column1").getObject(i);
                                            assertThat(value)
                                                    .isEqualTo(rowsRead.get() - rowCount + i + 1);
                                        }
                                    }
                                } catch (Exception e) {
                                    e.printStackTrace();
                                }
                            });

            writerThread.start();
            readerThread.start();

            writerThread.join();
            readerThread.join();
            assertThat(totalRows).isEqualTo(rowsWritten.get());
            assertThat(totalRows).isEqualTo(rowsRead.get());
            arrowWriter.close();
        }
    }
}
