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

package org.apache.fluss.record;

import org.apache.fluss.row.TestInternalRowGenerator;
import org.apache.fluss.row.indexed.IndexedRow;
import org.apache.fluss.testutils.DataTestUtils;
import org.apache.fluss.types.RowType;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import static org.apache.fluss.testutils.DataTestUtils.indexedRow;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link FileLogRecords} and {@link MemoryLogRecords}. */
final class LogRecordsTest extends LogTestBase {

    private @TempDir File tempDir;
    private File file;
    private FileChannel fileChannel;

    private FileLogRecords fileLogRecords;

    @BeforeEach
    protected void before() throws IOException {
        super.before();
        file = new File(tempDir, "test.log");
        file.createNewFile();
        fileChannel =
                FileChannel.open(file.toPath(), StandardOpenOption.READ, StandardOpenOption.WRITE);
    }

    @Test
    void testSimpleData() throws Exception {
        Object[] objects1 = new Object[] {1, "1"};
        Object[] objects2 = new Object[] {2, "2"};

        List<IndexedRow> rows1 =
                Arrays.asList(indexedRow(baseRowType, objects1), indexedRow(baseRowType, objects2));
        MemoryLogRecords memoryLogRecords1 = DataTestUtils.genIndexedMemoryLogRecords(rows1);

        // Create FileLogRecords.
        fileLogRecords = new FileLogRecords(file, fileChannel, 0, Integer.MAX_VALUE, false);
        fileLogRecords.append(memoryLogRecords1);

        // verify data.
        Iterator<LogRecordBatch> iterator = fileLogRecords.batches().iterator();
        Iterator<LogRecordBatch> memoryIterator = memoryLogRecords1.batches().iterator();

        assertThat(iterator.hasNext()).isTrue();
        assertThat(memoryIterator.hasNext()).isTrue();
        assertIndexedLogRecordBatchAndRowEquals(
                iterator.next(), memoryIterator.next(), baseRowType, rows1);

        assertThat(iterator.hasNext()).isFalse();
        assertThat(memoryIterator.hasNext()).isFalse();
    }

    @Test
    void testAllTypeData() throws Exception {
        RowType allRowType = TestInternalRowGenerator.createAllRowType();

        List<IndexedRow> rows1 = createAllTypeRowDataList();
        MemoryLogRecords memoryLogRecords1 = DataTestUtils.genIndexedMemoryLogRecords(rows1);

        // Create FileLogRecords.
        fileLogRecords = new FileLogRecords(file, fileChannel, 0, Integer.MAX_VALUE, false);
        fileLogRecords.append(memoryLogRecords1);

        Iterator<LogRecordBatch> iterator = fileLogRecords.batches().iterator();
        Iterator<LogRecordBatch> memoryIterator = memoryLogRecords1.batches().iterator();

        assertThat(iterator.hasNext()).isTrue();
        assertThat(memoryIterator.hasNext()).isTrue();
        assertIndexedLogRecordBatchAndRowEquals(
                iterator.next(), memoryIterator.next(), allRowType, rows1);

        assertThat(iterator.hasNext()).isFalse();
        assertThat(memoryIterator.hasNext()).isFalse();
    }

    @AfterEach
    public void after() throws IOException {
        if (fileLogRecords != null) {
            fileLogRecords.close();
        }
    }
}
