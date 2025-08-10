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

package com.alibaba.fluss.record;

import com.alibaba.fluss.exception.FlussRuntimeException;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static com.alibaba.fluss.record.TestData.DATA1;
import static com.alibaba.fluss.record.TestData.DATA1_ROW_TYPE;
import static com.alibaba.fluss.testutils.DataTestUtils.assertLogRecordBatchEquals;
import static com.alibaba.fluss.testutils.DataTestUtils.assertLogRecordsEquals;
import static com.alibaba.fluss.testutils.DataTestUtils.genLogRecordsWithBaseOffsetAndTimestamp;
import static com.alibaba.fluss.testutils.DataTestUtils.genMemoryLogRecordsWithBaseOffset;
import static org.apache.fluss.shaded.guava32.com.google.common.collect.Lists.newArrayList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Test for {@link FileLogRecords}. */
public class FileLogRecordsTest extends LogTestBase {
    private @TempDir File tempDir;
    private List<MemoryLogRecords> values;
    private FileLogRecords fileLogRecords;

    @BeforeEach
    void setup() throws Exception {
        values =
                Arrays.asList(
                        genMemoryLogRecordsWithBaseOffset(
                                0L, Collections.singletonList(new Object[] {0, " abcd"})),
                        genMemoryLogRecordsWithBaseOffset(
                                1L, Collections.singletonList(new Object[] {1, " efgh"})),
                        genMemoryLogRecordsWithBaseOffset(
                                2L, Collections.singletonList(new Object[] {2, " hijk"})));
        fileLogRecords = createFileLogRecords();
    }

    @AfterEach
    void tearDown() throws Exception {
        fileLogRecords.close();
    }

    @Test
    void testAppendProtectsFromOverflow() throws Exception {
        File fileMock = mock(File.class);
        FileChannel fileChannelMock = mock(FileChannel.class);
        when(fileChannelMock.size()).thenReturn((long) Integer.MAX_VALUE);

        FileLogRecords records =
                new FileLogRecords(fileMock, fileChannelMock, 0, Integer.MAX_VALUE, false);
        assertThatThrownBy(
                        () ->
                                records.append(
                                        genMemoryLogRecordsWithBaseOffset(
                                                1L,
                                                Collections.singletonList(
                                                        new Object[] {1, "efgh"}))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("bytes is too large for segment with current file position");
    }

    @Test
    void testOpenOversizeFile() throws Exception {
        File fileMock = mock(File.class);
        FileChannel fileChannelMock = mock(FileChannel.class);
        when(fileChannelMock.size()).thenReturn(Integer.MAX_VALUE + 5L);

        assertThatThrownBy(() -> new FileLogRecords(fileMock, fileChannelMock, 0, 100, false))
                .isInstanceOf(FlussRuntimeException.class)
                .hasMessageContaining("larger than the maximum allowed segment size");
    }

    @Test
    void testOutOfRangeSlice() {
        assertThatThrownBy(
                        () ->
                                fileLogRecords
                                        .slice(fileLogRecords.sizeInBytes() + 1, 15)
                                        .sizeInBytes())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("exceeds end position of FileRecords");
    }

    @Test
    void testFileSize() throws Exception {
        // Test that the cached size variable matches the actual file size as we append messages.
        assertThat(fileLogRecords.sizeInBytes()).isEqualTo(fileLogRecords.channel().size());
        for (int i = 3; i < 13; i++) {
            fileLogRecords.append(
                    genMemoryLogRecordsWithBaseOffset(
                            i, Collections.singletonList(new Object[] {i, "ijkl"})));
            assertThat(fileLogRecords.sizeInBytes()).isEqualTo(fileLogRecords.channel().size());
        }
    }

    @Test
    void testIterationOverPartialAndTruncation() throws Exception {
        // Test that adding invalid bytes to the end of the log doesn't break iteration.
        testPartialWrite(0, fileLogRecords);
        testPartialWrite(2, fileLogRecords);
        testPartialWrite(4, fileLogRecords);
        testPartialWrite(5, fileLogRecords);
        testPartialWrite(6, fileLogRecords);
    }

    @Test
    void testSliceSizeLimitWithConcurrentWrite() throws Exception {
        FileLogRecords log = FileLogRecords.open(new File(tempDir, "test2.tmp"));
        ExecutorService executor = Executors.newFixedThreadPool(2);
        int maxSizeInBytes = 16384;

        try {
            Future<Object> readerCompletion =
                    executor.submit(
                            () -> {
                                while (log.sizeInBytes() < maxSizeInBytes) {
                                    int currentSize = log.sizeInBytes();
                                    FileLogRecords slice = log.slice(0, currentSize);
                                    assertThat(slice.sizeInBytes()).isEqualTo(currentSize);
                                }
                                return null;
                            });

            Future<Object> writerCompletion =
                    executor.submit(
                            () -> {
                                while (log.sizeInBytes() < maxSizeInBytes) {
                                    append(log, values);
                                }
                                return null;
                            });

            writerCompletion.get();
            readerCompletion.get();
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void testIterationDoesntChangePosition() throws IOException {
        // Iterating over the file does file reads but shouldn't change the position of the
        // underlying FileChannel.
        long position = fileLogRecords.channel().position();
        Iterator<LogRecordBatch> fileLogRecordsIterator = fileLogRecords.batches().iterator();
        for (MemoryLogRecords value : values) {
            assertThat(fileLogRecordsIterator.hasNext()).isTrue();
            assertLogRecordBatchEquals(
                    DATA1_ROW_TYPE, fileLogRecordsIterator.next(), value.batchIterator().next());
        }
        assertThat(fileLogRecords.channel().position()).isEqualTo(position);
    }

    @Test
    void testRead() throws Exception {
        FileLogRecords read = fileLogRecords.slice(0, fileLogRecords.sizeInBytes());
        assertThat(read.sizeInBytes()).isEqualTo(fileLogRecords.sizeInBytes());
        assertLogRecordsEquals(DATA1_ROW_TYPE, read, fileLogRecords);

        List<LogRecordBatch> items = batches(read);
        LogRecordBatch first = items.get(0);

        // read from second message until the end.
        read =
                fileLogRecords.slice(
                        first.sizeInBytes(), fileLogRecords.sizeInBytes() - first.sizeInBytes());
        assertThat(read.sizeInBytes())
                .isEqualTo(fileLogRecords.sizeInBytes() - first.sizeInBytes());
        assertThat(batches(read)).isEqualTo(items.subList(1, items.size()));

        // read from second message and size is past the end of the file.
        read = fileLogRecords.slice(first.sizeInBytes(), fileLogRecords.sizeInBytes());
        assertThat(read.sizeInBytes())
                .isEqualTo(fileLogRecords.sizeInBytes() - first.sizeInBytes());
        assertThat(batches(read)).isEqualTo(items.subList(1, items.size()));

        // read from second message and position + size overflows.
        read = fileLogRecords.slice(first.sizeInBytes(), Integer.MAX_VALUE);
        assertThat(read.sizeInBytes())
                .isEqualTo(fileLogRecords.sizeInBytes() - first.sizeInBytes());
        assertThat(batches(read)).isEqualTo(items.subList(1, items.size()));

        // read from second message and size is past the end of the file on a view/slice.
        read =
                fileLogRecords
                        .slice(1, fileLogRecords.sizeInBytes() - 1)
                        .slice(first.sizeInBytes() - 1, fileLogRecords.sizeInBytes());
        assertThat(read.sizeInBytes())
                .isEqualTo(fileLogRecords.sizeInBytes() - first.sizeInBytes());
        assertThat(batches(read)).isEqualTo(items.subList(1, items.size()));

        // read from second message and position + size overflows on a view/slice.
        read =
                fileLogRecords
                        .slice(1, fileLogRecords.sizeInBytes() - 1)
                        .slice(first.sizeInBytes() - 1, Integer.MAX_VALUE);
        assertThat(read.sizeInBytes())
                .isEqualTo(fileLogRecords.sizeInBytes() - first.sizeInBytes());
        assertThat(batches(read)).isEqualTo(items.subList(1, items.size()));

        // read a single message starting from second message
        LogRecordBatch second = items.get(1);
        read = fileLogRecords.slice(first.sizeInBytes(), second.sizeInBytes());
        assertThat(read.sizeInBytes()).isEqualTo(second.sizeInBytes());
        assertThat(batches(read)).isEqualTo(Collections.singletonList(second));
    }

    @Test
    void testSearch() throws Exception {
        fileLogRecords.append(
                genMemoryLogRecordsWithBaseOffset(
                        50L, Collections.singletonList(new Object[] {0, " test"})));

        List<LogRecordBatch> batches = batches(fileLogRecords);
        int position = 0;

        int message1Size = batches.get(0).sizeInBytes();
        assertThat(fileLogRecords.searchForOffsetWithSize(0, 0))
                .isEqualTo(new FileLogRecords.LogOffsetPosition(0L, position, message1Size));
        position += message1Size;

        int message2Size = batches.get(1).sizeInBytes();
        assertThat(fileLogRecords.searchForOffsetWithSize(1, 0))
                .isEqualTo(new FileLogRecords.LogOffsetPosition(1L, position, message2Size));
        assertThat(fileLogRecords.searchForOffsetWithSize(1, position))
                .isEqualTo(new FileLogRecords.LogOffsetPosition(1L, position, message2Size));
        position += message2Size + batches.get(2).sizeInBytes();

        int message4Size = batches.get(3).sizeInBytes();
        assertThat(fileLogRecords.searchForOffsetWithSize(3, position))
                .isEqualTo(new FileLogRecords.LogOffsetPosition(50L, position, message4Size));
        assertThat(fileLogRecords.searchForOffsetWithSize(50, position))
                .isEqualTo(new FileLogRecords.LogOffsetPosition(50L, position, message4Size));
    }

    @Test
    void testIteratorWithLimits() throws Exception {
        // Test that the message set iterator obeys start and end slicing.
        LogRecordBatch batch = batches(fileLogRecords).get(1);
        int start = fileLogRecords.searchForOffsetWithSize(1, 0).position;
        int size = batch.sizeInBytes();
        FileLogRecords slice = fileLogRecords.slice(start, size);
        assertThat(batches(slice)).isEqualTo(Collections.singletonList(batch));
        FileLogRecords slice2 = fileLogRecords.slice(start, size - 1);
        assertThat(batches(slice2)).isEqualTo(Collections.emptyList());
    }

    @Test
    void testTruncate() throws Exception {
        LogRecordBatch batch = batches(fileLogRecords).get(0);
        int end = fileLogRecords.searchForOffsetWithSize(1, 0).position;
        fileLogRecords.truncateTo(end);
        assertThat(batches(fileLogRecords)).isEqualTo(Collections.singletonList(batch));
        assertThat(fileLogRecords.sizeInBytes()).isEqualTo(batch.sizeInBytes());
    }

    @Test
    void testTruncateNotCalledIfSizeIsSameAsTargetSize() throws Exception {
        // Test that truncateTo only calls truncate on the FileChannel if the size of the
        // FileChannel is bigger than the target size. This is important because some JVMs change
        // the mtime of the file, even if truncate should do nothing.
        FileChannel channelMock = mock(FileChannel.class);

        when(channelMock.size()).thenReturn(42L);
        when(channelMock.position(42L)).thenReturn(null);

        FileLogRecords fileRecords =
                new FileLogRecords(
                        new File(tempDir, "test2.tmp"), channelMock, 0, Integer.MAX_VALUE, false);
        fileRecords.truncateTo(42);

        verify(channelMock, atLeastOnce()).size();
        verify(channelMock, times(0)).truncate(anyLong());
    }

    @Test
    void testTruncateNotCalledIfSizeIsBiggerThanTargetSize() throws Exception {
        FileChannel channelMock = mock(FileChannel.class);

        when(channelMock.size()).thenReturn(42L);

        FileLogRecords fileRecords =
                new FileLogRecords(
                        new File(tempDir, "test2.tmp"), channelMock, 0, Integer.MAX_VALUE, false);

        assertThatThrownBy(() -> fileRecords.truncateTo(43))
                .isInstanceOf(FlussRuntimeException.class)
                .hasMessageContaining("Attempt to truncate log segment");
        verify(channelMock, atLeastOnce()).size();
    }

    @Test
    void testTruncateIfSizeIsDifferentToTargetSize() throws Exception {
        FileChannel channelMock = mock(FileChannel.class);

        when(channelMock.size()).thenReturn(42L);
        when(channelMock.truncate(anyLong())).thenReturn(channelMock);

        FileLogRecords fileRecords =
                new FileLogRecords(
                        new File(tempDir, "test2.tmp"), channelMock, 0, Integer.MAX_VALUE, false);
        fileRecords.truncateTo(23);

        verify(channelMock, atLeastOnce()).size();
        verify(channelMock).truncate(23);
    }

    @Test
    void testPreallocateTrue() throws Exception {
        File tempFile = new File(tempDir, "test2.tmp");
        FileLogRecords log = FileLogRecords.open(tempFile, false, 1024 * 1024, true);
        long position = log.channel().position();
        int size = log.sizeInBytes();
        assertThat(position).isEqualTo(0);
        assertThat(size).isEqualTo(0);
        assertThat(tempFile.length()).isEqualTo(1024 * 1024);
    }

    @Test
    void testPreallocateFalse() throws Exception {
        File tempFile = new File(tempDir, "test2.tmp");
        FileLogRecords log = FileLogRecords.open(tempFile, false, 1024 * 1024, false);
        long position = log.channel().position();
        int size = log.sizeInBytes();
        assertThat(position).isEqualTo(0);
        assertThat(size).isEqualTo(0);
        assertThat(tempFile.length()).isEqualTo(0);
    }

    @Test
    void testPreallocateClearShutdown() throws Exception {
        File tempFile = new File(tempDir, "test2.tmp");
        FileLogRecords log = FileLogRecords.open(tempFile, false, 1024 * 1024, true);
        append(log, values);

        int oldPosition = (int) log.channel().position();
        int oldSize = log.sizeInBytes();
        assertThat(oldPosition).isEqualTo(this.fileLogRecords.sizeInBytes());
        assertThat(oldSize).isEqualTo(this.fileLogRecords.sizeInBytes());
        log.close();

        File tempReopen = new File(tempFile.getAbsolutePath());
        FileLogRecords setReopen = FileLogRecords.open(tempReopen, true, 1024 * 1024, true);
        int position = (int) setReopen.channel().position();
        int size = setReopen.sizeInBytes();

        assertThat(position).isEqualTo(oldPosition);
        assertThat(size).isEqualTo(oldPosition);
        assertThat(tempReopen.length()).isEqualTo(oldPosition);
    }

    @Test
    void testSearchForTimestamp() throws Exception {
        FileLogRecords log = FileLogRecords.open(new File(tempDir, "test2.tmp"));
        log.append(genLogRecordsWithBaseOffsetAndTimestamp(0, 10L, DATA1));
        log.append(genLogRecordsWithBaseOffsetAndTimestamp(10, 20L, DATA1));
        log.append(genLogRecordsWithBaseOffsetAndTimestamp(20, 30L, DATA1));

        assertThat(log.searchForTimestamp(9L, 0, 0L)).isEqualTo(new TimestampAndOffset(10L, 0));
        assertThat(log.searchForTimestamp(10L, 0, 0L)).isEqualTo(new TimestampAndOffset(10L, 0));
        assertThat(log.searchForTimestamp(22L, 0, 0L)).isEqualTo(new TimestampAndOffset(30L, 20));
        assertThat(log.searchForTimestamp(30L, 0, 0L)).isEqualTo(new TimestampAndOffset(30L, 20));
        assertThat(log.searchForTimestamp(32L, 0, 0L)).isNull();
    }

    @Test
    void testLargestTimestampAfter() throws Exception {
        FileLogRecords log = FileLogRecords.open(new File(tempDir, "test2.tmp"));
        log.append(genLogRecordsWithBaseOffsetAndTimestamp(0, 10L, DATA1));
        log.append(genLogRecordsWithBaseOffsetAndTimestamp(10, 20L, DATA1));

        assertFoundTimestamp(new TimestampAndOffset(20L, 10), log.largestTimestampAfter(0));
    }

    private void assertFoundTimestamp(TimestampAndOffset expected, TimestampAndOffset actual) {
        assertThat(actual).isNotNull();
        assertThat(actual.getTimestamp()).isEqualTo(expected.getTimestamp());
        assertThat(actual.getOffset()).isEqualTo(expected.getOffset());
    }

    private FileLogRecords createFileLogRecords() throws Exception {
        FileLogRecords fileRecords = FileLogRecords.open(new File(tempDir, "test.tmp"));
        append(fileRecords, values);
        fileRecords.flush();
        return fileRecords;
    }

    private void append(FileLogRecords fileLogRecords, List<MemoryLogRecords> values)
            throws Exception {
        for (MemoryLogRecords records : values) {
            fileLogRecords.append(records);
        }
        fileLogRecords.flush();
    }

    private void testPartialWrite(int size, FileLogRecords fileRecords) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(size);
        for (int i = 0; i < size; i++) {
            buffer.put((byte) 0);
        }
        buffer.rewind();
        fileRecords.channel().write(buffer);

        // appending those bytes should not change the contents]
        assertLogRecordsListEquals(values, fileRecords);
    }

    private static List<LogRecordBatch> batches(FileLogRecords fileLogRecords) {
        return newArrayList(fileLogRecords.batches());
    }
}
