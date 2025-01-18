/*
 * Copyright (c) 2024 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.row.arrow;

import com.alibaba.fluss.compression.ArrowCompressionInfo;
import com.alibaba.fluss.shaded.arrow.org.apache.arrow.memory.BufferAllocator;
import com.alibaba.fluss.shaded.arrow.org.apache.arrow.memory.RootAllocator;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Deque;
import java.util.Map;

import static com.alibaba.fluss.record.TestData.DATA1_ROW_TYPE;
import static com.alibaba.fluss.row.arrow.ArrowWriter.BUFFER_USAGE_RATIO;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link ArrowWriterPool}. */
public class ArrowWriterPoolTest {
    private BufferAllocator allocator;

    @BeforeEach
    void setup() {
        this.allocator = new RootAllocator(Long.MAX_VALUE);
    }

    @AfterEach
    void tearDown() {
        allocator.close();
    }

    @Test
    void testWriterMap() {
        ArrowWriterPool arrowWriterPool = new ArrowWriterPool(allocator);
        Map<String, Deque<ArrowWriter>> freeWritersMap = arrowWriterPool.freeWriters();
        ArrowWriter writer1 =
                arrowWriterPool.getOrCreateWriter(
                        1L, 1, 1024, DATA1_ROW_TYPE, ArrowCompressionInfo.NO_COMPRESSION);
        assertThat(writer1.getWriteLimitInBytes()).isEqualTo((int) (1024 * BUFFER_USAGE_RATIO));
        assertThat(freeWritersMap.isEmpty()).isTrue();
        long epoch = writer1.getEpoch();
        writer1.recycle(epoch);
        assertThat(freeWritersMap.size()).isEqualTo(1);
        assertThat(freeWritersMap.get("1-1-NONE")).hasSize(1);
        assertThat(writer1.getEpoch()).isEqualTo(epoch + 1);
        // recycle the same epoch again, doesn't add it to pool
        writer1.recycle(epoch);
        assertThat(freeWritersMap.size()).isEqualTo(1);
        assertThat(freeWritersMap.get("1-1-NONE")).hasSize(1);

        ArrowWriter writer2 =
                arrowWriterPool.getOrCreateWriter(
                        1L, 2, 10, DATA1_ROW_TYPE, ArrowCompressionInfo.NO_COMPRESSION);
        assertThat(freeWritersMap.size()).isEqualTo(1);
        writer2.recycle(writer2.getEpoch());
        assertThat(freeWritersMap.size()).isEqualTo(2);

        // test key1: "tableId_schemaId"
        Deque<ArrowWriter> arrowWriters = freeWritersMap.get("1-1-NONE");
        assertThat(arrowWriters.size()).isEqualTo(1);
        writer1 =
                arrowWriterPool.getOrCreateWriter(
                        1L, 1, 1000, DATA1_ROW_TYPE, ArrowCompressionInfo.NO_COMPRESSION);
        assertThat(arrowWriters.size()).isEqualTo(0);
        assertThat(writer1.getWriteLimitInBytes()).isEqualTo((int) (1000 * BUFFER_USAGE_RATIO));
        ArrowWriter writer3WithKey1 =
                arrowWriterPool.getOrCreateWriter(
                        1L, 1, 100, DATA1_ROW_TYPE, ArrowCompressionInfo.NO_COMPRESSION);
        writer3WithKey1.recycle(writer3WithKey1.getEpoch());
        writer1.recycle(writer1.getEpoch());
        assertThat(arrowWriters.size()).isEqualTo(2);
        arrowWriterPool.close();
    }
}
