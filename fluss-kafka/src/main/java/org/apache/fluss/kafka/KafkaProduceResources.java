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

package org.apache.fluss.kafka;

import org.apache.fluss.config.Configuration;
import org.apache.fluss.kafka.transcode.ArrowKafkaRecordTranscoder;
import org.apache.fluss.kafka.transcode.KafkaRecordTranscoder;
import org.apache.fluss.memory.LazyMemorySegmentPool;
import org.apache.fluss.shaded.arrow.org.apache.arrow.memory.BufferAllocator;
import org.apache.fluss.shaded.arrow.org.apache.arrow.memory.BufferAllocatorUtil;
import org.apache.fluss.utils.IOUtils;

/** Shared conversion memory, closed after the RPC request processors have stopped. */
final class KafkaProduceResources implements AutoCloseable {
    private final LazyMemorySegmentPool memorySegmentPool;
    private final BufferAllocator allocator;

    KafkaProduceResources(Configuration conf) {
        memorySegmentPool = LazyMemorySegmentPool.createServerBufferPool(conf);
        allocator = BufferAllocatorUtil.createBufferAllocator(null);
    }

    KafkaRecordTranscoder createTranscoder() {
        return new ArrowKafkaRecordTranscoder(allocator, memorySegmentPool);
    }

    @Override
    public void close() throws Exception {
        IOUtils.closeAll(allocator, memorySegmentPool);
    }
}
