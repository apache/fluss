/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
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

package com.alibaba.fluss.record;

import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.memory.MemorySegmentOutputView;
import com.alibaba.fluss.types.DataType;

import org.junit.jupiter.api.BeforeEach;

import java.nio.ByteBuffer;

/** base test class for kv. */
abstract class KvTestBase {

    protected final byte magic = (byte) 0;
    protected final short schemaId = 1;
    protected final DataType[] baseRowFieldTypes =
            TestData.DATA1_ROW_TYPE.getChildren().toArray(new DataType[0]);

    protected MemorySegmentOutputView outputView;

    protected Configuration conf;

    @BeforeEach
    void beforeEach() {
        outputView = new MemorySegmentOutputView(100);
        conf = new Configuration();
    }

    protected static byte[] keyToBytes(KvRecord kvRecord) {
        ByteBuffer keyByteBuffer = kvRecord.getKey();
        byte[] keyBytes = new byte[keyByteBuffer.remaining()];
        keyByteBuffer.get(keyBytes);
        return keyBytes;
    }
}
