/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.flink.tiering.committer;

import com.alibaba.fluss.lake.serializer.SimpleVersionedSerializer;

import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/** Simple serializer for int array for testing purpose. */
class TestingCommittableSerializer implements SimpleVersionedSerializer<TestingCommittable> {

    @Override
    public int getVersion() {
        return 0;
    }

    @Override
    public byte[] serialize(TestingCommittable obj) throws IOException {
        final DataOutputSerializer out = new DataOutputSerializer(256);
        out.writeInt(obj.writeResults().size());
        for (Integer integer : obj.writeResults()) {
            out.writeInt(integer);
        }
        return out.getCopyOfBuffer();
    }

    @Override
    public TestingCommittable deserialize(int version, byte[] serialized) throws IOException {
        final DataInputDeserializer in = new DataInputDeserializer(serialized);
        int numResults = in.readInt();
        List<Integer> writeResults = new ArrayList<>(numResults);
        for (int i = 0; i < numResults; i++) {
            writeResults.add(in.readInt());
        }
        return new TestingCommittable(writeResults);
    }
}
