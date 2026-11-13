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

package org.apache.fluss.trino.functions;

import io.airlift.slice.Slices;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.AccumulatorStateSerializer;
import io.trino.spi.type.Type;
import org.apache.datasketches.hll.HllUnion;
import org.apache.datasketches.kll.KllDoublesSketch;

import java.lang.foreign.MemorySegment;
import java.util.Arrays;

import static io.trino.spi.type.VarbinaryType.VARBINARY;

/** Intermediate state format: a sketch-family byte followed by the native sketch bytes. */
public class SketchStateSerializer implements AccumulatorStateSerializer<SketchState> {
    @Override
    public Type getSerializedType() {
        return VARBINARY;
    }

    @Override
    public void serialize(SketchState state, BlockBuilder out) {
        if (state.getSketch() == null) {
            out.appendNull();
            return;
        }
        byte[] sketch = SketchSupport.bytes(state.getSketch());
        byte[] tagged = new byte[sketch.length + 1];
        tagged[0] = (byte) (state.getSketch() instanceof HllUnion ? 0 : 1);
        System.arraycopy(sketch, 0, tagged, 1, sketch.length);
        VARBINARY.writeSlice(out, Slices.wrappedBuffer(tagged));
    }

    @Override
    public void deserialize(Block block, int index, SketchState state) {
        if (block.isNull(index)) {
            state.setSketch(null);
            return;
        }
        byte[] tagged = VARBINARY.getSlice(block, index).getBytes();
        if (tagged.length < 2 || (tagged[0] != 0 && tagged[0] != 1)) {
            throw new IllegalArgumentException("Invalid sketch state family");
        }
        byte[] bytes = Arrays.copyOfRange(tagged, 1, tagged.length);
        state.setSketch(
                tagged[0] == 0
                        ? HllUnion.heapify(bytes)
                        : KllDoublesSketch.heapify(MemorySegment.ofArray(bytes)));
    }
}
