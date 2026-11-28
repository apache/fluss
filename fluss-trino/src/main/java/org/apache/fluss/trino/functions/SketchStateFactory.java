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

import io.trino.spi.function.AccumulatorStateFactory;
import io.trino.spi.function.GroupedAccumulatorState;

import java.util.ArrayList;
import java.util.List;

/** Creates independent single and grouped sketch accumulators. */
public class SketchStateFactory implements AccumulatorStateFactory<SketchState> {
    @Override
    public SketchState createSingleState() {
        return new SingleState();
    }

    @Override
    public SketchState createGroupedState() {
        return new GroupedState();
    }

    /** Single aggregate state. */
    public static class SingleState implements SketchState {
        private Object sketch;

        @Override
        public Object getSketch() {
            return sketch;
        }

        @Override
        public void setSketch(Object value) {
            sketch = value;
        }

        @Override
        public void addMemoryUsage(long bytes) {}

        @Override
        public long getEstimatedSize() {
            return 32 + SketchSupport.estimatedSize(sketch);
        }
    }

    /** Grouped aggregate state with explicit retained-size accounting. */
    public static class GroupedState implements SketchState, GroupedAccumulatorState {
        private final List<Object> sketches = new ArrayList<>();
        private int groupId;
        private long sketchBytes;

        @Override
        public void setGroupId(int id) {
            groupId = id;
        }

        @Override
        public void ensureCapacity(int size) {
            while (sketches.size() < size) {
                sketches.add(null);
            }
        }

        @Override
        public Object getSketch() {
            return sketches.get(groupId);
        }

        @Override
        public void setSketch(Object sketch) {
            sketchBytes +=
                    SketchSupport.estimatedSize(sketch) - SketchSupport.estimatedSize(getSketch());
            sketches.set(groupId, sketch);
        }

        @Override
        public void addMemoryUsage(long bytes) {
            sketchBytes += bytes;
        }

        @Override
        public long getEstimatedSize() {
            return 64 + 16L * sketches.size() + sketchBytes;
        }
    }
}
