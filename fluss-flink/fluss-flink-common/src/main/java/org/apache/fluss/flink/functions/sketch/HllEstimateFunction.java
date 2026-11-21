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

package org.apache.fluss.flink.functions.sketch;

import org.apache.datasketches.hll.HllSketch;
import org.apache.flink.table.functions.ScalarFunction;

/** Reads the approximate distinct count from serialized DataSketches HLL bytes. */
public class HllEstimateFunction extends ScalarFunction {
    /** Returns null for null or empty sketches. */
    public Double eval(byte[] bytes) {
        if (bytes == null) {
            return null;
        }
        HllSketch sketch = HllSketch.heapify(bytes);
        return sketch.isEmpty() ? null : sketch.getEstimate();
    }
}
