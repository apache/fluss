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

package org.apache.fluss.server.kv.rowmerger.aggregate.functions;

import org.apache.fluss.types.DataType;

import org.apache.datasketches.hll.HllSketch;
import org.apache.datasketches.hll.Union;

/** HLL sketch aggregator for serialized Apache DataSketches HLL sketches. */
public class FieldHllSketchAgg extends FieldAggregator {

    private static final long serialVersionUID = 1L;

    public FieldHllSketchAgg(DataType dataType) {
        super(dataType);
    }

    @Override
    public Object agg(Object accumulator, Object inputField) {
        if (accumulator == null || inputField == null) {
            return accumulator == null ? inputField : accumulator;
        }

        try {
            HllSketch accumulatorSketch = HllSketch.heapify((byte[]) accumulator);
            HllSketch inputSketch = HllSketch.heapify((byte[]) inputField);
            int lgMaxK = Math.max(accumulatorSketch.getLgConfigK(), inputSketch.getLgConfigK());
            Union union = new Union(lgMaxK);
            union.update(accumulatorSketch);
            union.update(inputSketch);
            return union.getResult().toCompactByteArray();
        } catch (RuntimeException e) {
            throw new RuntimeException("Unable to deserialize or merge HLL sketch bytes.", e);
        }
    }
}
