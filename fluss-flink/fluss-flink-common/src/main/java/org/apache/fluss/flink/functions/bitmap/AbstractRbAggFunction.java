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

package org.apache.fluss.flink.functions.bitmap;

import org.apache.fluss.exception.FlussRuntimeException;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.AggregateFunction;
import org.roaringbitmap.RoaringBitmap;

import javax.annotation.Nullable;

import java.io.IOException;

/**
 * Shared base for bitmap aggregate UDFs that use {@link RoaringBitmap} as the accumulator.
 *
 * <p>The {@code @FunctionHint} annotation with {@code accumulator = @DataTypeHint("RAW")} tells
 * Flink's Table planner to skip reflection-based POJO extraction and instead use the {@link
 * TypeInformation} returned by {@link #getAccumulatorType()}, which provides the custom {@link
 * RoaringBitmapSerializer}. Without this annotation, Flink attempts POJO field extraction on
 * RoaringBitmap and fails.
 */
@FunctionHint(accumulator = @DataTypeHint(value = "RAW", bridgedTo = RoaringBitmap.class))
abstract class AbstractRbAggFunction extends AggregateFunction<byte[], RoaringBitmap> {

    @Override
    public RoaringBitmap createAccumulator() {
        return new RoaringBitmap();
    }

    /** Merges partial accumulators, required for two-phase aggregation in the Flink Table API. */
    public void merge(RoaringBitmap acc, Iterable<RoaringBitmap> it) {
        for (RoaringBitmap other : it) {
            if (other != null) {
                acc.or(other);
            }
        }
    }

    public void resetAccumulator(RoaringBitmap acc) {
        acc.clear();
    }

    @Override
    @Nullable
    public byte[] getValue(RoaringBitmap acc) {
        if (acc == null || acc.isEmpty()) {
            return null;
        }
        try {
            return BitmapUtils.toBytes(acc);
        } catch (IOException e) {
            throw new FlussRuntimeException("Failed to serialize bitmap accumulator.", e);
        }
    }

    @Override
    public TypeInformation<RoaringBitmap> getAccumulatorType() {
        return RoaringBitmapTypeInfo.INSTANCE;
    }
}
