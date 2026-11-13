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

import org.apache.datasketches.kll.KllDoublesSketch;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;

/** Flink type information with a binary sketch accumulator serializer. */
public final class KllDoublesTypeInfo extends TypeInformation<KllDoublesSketch> {
    public static final KllDoublesTypeInfo INSTANCE = new KllDoublesTypeInfo();
    private static final long serialVersionUID = 1L;

    private KllDoublesTypeInfo() {}

    @Override
    public boolean isBasicType() {
        return false;
    }

    @Override
    public boolean isTupleType() {
        return false;
    }

    @Override
    public int getArity() {
        return 1;
    }

    @Override
    public int getTotalFields() {
        return 1;
    }

    @Override
    public Class<KllDoublesSketch> getTypeClass() {
        return KllDoublesSketch.class;
    }

    @Override
    public boolean isKeyType() {
        return false;
    }

    @Override
    public TypeSerializer<KllDoublesSketch> createSerializer(ExecutionConfig config) {
        return KllDoublesSerializer.INSTANCE;
    }

    @Override
    public boolean canEqual(Object other) {
        return other instanceof KllDoublesTypeInfo;
    }

    @Override
    public boolean equals(Object other) {
        return canEqual(other);
    }

    @Override
    public int hashCode() {
        return KllDoublesSketch.class.hashCode();
    }

    @Override
    public String toString() {
        return "KllDoubles";
    }
}
