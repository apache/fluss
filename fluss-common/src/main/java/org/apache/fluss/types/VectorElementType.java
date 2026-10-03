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

package org.apache.fluss.types;

import org.apache.fluss.annotation.PublicEvolving;

/**
 * Enum representing the element precision of a {@link VectorType} column.
 *
 * <p>FLOAT32 is the baseline default. FLOAT16 and INT8 are reserved in the type descriptor for
 * future scalar-quantization support and must not be used in writer/reader code until explicitly
 * implemented.
 *
 * @since 0.7
 */
@PublicEvolving
public enum VectorElementType {

    /** 32-bit IEEE 754 floating point. Fully supported. */
    FLOAT32,

    /**
     * 16-bit half-precision floating point.
     *
     * <p>TODO: reserved for future scalar quantization — NOT YET IMPLEMENTED.
     */
    FLOAT16,

    /**
     * 8-bit signed integer (scalar quantized).
     *
     * <p>TODO: reserved for future scalar quantization — NOT YET IMPLEMENTED.
     */
    INT8
}
