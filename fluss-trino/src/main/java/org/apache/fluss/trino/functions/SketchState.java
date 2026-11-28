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

import io.trino.spi.function.AccumulatorState;
import io.trino.spi.function.AccumulatorStateMetadata;

/** Mutable sketch state with portable intermediate serialization. */
@AccumulatorStateMetadata(
        stateFactoryClass = SketchStateFactory.class,
        stateSerializerClass = SketchStateSerializer.class)
public interface SketchState extends AccumulatorState {
    /** Returns the current HLL union or KLL doubles sketch, or null. */
    Object getSketch();

    /** Replaces the sketch, updating grouped memory accounting. */
    void setSketch(Object sketch);

    /** Accounts for a change in the retained sketch size after mutation. */
    void addMemoryUsage(long bytes);
}
