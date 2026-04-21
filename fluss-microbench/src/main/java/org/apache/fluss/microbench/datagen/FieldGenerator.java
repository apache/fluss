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

package org.apache.fluss.microbench.datagen;

import java.util.Map;

/** Generates values for a single field in a benchmark row. */
public interface FieldGenerator {

    /** Returns the type name of this generator (e.g. "random-int"). */
    String type();

    /** Configures this generator with the given parameters and seed. */
    void configure(Map<String, Object> params, long seed);

    /** Generates a value for the given row index. */
    Object generate(long index);
}
