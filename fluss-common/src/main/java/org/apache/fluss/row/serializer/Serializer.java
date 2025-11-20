/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.row.serializer;

import org.apache.fluss.memory.InputView;
import org.apache.fluss.memory.OutputView;

import java.io.IOException;
import java.io.Serializable;

/** Base interface for all serializers. */
public interface Serializer<T> extends Serializable {

    /** Creates a deep copy of the serializer. */
    Serializer<T> duplicate();

    /** Creates a copy of the given data structure. */
    T copy(T from);

    /** Serializes the given data structure to the output view. */
    void serialize(T record, OutputView target) throws IOException;

    /** De-serializes the given data structure from the input view. */
    T deserialize(InputView source) throws IOException;
}
